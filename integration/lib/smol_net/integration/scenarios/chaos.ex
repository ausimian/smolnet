defmodule SmolNet.Integration.Scenarios.Chaos do
  @moduledoc """
  The chaos scenario: faults injected into a stack while it carries real
  traffic, and what each must leave behind.

  A run is a series of episodes. Each starts a fresh stack on the device,
  under one `:link_down` policy, and loads it with TLS bulk transfers, a
  connect/close churn loop, a stream read in active mode, and one of each
  call that blocks for good (see `SmolNet.Integration.Chaos.Workload`).
  Once traffic flows, one fault is injected:

    * `helper_kill` - `kill -KILL` the TUN helper, so that the link exits.
    * `link_kill` - kill the link process itself.
    * `stop_stack` - `SmolNet.stop_stack/1`, with every call still blocked.
    * `device_flap` - `ip link set <device> down`, then up again.
    * `route_flap` - a blackhole, unreachable or prohibit route on the host
      to SmolNet's addresses, then none.
    * `credit_delay`, `credit_trickle`, `credit_stop` - the link grants
      egress credit late, a packet at a time, or not at all
      (`SmolNet.Integration.TunLink.starve/2`), then as normal.
    * `owner_kill` - kill the owners of some blocked sockets, or the
      stream's.
    * `owner_swap` - hand the stream's socket on with `controlling_process`
      mid-transfer, each old owner exiting abruptly.

  The kills run under each of `--policies` (`stop`, `mark_down`, `notify`);
  every other fault under one drawn at random. Each cycle of episodes runs
  every combination once, in a random order. The stack is then stopped, if
  the fault did not stop it, and a fresh stack must work.

  ## Oracles

  An episode fails unless:

    * every call blocked when the stack stops returns an error within
      `--stop-deadline` of it: nothing hangs. The error is `:enetdown`
      when the link died under `:stop`, and the stream's owner gets
      `tcp_error` before `tcp_closed`; after `SmolNet.stop_stack/1` it is
      `:closed` or `:enetdown`;
    * `SmolNet.monitor/1` delivers `:DOWN` whenever the stack stops, and
      not before; `{:notify, pid}` delivers its message; `:mark_down` and
      `:notify` keep the stack running, marked down;
    * a fault that leaves the stack running costs no errors, and each loop
      completes a round within `--transfer-timeout` once it is lifted;
      every transfer's length and SHA-256 are right. A flap may cost a
      round it catches, whose connection the host's end gives up on, as
      Linux does with a handshake it could not finish; that is recorded as
      a casualty. A round begun after the flap may not fail;
    * once the traffic stops, all the egress credit is back;
    * a socket whose owner died is closed, with its adapter gone;
    * once the stack stops, its bundle, adapters, link and helper are gone,
      and the process count returns to where it started;
    * a fresh stack started straight after works.

  ## Reproducing a run

  Which faults run, in what order, when, and with what parameters, all
  come from `--seed`, which is logged and recorded as the `seed` result.
  The same seed and options replay the same schedule, although what the
  stack does under it may vary with timing. Every event, timed from the
  start of the run, is in `run.log` and the `events` result, and each
  episode's plan and outcome in `episodes`. A failed episode writes
  `episodes/NN-<fault>-<policy>.txt`, with its plan, timeline, failures
  and snapshots, and keeps its capture of the device in `captures/NN/`.

  ## For #43

  Under `:mark_down` and `:notify`, a link that dies leaves the stack up
  but unable to send. Each such episode records, in the `link_restart`
  result, what a link that could be replaced would have saved: the sockets
  and blocked calls still alive `--observe` ms after the link died, and the
  egress dropped meanwhile; and what replacing the stack cost instead:
  every one of those connections, and the time to a fresh stack that works.
  """

  alias SmolNet.Integration.Chaos.Episode
  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Ownership
  alias SmolNet.Integration.Soak

  @faults ~w(helper_kill link_kill stop_stack device_flap route_flap credit_delay
             credit_trickle credit_stop owner_kill owner_swap)a
  @kills [:helper_kill, :link_kill]
  @device_faults [:device_flap, :route_flap]
  @credit_faults [:credit_delay, :credit_trickle, :credit_stop]
  @policies [:stop, :mark_down, :notify]
  @owner_targets [:recv, :send, :accept, :udp, :stream]
  @max_events 20_000

  @doc "Returns the scenario's runner config."
  @spec config() :: keyword()
  def config do
    [
      name: "chaos",
      default_duration: "5m",
      default_concurrency: 2,
      network: :scenario,
      switches: [
        seed: :integer,
        faults: :string,
        policies: :string,
        episodes: :integer,
        churn: :integer,
        bulk_bytes: :integer,
        max_hold: :integer,
        observe: :integer,
        stop_deadline: :integer,
        transfer_timeout: :integer,
        keep_going: :boolean,
        snaplen: :integer
      ],
      defaults: [
        seed: nil,
        faults: "all",
        policies: "stop,mark_down,notify",
        episodes: 0,
        churn: 2,
        bulk_bytes: 1_048_576,
        max_hold: 4_000,
        observe: 3_000,
        stop_deadline: 5_000,
        transfer_timeout: 60_000,
        keep_going: false,
        snaplen: 256
      ],
      counters: [:episodes, :injections, :transfers, :connections],
      usage: """

      chaos options:
        --seed N              the seed of the schedule of faults (default: random, logged)
        --faults LIST         the faults to inject, comma-separated, or all (default all):
                              #{Enum.join(@faults, ", ")}
        --policies LIST       the link_down policies the kills run under (default
                              stop,mark_down,notify)
        --episodes N          stop after N episodes; 0 runs until --duration (default 0)
        --churn N             connect/close loops per episode (default 2)
        --bulk-bytes N        each TLS transfer's size (default 1048576)
        --max-hold MS         the longest a flap or credit fault lasts (default 4000)
        --observe MS          how long a marked-down stack is watched (default 3000)
        --stop-deadline MS    how soon blocked calls must return once the stack stops,
                              and the DOWN arrive (default 5000)
        --transfer-timeout MS how soon every loop must recover from a fault (default 60000)
        --keep-going          run every episode, and fail at the end, not at the first
        --snaplen N           the bytes of each packet the captures keep (default 256)

      --concurrency sets the TLS bulk transfers each episode runs (default 2).
      Device faults need root; without it, or with --self-check, they are skipped.
      """
    ]
  end

  @doc "Runs the scenario's workload."
  @spec run(SmolNet.Integration.Soak.Context.t()) :: :ok
  def run(context) do
    {:ok, _started} = Application.ensure_all_started(:ssl)

    case settings(context) do
      {:ok, settings} ->
        Enum.each(~w(episodes captures), &File.rm_rf!(Path.join(context.out_dir, &1)))
        Soak.log(context, "seed #{settings.seed}: rerun with --seed #{settings.seed}")
        Soak.record(context, :seed, settings.seed)
        Soak.record(context, :faults, settings.faults)

        state = %{
          rand: :rand.seed_s(:exsss, settings.seed),
          queue: [],
          n: 0,
          events: [],
          episodes: [],
          findings: [],
          failed: []
        }

        state = episodes(context, settings, state)
        restore_ownership(context)
        finish(context, settings, state)

      {:abandon, reason} ->
        Soak.abandon(context, reason)

      {:error, message} ->
        Soak.fail(context, :usage, message)
    end
  end

  defp episodes(context, settings, state) do
    if more?(context, settings, state) do
      {plan, state} = next_plan(settings, state)
      {record, events} = Episode.run(context, settings, plan)
      state = absorb(context, settings, state, record, events)
      episodes(context, settings, state)
    else
      state
    end
  end

  # --episodes N runs N whatever the duration; otherwise they run for it.
  defp more?(context, %{episodes: 0}, _state), do: Soak.running?(context)

  defp more?(context, settings, state),
    do: state.n < settings.episodes and not Soak.failed?(context)

  defp restore_ownership(context) do
    Enum.each(~w(episodes captures), &Ownership.restore(Path.join(context.out_dir, &1)))
  end

  # Settings

  defp settings(%{mode: :kernel}) do
    {:abandon, "the chaos scenario injects faults into SmolNet, so it has no --baseline mode"}
  end

  defp settings(context) do
    extra = context.extra
    credit = context.egress_credit || {64, 131_072}

    with {:ok, faults} <- names(extra.faults, @faults, "--faults"),
         {:ok, policies} <- names(extra.policies, @policies, "--policies"),
         :ok <- at_least(0, episodes: extra.episodes, churn: extra.churn),
         :ok <- at_least(1, bulk_bytes: extra.bulk_bytes, observe: extra.observe),
         :ok <- at_least(1, stop_deadline: extra.stop_deadline, snaplen: extra.snaplen),
         :ok <- at_least(1, transfer_timeout: extra.transfer_timeout),
         :ok <- at_least(500, max_hold: extra.max_hold),
         {:ok, faults} <- available(context, faults, extra.faults == "all", credit) do
      {server_tls, client_tls} = tls_options()
      payload = :crypto.strong_rand_bytes(1_048_576)

      {:ok,
       %{
         seed: extra.seed || :rand.uniform(2_147_483_646),
         faults: faults,
         policies: policies,
         episodes: extra.episodes,
         bulk: context.concurrency,
         churn: extra.churn,
         bulk_bytes: extra.bulk_bytes,
         max_hold: extra.max_hold,
         observe: extra.observe,
         stop_deadline: extra.stop_deadline,
         transfer_timeout: extra.transfer_timeout,
         keep_going: extra.keep_going,
         snaplen: extra.snaplen,
         server_tls: server_tls,
         client_tls: client_tls,
         buffer: 262_144,
         stream_payload: payload,
         stream_sha: Base.encode16(:crypto.hash(:sha256, payload), case: :lower),
         credit: credit,
         stack_options:
           Network.stack_options() ++ [limits: %{sockets: 512}, egress_credit: credit]
       }}
    end
  end

  defp names("all", known, _switch), do: {:ok, known}

  defp names(text, known, switch) do
    names = text |> String.split(",", trim: true) |> Enum.map(&String.trim/1)
    by_name = Map.new(known, &{Atom.to_string(&1), &1})

    case Enum.reject(names, &Map.has_key?(by_name, &1)) do
      [] when names != [] ->
        {:ok, known |> Enum.filter(&(Atom.to_string(&1) in names))}

      _unknown ->
        {:error, "#{switch} must name some of #{Enum.join(known, ", ")}, got #{inspect(text)}"}
    end
  end

  defp at_least(floor, values) do
    case Enum.find(values, fn {_name, value} -> not is_integer(value) or value < floor end) do
      nil -> :ok
      {name, value} -> {:error, "--#{dasherize(name)} must be at least #{floor}, got #{value}"}
    end
  end

  defp dasherize(name), do: name |> to_string() |> String.replace("_", "-")

  # Device faults need a device, and root to change it; credit faults a
  # stack with credit. A fault named in --faults that cannot run abandons
  # the run; one that "all" includes is skipped, with a note.
  defp available(context, faults, all?, credit) do
    credit_reason = if credit == :infinity, do: "--egress-credit infinity has no credit to starve"

    missing =
      for {group, reason} <- [
            {@device_faults, device_reason(context)},
            {@credit_faults, credit_reason}
          ],
          reason != nil,
          fault <- group,
          fault in faults,
          do: {fault, reason}

    remaining = faults -- Enum.map(missing, &elem(&1, 0))

    cond do
      missing == [] ->
        {:ok, faults}

      not all? or remaining == [] ->
        {fault, reason} = hd(missing)
        {:abandon, "cannot inject #{fault}: #{reason}"}

      true ->
        missing
        |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
        |> Enum.each(fn {reason, skipped} ->
          Soak.note(context, "skipping #{Enum.join(skipped, ", ")}: #{reason}")
        end)

        {:ok, remaining}
    end
  end

  defp device_reason(%{mode: :self_check}), do: "the helper's loopback has no device"

  defp device_reason(context) do
    case Episode.ip(["link", "set", "dev", context.device, "up"]) do
      :ok -> nil
      {:error, output} -> "cannot change #{context.device} (#{output}); device faults need root"
    end
  end

  defp tls_options do
    key = [key: {:namedCurve, :secp256r1}, digest: :sha256]
    chain = %{root: key, intermediates: [], peer: key}

    %{server_config: server, client_config: client} =
      :public_key.pkix_test_data(%{server_chain: chain, client_chain: chain})

    server_tls = [cert: server[:cert], key: server[:key], active: false, mode: :binary]

    client_tls = [
      verify: :verify_peer,
      cacerts: client[:cacerts],
      server_name_indication: :disable,
      active: false,
      mode: :binary
    ]

    {server_tls, client_tls}
  end

  # Planning. Every random choice is drawn here, in order, from the seed.

  defp next_plan(settings, %{queue: []} = state) do
    {queue, rand} = shuffle(cells(settings.faults, settings.policies), state.rand)
    next_plan(settings, %{state | queue: queue, rand: rand})
  end

  defp next_plan(settings, %{queue: [{fault, policy} | queue]} = state) do
    {plan, rand} = plan(settings, state.n + 1, fault, policy, state.rand)
    {plan, %{state | queue: queue, rand: rand, n: state.n + 1}}
  end

  @doc """
  Returns one cycle's combinations of fault and policy: each kill under
  each policy, and every other fault under `:any`, a policy drawn per
  episode.
  """
  @spec cells([atom()], [atom()]) :: [{atom(), atom()}]
  def cells(faults, policies) do
    for fault <- faults,
        policy <- if(fault in @kills, do: policies, else: [:any]),
        do: {fault, policy}
  end

  @doc """
  Plans episode `n`: `fault` under `policy`, with its timing and
  parameters drawn from `rand`, an `:rand` state. Returns the plan and the
  next state.
  """
  @spec plan(map(), pos_integer(), atom(), atom(), :rand.state()) :: {map(), :rand.state()}
  def plan(settings, n, fault, policy, rand) do
    {policy, rand} = if policy == :any, do: pick(settings.policies, rand), else: {policy, rand}
    {delay, rand} = between(300, 2_000, rand)
    {hold, rand} = between(500, settings.max_hold, rand)
    {teardown, rand} = pick([:stop_blocked, :release], rand)
    {params, rand} = params(fault, rand)

    plan = %{
      n: n,
      fault: fault,
      policy: policy,
      delay_ms: delay,
      hold_ms: hold,
      teardown: teardown,
      params: params
    }

    {plan, rand}
  end

  defp params(:credit_delay, rand), do: starve(:delay, between(20, 300, rand))
  defp params(:credit_trickle, rand), do: starve(:trickle, between(1, 20, rand))
  defp params(:credit_stop, rand), do: {%{starve: :stop}, rand}

  defp params(:route_flap, rand) do
    {route, rand} = pick([:blackhole, :unreachable, :prohibit], rand)
    {%{route: route}, rand}
  end

  defp params(:owner_kill, rand) do
    {targets, rand} = subset(@owner_targets, rand)
    {%{targets: targets}, rand}
  end

  defp params(:owner_swap, rand) do
    {swaps, rand} = between(1, 4, rand)
    {%{swaps: swaps}, rand}
  end

  defp params(_fault, rand), do: {%{}, rand}

  defp starve(mode, {ms, rand}), do: {%{starve: mode, ms: ms}, rand}

  defp between(low, high, rand) do
    {value, rand} = :rand.uniform_s(high - low + 1, rand)
    {low + value - 1, rand}
  end

  defp pick(list, rand) do
    {index, rand} = :rand.uniform_s(length(list), rand)
    {Enum.at(list, index - 1), rand}
  end

  defp shuffle(list, rand) do
    {keyed, rand} =
      Enum.map_reduce(list, rand, fn item, rand ->
        {key, rand} = :rand.uniform_s(rand)
        {{key, item}, rand}
      end)

    {keyed |> Enum.sort() |> Enum.map(&elem(&1, 1)), rand}
  end

  # A non-empty subset, each member in with even odds.
  defp subset(list, rand) do
    {chosen, rand} =
      Enum.flat_map_reduce(list, rand, fn item, rand ->
        {coin, rand} = :rand.uniform_s(2, rand)
        {if(coin == 1, do: [item], else: []), rand}
      end)

    case chosen do
      [] ->
        {item, rand} = pick(list, rand)
        {[item], rand}

      chosen ->
        {chosen, rand}
    end
  end

  # Recording

  defp absorb(context, settings, state, record, events) do
    Soak.count(context, :episodes)
    if record.injected, do: Soak.count(context, :injections)
    Soak.count(context, :transfers, record.transfers)
    Soak.count(context, :connections, record.connections)

    state = %{
      state
      | events: Enum.take(state.events ++ events, @max_events),
        episodes: state.episodes ++ [record],
        findings: state.findings ++ List.wrap(record.finding),
        failed: if(record.outcome == :fail, do: state.failed ++ [record], else: state.failed)
    }

    Soak.record(context, :events, state.events)
    Soak.record(context, :episodes, state.episodes)
    Soak.record(context, :matrix, matrix(state.episodes))
    Soak.record(context, :link_restart, state.findings)

    # Failing stops the workload, so everything is recorded first.
    if record.outcome == :fail and not settings.keep_going do
      restore_ownership(context)
      fail_run(context, [record])
    end

    state
  end

  defp finish(context, settings, state) do
    Enum.each(matrix(state.episodes), &Soak.note(context, describe_cell(&1)))
    Enum.each(restart_notes(state.findings), &Soak.note(context, &1))

    if settings.keep_going and state.failed != [] do
      fail_run(context, state.failed)
    end

    :ok
  end

  defp fail_run(context, [first | _more] = failed) do
    [%{kind: kind} | _others] = first.failures
    about = "episode #{first.n} (#{first.fault} under #{first.policy})"

    summary =
      case failed do
        [_one] ->
          "#{about}: #{Enum.map_join(first.failures, "; ", & &1.summary)}"

        _many ->
          "#{length(failed)} episodes failed, the first #{about}: #{hd(first.failures).summary}"
      end

    details =
      Enum.map(failed, fn record ->
        {"episode #{record.n}, #{record.fault} under #{record.policy}: #{record.file}",
         record.failures}
      end)

    Soak.fail(context, kind, summary, details)
  end

  @doc """
  Summarizes episode records by fault and policy: how many ran and failed,
  the kinds of failure, what the blocked calls returned when the stack
  stopped, and the slowest of those returns and of the `:DOWN`s.
  """
  @spec matrix([map()]) :: [map()]
  def matrix(records) do
    records
    |> Enum.group_by(&{&1.fault, &1.policy})
    |> Enum.map(fn {{fault, policy}, records} ->
      %{
        fault: fault,
        policy: policy,
        episodes: length(records),
        failed: Enum.count(records, &(&1.outcome == :fail)),
        casualties: Enum.sum_by(records, & &1.casualties),
        failures: records |> Enum.flat_map(& &1.failures) |> Enum.frequencies_by(& &1.kind),
        returns:
          records
          |> Enum.flat_map(&Map.to_list(&1.returns))
          |> Enum.frequencies_by(fn {kind, result} -> "#{kind} #{result}" end),
        slowest_return_ms: slowest(records, :slowest_return_ms),
        slowest_down_ms: slowest(records, :down_ms)
      }
    end)
    |> Enum.sort_by(&{&1.fault, &1.policy})
  end

  defp slowest(records, key) do
    records |> Enum.map(&Map.get(&1, key)) |> Enum.reject(&is_nil/1) |> Enum.max(fn -> nil end)
  end

  defp describe_cell(cell) do
    returns = Enum.map_join(cell.returns, ", ", fn {result, count} -> "#{result} x#{count}" end)
    returns = if returns == "", do: "", else: "; blocked calls returned #{returns}"

    slowest =
      if cell.slowest_return_ms, do: ", the slowest #{cell.slowest_return_ms} ms", else: ""

    casualties =
      if cell.casualties > 0, do: "; #{cell.casualties} rounds lost to the flaps", else: ""

    "#{cell.fault} under #{cell.policy}: #{cell.episodes} episodes, #{cell.failed} failed" <>
      casualties <> returns <> slowest
  end

  defp restart_notes(findings) do
    findings
    |> Enum.group_by(& &1.policy)
    |> Enum.map(fn {policy, findings} ->
      mean = fn key ->
        Float.round(Enum.sum_by(findings, &Map.fetch!(&1, key)) / length(findings), 1)
      end

      "for #43, under #{policy}, #{length(findings)} links died: after --observe, the stack " <>
        "still held #{mean.(:sockets_alive)} sockets, #{mean.(:calls_alive)} blocked calls and " <>
        "#{mean.(:loops_alive)} running loops on average, and dropped #{mean.(:dropped_egress)} packets meanwhile; replacing the " <>
        "stack ended every one, and took #{mean.(:restart_ms)} ms to a fresh stack that works"
    end)
  end
end
