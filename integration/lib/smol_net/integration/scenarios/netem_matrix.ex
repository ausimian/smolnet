defmodule SmolNet.Integration.Scenarios.NetemMatrix do
  @moduledoc """
  The netem matrix: bulk TCP transfers under each `tc netem` profile,
  between SmolNet and the kernel, beside the kernel and itself on a path
  impaired the same way.

  `integration/netem-topology.sh` gives this namespace a second path
  beside the TUN device: a veth pair to a kernel peer in the namespace
  `netem-peer`, with GSO limited to one segment per packet on it and on
  the device, so that netem sees the kernel's segments one at a time, as
  it sees SmolNet's. For each profile, `SmolNet.Integration.Soak.Netem`
  impairs the device and this end of the veth alike, both ways, and these
  flows run:

    * `send` - SmolNet sends to the kernel here, through the device;
    * `receive` - the kernel here sends to SmolNet, through the device;
    * `kernel` - the kernel here sends to the kernel in `netem-peer`,
      across the veth: the baseline.

  `--thin-acks N` reproduces the stretch ACKs of a real server, whose NIC
  coalesces what it receives (GRO, LRO), so that it acknowledges several
  segments at once (#126). `netem-topology.sh up --thin-acks N` has the
  kernel in `netem-peer` send only one in N of its pure ACKs, and forwards
  between the device and the veth. That kernel is then the kernel end of
  `send` and `receive` too, so that every flow crosses the veth, which
  alone the profile impairs.

  SmolNet is the client of every connection over the device, and this
  namespace's kernel of every one across the veth. A transfer carries
  `--bytes` in all, split evenly over each of `--streams` connections at
  once, of random data, and each receiver checks its length and SHA-256.
  It takes from the first connect to the last byte received, and its
  throughput is its bytes over that time.

  Each case, a profile, flow, stream count and family, runs `--repeats`
  times. `--order interleaved`, the default, runs the whole matrix once
  per repeat, so that a change in the host's load during the run touches
  every case alike, and a run cut short by `--duration` still has every
  case; `grouped` runs each case's repeats back to back. A case's figures
  are the medians over its repeats that completed.

  ## Verdict

  The run fails when a transfer over the device, or the baseline's,
  delivers anything but what was sent, or does not complete within
  `--transfer-timeout`: every profile must keep its data intact. It fails
  too when a SmolNet transfer stalls: its receiver waits for more data
  longer than `--stall-rtos` retransmission timeouts in a row take, each
  twice the last, from the profile's round trip
  (`SmolNet.Integration.Soak.Netem.round_trip_ms/1`) plus the 200 ms least
  timeout both stacks use (`stall_limit/2`). A receiver waits on, so that
  each transfer's longest wait is measured whole. The baseline's own
  stalls are noted, not failed, and so are SmolNet's under `loss-burst`,
  whose bursts outlast the timer for Linux too: netem's Gilbert-Elliott
  state moves on only as packets pass, so each lone retransmission meets
  the same burst.

  Throughput is measured, not judged. Each case's median is recorded
  beside the kernel's for the same profile, stream count and family, and
  a case below `--gap` times the kernel is noted as a gap, with the issues
  that explain it (`triage/2`), or as untriaged. `verdict.json` holds every
  transfer as the `transfers` result and a row per case as `matrix`.
  """

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak
  alias SmolNet.Integration.Soak.Netem

  @topology Path.expand("../../../../netem-topology.sh", __DIR__)
  @veth "netem-h"
  @peer_netns "/run/netns/netem-peer"
  @peer %{inet: {10, 80, 0, 2}, inet6: {0xFD00, 0x80, 0, 0, 0, 0, 0, 2}}
  @flows [:send, :receive, :kernel]
  @orders %{"interleaved" => :interleaved, "grouped" => :grouped}
  # Both stacks' least retransmission timeout.
  @min_rto_ms 200
  @chunk 65_536
  @connect_timeout 30_000
  @pause_ms 300
  # Under a profile with no delay, a SmolNet case at least this share of
  # the unimpaired ratio to the kernel is as far behind as the CPU puts it.
  @cpu_share 0.75
  @cpu_bound "no delay on the path, so both stacks run as fast as the CPU lets them, and " <>
               "SmolNet, through its NIF and the TUN helper, costs more a packet than the " <>
               "kernel across a veth: the gap is as large unimpaired (profile none)"

  # Profiles under which a stall past the limit is the path's: SmolNet's
  # are recorded and noted beside the kernel's, not failed.
  @stall_prone %{
    "loss-burst" =>
      "netem's Gilbert-Elliott state moves on only as packets pass, so a burst outlasts " <>
        "the retransmission timer: each lone retransmission meets it again and the timer " <>
        "backs off, and Linux stalls as long across the veth"
  }

  # The issues that explain a gap SmolNet shows against the kernel, by
  # profile and flow, from the runs in integration/netem.md.
  @triage %{
    {"loss-burst", :send} => [
      {138,
       "tail and retransmission losses wait for the retransmission timer, " <>
         "backed off in a burst to minutes, with no TLP or RACK"},
      {139, "SACK recovery overshoots the window and uses the peer's MSS"}
    ],
    {"loss-burst", :receive} => [
      {117, "the SmolNet receiver SACKs one block, and drops segments past its fourth hole"}
    ],
    {"high-bdp", :send} => [
      {103,
       "the 256 KiB default buffers, without autotuning, hold a 200 ms round trip " <>
         "to about 10 Mbit/s; recbuf and sndbuf of up to 1 MiB lift it, since the window " <>
         "scales (#49)"}
    ],
    {"high-bdp", :receive} => [
      {103,
       "the 256 KiB default receive buffer, without autotuning, holds a 200 ms round " <>
         "trip to about 10 Mbit/s; a recbuf of up to 1 MiB lifts it, since the window " <>
         "scales (#49)"}
    ],
    {"reorder", :send} => [
      {138,
       "SmolNet takes three duplicate ACKs as a loss, so reordering as congestion; " <>
         "RACK's reordering window would tell them apart"}
    ],
    {"reorder", :receive} => [
      {86,
       "Linux's sender backs off under reordering against SmolNet, which sends " <>
         "no timestamps and no DSACK to undo it"}
    ]
  }

  @type flow :: :send | :receive | :kernel
  @type test_case :: %{
          id: String.t(),
          profile: String.t(),
          flow: flow(),
          streams: pos_integer(),
          family: :inet | :inet6,
          repeat: pos_integer()
        }

  @doc "Returns the scenario's runner config."
  @spec config() :: keyword()
  def config do
    [
      name: "netem_matrix",
      default_duration: "2h",
      switches: [
        profiles: :string,
        flows: :string,
        streams: :string,
        bytes: :integer,
        repeats: :integer,
        order: :string,
        stall_rtos: :integer,
        transfer_timeout: :integer,
        gap: :float,
        buffer: :integer,
        thin_acks: :integer
      ],
      defaults: [
        profiles: Enum.join(["none" | Netem.profiles()], ","),
        flows: "send,receive,kernel",
        streams: "1,4",
        bytes: 8_388_608,
        repeats: 3,
        order: "interleaved",
        stall_rtos: 5,
        transfer_timeout: 600_000,
        gap: 0.5,
        buffer: nil,
        thin_acks: nil
      ],
      counters: [:transfers, :intact, :stalled],
      stack: [limits: %{sockets: 256}],
      usage: """

      netem_matrix options:
        --profiles LIST       the profiles to run, comma-separated, none for no
                              impairment (default none and every profile)
        --flows LIST          send, receive and kernel, comma-separated: SmolNet
                              sending, SmolNet receiving, and the kernel baseline
                              (default all three)
        --streams LIST        the connections a transfer is split over, comma-
                              separated (default 1,4)
        --bytes N             what a transfer carries in all (default 8388608)
        --repeats N           the runs of each case (default 3)
        --order O             interleaved (each repeat of the whole matrix in
                              turn) or grouped (each case's repeats together)
                              (default interleaved)
        --stall-rtos N        how many retransmission timeouts in a row, each
                              twice the last, a receiver may wait through for
                              data before a SmolNet transfer has stalled
                              (default 5)
        --transfer-timeout MS how long a transfer may take (default 600000)
        --gap R               the share of the kernel's throughput below which a
                              case is noted as a gap (default 0.5)
        --buffer N            SmolNet's recbuf and sndbuf (default: the stack's)
        --thin-acks N         stretch ACKs: the kernel in netem-peer sends one in N
                              of its pure ACKs, and is the kernel end of every
                              flow; the profile impairs the veth alone

      Run it in a network namespace of its own:

        integration/netem-topology.sh isolate mix run integration/netem_matrix.exs

      The matrix applies each profile itself, so it takes --profiles, not
      --netem. --duration bounds it: transfers that do not fit are skipped,
      with a note. --no-pcap keeps tcpdump off the CPU the transfers share.
      """
    ]
  end

  @doc "Runs the scenario's workload."
  @spec run(SmolNet.Integration.Soak.Context.t()) :: :ok
  def run(context) do
    with {:ok, settings} <- settings(context),
         :ok <- check_mode(context),
         :ok <- topology(context, ["up" | thin_arguments(settings)]) do
      try do
        run_matrix(context, settings)
      after
        Netem.clear(context.device)
        Netem.clear(@veth)
        topology(context, ["down"])
      end

      Soak.await_socket_count(context, 0)
    else
      {:abandon, reason} -> Soak.abandon(context, reason)
      {:error, message} -> Soak.fail(context, :usage, message)
    end
  end

  defp check_mode(%{mode: :smolnet, netem: nil}), do: :ok

  defp check_mode(%{mode: :smolnet}) do
    {:error, "the matrix applies each profile itself: pass --profiles, not --netem"}
  end

  defp check_mode(_context) do
    {:abandon,
     "the netem matrix compares SmolNet over the device with the kernel beside it, " <>
       "so it has no --baseline or --self-check mode"}
  end

  defp settings(context) do
    extra = context.extra

    with {:ok, profiles} <- profiles(extra.profiles),
         {:ok, flows} <- flows(extra.flows),
         {:ok, streams} <- streams(extra.streams),
         {:ok, order} <- order(extra.order),
         :ok <- positive(:repeats, extra.repeats),
         :ok <- positive(:stall_rtos, extra.stall_rtos),
         :ok <- positive(:transfer_timeout, extra.transfer_timeout),
         :ok <- enough_bytes(extra.bytes, streams),
         :ok <- gap(extra.gap),
         :ok <- buffer(extra.buffer),
         :ok <- thin_acks(extra.thin_acks) do
      {:ok,
       %{
         profiles: profiles,
         flows: flows,
         streams: streams,
         order: order,
         bytes: extra.bytes,
         repeats: extra.repeats,
         stall_rtos: extra.stall_rtos,
         transfer_timeout: extra.transfer_timeout,
         gap: extra.gap,
         buffer: extra.buffer,
         thin_acks: extra.thin_acks,
         impaired: if(extra.thin_acks, do: [@veth], else: [context.device, @veth])
       }}
    end
  end

  defp profiles(text) do
    names = list(text)

    case Enum.reject(names, &(&1 == "none" or Netem.profile?(&1))) do
      [] when names != [] ->
        {:ok, names}

      _unknown ->
        {:error,
         "--profiles must name none or #{Enum.join(Netem.profiles(), ", ")}, got #{inspect(text)}"}
    end
  end

  defp flows(text) do
    names = list(text)
    flows = Enum.filter(@flows, &(Atom.to_string(&1) in names))

    if names != [] and length(flows) == length(names) do
      {:ok, flows}
    else
      {:error, "--flows must name send, receive or kernel, got #{inspect(text)}"}
    end
  end

  defp streams(text) do
    parsed = text |> list() |> Enum.map(&Integer.parse/1)

    if parsed != [] and Enum.all?(parsed, &match?({count, ""} when count > 0, &1)) do
      {:ok, parsed |> Enum.map(&elem(&1, 0)) |> Enum.uniq()}
    else
      {:error, "--streams must be positive counts, comma-separated, got #{inspect(text)}"}
    end
  end

  defp order(text) do
    case Map.fetch(@orders, text) do
      {:ok, order} -> {:ok, order}
      :error -> {:error, "--order must be interleaved or grouped, got #{inspect(text)}"}
    end
  end

  defp list(text) do
    text
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp positive(_name, value) when is_integer(value) and value > 0, do: :ok
  defp positive(name, value), do: {:error, "--#{dashed(name)} must be positive, got #{value}"}

  defp enough_bytes(bytes, streams) when is_integer(bytes) do
    if bytes >= Enum.max(streams),
      do: :ok,
      else: {:error, "--bytes must give every stream a byte at least, got #{bytes}"}
  end

  defp gap(ratio) when is_float(ratio) and ratio > 0, do: :ok
  defp gap(ratio), do: {:error, "--gap must be a positive ratio, got #{ratio}"}

  defp buffer(nil), do: :ok
  defp buffer(bytes), do: positive(:buffer, bytes)

  defp thin_acks(nil), do: :ok
  defp thin_acks(n) when is_integer(n) and n >= 2, do: :ok
  defp thin_acks(n), do: {:error, "--thin-acks must be 2 or more, got #{n}"}

  defp dashed(name), do: name |> to_string() |> String.replace("_", "-")

  @doc """
  Returns the transfers of the matrix, in the order they run: each repeat
  of the whole matrix in turn for `:interleaved`, or each case's repeats
  together for `:grouped`. Profiles vary slowest, so that each is applied
  once per pass.
  """
  @spec plan(
          [String.t()],
          [flow()],
          [pos_integer()],
          [:inet | :inet6],
          pos_integer(),
          :interleaved | :grouped
        ) :: [test_case()]
  def plan(profiles, flows, streams, families, repeats, :interleaved) do
    for repeat <- 1..repeats//1,
        profile <- profiles,
        family <- families,
        flow <- flows,
        count <- streams,
        do: test_case(profile, flow, count, family, repeat)
  end

  def plan(profiles, flows, streams, families, repeats, :grouped) do
    for profile <- profiles,
        family <- families,
        flow <- flows,
        count <- streams,
        repeat <- 1..repeats//1,
        do: test_case(profile, flow, count, family, repeat)
  end

  defp test_case(profile, flow, count, family, repeat) do
    %{
      id: "#{profile}-#{flow}-x#{count}-#{family}",
      profile: profile,
      flow: flow,
      streams: count,
      family: family,
      repeat: repeat
    }
  end

  @doc """
  Returns how long, in milliseconds, a receiver may wait for more data
  under `profile` before its transfer has stalled: what `rtos`
  retransmission timeouts in a row take, each twice the last, from the
  profile's round trip plus the 200 ms least timeout.
  """
  @spec stall_limit(String.t(), pos_integer()) :: pos_integer()
  def stall_limit(profile, rtos) do
    (round_trip(profile) + @min_rto_ms) * (Integer.pow(2, rtos) - 1)
  end

  defp round_trip("none"), do: 0
  defp round_trip(profile), do: Netem.round_trip_ms(profile)

  @doc """
  Returns the issues that explain SmolNet's gap against the kernel under
  `profile` in `flow`, each as `%{issue: number, why: text}`, or `[]` if
  none is known. `matrix/2` also explains, with no issue, a gap under a
  profile with no delay that the CPU accounts for.
  """
  @spec triage(String.t(), flow()) :: [%{issue: pos_integer() | nil, why: String.t()}]
  def triage(profile, flow) do
    @triage
    |> Map.get({profile, flow}, [])
    |> Enum.map(fn {issue, why} -> %{issue: issue, why: why} end)
  end

  defp thin_arguments(%{thin_acks: nil}), do: []
  defp thin_arguments(%{thin_acks: n}), do: ["--thin-acks", Integer.to_string(n)]

  defp topology(context, arguments) do
    case System.cmd(@topology, arguments,
           stderr_to_stdout: true,
           env: [{"SMOLNET_TUN", context.device}]
         ) do
      {_output, 0} ->
        :ok

      {output, status} ->
        {:abandon,
         "netem-topology.sh #{Enum.join(arguments, " ")} exited #{status}: #{String.trim(output)}"}
    end
  end

  defp run_matrix(context, settings) do
    cases =
      plan(
        settings.profiles,
        settings.flows,
        settings.streams,
        context.families,
        settings.repeats,
        settings.order
      )

    Soak.log(
      context,
      "#{length(cases)} transfers of #{settings.bytes} bytes: profiles " <>
        "#{Enum.join(settings.profiles, ", ")}, #{settings.repeats} repeats, #{settings.order}"
    )

    {results, skipped} = run_cases(context, settings, cases, nil, [])

    if skipped > 0 do
      Soak.note(
        context,
        "the run's duration ended before #{skipped} of #{length(cases)} transfers; " <>
          "run longer for the whole matrix"
      )
    end

    summarize(context, settings, results)
  end

  defp run_cases(_context, _settings, [], _profile, results), do: {Enum.reverse(results), 0}

  defp run_cases(context, settings, [test_case | rest] = remaining, profile, results) do
    with true <- Soak.running?(context),
         :ok <- switch(context, settings, profile, test_case.profile) do
      results = [run_case(context, settings, test_case) | results]
      Soak.record(context, :transfers, Enum.reverse(results))
      run_cases(context, settings, rest, test_case.profile, results)
    else
      false ->
        {Enum.reverse(results), length(remaining)}

      {:error, message} ->
        Soak.abandon(context, message)
        {Enum.reverse(results), length(remaining)}
    end
  end

  # Both paths take the same profile together, each way.
  defp switch(_context, _settings, profile, profile), do: :ok

  defp switch(context, settings, _previous, profile) do
    devices = settings.impaired
    Enum.each(devices, &Netem.clear/1)

    if profile == "none" do
      :ok
    else
      with :ok <- impair_all(devices, profile) do
        Soak.log(context, "profile #{profile}")
      end
    end
  end

  defp impair_all(devices, profile) do
    Enum.reduce_while(devices, :ok, fn device, :ok ->
      case Netem.impair(profile, device) do
        :ok ->
          {:cont, :ok}

        {:error, message} ->
          {:halt, {:error, "could not apply #{profile} to #{device}: #{message}"}}
      end
    end)
  end

  defp run_case(context, settings, test_case) do
    stall_ms = stall_limit(test_case.profile, settings.stall_rtos)
    started_at = DateTime.utc_now()
    deadline = settings.transfer_timeout + 2 * @connect_timeout + 30_000

    observed =
      Soak.within(context, {:transfer, test_case.id, test_case.repeat}, deadline, fn ->
        transfer(context, settings, test_case, stall_ms)
      end)

    Soak.count(context, :transfers)
    if observed.intact, do: Soak.count(context, :intact)
    if observed.stalled, do: Soak.count(context, :stalled)

    result =
      Map.merge(test_case, observed)
      |> Map.merge(%{stall_limit_ms: stall_ms, started_at: DateTime.to_iso8601(started_at)})

    Soak.log(context, "#{test_case.id} ##{test_case.repeat}: #{describe(result)}")
    Process.sleep(@pause_ms)
    result
  end

  defp describe(%{intact: true} = result) do
    "#{result.mbit_s} Mbit/s, #{result.completion_ms} ms, connect #{result.connect_ms} ms, " <>
      "longest wait #{result.longest_wait_ms} ms#{if result.stalled, do: " (stalled)"}"
  end

  defp describe(result), do: "#{result.outcome}: #{result.detail}"

  # Every stream's receiver checks what it gets against the one payload
  # every sender sends, so that which connection is whose does not matter.
  defp transfer(context, settings, test_case, stall_ms) do
    %{flow: flow, family: family, streams: count} = test_case
    payload = :crypto.strong_rand_bytes(div(settings.bytes, count))
    {server_options, server, client_options} = endpoints(context, settings, flow, family)

    {:ok, listener} =
      :gen_tcp.listen(0, server_options ++ [:binary, active: false, ip: server, backlog: 64])

    {:ok, port} = :inet.port(listener)
    started = now()

    job = %{
      payload: payload,
      digest: :crypto.hash(:sha256, payload),
      started: started,
      timeout_at: started + settings.transfer_timeout,
      # A transfer the run's end overtakes gives up well within the
      # runner's grace for the workload.
      give_up_at: min(started + settings.transfer_timeout, context.ends_at + 30_000)
    }

    {client_role, server_role} = roles(flow)
    servers = for _ <- 1..count//1, do: Task.async(fn -> serve(listener, server_role, job) end)

    clients =
      for _ <- 1..count//1,
          do: Task.async(fn -> connect(server, port, client_options, client_role, job) end)

    {receivers, senders} =
      if client_role == :receiver, do: {clients, servers}, else: {servers, clients}

    received = await(receivers, job.give_up_at - now() + @connect_timeout + 5_000)
    :gen_tcp.close(listener)
    sent = await(senders, 10_000)
    outcome(received ++ sent, count, byte_size(payload) * count, stall_ms)
  end

  defp endpoints(_context, _settings, :kernel, family) do
    {[family, netns: @peer_netns], Map.fetch!(@peer, family), [family]}
  end

  defp endpoints(context, settings, _flow, family) do
    buffers =
      if settings.buffer, do: [recbuf: settings.buffer, sndbuf: settings.buffer], else: []

    smolnet = Network.tcp_options(context, :subject, family) ++ buffers

    if settings.thin_acks,
      do: {[family, netns: @peer_netns], Map.fetch!(@peer, family), smolnet},
      else: {[family], Network.host_address(family), smolnet}
  end

  defp roles(:receive), do: {:receiver, :sender}
  defp roles(_flow), do: {:sender, :receiver}

  defp serve(listener, role, job) do
    case :gen_tcp.accept(listener, @connect_timeout) do
      {:ok, socket} -> work(role, socket, job)
      {:error, reason} -> %{role: role, error: "accept failed: #{inspect(reason)}"}
    end
  end

  defp connect(address, port, options, role, job) do
    before = now()

    case :gen_tcp.connect(address, port, options ++ [:binary, active: false], @connect_timeout) do
      {:ok, socket} ->
        connect_ms = now() - before
        role |> work(socket, job) |> Map.put(:connect_ms, connect_ms)

      {:error, reason} ->
        %{role: role, error: "connect failed: #{inspect(reason)}", connect_ms: now() - before}
    end
  end

  defp work(:sender, socket, job) do
    sent = send_all(socket, job.payload, 0)
    # The receiver closes once it has everything, or has given up, and the
    # sender only then: nothing is left unsent behind its close.
    _closed = :gen_tcp.recv(socket, 0, max(job.give_up_at - now(), 0) + 1_000)
    :gen_tcp.close(socket)

    case sent do
      :ok -> %{role: :sender}
      {:error, reason} -> %{role: :sender, error: "send failed: #{inspect(reason)}"}
    end
  end

  defp work(:receiver, socket, job) do
    state = %{got: 0, hash: :crypto.hash_init(:sha256), last: now(), longest: 0}
    received = receive_all(socket, byte_size(job.payload), job, state)
    :gen_tcp.close(socket)
    Map.merge(received, %{role: :receiver, done_ms: now() - job.started})
  end

  defp send_all(_socket, payload, offset) when offset >= byte_size(payload), do: :ok

  defp send_all(socket, payload, offset) do
    size = min(@chunk, byte_size(payload) - offset)

    case :gen_tcp.send(socket, binary_part(payload, offset, size)) do
      :ok -> send_all(socket, payload, offset + size)
      {:error, _reason} = error -> error
    end
  end

  # `longest` is the longest wait for data so far, from the start for the
  # first. A receiver waits as long as the transfer may take, so that a
  # stall's whole length is measured, not only that it passed the limit.
  defp receive_all(_socket, expected, job, %{got: got} = state) when got >= expected do
    intact = got == expected and :crypto.hash_final(state.hash) == job.digest
    %{bytes: got, intact: intact, corrupt: not intact, longest_wait_ms: state.longest}
  end

  defp receive_all(socket, expected, job, state) do
    wait = max(job.give_up_at - now(), 0)

    case :gen_tcp.recv(socket, 0, wait) do
      {:ok, data} ->
        at = now()

        receive_all(socket, expected, job, %{
          state
          | got: state.got + byte_size(data),
            hash: :crypto.hash_update(state.hash, data),
            last: at,
            longest: max(state.longest, at - state.last)
        })

      {:error, :timeout} ->
        waited = now() - state.last

        ended = if now() >= job.timeout_at, do: :timeout, else: :cut

        %{
          bytes: state.got,
          intact: false,
          ended: ended,
          longest_wait_ms: max(state.longest, waited)
        }

      {:error, reason} ->
        %{
          bytes: state.got,
          intact: false,
          ended: :failed,
          error: "receive failed after #{state.got} bytes: #{inspect(reason)}",
          longest_wait_ms: state.longest
        }
    end
  end

  defp await(tasks, timeout) do
    tasks
    |> Task.yield_many(max(timeout, 0))
    |> Enum.map(fn
      {_task, {:ok, result}} ->
        result

      {_task, {:exit, reason}} ->
        %{role: :crashed, error: "a stream crashed: #{inspect(reason, limit: 10)}"}

      {task, nil} ->
        Task.shutdown(task, :brutal_kill)
        %{role: :stuck, error: "a stream did not finish"}
    end)
  end

  defp outcome(results, count, total, stall_ms) do
    receivers = Enum.filter(results, &(&1.role == :receiver))
    errors = results |> Enum.map(&Map.get(&1, :error)) |> Enum.reject(&is_nil/1)
    bytes = receivers |> Enum.map(&Map.get(&1, :bytes, 0)) |> Enum.sum()
    completion_ms = receivers |> Enum.map(&Map.get(&1, :done_ms, 0)) |> Enum.max(fn -> 0 end)
    longest = receivers |> Enum.map(&Map.get(&1, :longest_wait_ms, 0)) |> Enum.max(fn -> 0 end)
    intact = length(receivers) == count and Enum.all?(receivers, &Map.get(&1, :intact, false))
    {outcome, detail} = classify(receivers, intact, errors, {bytes, total, longest})

    %{
      outcome: outcome,
      detail: detail,
      intact: intact,
      stalled: longest > stall_ms,
      bytes: total,
      completion_ms: completion_ms,
      mbit_s:
        if(intact and completion_ms > 0, do: Float.round(total * 8 / (completion_ms * 1_000), 2)),
      connect_ms: results |> Enum.map(&Map.get(&1, :connect_ms, 0)) |> Enum.max(fn -> 0 end),
      longest_wait_ms: longest
    }
  end

  defp classify(_receivers, true, _errors, {bytes, _total, _longest}),
    do: {:complete, "#{bytes} bytes intact"}

  defp classify(receivers, false, errors, {bytes, total, longest}) do
    ended = Enum.map(receivers, &Map.get(&1, :ended))

    cond do
      Enum.any?(receivers, &Map.get(&1, :corrupt, false)) ->
        {:corrupt, "#{bytes} bytes arrived, not the ones sent"}

      :cut in ended ->
        {:cut, "the run ended after #{bytes} of #{total} bytes"}

      :timeout in ended ->
        {:timeout,
         "#{bytes} of #{total} bytes arrived within the transfer timeout, " <>
           "the longest wait for data #{longest} ms"}

      true ->
        {:failed, Enum.join(Enum.uniq(errors), "; ")}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)

  @doc """
  Summarizes `transfers` into a row per case, in the order they first ran:
  medians over the transfers that completed intact, SmolNet's median as a
  ratio of the kernel's for the same profile, family and stream count, and
  for a ratio below `gap` the issues that explain it. Transfers the run's
  end cut short are left out.
  """
  @spec matrix([map()], float()) :: [map()]
  def matrix(transfers, gap) do
    transfers = Enum.reject(transfers, &(&1.outcome == :cut))
    groups = Enum.group_by(transfers, &key/1)

    kernel =
      for {{profile, family, streams, :kernel}, group} <- groups,
          into: %{},
          do: {{profile, family, streams}, group |> completed(:mbit_s) |> median()}

    rows =
      transfers
      |> Enum.map(&key/1)
      |> Enum.uniq()
      |> Enum.map(&row(&1, Map.fetch!(groups, &1), kernel, gap))

    control =
      for %{profile: "none"} = row <- rows,
          into: %{},
          do: {{row.family, row.streams, row.flow}, row.ratio_to_kernel}

    Enum.map(rows, &cpu_bound(&1, control))
  end

  # A gap under a profile with no delay that is no wider than the
  # unimpaired one is the CPU's, not the impairment's.
  defp cpu_bound(%{gap: true, triage: []} = row, control) do
    unimpaired = Map.get(control, {row.family, row.streams, row.flow})

    cpu? =
      round_trip(row.profile) == 0 and
        (row.profile == "none" or
           (is_float(unimpaired) and row.ratio_to_kernel >= @cpu_share * unimpaired))

    if cpu?, do: %{row | triage: [%{issue: nil, why: @cpu_bound}]}, else: row
  end

  defp cpu_bound(row, _control), do: row

  defp key(transfer), do: {transfer.profile, transfer.family, transfer.streams, transfer.flow}

  defp row({profile, family, streams, flow}, group, kernel, gap) do
    rates = completed(group, :mbit_s)
    median = rates |> median() |> rounded(2)
    baseline = if flow != :kernel, do: Map.get(kernel, {profile, family, streams})

    ratio =
      if is_number(median) and is_number(baseline) and baseline > 0,
        do: Float.round(median / baseline, 2)

    gap? = is_float(ratio) and ratio < gap

    %{
      profile: profile,
      family: family,
      streams: streams,
      flow: flow,
      runs: length(group),
      intact: length(rates),
      stalls: Enum.count(group, & &1.stalled),
      median_mbit_s: median,
      min_mbit_s: Enum.min(rates, fn -> nil end),
      max_mbit_s: Enum.max(rates, fn -> nil end),
      median_completion_ms: group |> completed(:completion_ms) |> median() |> rounded(0),
      longest_wait_ms: group |> Enum.map(& &1.longest_wait_ms) |> Enum.max(fn -> nil end),
      kernel_mbit_s: baseline,
      ratio_to_kernel: ratio,
      gap: gap?,
      triage: if(gap?, do: triage(profile, flow), else: [])
    }
  end

  defp completed(group, field),
    do: for(%{intact: true} = transfer <- group, do: Map.fetch!(transfer, field))

  @doc "Returns the median of `values`, or `nil` for none."
  @spec median([number()]) :: number() | nil
  def median([]), do: nil

  def median(values) do
    sorted = Enum.sort(values)
    middle = div(length(sorted), 2)

    if rem(length(sorted), 2) == 1,
      do: Enum.at(sorted, middle),
      else: (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
  end

  defp summarize(context, settings, transfers) do
    rows = matrix(transfers, settings.gap)
    Soak.record(context, :matrix, rows)
    Enum.each(rows, &Soak.log(context, describe_row(&1)))

    for %{gap: true} = row <- rows do
      Soak.note(context, "gap: #{describe_row(row)}")
    end

    lost =
      transfers
      |> Enum.filter(&(&1.outcome in [:corrupt, :failed, :timeout]))
      |> Enum.group_by(&{&1.id, &1.outcome})
      |> Enum.map(fn {{id, outcome}, group} -> judge(id, outcome, group) end)

    {noted, stalls} =
      transfers
      |> Enum.filter(& &1.stalled)
      |> Enum.group_by(& &1.id)
      |> Enum.map(fn {id, group} -> judge(id, :stalled, group) end)
      |> Enum.split_with(&match?({:noted, _summary, _group}, &1))

    Enum.each(noted, fn {:noted, summary, _group} -> Soak.note(context, summary) end)

    # One failure for them all: the first stops the workload.
    case lost ++ stalls do
      [] ->
        :ok

      problems ->
        kind = if lost == [], do: :stall, else: :integrity
        summary = Enum.map_join(problems, "; ", &elem(&1, 1))
        Soak.fail(context, kind, summary, Enum.map(problems, fn {_kind, s, g} -> {s, g} end))
    end
  end

  defp judge(id, :stalled, [first | _rest] = group) do
    waits = Enum.map_join(group, ", ", &"#{&1.longest_wait_ms} ms")

    summary =
      "#{id} stalled in #{runs(group)}, past #{first.stall_limit_ms} ms: waits of #{waits}"

    # The kernel's own stalls are the path's, or the kernel's, not SmolNet's.
    cond do
      first.flow == :kernel ->
        {:noted, "the kernel baseline: " <> summary, group}

      prone = @stall_prone[first.profile] ->
        {:noted, "#{summary}, as expected: #{prone}" <> known(first), group}

      true ->
        {:stall, summary <> known(first), group}
    end
  end

  defp judge(id, outcome, [first | _rest] = group) do
    who = if first.flow == :kernel, do: " (the kernel baseline)", else: ""

    {:integrity, "#{id}#{who} #{outcome} in #{runs(group)}: #{first.detail}" <> known(first),
     group}
  end

  defp rounded(nil, _digits), do: nil
  defp rounded(value, 0), do: round(value)
  defp rounded(value, digits), do: Float.round(value / 1, digits)

  defp runs([_one]), do: "1 run"
  defp runs(group), do: "#{length(group)} runs"

  defp known(%{profile: profile, flow: flow}) do
    case triage(profile, flow) do
      [] -> ""
      issues -> "; see " <> Enum.map_join(issues, ", ", &"##{&1.issue}")
    end
  end

  defp explain(%{issue: nil, why: why}), do: why
  defp explain(%{issue: issue, why: why}), do: "##{issue}: #{why}"

  defp describe_row(row) do
    kernel =
      if row.flow == :kernel,
        do: "",
        else: ", #{row.ratio_to_kernel || "-"}x the kernel's #{row.kernel_mbit_s || "-"}"

    triage =
      cond do
        row.triage != [] ->
          " (" <> Enum.map_join(row.triage, "; ", &explain/1) <> ")"

        row.gap ->
          " (untriaged)"

        true ->
          ""
      end

    "#{row.profile} #{row.flow} x#{row.streams} #{row.family}: " <>
      "#{row.median_mbit_s || "-"} Mbit/s in #{row.median_completion_ms || "-"} ms, " <>
      "#{row.intact}/#{row.runs} intact, longest wait #{row.longest_wait_ms} ms#{kernel}#{triage}"
  end
end
