defmodule SmolNet.Integration.Scenarios.Idle do
  @moduledoc """
  The idle scenario: holds hundreds of TCP and TLS connections open for the
  whole run, mostly idle, to exercise SmolNet's timers over long periods,
  and measures whether, and how soon, it notices a peer that has vanished.

  SmolNet is the client of every connection. Its peer is an echo server on
  the peer role (the kernel, through the device), run in this VM so that
  the scenario can pause, kill or cut off the far end of any one connection.
  Connections are spread evenly over the families and over plain TCP and
  TLS, and each gets one behaviour:

    * `echo` - idles for a random interval, log-uniform between
      `--idle-min` and `--idle-max`, then echoes a small payload, and again
      until the run ends;
    * `trickle` - echoes a single byte every `--trickle-min` to
      `--trickle-max`;
    * `burst` - idles as `echo` does, then echoes `--burst-bytes` at once;
    * `stall` - idles, then sends `--burst-bytes` to a peer with a small
      receive window that stops reading for up to `--outage-max`, so that
      SmolNet's send window closes and it probes the zero window until the
      peer reads again.

  Every working connection makes one last exchange as the run ends, after
  its longest idle, and every byte echoed is checked.

  `--vanish` percent of the connections (default 20) instead lose their
  peer silently at a random time. `SmolNet.Integration.Blackhole` drops
  every packet of the connection on the device, so that nothing arrives
  from the peer, not even a FIN or RST. Each vanishes one of these ways:

    * `silent` - with nothing outstanding. Without keepalive, TCP cannot
      notice this, so SmolNet correctly never does; with `--keepalive`, its
      unanswered probes end the connection;
    * `unacked` - SmolNet then sends data, which it retransmits and which is
      never acknowledged;
    * `zero_window` - the peer stopped reading first and closed its window,
      so SmolNet was probing it. A TLS peer's `:ssl` reads on, megabytes
      ahead, so over TLS its window stays open and this is `silent`;
    * `reboot` - SmolNet sends data, the peer's socket is closed too (its FIN
      or RST is dropped), and after an outage of up to `--outage-max` the
      path returns, so that SmolNet's next retransmission reaches a host
      that answers it with a RST;
    * `nat` - the connection idles past `--nat-timeout`, then its packets
      are dropped and SmolNet sends: a NAT or stateful firewall that
      forgot the idle connection's mapping, and drops every packet after.
      (nftables' conntrack timeout policies, which would let the kernel
      expire the mapping itself, did not take effect when tried.)

  Each waits for SmolNet to fail the connection until the run ends, and
  how long that took, or that it did not happen, is recorded in the
  `detection` results and noted. A `reboot` must be detected within 65 s of
  the path returning, since SmolNet's retransmission timeout backs off to
  at most 60 s, or the run fails. `unacked`, `nat` and TCP `zero_window`
  must be detected too, with `:etimedout`, by SmolNet's user timeout:
  924.6 s without an answer while it has data outstanding. With
  `--keepalive`, `silent` and TLS `zero_window` must be, after 2 h and 9
  probes 75 s apart. Each vanishes early
  enough for that to happen before the run ends, or is skipped if the run
  is too short. `--no-require-detection` only records them.

  The timer switches, such as `--user-timeout 45s` or `--keepalive-idle
  40s`, shorten SmolNet's fixed timers through its test-only
  `:test_tcp_timers` stack option, so that a short run can see them. They
  do not apply to `--baseline`, where the kernel's own, the same Linux
  defaults, do.

  Once every connection is open, and before any behaviour starts, all of
  them are idle for `--quiet`. The stack's timer polls and native calls,
  and the VM's CPU time, over that window are the `quiet` result, and more
  than `--quiet-poll-limit` polls a second fails the run as busy polling.
  The same counters, the timer generation and the time to the stack's next
  poll are sampled into `metrics.csv` throughout the run.

  The run ends with every socket closed and the stack's socket count back
  to zero. `--self-check` runs the working behaviours over the helper's
  loopback, and `--baseline` runs everything over the kernel's loopback,
  which shows how Linux handles the same vanished peers. A peer vanishes
  only where nftables can reach the connection: not over the helper's
  loopback, and not without root.
  """

  alias SmolNet.Integration.Blackhole
  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak
  alias SmolNet.Integration.Soak.Options
  alias SmolNet.Integration.Tls

  @behaviours [:echo, :trickle, :burst, :stall]
  @vanishings [:silent, :unacked, :zero_window, :reboot, :nat]
  @transports [:tcp, :tls]
  @max_connections 240
  @connect_timeout 30_000
  @settle 5_000
  @sample_interval 5_000
  @final_lead 60_000
  # SmolNet's retransmission timeout backs off to at most 60 s, so the first
  # retransmission after the path returns comes within that.
  @reboot_bound 65_000
  @min_outage 5_000
  # Long enough for SmolNet to be probing the zero window before it
  # vanishes.
  @probe_lead 5_000
  @nat_margin 10_000
  @window_bytes 4_096
  @unacked_bytes 4_096
  @zero_window_bytes 32_768
  @max_echo_bytes 4_096
  @detect_slack 30_000
  # How long a peer waits for more data before it collects its garbage.
  @collect_after 5_000
  # Closing a connection to a vanished peer takes SmolNet's 30 s close
  # deadline, and then TIME-WAIT.
  @release_timeout 90_000
  # Time left at the end for every socket to close: SmolNet's 30 s close
  # deadline for a vanished peer, then TIME-WAIT.
  @close_reserve 45_000

  @sampled [
    :poll_calls,
    :native_calls,
    :timer_generation,
    :ingress_packets,
    :emitted_packets,
    :beam_cpu_ms,
    :poll_in_ms
  ]

  @doc "Returns the scenario's runner config."
  @spec config() :: keyword()
  def config do
    [
      name: "idle",
      default_duration: "10m",
      switches: [
        connections: :integer,
        idle_min: :string,
        idle_max: :string,
        trickle_min: :string,
        trickle_max: :string,
        burst_bytes: :integer,
        vanish: :integer,
        outage_max: :string,
        nat_timeout: :string,
        quiet: :string,
        quiet_poll_limit: :float,
        op_timeout: :string,
        socket_buffer: :integer,
        require_detection: :boolean,
        keepalive: :boolean,
        user_timeout: :string,
        keepalive_idle: :string,
        keepalive_interval: :string,
        keepalive_probes: :integer,
        seed: :integer
      ],
      defaults: [
        connections: 200,
        idle_min: "1s",
        idle_max: "1h",
        trickle_min: "1s",
        trickle_max: "1m",
        burst_bytes: 262_144,
        vanish: 20,
        outage_max: "2m",
        nat_timeout: "30s",
        quiet: "1m",
        quiet_poll_limit: 1.0,
        op_timeout: "30s",
        socket_buffer: 65_536,
        require_detection: true,
        keepalive: false,
        user_timeout: nil,
        keepalive_idle: nil,
        keepalive_interval: nil,
        keepalive_probes: nil,
        seed: nil
      ],
      counters: [:connections, :exchanges, :bytes_echoed, :vanished, :detected | @sampled],
      # Each connection is one socket, or two over the helper's loopback,
      # where SmolNet is the peer too.
      stack: &stack_options/1,
      usage: """

      idle options:
        --connections N       connections to hold, 1 to #{@max_connections} (default 200)
        --idle-min D          the shortest idle between exchanges (default 1s)
        --idle-max D          the longest idle between exchanges (default 1h)
        --trickle-min D       the shortest wait between trickled bytes (default 1s)
        --trickle-max D       the longest wait between trickled bytes (default 1m)
        --burst-bytes N       the size of a burst, and of a stalled send (default 262144)
        --vanish PERCENT      the share of connections whose peer vanishes (default 20)
        --outage-max D        the longest a peer stops reading, or a reboot's path is
                              down (default 2m)
        --nat-timeout D       how long a nat connection idles before it vanishes (default 30s)
        --quiet D             how long every connection idles before the behaviours
                              start, measuring timer activity; 0 for none (default 1m)
        --quiet-poll-limit R  the most stack polls a second while quiet (default 1.0)
        --op-timeout D        each exchange's deadline, beyond any stall (default 30s)
        --socket-buffer N     SmolNet's receive and send buffers, in bytes (default 65536)
        --no-require-detection  do not fail when SmolNet misses a vanished peer that
                              it has data outstanding to, or with --keepalive any
                              vanished peer (by default it fails)
        --keepalive           turn keepalive on for SmolNet's connections
        --user-timeout D      shorten SmolNet's fixed user timeout (default 924.6s),
                              through a test-only stack option
        --keepalive-idle D    shorten its fixed keep-alive idle time (default 2h)
        --keepalive-interval D  and its probe interval (default 75s)
        --keepalive-probes N  and its probe count (default 9)
        --seed N              the random seed, to repeat a run's plan (default random)

      Idle intervals are capped by the run: a connection whose next idle would
      outlast it idles until its last exchange instead. A peer vanishes early
      enough to be detected before the run ends, or is skipped.
      """
    ]
  end

  @doc "Runs the scenario's workload."
  @spec run(Soak.Context.t()) :: :ok
  def run(context) do
    {:ok, _started} = Application.ensure_all_started(:ssl)

    case settings(context) do
      {:ok, settings} -> hold(context, settings)
      {:error, message} -> Soak.fail(context, :usage, message)
    end
  end

  # Settings

  defp settings(context) do
    extra = context.extra

    with {:ok, idle_min} <- duration(:idle_min, extra.idle_min),
         {:ok, idle_max} <- duration(:idle_max, extra.idle_max),
         :ok <- ordered(:idle_min, idle_min, :idle_max, idle_max),
         {:ok, trickle_min} <- duration(:trickle_min, extra.trickle_min),
         {:ok, trickle_max} <- duration(:trickle_max, extra.trickle_max),
         :ok <- ordered(:trickle_min, trickle_min, :trickle_max, trickle_max),
         {:ok, outage_max} <- duration(:outage_max, extra.outage_max),
         {:ok, nat_timeout} <- duration(:nat_timeout, extra.nat_timeout),
         {:ok, quiet} <- duration(:quiet, extra.quiet, 0),
         :ok <- quiet_fits(quiet, context.duration_ms),
         {:ok, op_timeout} <- duration(:op_timeout, extra.op_timeout),
         :ok <- in_range(:connections, extra.connections, 1..@max_connections),
         :ok <- in_range(:vanish, extra.vanish, 0..100),
         :ok <- in_range(:burst_bytes, extra.burst_bytes, 1..67_108_864),
         :ok <- in_range(:socket_buffer, extra.socket_buffer, 1_024..1_048_576),
         :ok <- positive(:quiet_poll_limit, extra.quiet_poll_limit),
         {:ok, overrides} <- timer_overrides(extra) do
      {:ok,
       %{
         keepalive: extra.keepalive,
         timers: timers(context, overrides),
         connections: extra.connections,
         idle: {idle_min, idle_max},
         trickle: {trickle_min, trickle_max},
         burst_bytes: extra.burst_bytes,
         vanish: extra.vanish,
         outage: {min(@min_outage, outage_max), outage_max},
         nat_timeout: nat_timeout,
         quiet: quiet,
         quiet_poll_limit: extra.quiet_poll_limit,
         op_timeout: op_timeout,
         socket_buffer: extra.socket_buffer,
         require_detection: extra.require_detection,
         seed: extra.seed || :rand.uniform(1_000_000_000)
       }}
    end
  end

  # SmolNet's fixed TCP timers, which are Linux's defaults, and so the
  # kernel's under --baseline too.
  @tcp_timers %{
    user_timeout: 924_600,
    keepalive_idle: 7_200_000,
    keepalive_interval: 75_000,
    keepalive_probes: 9
  }
  @max_timer 86_400_000

  # The stack the runner starts: room for every socket, and the timer
  # switches' overrides, through SmolNet's test-only `:test_tcp_timers`.
  # settings/1 reports invalid switches.
  defp stack_options(extra) do
    case timer_overrides(extra) do
      {:ok, overrides} when map_size(overrides) > 0 ->
        [limits: %{sockets: 512}, test_tcp_timers: overrides]

      _none ->
        [limits: %{sockets: 512}]
    end
  end

  defp timer_overrides(extra) do
    with {:ok, user_timeout} <- timer(:user_timeout, extra.user_timeout),
         {:ok, idle} <- timer(:keepalive_idle, extra.keepalive_idle),
         {:ok, interval} <- timer(:keepalive_interval, extra.keepalive_interval),
         :ok <- probes(extra.keepalive_probes) do
      {:ok,
       %{
         user_timeout: user_timeout,
         keepalive_idle: idle,
         keepalive_interval: interval,
         keepalive_probes: extra.keepalive_probes
       }
       |> Map.reject(fn {_name, value} -> value == nil end)}
    end
  end

  defp timer(_name, nil), do: {:ok, nil}

  defp timer(name, text) do
    case duration(name, text) do
      {:ok, ms} when ms <= @max_timer -> {:ok, ms}
      {:ok, _ms} -> {:error, "--#{dasherize(name)} must be at most a day"}
      error -> error
    end
  end

  defp probes(nil), do: :ok
  defp probes(count), do: in_range(:keepalive_probes, count, 1..255)

  # The timers the run's connections have: the kernel keeps its own.
  defp timers(%{mode: :kernel}, _overrides), do: @tcp_timers
  defp timers(_context, overrides), do: Map.merge(@tcp_timers, overrides)

  defp duration(name, text, least \\ 1) do
    case Options.parse_duration(text) do
      {:ok, milliseconds} when milliseconds >= least -> {:ok, milliseconds}
      _invalid -> {:error, "--#{dasherize(name)} must be a duration of at least #{least} ms"}
    end
  end

  defp ordered(_low_name, low, _high_name, high) when low <= high, do: :ok

  defp ordered(low_name, _low, high_name, _high),
    do: {:error, "--#{dasherize(low_name)} must not exceed --#{dasherize(high_name)}"}

  defp quiet_fits(quiet, duration) when quiet * 2 <= duration, do: :ok
  defp quiet_fits(_quiet, _duration), do: {:error, "--quiet must be at most half of --duration"}

  defp in_range(name, value, range) do
    if is_integer(value) and value in range,
      do: :ok,
      else: {:error, "--#{dasherize(name)} must be #{range.first} to #{range.last}"}
  end

  defp positive(_name, value) when is_number(value) and value > 0, do: :ok
  defp positive(name, _value), do: {:error, "--#{dasherize(name)} must be positive"}

  defp dasherize(name), do: name |> to_string() |> String.replace("_", "-")

  # The run

  defp hold(context, settings) do
    Soak.record(context, :seed, settings.seed)
    :rand.seed(:exsss, {settings.seed, 0, 0})
    sampler = spawn_link(fn -> sample_loop(context, snapshot(context)) end)
    blackhole = start_blackhole(context, settings)
    plan = plan(context, settings, blackhole)
    rows = :ets.new(:idle_connections, [:public, :set])

    case listen_all(context, settings, plan) do
      {:ok, listeners} ->
        opened = open_all(context, settings, listeners, plan, rows)

        if opened != :error do
          Soak.log(context, "#{length(opened)} connections open")
          quiet(context, settings)
          go(context, blackhole, opened)
        end

        Enum.each(listeners, fn {_key, listener} -> close_listener(listener) end)

      :error ->
        :ok
    end

    if blackhole, do: Blackhole.stop(blackhole)
    summarize(context, rows)
    :ets.delete(rows)
    stop_sampler(sampler)
    Soak.await_socket_count(context, 0, @release_timeout)
  end

  defp start_blackhole(%{mode: :self_check} = context, settings) do
    if settings.vanish > 0 do
      Soak.note(context, "no peer vanishes over the helper's loopback, which nftables cannot see")
    end

    nil
  end

  defp start_blackhole(context, %{vanish: vanish}) when vanish > 0 do
    interface = if context.mode == :kernel, do: "lo", else: context.device

    case Blackhole.start(interface) do
      {:ok, blackhole} ->
        blackhole

      {:error, message} ->
        Soak.note(context, "no peer vanishes: could not load the nftables blackhole: #{message}")
        nil
    end
  end

  defp start_blackhole(_context, _settings), do: nil

  # Which connection does what. Consecutive connections take each family
  # and transport in turn, and the roles come in runs, so that every role
  # has connections of every kind.
  defp plan(context, settings, blackhole) do
    vanishings = if blackhole == nil, do: [], else: @vanishings

    vanishing_count =
      if vanishings == [], do: 0, else: div(settings.connections * settings.vanish + 50, 100)

    kinds = for transport <- @transports, family <- context.families, do: {family, transport}

    roles =
      spread(vanishings, vanishing_count) ++
        spread(@behaviours, settings.connections - vanishing_count)

    roles
    |> Enum.with_index()
    |> Enum.map(fn {role, index} ->
      {family, transport} = Enum.at(kinds, rem(index, length(kinds)))

      %{
        index: index,
        family: family,
        transport: transport,
        role: role,
        profile: profile(role),
        label: "##{index} #{family} #{transport} #{role}"
      }
    end)
  end

  # `count` roles, as even runs of each.
  defp spread([], _count), do: []

  defp spread(roles, count) do
    each = div(count, length(roles))
    extra = rem(count, length(roles))

    roles
    |> Enum.with_index()
    |> Enum.flat_map(fn {role, index} ->
      List.duplicate(role, each + if(index < extra, do: 1, else: 0))
    end)
  end

  # Which listener a role's peer is accepted from.
  defp profile(role) when role in [:stall, :zero_window], do: :small_window
  defp profile(_role), do: :normal

  # Listeners

  defp listen_all(context, settings, plan) do
    keys = plan |> Enum.map(&{&1.family, &1.transport, &1.profile}) |> Enum.uniq()
    listen_each(context, settings, keys, hibernating(Tls.local_options()), %{})
  end

  # `:ssl`'s processes, at both ends of a TLS connection, would otherwise
  # keep what the last exchange left on their heaps for as long as the
  # connection idles, as SmolNet's own sockets did before #135, and
  # metrics.csv would show that as binary memory rising.
  defp hibernating(%{server: server, client: client}) do
    %{
      server: [hibernate_after: @collect_after] ++ server,
      client: [hibernate_after: @collect_after] ++ client
    }
  end

  defp listen_each(_context, _settings, [], _tls, listeners), do: {:ok, listeners}

  defp listen_each(context, settings, [key | keys], tls, listeners) do
    case listen(context, settings, key, tls) do
      {:ok, listener} ->
        listen_each(context, settings, keys, tls, Map.put(listeners, key, listener))

      {:error, reason} ->
        Enum.each(listeners, fn {_key, listener} -> close_listener(listener) end)
        Soak.fail(context, :listen, "could not listen for #{inspect(key)}", [{"reason", reason}])
        :error
    end
  end

  defp listen(context, settings, {family, transport, profile}, tls) do
    # A small-window peer's receive buffer is set here, so that it holds
    # from the handshake on; the kernel doubles what it is given.
    {buffer, window} =
      if profile == :small_window,
        do: {nil, [recbuf: @window_bytes]},
        else: {settings.socket_buffer, []}

    listening =
      case transport do
        :tcp ->
          options =
            Network.tcp_options(context, :peer, family) ++
              Tls.buffer_options(context, :peer, buffer) ++
              [:binary, active: false, ip: Network.address(context, :peer, family)] ++ window

          with {:ok, socket} <- :gen_tcp.listen(0, options),
               {:ok, {_address, port}} <- :inet.sockname(socket) do
            {:ok, %{transport: :tcp, socket: socket, port: port, tls: nil}}
          end

        :tls ->
          options = tls.server ++ window

          with {:ok, socket} <-
                 Tls.listen(context, :peer, family, options, buffer: buffer, backlog: 32),
               {:ok, {_address, port}} <- :ssl.sockname(socket) do
            {:ok, %{transport: :tls, socket: socket, port: port, tls: tls.client}}
          end
      end

    listening
  end

  defp close_listener(%{transport: :tcp, socket: socket}), do: :gen_tcp.close(socket)
  defp close_listener(%{transport: :tls, socket: socket}), do: :ssl.close(socket)

  # Opening connections

  # One at a time, so that each accept pairs with its connect.
  defp open_all(context, settings, listeners, plan, rows) do
    Enum.reduce_while(plan, [], fn spec, opened ->
      listener = Map.fetch!(listeners, {spec.family, spec.transport, spec.profile})

      case open(context, settings, listener, spec, rows) do
        {:ok, connection} ->
          Soak.count(context, :connections)
          {:cont, [connection | opened]}

        :error ->
          {:halt, :error}
      end
    end)
  end

  defp open(context, settings, listener, spec, rows) do
    coordinator = self()
    address = Network.address(context, :peer, spec.family)

    client =
      spawn_link(fn ->
        connection(context, settings, spec, rows, coordinator, {address, listener})
      end)

    Soak.within(context, {:open, spec.label}, 2 * @connect_timeout, fn ->
      with {:ok, socket} <- accept(listener),
           {:ok, peer} <- start_peer(context, spec, listener, socket),
           {:ok, port} <- await_connected(client) do
        send(client, {:peer, peer, {port, listener.port}})
        {:ok, %{client: client, peer: peer, spec: spec}}
      end
    end)
    |> case do
      {:ok, connection} ->
        {:ok, connection}

      {:error, reason} ->
        Soak.fail(context, :connect, "#{spec.label} could not connect", [{"reason", reason}])
        :error
    end
  end

  defp accept(%{transport: :tcp, socket: listener}),
    do: :gen_tcp.accept(listener, @connect_timeout)

  defp accept(%{transport: :tls, socket: listener}),
    do: :ssl.transport_accept(listener, @connect_timeout)

  defp await_connected(client) do
    receive do
      {^client, {:connected, port}} -> {:ok, port}
      {^client, {:connect_failed, reason}} -> {:error, reason}
    after
      @connect_timeout -> {:error, :connect_timeout}
    end
  end

  # SmolNet's end

  defp connection(context, settings, spec, rows, coordinator, {address, listener}) do
    :rand.seed(:exsss, {settings.seed, spec.index, 1})

    case connect(context, settings, spec, address, listener) do
      {:ok, socket} ->
        {:ok, {_address, port}} = sockname(spec.transport, socket)
        send(coordinator, {self(), {:connected, port}})

        receive do
          {:peer, peer, pair} ->
            state = %{
              context: context,
              settings: settings,
              spec: spec,
              socket: socket,
              peer: peer,
              pair: pair,
              connected_at: now(),
              row: %{exchanges: 0, bytes: 0}
            }

            receive do
              {:go, timeline} ->
                row = behave(Map.put(state, :timeline, timeline))

                :ets.insert(
                  rows,
                  {spec.index, Map.merge(Map.take(spec, [:family, :transport, :role]), row)}
                )
            end

            close(spec.transport, socket)
            send(peer, :stop)
        end

      {:error, reason} ->
        send(coordinator, {self(), {:connect_failed, reason}})
    end
  end

  defp connect(context, settings, %{transport: :tcp} = spec, address, listener) do
    options =
      Network.tcp_options(context, :subject, spec.family) ++
        Tls.buffer_options(context, :subject, settings.socket_buffer) ++
        [:binary, active: false, keepalive: settings.keepalive]

    :gen_tcp.connect(address, listener.port, options, @connect_timeout)
  end

  defp connect(context, settings, %{transport: :tls}, address, listener) do
    options = listener.tls ++ [keepalive: settings.keepalive]

    Tls.connect(context, :subject, address, listener.port, options,
      timeout: @connect_timeout,
      buffer: settings.socket_buffer
    )
  end

  defp behave(%{spec: %{role: :echo}} = state) do
    cycle(state, &idle/1, &exchange(&1, echo_payload()))
  end

  defp behave(%{spec: %{role: :trickle}} = state) do
    cycle(state, &trickle_wait/1, &exchange(&1, :crypto.strong_rand_bytes(1)))
  end

  defp behave(%{spec: %{role: :burst}} = state) do
    cycle(state, &idle/1, &exchange(&1, :crypto.strong_rand_bytes(&1.settings.burst_bytes)))
  end

  defp behave(%{spec: %{role: :stall}} = state) do
    cycle(state, &idle/1, fn state ->
      pause = min(log_uniform(state.settings.outage), pause_room(state))
      exchange(state, :crypto.strong_rand_bytes(state.settings.burst_bytes), pause)
    end)
  end

  defp behave(state), do: vanish(state)

  # Waits, then acts, until the last action, which comes as the run ends.
  defp cycle(state, wait, act) do
    last_at = state.timeline.work_until
    at = now() + wait.(state)

    if at < last_at do
      collect_and_wait(fn -> sleep_until(at) end)

      case act.(state) do
        {:ok, state} -> cycle(state, wait, act)
        :error -> state.row
      end
    else
      collect_and_wait(fn -> sleep_until(last_at) end)

      case act.(state) do
        {:ok, state} -> Map.put(state.row, :outcome, :ok)
        :error -> state.row
      end
    end
  end

  # Releases what the last exchange left on the process's heap, such as a
  # burst's payload, before a long wait: an idle process never collects
  # garbage on its own, and would hold it until the run ends, which
  # metrics.csv would show as binary memory rising.
  defp collect_and_wait(wait) do
    :erlang.garbage_collect()
    wait.()
  end

  # The longest a stalled exchange may pause and still finish, at its
  # deadline, in time for the sockets to close before the run's duration
  # ends: the runner allows the workload only a short overrun.
  defp pause_room(state) do
    max(0, state.timeline.ends_at - @close_reserve - state.settings.op_timeout - now())
  end

  defp idle(state), do: log_uniform(state.settings.idle)
  defp trickle_wait(state), do: log_uniform(state.settings.trickle)

  defp echo_payload, do: :crypto.strong_rand_bytes(:rand.uniform(@max_echo_bytes))

  # Sends `payload` and reads its echo, concurrently, so that a payload
  # larger than the buffers cannot deadlock; with `pause`, the peer stops
  # reading for that long first.
  defp exchange(state, payload, pause \\ 0) do
    size = byte_size(payload)
    name = {:exchange, state.spec.label, size}

    result =
      Soak.within(state.context, name, state.settings.op_timeout + pause, fn ->
        if pause > 0, do: pause_peer(state.peer, pause)
        sender = Task.async(fn -> send_data(state, payload) end)
        received = recv(state, size, :infinity)
        {Task.await(sender, :infinity), received}
      end)

    case result do
      {:ok, {:ok, ^payload}} ->
        Soak.count(state.context, :exchanges)
        Soak.count(state.context, :bytes_echoed, size)
        row = %{state.row | exchanges: state.row.exchanges + 1, bytes: state.row.bytes + size}
        {:ok, %{state | row: row}}

      {:ok, {:ok, echoed}} ->
        Soak.fail(state.context, :integrity, "#{state.spec.label}: the echo differs", [
          {"sizes", %{sent: size, echoed: byte_size(echoed)}}
        ])

        :error

      {sent, received} ->
        summary = "#{state.spec.label}: an exchange of #{size} bytes failed"
        Soak.fail(state.context, :exchange, summary, [{"send", sent}, {"recv", received}])
        :error
    end
  end

  defp pause_peer(peer, pause) do
    send(peer, {:pause, pause, self()})

    receive do
      {^peer, :paused} -> :ok
    end
  end

  # Vanishing

  defp vanish(state) do
    case schedule(state) do
      {:ok, schedule} ->
        sleep_until(schedule.prepare_at)
        state = prepare(state)
        sleep_until(schedule.vanish_at)
        vanished_at = now()
        cut(state)
        Soak.count(state.context, :vanished)
        strike(state)
        detect(state, schedule, vanished_at)

      {:skip, reason} ->
        Map.merge(state.row, %{outcome: :skipped, reason: reason})
    end
  end

  # When each vanishing happens, in the first half of the working time so
  # that there is time left to detect it.
  defp schedule(%{spec: %{role: :reboot}} = state) do
    %{active_start: start, work_until: until} = state.timeline
    slack = until - start - @reboot_bound - @min_outage

    if slack >= @min_outage do
      outage = min(log_uniform(state.settings.outage), slack)
      vanish_at = start + trunc(:rand.uniform() * (slack - outage))
      {:ok, %{prepare_at: vanish_at, vanish_at: vanish_at, lift_at: vanish_at + outage}}
    else
      {:skip, "the run is too short to bound a reboot's detection"}
    end
  end

  # A NAT forgets a connection once it has been idle past its timeout.
  defp schedule(%{spec: %{role: :nat}} = state) do
    %{active_start: start, work_until: until} = state.timeline
    expired_at = max(start, state.connected_at + state.settings.nat_timeout + @nat_margin)

    window = div(until - @nat_margin - expired_at, 2)

    case detectable(state, expired_at, window) do
      {:ok, window} when window > 0 ->
        at = expired_at + trunc(:rand.uniform() * window)
        {:ok, %{prepare_at: at, vanish_at: at}}

      {:ok, _window} ->
        {:skip, "the run is too short for the NAT's timeout to pass"}

      :too_short ->
        {:skip, "the run is too short to detect it"}
    end
  end

  defp schedule(%{spec: %{role: role}} = state) do
    %{active_start: start, work_until: until} = state.timeline
    lead = if role == :zero_window, do: @probe_lead, else: 0
    window = div(until - start - lead, 2)

    case detectable(state, start + lead, window) do
      {:ok, window} when window > 0 ->
        vanish_at = start + lead + trunc(:rand.uniform() * window)
        {:ok, %{prepare_at: vanish_at - lead, vanish_at: vanish_at}}

      {:ok, _window} ->
        {:skip, "the run is too short"}

      :too_short ->
        {:skip, "the run is too short to detect it"}
    end
  end

  # Narrows the `window` after `from` in which a peer vanishes so that, when
  # detection is required, SmolNet has time to detect it before the run's
  # working time ends.
  defp detectable(state, from, window) do
    case detection_bound(state) do
      nil ->
        {:ok, window}

      bound ->
        room = state.timeline.work_until - @detect_slack - bound - from
        if room >= 0, do: {:ok, min(window, room)}, else: :too_short
    end
  end

  # The longest SmolNet may take to fail a connection after its peer
  # vanished, when the run requires it to: the user timeout, counted from
  # the last packet received or the first sent after it, or with keepalive,
  # for an idle connection, every probe after its last packet. None for an
  # idle connection without keepalive, which TCP cannot notice.
  #
  # A paused TLS peer's `:ssl` keeps reading its socket, megabytes ahead, so
  # its window does not close and a TLS `zero_window` connection has nothing
  # outstanding when its peer vanishes: to SmolNet it is `silent`.
  defp detection_bound(%{settings: %{require_detection: false}}), do: nil

  defp detection_bound(%{spec: %{role: :zero_window, transport: :tls}} = state),
    do: detection_bound(put_in(state.spec.role, :silent))

  defp detection_bound(%{spec: %{role: :silent}, settings: %{keepalive: false}}), do: nil

  defp detection_bound(%{spec: %{role: :silent}, settings: %{timers: timers}}),
    do: timers.keepalive_idle + timers.keepalive_probes * timers.keepalive_interval

  defp detection_bound(%{settings: %{timers: timers}}), do: timers.user_timeout

  # A zero-window connection's peer stops reading and SmolNet fills its
  # window before the peer vanishes.
  defp prepare(%{spec: %{role: :zero_window}} = state) do
    pause_peer(state.peer, :infinity)
    _sent = send_within(state, :crypto.strong_rand_bytes(@zero_window_bytes))
    state
  end

  defp prepare(state), do: state

  defp cut(state) do
    blackhole(state, :drop)

    if state.spec.role == :reboot do
      send(state.peer, :abort)
    end
  end

  # What SmolNet does after its peer has gone: sends data, which is
  # outstanding from then on, for the roles that test that.
  defp strike(%{spec: %{role: role}} = state) when role in [:unacked, :reboot, :nat] do
    send_within(state, :crypto.strong_rand_bytes(@unacked_bytes))
  end

  defp strike(_state), do: :ok

  defp send_within(state, data) do
    name = {:send, state.spec.label, byte_size(data)}

    case Soak.within(state.context, name, state.settings.op_timeout, fn ->
           send_data(state, data)
         end) do
      :ok ->
        :ok

      {:error, reason} ->
        Soak.note(state.context, "#{state.spec.label}: a send failed with #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Waits for SmolNet to fail the connection, until the run ends or, for a
  # reboot, until the bound after the path returns.
  defp detect(%{spec: %{role: :reboot}} = state, schedule, vanished_at) do
    sleep_until(schedule.lift_at)
    blackhole(state, :restore)
    lifted_at = now()
    result = await_failure(state, lifted_at + @reboot_bound)
    row = detection_row(state, result, vanished_at)
    row = Map.put(row, :after_lift_ms, now() - lifted_at)

    if row.outcome != :detected do
      summary =
        "#{state.spec.label}: SmolNet did not fail the connection within " <>
          "#{@reboot_bound} ms of its path returning to a peer that had rebooted"

      Soak.fail(state.context, :reboot_undetected, summary, [{"detection", row}])
    end

    row
  end

  defp detect(state, _schedule, vanished_at) do
    result = await_failure(state, state.timeline.work_until)
    row = detection_row(state, result, vanished_at)

    if detection_bound(state) != nil and row.outcome == :undetected do
      summary = "#{state.spec.label}: SmolNet did not fail a connection to a vanished peer"
      Soak.fail(state.context, :undetected, summary, [{"detection", row}])
    end

    row
  end

  defp await_failure(state, until) do
    wait = max(0, until - now())
    name = {:detect, state.spec.label}
    Soak.within(state.context, name, wait + @detect_slack, fn -> recv(state, 0, wait) end)
  end

  defp detection_row(state, result, vanished_at) do
    after_ms = now() - vanished_at

    {outcome, error} =
      case result do
        {:error, :timeout} -> {:undetected, nil}
        {:error, reason} -> {:detected, inspect(reason)}
        {:ok, _data} -> {:answered, nil}
      end

    if outcome == :detected, do: Soak.count(state.context, :detected)
    Soak.log(state.context, "#{state.spec.label}: #{outcome} after #{after_ms} ms #{error}")
    Map.merge(state.row, %{outcome: outcome, after_ms: after_ms, error: error})
  end

  defp blackhole(state, action) do
    blackhole = state.timeline.blackhole

    result =
      case action do
        :drop -> Blackhole.drop(blackhole, state.pair)
        :restore -> Blackhole.restore(blackhole, state.pair)
      end

    with {:error, message} <- result do
      Soak.fail(state.context, :blackhole, "#{state.spec.label}: #{message}")
    end
  end

  # The peer's end: echoes what arrives, in active mode so that it can be
  # paused or aborted at any time. A paused peer stops reading, so its
  # receive window closes.

  defp start_peer(context, spec, listener, socket) do
    peer = spawn_link(fn -> peer(context, spec) end)

    case controlling_process(listener.transport, socket, peer) do
      :ok ->
        send(peer, {:socket, listener.transport, socket})
        {:ok, peer}

      {:error, _reason} = error ->
        Process.unlink(peer)
        Process.exit(peer, :kill)
        close(listener.transport, socket)
        error
    end
  end

  defp peer(context, spec) do
    receive do
      {:socket, :tcp, socket} ->
        echo(%{transport: :tcp, socket: socket})

      {:socket, :tls, socket} ->
        case :ssl.handshake(socket, @connect_timeout) do
          {:ok, tls} ->
            echo(%{transport: :tls, socket: tls})

          {:error, reason} ->
            summary = "#{spec.label}: the peer's TLS handshake failed"
            Soak.fail(context, :handshake, summary, [{"reason", reason}])
        end
    end
  end

  defp echo(peer) do
    :ok = set_active(peer)
    echo_loop(peer)
  end

  defp echo_loop(%{socket: socket} = peer, wait \\ @collect_after) do
    receive do
      {tag, ^socket, data} when tag in [:tcp, :ssl] ->
        case send_data(peer, data) do
          :ok -> echo(peer)
          {:error, _reason} -> close(peer.transport, socket)
        end

      {tag, ^socket} when tag in [:tcp_closed, :ssl_closed] ->
        close(peer.transport, socket)

      {tag, ^socket, _reason} when tag in [:tcp_error, :ssl_error] ->
        close(peer.transport, socket)

      {:pause, pause, from} ->
        send(from, {self(), :paused})
        paused(peer, pause)

      :abort ->
        abort(peer)

      :stop ->
        close(peer.transport, socket)
    after
      wait ->
        collect_and_wait(fn -> echo_loop(peer, :infinity) end)
    end
  end

  defp paused(peer, pause) do
    timer = if pause != :infinity, do: Process.send_after(self(), :resume, pause)

    receive do
      :resume ->
        echo_loop(peer)

      :abort ->
        abort(peer)

      :stop ->
        if timer, do: Process.cancel_timer(timer)
        close(peer.transport, peer.socket)
    end
  end

  # A peer that is gone at once: an abortive close, whose RST, like
  # anything else it sends, the blackhole drops.
  defp abort(%{transport: :tcp, socket: socket}) do
    _set = :inet.setopts(socket, linger: {true, 0})
    :gen_tcp.close(socket)
  end

  defp abort(%{transport: :tls, socket: socket}) do
    _set = :ssl.setopts(socket, linger: {true, 0})
    :ssl.close(socket)
  end

  # The quiet window, and go

  defp quiet(_context, %{quiet: 0}), do: :ok

  defp quiet(context, settings) do
    Process.sleep(min(@settle, settings.quiet))
    before = snapshot(context)
    Process.sleep(settings.quiet)
    activity = activity(before, snapshot(context))
    Soak.record(context, :quiet, activity)

    Soak.note(
      context,
      "quiet for #{activity.seconds} s with every connection idle: " <> describe(activity)
    )

    rate = activity[:poll_calls_per_s]

    if rate != nil and rate > settings.quiet_poll_limit do
      summary =
        "the stack polled #{rate} times a second while every connection was idle, " <>
          "more than #{settings.quiet_poll_limit}"

      Soak.fail(context, :busy_poll, summary, [{"activity", activity}])
    end
  end

  defp go(context, blackhole, opened) do
    active_start = now()

    work_until =
      max(active_start, context.ends_at - min(@final_lead, div(context.duration_ms, 4)))

    timeline = %{
      active_start: active_start,
      work_until: work_until,
      ends_at: context.ends_at,
      blackhole: blackhole
    }

    before = snapshot(context)

    monitors =
      Enum.flat_map(opened, fn connection ->
        send(connection.client, {:go, timeline})
        [Process.monitor(connection.client), Process.monitor(connection.peer)]
      end)

    Enum.each(monitors, fn monitor ->
      receive do
        {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      end
    end)

    activity = activity(before, snapshot(context))
    Soak.record(context, :active, activity)
    Soak.note(context, "active for #{activity.seconds} s: " <> describe(activity))
  end

  # Measurements

  defp snapshot(context) do
    base = %{at: now(), beam_cpu_ms: cpu_ms()}

    stack =
      case context.stack && SmolNet.stack_info(context.stack) do
        {:ok, %{native: %{result: native}} = info} ->
          counters = native.counters

          %{
            poll_calls: counters.poll_calls,
            native_calls: counters.native_calls,
            ingress_packets: counters.ingress_packets,
            emitted_packets: counters.emitted_packets,
            timer_generation: info.timer_generation,
            poll_in_ms: poll_in(info.poll_at)
          }

        _none ->
          %{}
      end

    Map.merge(base, stack)
  end

  # The stack's timer deadline is in milliseconds since the VM started, as
  # SmolNet.Stack.Clock.System counts them; -1 for no timer.
  defp poll_in(nil), do: -1

  defp poll_in(poll_at) do
    since_start = :erlang.monotonic_time() - :erlang.system_info(:start_time)
    max(0, poll_at - System.convert_time_unit(since_start, :native, :millisecond))
  end

  defp activity(before, later) do
    seconds = max(later.at - before.at, 1) / 1_000

    rates =
      for key <- [
            :poll_calls,
            :native_calls,
            :timer_generation,
            :ingress_packets,
            :emitted_packets
          ],
          is_integer(before[key]) and is_integer(later[key]),
          into: %{} do
        {:"#{key}_per_s", Float.round((later[key] - before[key]) / seconds, 3)}
      end

    cpu =
      if is_integer(before.beam_cpu_ms) and is_integer(later.beam_cpu_ms),
        do: %{
          beam_cpu_percent:
            Float.round((later.beam_cpu_ms - before.beam_cpu_ms) / seconds / 10, 3)
        },
        else: %{}

    %{seconds: Float.round(seconds, 1)} |> Map.merge(rates) |> Map.merge(cpu)
  end

  defp describe(activity) do
    [
      {:poll_calls_per_s, "polls/s"},
      {:native_calls_per_s, "native calls/s"},
      {:timer_generation_per_s, "timer re-arms/s"},
      {:ingress_packets_per_s, "packets in/s"},
      {:emitted_packets_per_s, "packets out/s"},
      {:beam_cpu_percent, "% of a core"}
    ]
    |> Enum.filter(fn {key, _label} -> Map.has_key?(activity, key) end)
    |> Enum.map_join(", ", fn {key, label} -> "#{activity[key]} #{label}" end)
  end

  # The VM's user and system CPU time, from /proc; nil elsewhere.
  defp cpu_ms do
    with {:ok, stat} <- File.read("/proc/self/stat"),
         [_head, tail] <- String.split(stat, ") ", parts: 2),
         fields = String.split(tail, " "),
         {utime, ""} <- Integer.parse(Enum.at(fields, 11, "")),
         {stime, ""} <- Integer.parse(Enum.at(fields, 12, "")) do
      # Linux counts in ticks of 1/100 s (USER_HZ).
      (utime + stime) * 10
    else
      _unavailable -> nil
    end
  end

  # Keeps the sampled counters equal to the stack's, for metrics.csv.
  defp sample_loop(context, last) do
    Process.sleep(@sample_interval)
    latest = snapshot(context)

    Enum.each(@sampled, fn key ->
      if is_integer(latest[key]) do
        Soak.count(context, key, latest[key] - (last[key] || 0))
      end
    end)

    sample_loop(context, Map.merge(last, Map.take(latest, @sampled)))
  end

  defp stop_sampler(sampler) do
    Process.unlink(sampler)
    Process.exit(sampler, :kill)
  end

  # The verdict's results

  defp summarize(context, table) do
    rows = table |> :ets.tab2list() |> Enum.sort() |> Enum.map(fn {_index, row} -> row end)

    vanished = Enum.filter(rows, &(&1.role in @vanishings))
    Soak.record(context, :detection, vanished)

    summary =
      vanished
      |> Enum.group_by(&{&1.role, &1.transport})
      |> Enum.sort()
      |> Enum.map(fn {{role, transport}, group} -> detection_summary(role, transport, group) end)

    Soak.record(context, :detection_summary, summary)
    Enum.each(summary, &Soak.note(context, describe_detection(&1)))

    working =
      rows
      |> Enum.filter(&(&1.role in @behaviours))
      |> Enum.group_by(& &1.role)
      |> Enum.sort()
      |> Enum.map(fn {role, group} ->
        %{
          role: role,
          connections: length(group),
          exchanges: group |> Enum.map(& &1.exchanges) |> Enum.sum(),
          bytes: group |> Enum.map(& &1.bytes) |> Enum.sum()
        }
      end)

    Soak.record(context, :working, working)
  end

  defp detection_summary(role, transport, group) do
    times = for %{outcome: :detected, after_ms: ms} <- group, do: ms
    waits = for %{outcome: :undetected, after_ms: ms} <- group, do: ms

    %{
      role: role,
      transport: transport,
      connections: length(group),
      detected: length(times),
      undetected: length(waits),
      skipped: Enum.count(group, &(&1.outcome == :skipped)),
      answered: Enum.count(group, &(&1.outcome == :answered)),
      detected_ms: spread_of(times),
      undetected_after_ms: spread_of(waits),
      after_lift_ms: spread_of(for %{outcome: :detected, after_lift_ms: ms} <- group, do: ms),
      errors: group |> Enum.map(&Map.get(&1, :error)) |> Enum.reject(&is_nil/1) |> Enum.uniq()
    }
  end

  defp spread_of([]), do: nil

  defp spread_of(values) do
    sorted = Enum.sort(values)
    %{min: hd(sorted), median: Enum.at(sorted, div(length(sorted), 2)), max: List.last(sorted)}
  end

  defp describe_detection(summary) do
    counts =
      "#{summary.detected} of #{summary.connections} detected" <>
        if(summary.skipped > 0, do: ", #{summary.skipped} skipped", else: "") <>
        if(summary.answered > 0, do: ", #{summary.answered} answered", else: "")

    times =
      [
        {summary.detected_ms, "detected in"},
        {summary.after_lift_ms, "after the path returned"},
        {summary.undetected_after_ms, "undetected after"}
      ]
      |> Enum.reject(fn {spread, _label} -> spread == nil end)
      |> Enum.map(fn {spread, label} -> "#{label} #{describe_spread(spread)}" end)

    errors = if summary.errors == [], do: [], else: ["errors #{Enum.join(summary.errors, ", ")}"]

    "vanished #{summary.role} (#{summary.transport}): " <>
      Enum.join([counts | times ++ errors], "; ")
  end

  defp describe_spread(%{min: low, median: median, max: high}) do
    "#{seconds(low)} to #{seconds(high)} s (median #{seconds(median)} s)"
  end

  defp seconds(ms), do: Float.round(ms / 1_000, 1)

  # Sockets, by transport

  # A connection's state carries its spec, and a peer's its transport.
  defp send_data(%{spec: %{transport: transport}, socket: socket}, data),
    do: send_data(%{transport: transport, socket: socket}, data)

  defp send_data(%{transport: :tcp, socket: socket}, data), do: :gen_tcp.send(socket, data)
  defp send_data(%{transport: :tls, socket: socket}, data), do: :ssl.send(socket, data)

  defp recv(%{spec: %{transport: transport}, socket: socket}, length, timeout),
    do: recv(transport, socket, length, timeout)

  defp recv(:tcp, socket, length, timeout), do: :gen_tcp.recv(socket, length, timeout)
  defp recv(:tls, socket, length, timeout), do: :ssl.recv(socket, length, timeout)

  defp set_active(%{transport: :tcp, socket: socket}), do: :inet.setopts(socket, active: :once)
  defp set_active(%{transport: :tls, socket: socket}), do: :ssl.setopts(socket, active: :once)

  defp sockname(:tcp, socket), do: :inet.sockname(socket)
  defp sockname(:tls, socket), do: :ssl.sockname(socket)

  defp controlling_process(:tcp, socket, pid), do: :gen_tcp.controlling_process(socket, pid)
  defp controlling_process(:tls, socket, pid), do: :ssl.controlling_process(socket, pid)

  defp close(:tcp, socket), do: :gen_tcp.close(socket)
  defp close(:tls, socket), do: :ssl.close(socket)

  # Time

  defp now, do: System.monotonic_time(:millisecond)

  defp sleep_until(at), do: Process.sleep(max(0, at - now()))

  # A random duration between `low` and `high`, uniform in its logarithm, so
  # that seconds and hours are both common.
  defp log_uniform({low, high}) when low >= high, do: low
  defp log_uniform({low, high}), do: trunc(low * :math.pow(high / low, :rand.uniform()))
end
