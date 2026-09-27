defmodule SmolNet.Integration.Scenarios.Tls do
  @moduledoc """
  The TLS scenario: bulk HTTPS transfers over `:ssl` on SmolNet, checked
  end to end.

  TLS is the cheapest end-to-end integrity check there is: a corrupted,
  lost or reordered byte fails a record's MAC, and the connection with an
  alert. On top of that, every body's length and SHA-256 are checked.

  Every round, for each family, each over fresh TLS connections:

    * one stream downloads `--down-bytes`, and one uploads `--up-bytes`;
    * `--concurrency` streams download the same total between them, and
      then upload it.

  Receives alternate between `:ssl`'s passive (`recv`) and active
  (`{:active, N}`) modes. Rounds repeat `--round-pause` apart for the run's
  duration, and then the stack's sockets must all be released.

  SmolNet's sockets get `--socket-buffer` receive and send buffers, 256 KiB
  by default, so that its windows exceed 64 KiB and need window scaling; at
  SmolNet's default of 64 KiB, a 100 ms path caps a stream near 5 Mbit/s.
  The kernel's buffers tune themselves.

  `--target internet`, the default, transfers to `speed.cloudflare.com`,
  whose `/__down` and `/__up` exist for speed tests: a download is that many
  ASCII zeros, and an upload's length comes back in `cf-meta-upload-bytes`.
  Over IPv6, each round also fetches the page of `ipv6.google.com`, a host
  with no IPv4 address. SmolNet reaches both through the host's NAT, so a
  family the host itself cannot reach is skipped, with a note. In SmolNet
  mode each phase is repeated over the kernel's stack to the same address,
  unless `--no-compare`, so that throughput is reported next to the
  kernel's on the same path.

  `--target local` serves the same transfers from this host, with
  certificates made for the run: one HTTPS server on the peer (the kernel,
  through the device) for SmolNet's clients, and one on SmolNet for the
  peer's, so that SmolNet is a TLS server too. It needs no internet, and
  runs in every mode, `--self-check` and `--baseline` included.

  Throughput is noted at the end and recorded as the `throughput` result in
  `verdict.json`; for several streams it is their total over the phase's
  time. See `SmolNet.Integration.Tls` for how `:ssl` runs over SmolNet.
  """

  alias SmolNet.Integration.Https
  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak
  alias SmolNet.Integration.Tls

  @speed_host "speed.cloudflare.com"
  @v6_only_host "ipv6.google.com"
  @local_host "smolnet.test"
  @https_port 443
  @connect_timeout 30_000
  @preflight_timeout 5_000
  # SmolNet's SYN is retried 1, 2 and 4 s after the first.
  @forward_timeout 10_000
  @accept_poll 250

  @doc "Returns the scenario's runner config."
  @spec config() :: keyword()
  def config do
    [
      name: "tls",
      default_duration: "10m",
      default_concurrency: 4,
      switches: [
        target: :string,
        down_bytes: :integer,
        up_bytes: :integer,
        round_pause: :integer,
        transfer_timeout: :integer,
        socket_buffer: :integer,
        compare: :boolean
      ],
      defaults: [
        target: "internet",
        down_bytes: 4_194_304,
        up_bytes: 1_048_576,
        round_pause: 60_000,
        transfer_timeout: 120_000,
        socket_buffer: 262_144,
        compare: true
      ],
      counters: [:rounds, :transfers, :bytes, :baseline_transfers],
      # Every transfer is a connection, and a socket that closes first holds
      # its slot for TIME-WAIT.
      stack: [limits: %{sockets: 256}],
      usage: """

      tls options:
        --target T            internet (speed.cloudflare.com) or local (default internet)
        --down-bytes N        each round's download, in bytes (default 4194304)
        --up-bytes N          each round's upload, in bytes (default 1048576)
        --round-pause MS      the pause between rounds (default 60000)
        --transfer-timeout MS each transfer's deadline, handshake included (default 120000)
        --socket-buffer N     SmolNet's receive and send buffers, in bytes, 1024 to 1048576,
                              or 0 for its default of 65536 (default 262144)
        --no-compare          do not repeat each phase over the kernel's stack

      --concurrency sets the streams of the multi-stream phases (default 4).
      """
    ]
  end

  @doc "Runs the scenario's workload."
  @spec run(SmolNet.Integration.Soak.Context.t()) :: :ok
  def run(context) do
    {:ok, _started} = Application.ensure_all_started(:ssl)

    case settings(context) do
      {:ok, settings} ->
        table = :ets.new(:tls_phases, [:public, :duplicate_bag])
        settings = Map.put(settings, :table, table)

        case settings.target do
          :internet -> internet(context, settings)
          :local -> local(context, settings)
        end

        summarize(context, table)
        Soak.await_socket_count(context, 0)

      {:error, message} ->
        Soak.fail(context, :usage, message)
    end
  end

  defp settings(context) do
    extra = context.extra

    with {:ok, target} <- target(extra.target, context.mode),
         :ok <- positive(:down_bytes, extra.down_bytes),
         :ok <- positive(:up_bytes, extra.up_bytes),
         :ok <- positive(:transfer_timeout, extra.transfer_timeout),
         :ok <- non_negative(:round_pause, extra.round_pause),
         :ok <- buffer(extra.socket_buffer) do
      {:ok,
       %{
         target: target,
         down_bytes: extra.down_bytes,
         up_bytes: extra.up_bytes,
         round_pause: extra.round_pause,
         transfer_timeout: extra.transfer_timeout,
         buffer: if(extra.socket_buffer == 0, do: nil, else: extra.socket_buffer),
         compare: extra.compare and context.mode == :smolnet and target == :internet
       }}
    end
  end

  defp target("internet", :self_check),
    do: {:error, "the helper's loopback has no route to the internet; use --target local"}

  defp target("internet", _mode), do: {:ok, :internet}
  defp target("local", _mode), do: {:ok, :local}

  defp target(other, _mode),
    do: {:error, "--target must be internet or local, got #{inspect(other)}"}

  defp positive(_name, value) when is_integer(value) and value > 0, do: :ok
  defp positive(name, value), do: {:error, "--#{dasherize(name)} must be positive, got #{value}"}

  defp buffer(bytes) when bytes == 0 or bytes in 1_024..1_048_576, do: :ok
  defp buffer(bytes), do: {:error, "--socket-buffer must be 0 or 1024 to 1048576, got #{bytes}"}

  defp non_negative(_name, value) when is_integer(value) and value >= 0, do: :ok

  defp non_negative(name, value),
    do: {:error, "--#{dasherize(name)} must not be negative, got #{value}"}

  defp dasherize(name), do: name |> to_string() |> String.replace("_", "-")

  # The internet target

  defp internet(context, settings) do
    families = Enum.filter(context.families, &reachable?(context, @speed_host, &1))
    v6_only? = :inet6 in families and reachable?(context, @v6_only_host, :inet6)

    cond do
      families == [] ->
        reason = "this host reaches #{@speed_host} over none of #{inspect(context.families)}"
        Soak.abandon(context, reason)

      not Enum.all?(families, &forwarded?(context, &1)) ->
        :ok

      true ->
        round = fn -> internet_round(context, settings, families, v6_only?) end
        Soak.loop(context, round, pause: settings.round_pause)
    end
  end

  defp internet_round(context, settings, families, v6_only?) do
    round = Soak.count(context, :rounds)

    Enum.each(families, fn family ->
      host_phases = remote_phases(context, settings, family, @speed_host, round)

      page_phases =
        if family == :inet6 and v6_only?,
          do: remote_phases(context, settings, family, @v6_only_host, round),
          else: []

      run_phases(context, settings, host_phases ++ page_phases)
    end)
  end

  # Whether the host's own stack reaches `host` over `family`: SmolNet's
  # traffic leaves through the host, so without that there is nothing to
  # test.
  defp reachable?(context, host, family) do
    result =
      with {:ok, address} <- :inet.getaddr(String.to_charlist(host), family),
           {:ok, socket} <-
             :gen_tcp.connect(address, @https_port, [family], @preflight_timeout) do
        :gen_tcp.close(socket)
      end

    case result do
      :ok ->
        true

      {:error, reason} ->
        Soak.note(
          context,
          "skipping #{host} over #{family}: this host cannot reach it (#{inspect(reason)})"
        )

        false
    end
  end

  # Whether SmolNet reaches the target at all, before the run counts on it:
  # the host reaches it, so if SmolNet cannot, the host is almost surely not
  # forwarding or masquerading the device's traffic, which fails every
  # transfer on its first connect's timeout. That abandons the run.
  defp forwarded?(%{mode: :smolnet} = context, family) do
    options = Network.tcp_options(context, :subject, family) ++ [:binary, active: false]

    result =
      with {:ok, address} <- :inet.getaddr(String.to_charlist(@speed_host), family),
           {:ok, socket} <- :gen_tcp.connect(address, @https_port, options, @forward_timeout) do
        :gen_tcp.close(socket)
      end

    case result do
      :ok ->
        true

      {:error, reason} ->
        Soak.abandon(
          context,
          "SmolNet cannot reach #{@speed_host} over #{family} through this host " <>
            "(#{inspect(reason)}), though the host itself can: the host is not forwarding " <>
            "or masquerading #{context.device}'s traffic; see integration/setup.sh"
        )

        false
    end
  end

  defp forwarded?(_context, _family), do: true

  defp remote_phases(context, settings, family, host, round) do
    case resolve(host, family) do
      {:ok, address} ->
        clients = if settings.compare, do: [:subject, :peer], else: [:subject]
        tls = remote_tls_options(host)
        server = %{label: host, host: host, address: address, port: @https_port, tls: tls}
        kinds = if host == @speed_host, do: bulk_kinds(context, settings), else: [{:page, 1, 0}]

        for kind <- kinds, client <- clients do
          phase(context, family, round, kind, client, server, :remote)
        end

      {:error, reason} ->
        Soak.fail(context, :dns, "could not resolve #{host} over #{family}", [
          {"reason", reason}
        ])

        []
    end
  end

  # Resolution is the host's, not SmolNet's; one retry rides out a blip.
  defp resolve(host, family) do
    case :inet.getaddr(String.to_charlist(host), family) do
      {:ok, address} ->
        {:ok, address}

      {:error, _reason} ->
        Process.sleep(1_000)
        :inet.getaddr(String.to_charlist(host), family)
    end
  end

  defp remote_tls_options(host) do
    [
      verify: :verify_peer,
      cacerts: :public_key.cacerts_get(),
      server_name_indication: String.to_charlist(host),
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)],
      alpn_advertised_protocols: ["http/1.1"],
      active: false,
      mode: :binary
    ]
  end

  # The local target

  defp local(context, settings) do
    {server_tls, client_tls} = local_tls_options()

    servers =
      for family <- context.families, role <- [:peer, :subject] do
        start_server(context, settings, family, role, server_tls)
      end

    if Enum.all?(servers, &match?({:ok, _server}, &1)) do
      servers = Enum.map(servers, fn {:ok, server} -> server end)

      round = fn -> local_round(context, settings, servers, client_tls) end
      Soak.loop(context, round, pause: settings.round_pause)
    end

    Enum.each(servers, fn
      {:ok, server} -> stop_server(server)
      {:error, _reason} -> :ok
    end)
  end

  defp local_round(context, settings, servers, client_tls) do
    round = Soak.count(context, :rounds)

    Enum.each(context.families, fn family ->
      phases = local_phases(context, settings, servers, family, round, client_tls)
      run_phases(context, settings, phases)
    end)
  end

  defp local_phases(context, settings, servers, family, round, client_tls) do
    for %{family: ^family} = server <- servers,
        kind <- bulk_kinds(context, settings) do
      client = if server.role == :peer, do: :subject, else: :peer

      target = %{
        label: stack_label(context, server.role),
        host: @local_host,
        address: Network.address(context, server.role, family),
        port: server.port,
        tls: client_tls
      }

      phase(context, family, round, kind, client, target, :local)
    end
  end

  defp local_tls_options do
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

  # A server listens with `:ssl.listen/2` and accepts with
  # `:ssl.transport_accept/2`; each connection's handler then runs the
  # handshake.
  defp start_server(context, settings, family, role, tls) do
    parent = self()
    server = %{family: family, role: role, tls: tls}
    pid = spawn_link(fn -> listen(parent, context, settings, server) end)

    receive do
      {^pid, {:ok, port}} ->
        {:ok, Map.merge(server, %{pid: pid, port: port})}

      {^pid, {:error, reason}} ->
        summary = "could not listen on #{stack_label(context, role)} over #{family}"
        Soak.fail(context, :listen, summary, [{"reason", reason}])
        {:error, reason}
    end
  end

  defp listen(parent, context, settings, server) do
    options = [buffer: settings.buffer]

    case Tls.listen(context, server.role, server.family, server.tls, options) do
      {:ok, listener} ->
        {:ok, {_address, port}} = :ssl.sockname(listener)
        send(parent, {self(), {:ok, port}})
        accept(context, settings, Map.put(server, :listener, listener), 0)

      {:error, _reason} = error ->
        send(parent, {self(), error})
    end
  end

  defp accept(context, settings, server, served) do
    receive do
      :stop -> :ssl.close(server.listener)
    after
      0 ->
        case :ssl.transport_accept(server.listener, @accept_poll) do
          {:ok, socket} ->
            mode = if rem(served, 2) == 0, do: :passive, else: :active
            handler = spawn(fn -> handle(context, settings, server, mode) end)

            case :ssl.controlling_process(socket, handler) do
              :ok ->
                send(handler, {:socket, socket})
                accept(context, settings, server, served + 1)

              {:error, reason} ->
                Process.exit(handler, :kill)
                :ssl.close(socket)
                summary = "could not hand an accepted connection to its handler"
                Soak.fail(context, :accept, summary, [{"reason", reason}])
            end

          {:error, :timeout} ->
            accept(context, settings, server, served)

          {:error, reason} ->
            label = stack_label(context, server.role)
            summary = "the #{label} server over #{server.family} could not accept"
            Soak.fail(context, :accept, summary, [{"reason", reason}])
        end
    end
  end

  defp handle(context, settings, server, mode) do
    label = stack_label(context, server.role)
    name = {:serve, server.family, label}

    result =
      receive do
        {:socket, socket} ->
          Soak.within(context, name, settings.transfer_timeout, fn ->
            serve(socket, mode)
          end)
      end

    case result do
      {:ok, _served} ->
        :ok

      {:error, reason} ->
        summary = "the #{label} server over #{server.family} failed a request (#{mode})"
        Soak.fail(context, failure_kind(reason, :server), summary, [{"reason", reason}])
    end
  catch
    kind, reason ->
      summary = "a #{stack_label(context, server.role)} server's handler crashed"
      Soak.fail(context, :server_crashed, summary, [{"reason", {kind, reason, __STACKTRACE__}}])
  end

  defp serve(socket, mode) do
    case :ssl.handshake(socket, :infinity) do
      {:ok, tls} ->
        try do
          Https.serve(tls, mode)
        after
          :ssl.close(tls)
        end

      {:error, _reason} = error ->
        :ssl.close(socket)
        error
    end
  end

  defp stop_server(server) do
    monitor = Process.monitor(server.pid)
    send(server.pid, :stop)

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end
  end

  # Phases

  defp bulk_kinds(context, settings) do
    streams = context.concurrency
    single = [{:down, 1, settings.down_bytes}, {:up, 1, settings.up_bytes}]
    multi = [{:down, streams, settings.down_bytes}, {:up, streams, settings.up_bytes}]
    if streams == 1, do: single, else: single ++ multi
  end

  defp phase(context, family, round, {direction, streams, bytes}, client, server, target) do
    name = if direction == :page, do: "page", else: "#{direction} x#{streams}"

    %{
      name: name,
      family: family,
      round: round,
      direction: direction,
      streams: streams,
      bytes: bytes,
      client: client,
      client_label: stack_label(context, client),
      server: server,
      target: target
    }
  end

  defp run_phases(context, settings, phases) do
    Enum.each(phases, fn phase ->
      if continue?(context, phase), do: run_phase(context, settings, phase)
    end)
  end

  # A round on a slow path can take a while, so after the first, which runs
  # whole, a round stops when the run does.
  defp continue?(context, %{round: 1}), do: not Soak.failed?(context)
  defp continue?(context, _phase), do: Soak.running?(context)

  defp run_phase(context, settings, phase) do
    started = now()

    results =
      1..phase.streams
      |> Enum.map(fn index -> Task.async(fn -> transfer(context, settings, phase, index) end) end)
      |> Task.await_many(:infinity)

    elapsed = now() - started

    case Enum.find(results, &match?({:error, _kind, _summary, _details}, &1)) do
      nil ->
        measured = Enum.map(results, fn {:ok, measured} -> measured end)
        record_phase(context, settings, phase, measured, elapsed)

      {:error, kind, summary, details} ->
        Soak.fail(context, kind, summary, details)
    end
  end

  defp transfer(context, settings, phase, index) do
    mode = if rem(phase.round + index, 2) == 0, do: :passive, else: :active
    bytes = stream_bytes(phase, index)
    label = "#{phase.family} #{phase.name} ##{index}, #{describe(phase)}, #{mode}"
    name = {phase.name, phase.family, phase.client_label, phase.server.label, index}

    Soak.within(context, name, settings.transfer_timeout, fn ->
      started = now()
      server = phase.server
      options = [timeout: @connect_timeout, buffer: settings.buffer]

      connected =
        Tls.connect(context, phase.client, server.address, server.port, server.tls, options)

      handshake = now() - started

      outcome =
        case connected do
          {:ok, socket} -> transfer_over(socket, phase, bytes, mode)
          {:error, reason} -> {:error, failure_kind(reason, :connect), [{"reason", reason}]}
        end

      case outcome do
        :ok ->
          {:ok, %{bytes: bytes, handshake_us: handshake}}

        {:error, kind, details} ->
          {:error, kind, "#{label}: #{summary(kind)}", details ++ [{"server", server.address}]}
      end
    end)
  end

  defp transfer_over(socket, phase, bytes, mode) do
    result =
      try do
        exchange(socket, phase, bytes, mode)
      after
        :ssl.close(socket)
      end

    check(result, phase, bytes)
  end

  defp stream_bytes(phase, index) do
    share = div(phase.bytes, phase.streams)
    if index <= rem(phase.bytes, phase.streams), do: share + 1, else: share
  end

  defp exchange(socket, %{direction: :page} = phase, _bytes, mode) do
    {Https.get(socket, phase.server.host, "/", mode), nil}
  end

  defp exchange(socket, %{direction: :down} = phase, bytes, mode) do
    path = if phase.target == :remote, do: "/__down?bytes=#{bytes}", else: "/down?bytes=#{bytes}"
    {Https.get(socket, phase.server.host, path, mode), nil}
  end

  defp exchange(socket, %{direction: :up} = phase, bytes, mode) do
    body = :crypto.strong_rand_bytes(bytes)
    path = if phase.target == :remote, do: "/__up", else: "/up"
    {Https.post(socket, phase.server.host, path, body, mode), Https.sha256(body)}
  end

  defp check({{:error, reason}, _sent}, _phase, _bytes) do
    {:error, failure_kind(reason, :transfer), [{"reason", reason}]}
  end

  defp check({{:ok, %{status: status} = response}, _sent}, %{direction: :page}, _bytes) do
    if status in 200..399,
      do: :ok,
      else: {:error, :http, [{"response", response}]}
  end

  defp check({{:ok, %{status: status} = response}, _sent}, _phase, _bytes) when status != 200 do
    {:error, :http, [{"response", response}]}
  end

  defp check({{:ok, response}, nil}, %{direction: :down} = phase, bytes) do
    expected =
      if phase.target == :remote,
        do: Https.sha256(:binary.copy("0", bytes)),
        else: response.headers["x-sha256"]

    if response.bytes == bytes and response.sha256 == expected do
      :ok
    else
      {:error, :integrity,
       [{"expected", %{bytes: bytes, sha256: expected}}, {"response", response}]}
    end
  end

  # Cloudflare reports the length it received, not a hash; TLS vouches for
  # the content.
  defp check({{:ok, response}, _sent}, %{direction: :up, target: :remote}, bytes) do
    reported = response.headers["cf-meta-upload-bytes"]

    if reported in [nil, Integer.to_string(bytes)] do
      :ok
    else
      {:error, :integrity,
       [{"sent", %{bytes: bytes}}, {"reported", reported}, {"response", response}]}
    end
  end

  defp check({{:ok, response}, sent}, %{direction: :up, target: :local}, bytes) do
    received = %{bytes: response.headers["x-upload-bytes"], sha256: response.headers["x-sha256"]}

    if received == %{bytes: Integer.to_string(bytes), sha256: sent} do
      :ok
    else
      {:error, :integrity,
       [{"sent", %{bytes: bytes, sha256: sent}}, {"received", received}, {"response", response}]}
    end
  end

  defp failure_kind({:tls_alert, _alert}, _default), do: :tls_alert
  defp failure_kind({{:tls_alert, _alert}, _progress}, _default), do: :tls_alert
  defp failure_kind(_reason, default), do: default

  defp summary(:tls_alert), do: "TLS alert"
  defp summary(:connect), do: "could not connect"
  defp summary(:transfer), do: "the transfer failed"
  defp summary(:http), do: "unexpected HTTP status"
  defp summary(:integrity), do: "the body's length or SHA-256 is wrong"

  defp describe(phase), do: "#{phase.client_label} client to #{phase.server.label}"

  # In self-check and baseline modes both roles are the same stack, so the
  # peer's is named apart.
  defp stack_label(context, role) do
    stack = if Network.smolnet?(context, role), do: "smolnet", else: "kernel"
    if role == :peer and context.mode != :smolnet, do: stack <> " peer", else: stack
  end

  defp now, do: System.monotonic_time(:microsecond)

  # Throughput

  defp record_phase(context, settings, phase, measured, elapsed) do
    total = Enum.sum_by(measured, & &1.bytes)

    if phase.client == :peer and phase.target == :remote do
      Soak.count(context, :baseline_transfers, phase.streams)
    else
      Soak.count(context, :transfers, phase.streams)
      Soak.count(context, :bytes, total)
    end

    handshake = median(Enum.map(measured, & &1.handshake_us))
    key = {phase.family, phase.name, phase.client_label, phase.server.label}

    :ets.insert(
      settings.table,
      {key, %{bytes: total, elapsed_us: elapsed, handshake_us: handshake}}
    )
  end

  defp summarize(context, table) do
    rows =
      table
      |> :ets.tab2list()
      |> Enum.group_by(fn {key, _run} -> key end, fn {_key, run} -> run end)
      |> Enum.map(fn {key, runs} -> row(key, runs) end)
      |> Enum.sort_by(&{&1.family, &1.phase, &1.server, &1.client != "smolnet"})
      |> with_ratios()

    Soak.record(context, :throughput, rows)
    Enum.each(rows, &Soak.note(context, describe_row(&1)))
  end

  defp row({family, phase, client, server}, runs) do
    # Bytes per microsecond, times 8, is megabits per second.
    rates = Enum.map(runs, &(&1.bytes * 8 / max(&1.elapsed_us, 1)))

    %{
      family: family,
      phase: phase,
      client: client,
      server: server,
      runs: length(runs),
      bytes: Enum.sum_by(runs, & &1.bytes),
      median_mbit_s: Float.round(median(rates), 2),
      min_mbit_s: Float.round(Enum.min(rates), 2),
      max_mbit_s: Float.round(Enum.max(rates), 2),
      median_handshake_ms: Float.round(median(Enum.map(runs, & &1.handshake_us)) / 1_000, 1)
    }
  end

  # A SmolNet client's median as a fraction of the kernel's, on the same
  # phase and server.
  defp with_ratios(rows) do
    kernel =
      for %{client: "kernel"} = row <- rows,
          into: %{},
          do: {{row.family, row.phase, row.server}, row.median_mbit_s}

    Enum.map(rows, fn row ->
      case {row.client, kernel[{row.family, row.phase, row.server}]} do
        {"smolnet", baseline} when is_float(baseline) and baseline > 0 ->
          Map.put(row, :ratio_to_kernel, Float.round(row.median_mbit_s / baseline, 3))

        _other ->
          row
      end
    end)
  end

  defp describe_row(row) do
    ratio = if row[:ratio_to_kernel], do: ", #{row.ratio_to_kernel}x the kernel's", else: ""

    "throughput #{row.family} #{row.phase}, #{row.client} client to #{row.server}: " <>
      "median #{row.median_mbit_s} Mbit/s#{ratio} (#{row.min_mbit_s}-#{row.max_mbit_s} " <>
      "over #{row.runs} runs), handshake #{row.median_handshake_ms} ms"
  end

  defp median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1,
      do: Enum.at(sorted, middle) * 1.0,
      else: (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
  end
end
