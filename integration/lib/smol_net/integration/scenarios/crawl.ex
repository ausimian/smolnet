defmodule SmolNet.Integration.Scenarios.Crawl do
  @moduledoc """
  The crawl scenario: `HEAD /` over TLS to the most popular sites, over
  SmolNet and over the kernel, host by host, to meet the TCP peers the
  rest of the harness never does: every MSS, window scale, SACK and
  timestamp choice, every way of resetting and closing, and the
  middleboxes in front of them.

  Each round visits every host of the list (see
  `SmolNet.Integration.Crawl.Hosts`) once per family and once per stack,
  SmolNet's visit and the kernel's back to back, to the same address, in
  an order that alternates between rounds. `--concurrency` hosts are
  visited at once, 16 by default, so at most that many connections are in
  flight. A visit connects, handshakes and asks for `HEAD /`, each stage
  within `--host-timeout`, and is never retried; its outcome is classified
  as `SmolNet.Integration.Crawl.Probe` describes. Rounds repeat
  `--round-pause` apart, and a round that the run's end catches stops
  visiting, except that `--duration 0` visits the whole list once.

  After each round the stack's sockets must return to their baseline, and
  the round is compared host by host: a host that failed over both stacks
  is down or flaky, never a SmolNet fault, and one that failed over
  SmolNet alone is listed with its outcomes and, where the run has a
  capture, the evidence `SmolNet.Integration.Crawl.Evidence` cuts from it.
  Every visit is a line of `hosts.csv`, and the verdict's results hold
  the outcome counts per stack, the rounds, the hosts that failed over
  SmolNet alone, grouped by cause, and the list's Tranco ID.

  Before the first round, SmolNet's socket ceiling is pushed past, over
  every family, against a listener on the host, never an internet host;
  see `SmolNet.Integration.Crawl.Ceiling`.

  `--target local` crawls servers on the peer instead, made to succeed,
  stay silent, hang up and refuse, which needs no internet and runs in
  every mode. The run fails on a deadline, a socket leak, a ceiling that
  misbehaves, or more than `--max-smolnet-only` hosts that SmolNet never
  reached though the kernel did in at least two rounds and half of them:
  hosts that fail over SmolNet persistently, not a flaky one.
  """

  alias SmolNet.Integration.Crawl.Ceiling
  alias SmolNet.Integration.Crawl.Evidence
  alias SmolNet.Integration.Crawl.Hosts
  alias SmolNet.Integration.Crawl.Probe
  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak
  alias SmolNet.Integration.Tls

  @https_port 443
  @dns_timeout 5_000
  @preflight_hosts 10
  @preflight_timeout 5_000
  # SmolNet's SYN is retried 1, 2 and 4 s after the first.
  @forward_timeout 10_000
  @max_internet_concurrency 32
  @local_domain "crawl.smolnet.test"
  @local_kinds [:ok, :silent, :hangup, :refused]
  @accept_poll 250
  @max_listed 200
  # Unspecified, private, shared, loopback, link-local, benchmarking and
  # multicast or reserved IPv4; unspecified, loopback, IPv4-mapped, unique
  # local, link-local and multicast IPv6.
  @not_global [
    {{0, 0, 0, 0}, 8},
    {{10, 0, 0, 0}, 8},
    {{100, 64, 0, 0}, 10},
    {{127, 0, 0, 0}, 8},
    {{169, 254, 0, 0}, 16},
    {{172, 16, 0, 0}, 12},
    {{192, 168, 0, 0}, 16},
    {{198, 18, 0, 0}, 15},
    {{224, 0, 0, 0}, 3},
    {{0, 0, 0, 0, 0, 0, 0, 0}, 127},
    {{0, 0, 0, 0, 0, 0xFFFF, 0, 0}, 96},
    {{0xFC00, 0, 0, 0, 0, 0, 0, 0}, 7},
    {{0xFE80, 0, 0, 0, 0, 0, 0, 0}, 10},
    {{0xFF00, 0, 0, 0, 0, 0, 0, 0}, 8}
  ]

  @doc "Returns the scenario's runner config."
  @spec config() :: keyword()
  def config do
    [
      name: "crawl",
      default_duration: "10m",
      default_concurrency: 16,
      switches: [
        target: :string,
        list: :string,
        top: :integer,
        round_pause: :integer,
        host_timeout: :integer,
        compare: :boolean,
        ceiling: :boolean,
        ceiling_step: :integer,
        sockets: :integer,
        socket_buffer: :integer,
        evidence: :integer,
        max_smolnet_only: :integer
      ],
      defaults: [
        target: "internet",
        list: "tranco",
        top: 1_000,
        round_pause: 300_000,
        host_timeout: 10_000,
        compare: true,
        ceiling: true,
        ceiling_step: 15_000,
        sockets: 512,
        socket_buffer: 0,
        evidence: 25,
        max_smolnet_only: -1
      ],
      counters: [
        :rounds,
        :visits,
        :unresolved,
        :smolnet_ok,
        :kernel_ok,
        :smolnet_only,
        :kernel_only,
        :both_failed,
        :ceiling_system_limit
      ],
      # With 512 slots, the 128 MiB buffer cap is the ceiling: 256 TCP
      # sockets at SmolNet's default buffers.
      stack: fn extra -> [limits: %{sockets: extra.sockets}] end,
      usage: """

      crawl options:
        --target T            internet (the host list) or local (servers on this host) (default internet)
        --list L              tranco (the latest list), tranco:<ID>, fixture or a file (default tranco)
        --top N               how many of the list's hosts to visit, at most 10000 (default 1000)
        --round-pause MS      the pause between rounds (default 300000)
        --host-timeout MS     each visit's connect, handshake and response timeout (default 10000)
        --no-compare          do not visit each host over the kernel's stack too
        --no-ceiling          do not push the socket ceiling before the first round
        --ceiling-step MS     how long each churn rate of the ceiling runs (default 15000)
        --sockets N           the stack's socket limit, 1 to 512 (default 512)
        --socket-buffer N     SmolNet's receive and send buffers, 1024 to 1048576, or 0 for
                              its default of 262144 (default 0)
        --evidence N          how many hosts that fail over SmolNet alone to cut captures
                              for (default 25)
        --max-smolnet-only N  fail if more hosts than this failed over SmolNet persistently:
                              never reached by SmolNet, and by the kernel in at least two
                              rounds and half of them; -1 never to fail (default -1)

      --concurrency sets how many hosts are visited at once, at most 32 for the
      internet (default 16).
      """
    ]
  end

  @doc "Runs the scenario's workload."
  @spec run(SmolNet.Integration.Soak.Context.t()) :: :ok
  def run(context) do
    {:ok, _started} = Application.ensure_all_started(:ssl)

    case settings(context) do
      {:ok, settings} ->
        table = :ets.new(:crawl, [:public, :set])
        settings = Map.put(settings, :table, table)

        case settings.target do
          :internet -> internet(context, settings)
          :local -> local(context, settings)
        end

      {:error, message} ->
        Soak.fail(context, :usage, message)
    end
  end

  defp settings(context) do
    extra = context.extra

    with {:ok, target} <- target(extra.target, context.mode),
         :ok <- within_range(:top, extra.top, 1..Hosts.max_top()),
         :ok <- within_range(:round_pause, extra.round_pause, 0..86_400_000),
         :ok <- within_range(:host_timeout, extra.host_timeout, 100..120_000),
         :ok <- within_range(:ceiling_step, extra.ceiling_step, 1_000..600_000),
         :ok <- within_range(:sockets, extra.sockets, 1..512),
         :ok <- within_range(:evidence, extra.evidence, 0..1_000),
         :ok <- within_range(:max_smolnet_only, extra.max_smolnet_only, -1..100_000),
         :ok <- buffer(extra.socket_buffer),
         :ok <- concurrency(context.concurrency, target) do
      {:ok,
       %{
         target: target,
         list: extra.list,
         top: extra.top,
         round_pause: extra.round_pause,
         host_timeout: extra.host_timeout,
         # A connect, a handshake and a response, each within the timeout.
         deadline: 3 * extra.host_timeout + 5_000,
         compare: extra.compare and context.mode != :kernel,
         ceiling: extra.ceiling,
         ceiling_step: extra.ceiling_step,
         buffer: if(extra.socket_buffer == 0, do: nil, else: extra.socket_buffer),
         evidence: extra.evidence,
         max_smolnet_only: extra.max_smolnet_only
       }}
    end
  end

  defp target("internet", :self_check),
    do: {:error, "the helper's loopback has no route to the internet; use --target local"}

  defp target("internet", _mode), do: {:ok, :internet}
  defp target("local", _mode), do: {:ok, :local}

  defp target(other, _mode),
    do: {:error, "--target must be internet or local, got #{inspect(other)}"}

  defp within_range(name, value, range) do
    if value in range,
      do: :ok,
      else: {:error, "--#{dasherize(name)} must be #{range.first} to #{range.last}, got #{value}"}
  end

  defp buffer(bytes) when bytes == 0 or bytes in 1_024..1_048_576, do: :ok
  defp buffer(bytes), do: {:error, "--socket-buffer must be 0 or 1024 to 1048576, got #{bytes}"}

  # Politeness: a crawl of the internet keeps few connections in flight.
  defp concurrency(count, :internet) when count > @max_internet_concurrency,
    do: {:error, "--concurrency must be at most #{@max_internet_concurrency} for the internet"}

  defp concurrency(_count, _target), do: :ok

  defp dasherize(name), do: name |> to_string() |> String.replace("_", "-")

  # The internet target

  defp internet(context, settings) do
    # At rest, before the preflight's connections, which close first and
    # so hold their slots through TIME-WAIT.
    baseline = Soak.socket_count(context) || 0

    case Hosts.load(settings.list, settings.top, context.out_dir) do
      {:ok, hosts, info} ->
        Soak.record(context, :list, info)
        Soak.log(context, "crawling #{length(hosts)} hosts from #{describe_list(info)}")
        preflights = Enum.map(context.families, &{&1, preflight(context, hosts, &1)})
        families = for {family, :ok} <- preflights, do: family

        cond do
          Enum.any?(preflights, &match?({_family, :abandon}, &1)) ->
            :ok

          families == [] ->
            Soak.abandon(
              context,
              "this host reaches none of the list's first hosts over #{inspect(context.families)}"
            )

          true ->
            sites = Enum.map(hosts, &%{host: &1, tls: :remote, targets: nil})
            crawl(context, settings, sites, families, baseline)
        end

      {:error, message} ->
        Soak.abandon(context, "could not load the host list: #{message}")
    end
  end

  defp describe_list(%{source: "tranco"} = info),
    do: "Tranco list #{info.id} of #{info.created_on}, saved as #{Path.basename(info.file)}"

  defp describe_list(info), do: "#{info.source} #{info.file}"

  # Whether the host's own stack reaches the list over `family`: SmolNet's
  # traffic leaves through the host, so without that there is nothing to
  # test. Then whether SmolNet reaches the same address, without which the
  # host is almost surely not forwarding the device's traffic.
  defp preflight(context, hosts, family) do
    case reach(context, Enum.take(hosts, @preflight_hosts), family, nil) do
      {:ok, host, address} ->
        forwarded(context, host, address, family)

      {:error, reason} ->
        Soak.note(
          context,
          "skipping #{family}: this host reaches none of the list's first " <>
            "#{@preflight_hosts} hosts over it (last #{inspect(reason)})"
        )

        :skip
    end
  end

  defp reach(_context, [], _family, reason), do: {:error, reason}

  defp reach(context, [host | rest], family, _reason) do
    with {:ok, address} <- :inet.getaddr(String.to_charlist(host), family, @dns_timeout),
         true <- global?(address) || {:error, :not_global},
         {:ok, socket} <-
           :gen_tcp.connect(address, @https_port, [family], @preflight_timeout) do
      :gen_tcp.close(socket)
      {:ok, host, address}
    else
      {:error, reason} -> reach(context, rest, family, reason)
    end
  end

  defp forwarded(%{mode: :smolnet} = context, host, address, family) do
    options = Network.tcp_options(context, :subject, family) ++ [:binary, active: false]

    case :gen_tcp.connect(address, @https_port, options, @forward_timeout) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        :ok

      {:error, reason} ->
        Soak.abandon(
          context,
          "SmolNet cannot reach #{host} over #{family} through this host " <>
            "(#{inspect(reason)}), though the host itself can: the host is not forwarding " <>
            "or masquerading #{context.device}'s traffic; see integration/setup.sh"
        )

        :abandon
    end
  end

  defp forwarded(_context, _host, _address, _family), do: :ok

  defp remote_tls(host) do
    [
      verify: :verify_peer,
      cacerts: :public_key.cacerts_get(),
      server_name_indication: String.to_charlist(host),
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)],
      alpn_advertised_protocols: ["http/1.1"],
      # Alerts are outcomes here, recorded with their reasons.
      log_level: :none,
      active: false,
      mode: :binary
    ]
  end

  # The local target: for each family, a server on the peer that answers,
  # one that accepts and stays silent, one that accepts and hangs up, and a
  # port that refuses. Both stacks should see the same outcomes of them.

  defp local(context, settings) do
    %{server: server_tls, client: client_tls} = Tls.local_options()
    settings = Map.put(settings, :local_tls, [log_level: :none] ++ client_tls)
    server_tls = [log_level: :none] ++ server_tls

    servers =
      for family <- context.families, kind <- @local_kinds do
        start_local(context, family, kind, server_tls)
      end

    case Enum.find(servers, &match?({:error, _reason}, &1)) do
      nil ->
        sites = local_sites(Enum.map(servers, fn {:ok, server} -> server end))
        # With the servers' listeners, which in self-check are SmolNet's.
        baseline = Soak.socket_count(context) || 0
        crawl(context, settings, sites, context.families, baseline)

      {:error, reason} ->
        Soak.fail(context, :listen, "could not start the local servers", [{"reason", reason}])
    end

    Enum.each(servers, fn
      {:ok, server} -> stop_local(server)
      {:error, _reason} -> :ok
    end)
  end

  # One site per kind of server, with its address and port per family.
  defp local_sites(servers) do
    for kind <- @local_kinds do
      targets =
        for %{kind: ^kind} = server <- servers,
            into: %{},
            do: {server.family, {server.address, server.port}}

      %{host: "#{kind}.#{@local_domain}", tls: :local, targets: targets}
    end
  end

  defp start_local(context, family, kind, server_tls) do
    parent = self()
    address = Network.address(context, :peer, family)
    pid = spawn(fn -> local_server(parent, context, family, kind, server_tls) end)
    monitor = Process.monitor(pid)

    receive do
      {^pid, {:ok, port}} ->
        Process.demonitor(monitor, [:flush])
        {:ok, %{kind: kind, family: family, address: address, port: port, pid: pid}}

      {^pid, {:error, _reason} = error} ->
        Process.demonitor(monitor, [:flush])
        error

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:error, reason}
    end
  end

  defp stop_local(server) do
    monitor = Process.monitor(server.pid)
    send(server.pid, :stop)

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end
  end

  defp local_server(parent, context, family, :ok, server_tls) do
    case Tls.listen(context, :peer, family, server_tls, backlog: 64) do
      {:ok, listener} ->
        {:ok, {_address, port}} = :ssl.sockname(listener)
        send(parent, {self(), {:ok, port}})
        accept = fn -> :ssl.transport_accept(listener, @accept_poll) end

        accept_loop(accept, &:ssl.controlling_process/2, &answer/1, fn -> :ssl.close(listener) end)

      {:error, _reason} = error ->
        send(parent, {self(), error})
    end
  end

  defp local_server(parent, context, family, kind, _server_tls) do
    address = Network.address(context, :peer, family)

    options =
      Network.tcp_options(context, :peer, family) ++
        [:binary, active: false, ip: address, backlog: 64]

    with {:ok, listener} <- :gen_tcp.listen(0, options),
         {:ok, {_address, port}} <- :inet.sockname(listener) do
      send(parent, {self(), {:ok, port}})
      accept = fn -> :gen_tcp.accept(listener, @accept_poll) end
      close = fn -> :gen_tcp.close(listener) end
      hand_over = &:gen_tcp.controlling_process/2

      case kind do
        :silent -> accept_loop(accept, hand_over, &stay_silent/1, close)
        :hangup -> accept_loop(accept, hand_over, &:gen_tcp.close/1, close)
        :refused -> refuse(close)
      end
    else
      {:error, _reason} = error -> send(parent, {self(), error})
    end
  end

  # The port was listened on, so it is this host's; closed, it refuses.
  defp refuse(close) do
    close.()

    receive do
      :stop -> :ok
    end
  end

  defp accept_loop(accept, hand_over, serve, close) do
    receive do
      :stop -> close.()
    after
      0 ->
        case accept.() do
          {:ok, socket} ->
            handler = spawn(fn -> receive(do: ({:socket, socket} -> serve.(socket))) end)

            case hand_over.(socket, handler) do
              :ok -> send(handler, {:socket, socket})
              {:error, _reason} -> Process.exit(handler, :kill)
            end

            accept_loop(accept, hand_over, serve, close)

          {:error, :timeout} ->
            accept_loop(accept, hand_over, serve, close)

          {:error, _reason} ->
            close.()
        end
    end
  end

  defp answer(socket) do
    with {:ok, tls} <- :ssl.handshake(socket, 10_000) do
      _read = read_request(tls, <<>>)
      _sent = :ssl.send(tls, "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
      :ssl.close(tls)
    end
  end

  defp read_request(tls, buffer) do
    if String.contains?(buffer, "\r\n\r\n") do
      :ok
    else
      with {:ok, data} <- :ssl.recv(tls, 0, 10_000), do: read_request(tls, buffer <> data)
    end
  end

  defp stay_silent(socket) do
    case :gen_tcp.recv(socket, 0, 120_000) do
      {:ok, _data} -> stay_silent(socket)
      {:error, _reason} -> :gen_tcp.close(socket)
    end
  end

  # The crawl

  defp crawl(context, settings, sites, families, baseline) do
    File.write!(
      csv_path(context),
      "round,family,host,address,client,class,stage,status,elapsed_ms,reason\n"
    )

    if settings.ceiling, do: ceiling(context, settings, baseline)

    unless Soak.failed?(context) do
      round = fn -> crawl_round(context, settings, sites, families, baseline) end
      Soak.loop(context, round, pause: settings.round_pause)
    end

    summarize(context, settings)
  end

  defp csv_path(context), do: Path.join(context.out_dir, "hosts.csv")

  # The ceiling is pushed over every family, even one the internet cannot
  # be reached over: its peer is on this host.
  defp ceiling(%{mode: :smolnet} = context, settings, baseline) do
    results =
      for family <- context.families, not Soak.failed?(context), into: %{} do
        options = [step: settings.ceiling_step, buffer: settings.buffer]
        {family, Ceiling.run(context, family, baseline, options)}
      end

    Soak.record(context, :ceiling, results)
  end

  defp ceiling(context, _settings, _baseline) do
    Soak.note(
      context,
      "the socket ceiling is not pushed in #{context.mode} mode: its peer is not the kernel"
    )
  end

  # The loop calls for a round at least once, and once more after a pause
  # the run's end cut short; that one is not a round.
  defp crawl_round(context, settings, sites, families, baseline) do
    if Soak.count(context, :rounds, 0) == 0 or Soak.running?(context),
      do: run_round(context, settings, sites, families, baseline)
  end

  defp run_round(context, settings, sites, families, baseline) do
    round = Soak.count(context, :rounds)
    started = System.monotonic_time(:millisecond)
    clients = clients(settings, round)
    jobs = for family <- families, site <- sites, do: {family, site}

    stats =
      jobs
      |> Task.async_stream(
        fn {family, site} -> visit(context, settings, round, clients, family, site) end,
        max_concurrency: context.concurrency,
        ordered: false,
        timeout: :infinity
      )
      |> Enum.reduce(%{visited: 0, smolnet_only: [], classes: %{}}, fn {:ok, visit}, stats ->
        tally(context, settings, visit, stats)
      end)

    Soak.await_socket_count(context, baseline)
    Enum.each(Enum.reverse(stats.smolnet_only), &gather_evidence(context, settings, &1))
    elapsed = System.monotonic_time(:millisecond) - started
    report_round(context, settings, round, stats, elapsed)
  end

  # SmolNet first in odd rounds and the kernel first in even ones, so that
  # neither always meets a host cold.
  defp clients(%{compare: false}, _round), do: [:subject]
  defp clients(_settings, round) when rem(round, 2) == 1, do: [:subject, :peer]
  defp clients(_settings, _round), do: [:peer, :subject]

  # The first round of a local crawl, or of a run of no duration, visits
  # every host; any other stops when the run does.
  defp continue?(context, settings, 1)
       when settings.target == :local or context.duration_ms == 0,
       do: not Soak.failed?(context)

  defp continue?(context, _settings, _round), do: Soak.running?(context)

  defp visit(context, settings, round, clients, family, site) do
    if continue?(context, settings, round) do
      base = %{round: round, family: family, host: site.host}

      case resolve(site, family) do
        {:ok, address, port} ->
          attempts = Map.new(clients, &{&1, attempt(context, settings, &1, site, address, port)})
          Map.merge(base, %{address: address, attempts: attempts})

        {:error, reason} ->
          Map.merge(base, %{address: nil, unresolved: inspect(reason), attempts: %{}})
      end
    else
      :skipped
    end
  end

  # Resolution is the host's, not SmolNet's, and both stacks visit the
  # address it gives, so that they meet the same server. Some names
  # resolve to loopback or private addresses, which are not internet
  # peers: the kernel would visit this host or its network, and SmolNet's
  # packets would be dropped as martians, so neither visits them.
  defp resolve(%{targets: nil, host: host}, family) do
    case :inet.getaddr(String.to_charlist(host), family, @dns_timeout) do
      {:ok, address} ->
        if global?(address),
          do: {:ok, address, @https_port},
          else: {:error, {:not_global, to_string(:inet.ntoa(address))}}

      {:error, _reason} = error ->
        error
    end
  end

  defp resolve(%{targets: targets}, family) do
    case Map.fetch(targets, family) do
      {:ok, {address, port}} -> {:ok, address, port}
      :error -> {:error, :nxdomain}
    end
  end

  @doc false
  # Whether `address` is a global unicast address, one an internet host
  # may have.
  @spec global?(:inet.ip_address()) :: boolean()
  def global?(address) do
    size = if tuple_size(address) == 4, do: 32, else: 128
    value = :binary.decode_unsigned(bits(address))

    not Enum.any?(@not_global, fn {prefix, length} ->
      tuple_size(prefix) == tuple_size(address) and
        Bitwise.bsr(value, size - length) ==
          Bitwise.bsr(:binary.decode_unsigned(bits(prefix)), size - length)
    end)
  end

  defp bits({a, b, c, d}), do: <<a, b, c, d>>
  defp bits(address), do: for(word <- Tuple.to_list(address), into: <<>>, do: <<word::16>>)

  defp attempt(context, settings, role, site, address, port) do
    family = if tuple_size(address) == 4, do: :inet, else: :inet6
    label = stack_label(context, role)
    tls = if site.tls == :local, do: settings.local_tls, else: remote_tls(site.host)
    from_us = System.os_time(:microsecond)
    started = System.monotonic_time(:millisecond)

    options = [port: port, timeout: settings.host_timeout, tls: tls, buffer: settings.buffer]

    outcome =
      Soak.within(context, {:crawl, family, label, site.host}, settings.deadline, fn ->
        Probe.visit(context, role, site.host, address, options)
      end)

    Map.merge(outcome, %{
      client: label,
      elapsed_ms: System.monotonic_time(:millisecond) - started,
      window: {from_us, System.os_time(:microsecond)}
    })
  end

  # In self-check and baseline modes both roles are the same stack, so the
  # peer's is named apart.
  defp stack_label(context, role) do
    stack = if Network.smolnet?(context, role), do: "smolnet", else: "kernel"
    if role == :peer and context.mode != :smolnet, do: stack <> " peer", else: stack
  end

  # A visit's outcomes: counted, written to hosts.csv, compared, and
  # folded into its host's record.
  defp tally(_context, _settings, :skipped, stats), do: stats

  defp tally(context, settings, visit, stats) do
    write_csv(context, visit)

    if visit.attempts == %{} do
      Soak.count(context, :unresolved)
      stats
    else
      Soak.count(context, :visits, map_size(visit.attempts))
      subject = visit.attempts.subject
      peer = visit.attempts[:peer]
      if subject.class == :ok, do: Soak.count(context, :smolnet_ok)
      if peer && peer.class == :ok, do: Soak.count(context, :kernel_ok)
      comparison = compare(subject, peer)
      if comparison, do: Soak.count(context, comparison)
      remember(settings.table, visit, comparison)

      classes =
        Enum.reduce(visit.attempts, stats.classes, fn {_role, attempt}, classes ->
          counts = Map.update(classes[attempt.client] || %{}, attempt.class, 1, &(&1 + 1))
          Map.put(classes, attempt.client, counts)
        end)

      stats = %{stats | visited: stats.visited + 1, classes: classes}

      if comparison == :smolnet_only,
        do: %{stats | smolnet_only: [visit | stats.smolnet_only]},
        else: stats
    end
  end

  @doc false
  # How SmolNet's outcome of a visit compares with the kernel's: nil when
  # there is no kernel outcome, or when both succeeded.
  @spec compare(map(), map() | nil) :: :smolnet_only | :kernel_only | :both_failed | nil
  def compare(_subject, nil), do: nil
  def compare(%{class: :ok}, %{class: :ok}), do: nil
  def compare(%{class: :ok}, _peer), do: :kernel_only
  def compare(_subject, %{class: :ok}), do: :smolnet_only
  def compare(_subject, _peer), do: :both_failed

  defp write_csv(context, visit) do
    address = if visit.address, do: to_string(:inet.ntoa(visit.address)), else: ""
    prefix = [visit.round, visit.family, visit.host, address]

    rows =
      if visit.attempts == %{} do
        [prefix ++ ["", "unresolved", "dns", "", "", visit.unresolved]]
      else
        for {_role, a} <- visit.attempts do
          prefix ++ [a.client, a.class, a.stage, a.status || "", a.elapsed_ms, a.reason || ""]
        end
      end

    lines = Enum.map(rows, fn row -> [Enum.map_join(row, ",", &csv_field/1), "\n"] end)
    File.write!(csv_path(context), lines, [:append])
  end

  defp csv_field(value) do
    text = to_string(value)

    if String.contains?(text, [",", "\"", "\n"]),
      do: ~s("#{String.replace(text, "\"", "\"\"")}"),
      else: text
  end

  # Each host's record, over every round: how its visits compared, the
  # outcomes over each stack, and why SmolNet's failed where the kernel's
  # did not.
  defp remember(table, visit, comparison) do
    key = {visit.family, visit.host}

    record =
      case :ets.lookup(table, key) do
        [{^key, record}] -> record
        [] -> new_record(visit)
      end

    subject = visit.attempts.subject
    peer = visit.attempts[:peer]

    record = %{
      record
      | address: to_string(:inet.ntoa(visit.address)),
        rounds: record.rounds + 1,
        smolnet: bump(record.smolnet, subject.class),
        kernel: if(peer, do: bump(record.kernel, peer.class), else: record.kernel)
    }

    record =
      case comparison do
        nil ->
          record

        :smolnet_only ->
          %{
            record
            | smolnet_only: record.smolnet_only + 1,
              causes: bump(record.causes, Probe.cause(subject)),
              reasons: Enum.take(Enum.uniq(record.reasons ++ [subject.reason]), 3)
          }

        :kernel_only ->
          %{record | kernel_only: record.kernel_only + 1}

        :both_failed ->
          %{record | both_failed: record.both_failed + 1}
      end

    :ets.insert(table, {key, record})
  end

  defp new_record(visit) do
    %{
      family: visit.family,
      host: visit.host,
      address: nil,
      rounds: 0,
      smolnet_only: 0,
      kernel_only: 0,
      both_failed: 0,
      smolnet: %{},
      kernel: %{},
      causes: %{},
      reasons: [],
      evidence: nil
    }
  end

  defp bump(counts, key), do: Map.update(counts, key, 1, &(&1 + 1))

  # The first time a host fails over SmolNet alone, while the budget
  # lasts, its packets are cut from the rolling capture.
  defp gather_evidence(context, settings, visit) do
    key = {visit.family, visit.host}
    [{^key, record}] = :ets.lookup(settings.table, key)
    pcap_dir = Path.join(context.out_dir, "pcap")
    gathered = :ets.update_counter(settings.table, :evidence, 0, {:evidence, 0})

    if record.evidence == nil and gathered < settings.evidence and File.dir?(pcap_dir) do
      :ets.update_counter(settings.table, :evidence, 1)
      name = "#{visit.host}-#{visit.family}"
      diag_dir = Path.join(context.out_dir, "diag")
      window = visit.attempts.subject.window

      evidence =
        case Evidence.collect(pcap_dir, diag_dir, name, visit.address, window) do
          {:ok, summary} -> Map.put(summary, :round, visit.round)
          {:error, message} -> %{error: message}
        end

      :ets.insert(settings.table, {key, %{record | evidence: evidence}})
    end
  end

  defp report_round(context, settings, round, stats, elapsed) do
    rows = %{
      round: round,
      hosts: stats.visited,
      elapsed_s: Float.round(elapsed / 1_000, 1),
      outcomes: stats.classes,
      smolnet_only: length(stats.smolnet_only)
    }

    :ets.insert(settings.table, {{:round, round}, rows})

    outcomes =
      Enum.map_join(stats.classes, "; ", fn {client, counts} ->
        "#{client} " <> describe_counts(counts)
      end)

    Soak.note(
      context,
      "round #{round}: #{stats.visited} visits in #{rows.elapsed_s} s; #{outcomes}; " <>
        "#{rows.smolnet_only} failed over SmolNet alone"
    )
  end

  defp describe_counts(counts) do
    counts
    |> Enum.sort_by(fn {class, count} -> {-count, class} end)
    |> Enum.map_join(", ", fn {class, count} -> "#{class} #{count}" end)
  end

  # The verdict's results

  defp summarize(context, settings) do
    entries = :ets.tab2list(settings.table)
    records = for {{family, _host}, record} <- entries, family in [:inet, :inet6], do: record
    rounds = for {{:round, _n}, row} <- Enum.sort(entries), do: row

    subject = stack_label(context, :subject)
    peer = stack_label(context, :peer)
    outcomes = %{subject => sum_counts(records, :smolnet)}

    outcomes =
      if settings.compare,
        do: Map.put(outcomes, peer, sum_counts(records, :kernel)),
        else: outcomes

    listed =
      records
      |> Enum.filter(&(&1.smolnet_only > 0))
      |> Enum.map(&Map.put(&1, :persistent, persistent?(&1)))
      |> Enum.sort_by(&{not &1.persistent, -&1.smolnet_only, &1.host})

    causes = causes(listed)
    persistent = Enum.count(listed, & &1.persistent)
    kernel_only = Enum.filter(records, &(&1.kernel_only > 0))

    comparison = %{
      hosts: length(records),
      smolnet_only: length(listed),
      persistent_smolnet_only: persistent,
      kernel_only: length(kernel_only),
      both_failed: Enum.count(records, &(&1.both_failed > 0))
    }

    Soak.record(context, :outcomes, outcomes)
    Soak.record(context, :rounds, rounds)
    Soak.record(context, :comparison, comparison)
    Soak.record(context, :smolnet_only, Enum.take(listed, @max_listed))
    Soak.record(context, :causes, causes)
    Soak.record(context, :kernel_only, Enum.take(Enum.map(kernel_only, &example/1), @max_listed))

    Enum.each(outcomes, fn {client, counts} ->
      Soak.note(context, "outcomes over #{client}: " <> describe_counts(counts))
    end)

    Soak.note(
      context,
      "of #{comparison.hosts} hosts, #{comparison.smolnet_only} failed over SmolNet alone " <>
        "(#{persistent} persistently), #{comparison.kernel_only} " <>
        "over the kernel alone, and #{comparison.both_failed} over both"
    )

    Enum.each(causes, fn cause ->
      Soak.note(
        context,
        "SmolNet alone failed #{cause.hosts} hosts with #{cause.cause}, such as " <>
          Enum.join(Enum.take(cause.examples, 5), ", ")
      )
    end)

    judge(context, settings, persistent, causes)
  end

  defp sum_counts(records, side) do
    Enum.reduce(records, %{}, fn record, total ->
      Map.merge(total, Map.fetch!(record, side), fn _class, a, b -> a + b end)
    end)
  end

  # A host SmolNet never reached, though the kernel reached it in at least
  # two rounds and at least half of them: one the kernel reached only now
  # and then is flaky, whatever SmolNet made of it.
  defp persistent?(record) do
    kernel_ok = Map.get(record.kernel, :ok, 0)
    Map.get(record.smolnet, :ok, 0) == 0 and kernel_ok >= 2 and 2 * kernel_ok >= record.rounds
  end

  defp causes(listed) do
    listed
    |> Enum.group_by(fn record ->
      {cause, _count} = Enum.max_by(record.causes, fn {cause, count} -> {count, cause} end)
      cause
    end)
    |> Enum.map(fn {cause, records} ->
      %{
        cause: cause,
        hosts: length(records),
        persistent: Enum.count(records, & &1.persistent),
        examples: Enum.map(Enum.take(records, 10), &example/1)
      }
    end)
    |> Enum.sort_by(&{-&1.hosts, &1.cause})
  end

  defp example(record), do: "#{record.host} (#{record.family})"

  defp judge(context, %{max_smolnet_only: max}, persistent, causes)
       when max >= 0 and persistent > max do
    Soak.fail(
      context,
      :smolnet_only,
      "#{persistent} hosts failed over SmolNet persistently, while the kernel reached them, " <>
        "more than --max-smolnet-only #{max}",
      [{"causes", causes}]
    )
  end

  defp judge(_context, _settings, _persistent, _causes), do: :ok
end
