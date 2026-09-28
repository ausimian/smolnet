defmodule SmolNet.Integration.Scenarios.Pmtu do
  @moduledoc """
  The path MTU scenario: what SmolNet does when a hop between it and its
  peer has a smaller MTU than its own.

  `integration/pmtu-topology.sh` builds the path: SmolNet on the TUN
  device, this namespace routing to a hop whose MTU is set per case, a
  router beyond it, and a kernel peer beyond that at 1500, or at `--mtu`
  if that is larger. Both ends of the hop take its MTU, so the peer
  advertises an MSS for its own link and SmolNet one for `--mtu`, and a packet too big for the hop is refused where it
  would enter it. The run must be in a network namespace of its own, which
  `integration/pmtu-topology.sh isolate` provides.

  For each hop MTU in `--hops` below `--mtu`, and for each family (IPv6
  only at 1280 and above, the least an IPv6 link may have), the matrix is:

    * TCP each way, with the routers sending ICMP errors or dropping them
      (`icmp`), and the MSS of SYNs left alone, clamped by the router at
      the hop to what the route carries as the usual `rt mtu` recipe does,
      or clamped to a fixed MSS (`clamp`). SmolNet is always the client;
      `out` has it send `--bytes`, `in` has the peer send them.
    * UDP each way: a small probe that must arrive, then `--datagrams` of
      the largest SmolNet sends at its MTU, with and without ICMP.
    * For IPv4, the peer sending without Don't Fragment (`peer_df` off),
      so that the router fragments what it sends: TCP with and without
      the `rt` clamp, and UDP.

  and once more with the hop at `--mtu`, where nothing is too big: the
  control, which must pass.

  Each case ends in one outcome:

    * `adapts` - every byte arrived, intact. For UDP, every datagram.
    * `stalls` - no error, but nothing more arrived for `--stall-timeout`:
      a silent black hole. For UDP, datagrams that were lost.
    * `fails` - the transfer ended in an error, such as a reset.

  ## Verdict

  Every case has an expected outcome and a reason for it, which
  `expect/2` gives and the notes and `verdict.json` explain. A case that
  should adapt and does not fails the run, once the matrix is done: the
  control, a clamped path, SmolNet or the peer sending with the router's
  ICMP errors reaching it. A case that is known not to adapt, because no
  ICMP error reaches SmolNet and it does no black-hole probing, because it
  does not reassemble fragments, or because the peer black-holes itself, is
  recorded, not failed. One of those that adapts after all is noted, as a
  sign that the expectation is out of date. A TCP handshake or UDP probe
  that fails, or a path the topology script cannot set, fails the run
  whatever the case: they are small enough for any hop, so the path or
  the harness is broken. The per-case results are the
  `cases` result in `verdict.json`.

  Each case keeps its captures whatever the verdict, in `captures/<id>/`:
  the device (SmolNet's side), `pmtu-h` (this namespace's end of the hop)
  and `pmtu-p` (the peer's link), `--snaplen` bytes of each packet.
  """

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Ownership
  alias SmolNet.Integration.Soak
  alias SmolNet.Integration.Soak.Pcap
  alias SmolNet.Integration.TunLink

  @topology Path.expand("../../../../pmtu-topology.sh", __DIR__)
  @peer_namespace "pmtu-peer"
  @peer_netns "/run/netns/pmtu-peer"
  @peer %{inet: {10, 79, 0, 2}, inet6: {0xFD00, 0x79, 0, 0, 0, 0, 0, 2}}
  # IP and UDP headers.
  @udp_overhead %{inet: 28, inet6: 48}
  @min_hop 576
  @probe_bytes 64
  @connect_timeout 10_000
  @datagram_gap 20

  @reasons %{
    fits: "the whole path carries SmolNet's MTU",
    clamped: "the router's MSS clamp keeps every segment within the hop",
    peer_pmtud: "the router's ICMP error reaches the peer, whose kernel sends smaller segments",
    pmtud:
      "the router's ICMP error reaches SmolNet, which lowers the connection's segment " <>
        "size and resends (RFC 1191, RFC 8201; #128)",
    icmp_black_hole:
      "no error reaches SmolNet, and it does not probe for a smaller MTU (no RFC 4821): " <>
        "a black hole, which an MSS clamp at the router or a lower :mtu avoids (path_mtu.md)",
    peer_black_hole:
      "no error reaches the peer, and its kernel does not probe for a smaller MTU " <>
        "(net.ipv4.tcp_mtu_probing=0): the peer's own black hole",
    black_hole: "the router drops the peer's datagrams, and no error reaches it",
    fragments:
      "what reaches SmolNet arrives in fragments, which SmolNet does not reassemble " <>
        "(documented unsupported; #129)",
    no_fragmentation:
      "SmolNet sets Don't Fragment and does not fragment, so a datagram larger than " <>
        "the path is lost (documented; #129)"
  }

  @type outcome :: :adapts | :stalls | :fails
  @type reason ::
          :fits
          | :clamped
          | :peer_pmtud
          | :pmtud
          | :icmp_black_hole
          | :peer_black_hole
          | :black_hole
          | :fragments
          | :no_fragmentation
  @type test_case :: %{
          id: String.t(),
          family: :inet | :inet6,
          hop: pos_integer(),
          transport: :tcp | :udp,
          direction: :out | :in,
          icmp: :on | :off,
          clamp: :none | :rt | :fixed,
          peer_df: :on | :off
        }

  @doc "Returns the scenario's runner config."
  @spec config() :: keyword()
  def config do
    [
      name: "pmtu",
      default_duration: "20m",
      switches: [
        mtu: :integer,
        hops: :string,
        bytes: :integer,
        datagrams: :integer,
        stall_timeout: :integer,
        snaplen: :integer,
        only: :string
      ],
      defaults: [
        mtu: 1500,
        hops: "1280,1000,576",
        bytes: 262_144,
        datagrams: 5,
        stall_timeout: 5_000,
        snaplen: 256,
        only: nil
      ],
      counters: [:cases, :adapted, :stalled, :failed],
      stack: fn extra -> [mtu: extra.mtu, limits: %{sockets: 256}] end,
      usage: """

      pmtu options:
        --mtu N               SmolNet's MTU and the device's (default 1500)
        --hops LIST           the hop MTUs to try, comma-separated, each at least 576;
                              those not below --mtu are skipped (default 1280,1000,576)
        --bytes N             what each TCP case sends, at least --mtu (default 262144)
        --datagrams N         the full-size datagrams each UDP case sends (default 5)
        --stall-timeout MS    how long without progress is a stall (default 5000)
        --snaplen N           the bytes of each packet the captures keep (default 256)
        --only REGEX          run only the cases whose ids match, such as
                              inet-1000-tcp-out (default: every case)

      Run it in a network namespace of its own:

        integration/pmtu-topology.sh isolate mix run integration/pmtu.exs

      --duration bounds the matrix: cases that do not fit are skipped, with a note.
      """
    ]
  end

  @doc "Runs the scenario's workload."
  @spec run(SmolNet.Integration.Soak.Context.t()) :: :ok
  def run(context) do
    with {:ok, settings} <- settings(context),
         :ok <- check_mode(context),
         :ok <- topology(context, ["up", "--mtu", Integer.to_string(settings.mtu)]) do
      File.rm_rf!(Path.join(context.out_dir, "captures"))

      try do
        run_matrix(context, settings)
      after
        topology(context, ["down"])
        Ownership.restore(Path.join(context.out_dir, "captures"))
      end

      Soak.await_socket_count(context, 0)
    else
      {:abandon, reason} -> Soak.abandon(context, reason)
      {:error, message} -> Soak.fail(context, :usage, message)
    end
  end

  defp check_mode(%{mode: :smolnet}), do: :ok

  defp check_mode(_context) do
    {:abandon,
     "the pmtu scenario measures SmolNet across a routed hop, so it has no " <>
       "--baseline or --self-check mode"}
  end

  defp settings(context) do
    extra = context.extra

    with {:ok, hops} <- hops(extra.hops),
         {:ok, only} <- only(extra.only),
         :ok <- full_segment(extra.bytes, extra.mtu),
         :ok <- positive(:datagrams, extra.datagrams),
         :ok <- positive(:stall_timeout, extra.stall_timeout),
         :ok <- positive(:snaplen, extra.snaplen) do
      {below, skipped} = Enum.split_with(hops, &(&1 < extra.mtu))

      if skipped != [] do
        Soak.note(context, "hops #{inspect(skipped)} are not below SmolNet's MTU of #{extra.mtu}")
      end

      {:ok,
       %{
         mtu: extra.mtu,
         hops: below,
         bytes: extra.bytes,
         datagrams: extra.datagrams,
         stall_timeout: extra.stall_timeout,
         snaplen: extra.snaplen,
         only: only
       }}
    end
  end

  defp hops(text) do
    parsed = text |> String.split(",", trim: true) |> Enum.map(&Integer.parse(String.trim(&1)))

    if parsed != [] and Enum.all?(parsed, &match?({hop, ""} when hop >= @min_hop, &1)) do
      {:ok, parsed |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort(:desc)}
    else
      {:error,
       "--hops must be MTUs of at least #{@min_hop}, comma-separated, got #{inspect(text)}"}
    end
  end

  defp only(nil), do: {:ok, nil}

  defp only(pattern) do
    case Regex.compile(pattern) do
      {:ok, regex} ->
        {:ok, regex}

      {:error, _reason} ->
        {:error, "--only must be a regular expression, got #{inspect(pattern)}"}
    end
  end

  # The expectations hold for a transfer that fills at least one segment
  # of SmolNet's MTU; anything smaller fits every hop.
  defp full_segment(bytes, mtu) when is_integer(bytes) and bytes >= mtu, do: :ok

  defp full_segment(bytes, mtu),
    do:
      {:error,
       "--bytes must be at least --mtu (#{mtu}), so that a segment fills it; got #{bytes}"}

  defp positive(_name, value) when is_integer(value) and value > 0, do: :ok
  defp positive(name, value), do: {:error, "--#{dashed(name)} must be positive, got #{value}"}

  defp dashed(name), do: name |> to_string() |> String.replace("_", "-")

  @doc """
  Returns the cases of the matrix, in the order they run: the control at
  `mtu`, then each hop in `hops`, IPv6 only where the hop allows it.
  """
  @spec cases(pos_integer(), [pos_integer()], [:inet | :inet6]) :: [test_case()]
  def cases(mtu, hops, families) do
    control =
      for family <- families,
          transport <- [:tcp, :udp],
          direction <- [:out, :in],
          do: test_case(family, mtu, transport, direction, :on, :none, :on)

    below =
      for hop <- hops,
          family <- families,
          family == :inet or hop >= 1280,
          test_case <- hop_cases(family, hop),
          do: test_case

    control ++ below
  end

  defp hop_cases(family, hop) do
    tcp =
      for direction <- [:out, :in],
          clamp <- [:none, :rt, :fixed],
          icmp <- [:on, :off],
          do: test_case(family, hop, :tcp, direction, icmp, clamp, :on)

    udp =
      for direction <- [:out, :in],
          icmp <- [:on, :off],
          do: test_case(family, hop, :udp, direction, icmp, :none, :on)

    # The router fragments what the peer sends, unless a clamp keeps TCP's
    # segments small enough not to need it.
    no_df =
      if family == :inet do
        [
          test_case(family, hop, :tcp, :in, :on, :none, :off),
          test_case(family, hop, :tcp, :in, :on, :rt, :off),
          test_case(family, hop, :udp, :in, :on, :none, :off)
        ]
      else
        []
      end

    tcp ++ udp ++ no_df
  end

  defp test_case(family, hop, transport, direction, icmp, clamp, peer_df) do
    id =
      Enum.join(
        [family, hop, transport, direction, "icmp-#{icmp}"] ++
          if(transport == :tcp, do: ["clamp-#{clamp}"], else: []) ++
          if(peer_df == :off, do: ["peer-df-off"], else: []),
        "-"
      )

    %{
      id: id,
      family: family,
      hop: hop,
      transport: transport,
      direction: direction,
      icmp: icmp,
      clamp: clamp,
      peer_df: peer_df
    }
  end

  @doc """
  Returns what `test_case` is expected to do with SmolNet at `mtu`, and why.
  """
  @spec expect(test_case(), pos_integer()) :: {outcome(), reason()}
  def expect(%{hop: hop}, mtu) when hop >= mtu, do: {:adapts, :fits}

  # Either clamp lowers the MSS each end advertises to the other, since
  # `rt mtu` takes the smaller of the route's MTU each way.
  def expect(%{transport: :tcp, clamp: clamp}, _mtu) when clamp != :none, do: {:adapts, :clamped}
  def expect(%{transport: :tcp, direction: :out, icmp: :on}, _mtu), do: {:adapts, :pmtud}

  def expect(%{transport: :tcp, direction: :out, icmp: :off}, _mtu),
    do: {:stalls, :icmp_black_hole}

  def expect(%{transport: :tcp, direction: :in, peer_df: :off}, _mtu), do: {:stalls, :fragments}
  def expect(%{transport: :tcp, direction: :in, icmp: :on}, _mtu), do: {:adapts, :peer_pmtud}

  def expect(%{transport: :tcp, direction: :in, icmp: :off}, _mtu),
    do: {:stalls, :peer_black_hole}

  def expect(%{transport: :udp, direction: :out}, _mtu), do: {:stalls, :no_fragmentation}

  def expect(%{transport: :udp, direction: :in, icmp: :off, peer_df: :on}, _mtu),
    do: {:stalls, :black_hole}

  def expect(%{transport: :udp, direction: :in}, _mtu), do: {:stalls, :fragments}

  @doc """
  Judges an observed outcome against the expected one: `:ok` when they
  match, `:regression` when the case should have adapted and did not, and
  `:changed` for any other difference, which is noted but does not fail.
  """
  @spec judge(outcome(), outcome()) :: :ok | :regression | :changed
  def judge(outcome, outcome), do: :ok
  def judge(:adapts, _observed), do: :regression
  def judge(_expected, _observed), do: :changed

  @doc "Explains a `t:reason/0`."
  @spec explain(reason()) :: String.t()
  def explain(reason), do: Map.fetch!(@reasons, reason)

  defp topology(context, arguments) do
    case System.cmd(@topology, arguments,
           stderr_to_stdout: true,
           env: [{"SMOLNET_TUN", context.device}]
         ) do
      {_output, 0} ->
        :ok

      {output, status} ->
        {:abandon,
         "pmtu-topology.sh #{Enum.join(arguments, " ")} exited #{status}: #{String.trim(output)}"}
    end
  end

  defp start_captures(context, settings, test_case) do
    directory = Path.join([context.out_dir, "captures", test_case.id])

    [{context.device, nil}, {"pmtu-h", nil}, {"pmtu-p", @peer_namespace}]
    |> Enum.flat_map(fn {device, netns} ->
      case Pcap.start(device, directory, netns: netns, snaplen: settings.snaplen, immediate: true) do
        {:ok, pcap} ->
          [pcap]

        {:error, message} ->
          Soak.log(context, "#{test_case.id}: not capturing on #{device}: #{message}")
          []
      end
    end)
  end

  defp run_matrix(context, settings) do
    cases =
      settings.mtu
      |> cases(settings.hops, context.families)
      |> Enum.filter(&(settings.only == nil or &1.id =~ settings.only))

    Soak.log(
      context,
      "#{length(cases)} cases: SmolNet at MTU #{settings.mtu}, hops #{inspect(settings.hops)}"
    )

    {results, skipped} = run_cases(context, settings, cases, [])

    if skipped != [] do
      Soak.note(
        context,
        "the run's duration ended before #{length(skipped)} of #{length(cases)} cases; " <>
          "run longer for the whole matrix"
      )
    end

    summarize(context, settings, results)
  end

  defp run_cases(_context, _settings, [], results), do: {Enum.reverse(results), []}

  defp run_cases(context, settings, [test_case | rest] = remaining, results) do
    if Soak.running?(context) do
      result = run_case(context, settings, test_case)
      results = [result | results]
      Soak.record(context, :cases, Enum.reverse(results))
      run_cases(context, settings, rest, results)
    else
      {Enum.reverse(results), remaining}
    end
  end

  defp run_case(context, settings, test_case) do
    arguments = [
      "set",
      "--hop-mtu",
      Integer.to_string(test_case.hop),
      "--icmp",
      to_string(test_case.icmp),
      "--clamp",
      to_string(test_case.clamp),
      "--peer-df",
      to_string(test_case.peer_df)
    ]

    started_at = DateTime.utc_now()
    started_ms = System.monotonic_time(:millisecond)
    refused_before = refused(context)

    observed =
      case topology(context, arguments) do
        :ok ->
          # A case has its stall timeout at least twice over, and more for
          # its connection and clean-up; one that overruns this is stuck in
          # the harness, not stalled on the path.
          deadline = 4 * settings.stall_timeout + 2 * @connect_timeout + 30_000
          captures = start_captures(context, settings, test_case)

          try do
            Soak.within(context, {:case, test_case.id}, deadline, fn ->
              transfer(context, settings, test_case)
            end)
          after
            Enum.each(captures, &Pcap.stop/1)
          end

        {:abandon, reason} ->
          %{outcome: :fails, broken: true, detail: reason, ports: []}
      end

    # A handshake or a probe is small enough for any hop, so one that fails
    # is the path's fault, or the harness's: never an expected outcome.
    {expected, reason} = expect(test_case, settings.mtu)

    verdict =
      if Map.get(observed, :broken, false),
        do: :regression,
        else: judge(expected, observed.outcome)

    count(context, observed.outcome)

    result =
      test_case
      |> Map.merge(%{
        outcome: observed.outcome,
        expected: expected,
        reason: reason,
        verdict: verdict,
        detail: observed.detail,
        ports: observed.ports,
        refused_by_smolnet: refused(context) - refused_before,
        started_at: DateTime.to_iso8601(started_at),
        elapsed_ms: System.monotonic_time(:millisecond) - started_ms
      })

    Soak.log(
      context,
      "#{test_case.id}: #{observed.outcome} (expected #{expected}: #{reason}) - #{observed.detail}"
    )

    result
  end

  defp count(context, outcome) do
    Soak.count(context, :cases)

    case outcome do
      :adapts -> Soak.count(context, :adapted)
      :stalls -> Soak.count(context, :stalled)
      :fails -> Soak.count(context, :failed)
    end
  end

  # Device packets the stack would not take, such as fragments.
  defp refused(context) do
    context.link |> TunLink.stats() |> Map.fetch!(:ingress_refused)
  end

  defp transfer(context, settings, %{transport: :tcp} = test_case),
    do: tcp_case(context, settings, test_case)

  defp transfer(context, settings, %{transport: :udp} = test_case),
    do: udp_case(context, settings, test_case)

  # SmolNet connects to a listener on the peer. The peer's sockets reset
  # rather than close, so that neither end lingers after a stall.
  defp tcp_case(context, settings, test_case) do
    family = test_case.family

    {:ok, listener} =
      :gen_tcp.listen(0, [
        family,
        :binary,
        active: false,
        ip: Map.fetch!(@peer, family),
        netns: @peer_netns
      ])

    {:ok, peer_port} = :inet.port(listener)
    parent = self()

    acceptor =
      Task.async(fn ->
        case :gen_tcp.accept(listener, @connect_timeout) do
          {:ok, socket} ->
            :ok = :inet.setopts(socket, linger: {true, 0})
            :ok = :gen_tcp.controlling_process(socket, parent)
            {:ok, socket}

          {:error, _reason} = error ->
            error
        end
      end)

    options = Network.tcp_options(context, :subject, family) ++ [:binary, active: false]

    result =
      case :gen_tcp.connect(Map.fetch!(@peer, family), peer_port, options, @connect_timeout) do
        {:ok, smolnet} ->
          {:ok, {_address, smolnet_port}} = :inet.sockname(smolnet)
          ports = [smolnet_port, peer_port]

          case Task.await(acceptor, @connect_timeout + 1_000) do
            {:ok, peer} ->
              stream(settings, test_case, smolnet, peer, ports)

            {:error, reason} ->
              :gen_tcp.close(smolnet)

              %{
                outcome: :fails,
                broken: true,
                detail: "the peer accepted nothing: #{inspect(reason)}",
                ports: ports
              }
          end

        {:error, reason} ->
          Task.shutdown(acceptor, :brutal_kill)

          %{
            outcome: :fails,
            broken: true,
            detail: "SmolNet could not connect: #{inspect(reason)}",
            ports: [peer_port]
          }
      end

    :gen_tcp.close(listener)
    result
  end

  defp stream(settings, test_case, smolnet, peer, ports) do
    {sender, receiver} =
      if test_case.direction == :out, do: {smolnet, peer}, else: {peer, smolnet}

    payload = :crypto.strong_rand_bytes(settings.bytes)
    started = System.monotonic_time(:millisecond)

    # Sending from another process, which may block in the send, while
    # this one receives.
    sending = Task.async(fn -> :gen_tcp.send(sender, payload) end)
    received = receive_all(receiver, settings.bytes, settings.stall_timeout, [], 0)
    elapsed = System.monotonic_time(:millisecond) - started

    # The peer's reset ends SmolNet's side of a stalled connection too.
    :gen_tcp.close(peer)
    sent = Task.yield(sending, 2_000) || Task.shutdown(sending, :brutal_kill)
    :gen_tcp.close(smolnet)

    received
    |> tcp_outcome(payload, {elapsed, settings.stall_timeout}, sent)
    |> Map.put(:ports, ports)
  end

  defp tcp_outcome({:complete, data}, payload, {elapsed, _stall}, _sent) do
    if data == payload do
      %{outcome: :adapts, detail: "#{byte_size(data)} bytes in #{elapsed} ms"}
    else
      %{outcome: :fails, detail: "#{byte_size(data)} bytes arrived, but not the ones sent"}
    end
  end

  defp tcp_outcome({:stalled, got}, payload, {elapsed, stall}, sent) do
    %{
      outcome: :stalls,
      detail:
        "#{got} of #{byte_size(payload)} bytes arrived in #{elapsed} ms, the last " <>
          "#{stall} ms of it without any#{describe_send(sent)}"
    }
  end

  defp tcp_outcome({:failed, reason, got}, payload, _times, sent) do
    %{
      outcome: :fails,
      detail:
        "receive failed with #{inspect(reason)} after #{got} of #{byte_size(payload)} " <>
          "bytes#{describe_send(sent)}"
    }
  end

  defp describe_send({:ok, :ok}), do: "; the send had returned"
  defp describe_send({:ok, {:error, reason}}), do: "; the send failed with #{inspect(reason)}"
  defp describe_send(nil), do: "; the send was still blocked"
  defp describe_send({:exit, reason}), do: "; the sender exited: #{inspect(reason)}"

  defp receive_all(_socket, expected, _stall, acc, got) when got >= expected do
    {:complete, acc |> Enum.reverse() |> IO.iodata_to_binary()}
  end

  defp receive_all(socket, expected, stall, acc, got) do
    case :gen_tcp.recv(socket, 0, stall) do
      {:ok, data} -> receive_all(socket, expected, stall, [data | acc], got + byte_size(data))
      {:error, :timeout} -> {:stalled, got}
      {:error, reason} -> {:failed, reason, got}
    end
  end

  # A small probe first, which must arrive, then datagrams as large as
  # SmolNet sends at its MTU.
  defp udp_case(context, settings, test_case) do
    family = test_case.family

    {:ok, peer} =
      :gen_udp.open(0, [
        family,
        :binary,
        active: false,
        ip: Map.fetch!(@peer, family),
        netns: @peer_netns
      ])

    options =
      Network.udp_options(context, :subject, family) ++
        [:binary, active: false, ip: Network.smolnet_address(family)]

    {:ok, smolnet} = :gen_udp.open(0, options)
    {:ok, peer_port} = :inet.port(peer)
    {:ok, smolnet_port} = :inet.port(smolnet)

    {sender, receiver, destination} =
      if test_case.direction == :out,
        do: {smolnet, peer, {Map.fetch!(@peer, family), peer_port}},
        else: {peer, smolnet, {Network.smolnet_address(family), smolnet_port}}

    size = settings.mtu - Map.fetch!(@udp_overhead, family)
    wait = min(settings.stall_timeout, 2_000)

    result =
      with :ok <- probe(sender, receiver, destination, wait) do
        datagrams(settings, sender, receiver, destination, size, wait)
      end

    :gen_udp.close(peer)
    :gen_udp.close(smolnet)
    Map.put(result, :ports, [smolnet_port, peer_port])
  end

  defp probe(sender, receiver, destination, wait) do
    probe = :crypto.strong_rand_bytes(@probe_bytes)

    with :ok <- :gen_udp.send(sender, destination, probe),
         {:ok, {_address, _port, ^probe}} <- :gen_udp.recv(receiver, 0, wait) do
      :ok
    else
      other ->
        %{
          outcome: :fails,
          broken: true,
          detail: "the #{@probe_bytes}-byte probe did not arrive: #{inspect(other)}"
        }
    end
  end

  # The receiver reads as the datagrams arrive, since a socket's buffer may
  # hold only one of them at a large MTU.
  defp datagrams(settings, sender, receiver, destination, size, wait) do
    collecting = Task.async(fn -> collect(receiver, wait, []) end)

    sent =
      for _index <- 1..settings.datagrams do
        datagram = :crypto.strong_rand_bytes(size)
        result = :gen_udp.send(sender, destination, datagram)
        Process.sleep(@datagram_gap)
        {result, datagram}
      end

    arrived = Task.await(collecting, :infinity)

    case Enum.find(sent, &match?({{:error, _reason}, _datagram}, &1)) do
      {{:error, reason}, _datagram} ->
        %{
          outcome: :fails,
          detail: "sending a #{size}-byte datagram failed with #{inspect(reason)}"
        }

      nil ->
        expected = MapSet.new(sent, fn {:ok, datagram} -> datagram end)
        intact = Enum.count(arrived, &MapSet.member?(expected, &1))
        outcome = if intact == settings.datagrams, do: :adapts, else: :stalls

        %{
          outcome: outcome,
          detail:
            "#{intact} of #{settings.datagrams} #{size}-byte datagrams arrived" <>
              if(length(arrived) > intact,
                do: ", and #{length(arrived) - intact} others",
                else: ""
              )
        }
    end
  end

  defp collect(socket, wait, acc) do
    case :gen_udp.recv(socket, 0, wait) do
      {:ok, {_address, _port, data}} -> collect(socket, wait, [data | acc])
      {:error, _reason} -> Enum.reverse(acc)
    end
  end

  defp summarize(context, settings, results) do
    Enum.each(results, fn result ->
      case result.verdict do
        :ok ->
          :ok

        :changed ->
          Soak.note(
            context,
            "#{result.id} #{result.outcome}, not #{result.expected} as expected " <>
              "(#{explain(result.reason)}): update the expectation"
          )

        :regression ->
          :ok
      end
    end)

    matrix =
      results
      |> Enum.group_by(&{&1.expected, &1.outcome, &1.reason})
      |> Enum.map(fn {{expected, outcome, reason}, group} ->
        %{
          expected: expected,
          outcome: outcome,
          reason: reason,
          explanation: explain(reason),
          cases: length(group)
        }
      end)
      |> Enum.sort_by(&{&1.reason, &1.outcome})

    Soak.record(context, :matrix, matrix)
    Soak.record(context, :smolnet_mtu, settings.mtu)

    Enum.each(matrix, fn row ->
      Soak.note(
        context,
        "#{row.cases} #{if row.cases == 1, do: "case", else: "cases"} #{row.outcome} " <>
          "(expected #{row.expected}): #{row.explanation}"
      )
    end)

    for %{verdict: :regression} = result <- results do
      Soak.fail(
        context,
        :regression,
        "#{result.id} #{result.outcome} (expected #{result.expected}): #{result.detail}",
        [{"case", result}]
      )
    end

    :ok
  end
end
