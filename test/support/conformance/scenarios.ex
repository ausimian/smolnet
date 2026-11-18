defmodule SmolNet.Conformance.Scenarios do
  @moduledoc false

  # The scenarios whose traces are committed in test/conformance/traces:
  # what the NIF did, call for call, for another backend to be compared
  # against once the NIF is gone. `scripts/backend_traces.exs` records them;
  # each must be deterministic on the NIF, which the differential test
  # checks by replaying them.

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.LinkSim
  alias SmolNet.Conformance.Packets
  alias SmolNet.Conformance.StackPackets
  alias SmolNet.Conformance.Trace
  alias SmolNet.Conformance.UdpCalls

  @transfers [
    {"tcp_clean_inet", "A 32 KiB transfer and close over a clean IPv4 link.",
     [seed: 1, family: :inet]},
    {"tcp_clean_inet6", "A 32 KiB transfer and close over a clean IPv6 link.",
     [seed: 2, family: :inet6]},
    {"tcp_loss_inet", "A 32 KiB transfer over IPv4 losing 5% of packets.",
     [seed: 3, family: :inet, profile: %{loss: 0.05}]},
    {"tcp_reorder_inet6", "A 32 KiB transfer over IPv6 reordering 10% of packets, with jitter.",
     [seed: 4, family: :inet6, profile: %{reorder: 0.1, jitter: 5}]},
    {"tcp_duplicate_inet", "A 32 KiB transfer over IPv4 duplicating 5% of packets.",
     [seed: 5, family: :inet, profile: %{duplicate: 0.05}]},
    {"tcp_impaired_inet",
     "A 32 KiB transfer over IPv4 with 3% loss, 3% duplication, 5% reordering and jitter.",
     [seed: 6, family: :inet, profile: %{loss: 0.03, duplicate: 0.03, reorder: 0.05, jitter: 10}]}
  ]

  # Transfers aimed at one loss-recovery path each, by losing chosen
  # packets: `{name, description, options, losses}`, where `losses` picks
  # the packets to lose from a run that loses none of them (see `losses/3`).
  @recovery [
    {"tcp_newreno_inet",
     "A 64 KiB transfer over IPv4 without SACK, losing three segments in a row: " <>
       "limited transmit, fast retransmit, then a resend per partial ACK (NewReno).",
     [seed: 7, family: :inet, bytes: 65_536, strip_sack: true], [{:nth_data, :client, 15, 3}]},
    {"tcp_tail_loss_inet6",
     "A 48 KiB transfer over IPv6 losing its last two segments, the FIN with them: " <>
       "RACK-TLP's loss probe resends the last MSS, and the recovery its SACK starts the rest.",
     [seed: 8, family: :inet6, bytes: 49_152], [{:tail, :client, 2}]},
    {"tcp_rto_backoff_inet",
     "A 32 KiB transfer over IPv4 losing 20 segments in a row from its 12th, resends " <>
       "included: the retransmission timer fires four times, its timeout doubling each time.",
     [seed: 9, family: :inet, bytes: 32_768], [{:nth_data, :client, 12, 20}]},
    {"tcp_cubic_inet",
     "A 256 KiB transfer over IPv4 with 256 KiB buffers and 25 ms each way, losing a " <>
       "segment of its first flight and then two more, each several round trips after " <>
       "the last: CUBIC's window falls at each loss and grows again between them.",
     [seed: 10, family: :inet, bytes: 262_144, buffer: 262_144, profile: %{delay: 25}],
     [{:nth_data, :client, 8, 1}, {:nth_data, :client, 80, 1}, {:nth_data, :client, 150, 1}]},
    {"tcp_dsack_inet6",
     "A 32 KiB transfer over IPv6 duplicating 20% of packets and reordering 10%: " <>
       "the receiver reports what it already had in D-SACK blocks.",
     [seed: 11, family: :inet6, profile: %{duplicate: 0.2, reorder: 0.1, jitter: 3}], []},
    {"tcp_lossy_probe_inet6",
     "A 3.6 KiB transfer over IPv6 from the server, with 2 KiB buffers, losing 23% " <>
       "and reordering 14% of packets over 100 ms each way: a resent loss probe " <>
       "repairs a loss, and CUBIC responds to it.",
     [
       seed: 1_867_224_222,
       family: :inet6,
       bytes: 3_658,
       sender: :server,
       buffer: 2_048,
       mtu: {1_280, 3_998},
       profile: %{loss: 0.2341796875, reorder: 0.1374114990234375, delay: 100, jitter: 30}
     ], []},
    {"tcp_small_window_inet",
     "A 24 KiB transfer over IPv4 from the server, with 2 KiB buffers and a 1280-byte " <>
       "MTU at the client: the server sends at most the client's MSS, into a window " <>
       "that fills and reopens as the client reads.",
     [
       seed: 12,
       family: :inet,
       bytes: 24_576,
       sender: :server,
       buffer: 2_048,
       mtu: {1_280, 9_000}
     ], []}
  ]

  @doc "Records every scenario on `backend`, as traces."
  def record(backend) do
    transfers =
      for {name, description, options} <- @transfers do
        Trace.new(name, description, transfer(name, backend, options))
      end

    recovery =
      for {name, description, options, losses} <- @recovery do
        options = Keyword.put(options, :drop, losses(backend, options, losses))
        Trace.new(name, description, transfer(name, backend, options))
      end

    transfers ++
      recovery ++ [ingress_replies(backend), stack_replies(backend), udp_sockets(backend)]
  end

  defp transfer(name, backend, options) do
    result = LinkSim.run([backend: backend] ++ options)

    if result.status != :ok,
      do: raise("scenario #{name} did not complete: #{inspect(result.status)}")

    result.entries
  end

  @doc """
  A `:drop` rule for `LinkSim.run/1` that loses the packets `losses`
  picks, each counted in a run that loses the ones picked before it, so a
  pick names a packet as the stacks then send it:

  - `{:nth_data, side, n, count}`: `side`'s `n`th segment with data and
    the `count - 1` it sends after it.
  - `{:tail, side, count}`: the first sending of `side`'s last `count`
    segments, by where they end, a FIN counting as data.
  - `{:resends, side, n, count}`: `side`'s `n`th segment with data and its
    first `count - 1` resends.
  """
  def losses(backend, options, losses) do
    dropped =
      Enum.reduce(losses, MapSet.new(), fn loss, dropped ->
        side = elem(loss, 1)

        {dropped, _picked} =
          Enum.reduce(1..picks(loss), {dropped, []}, fn _pick, {dropped, picked} ->
            packets = data_packets(backend, options, dropped, side)
            packet = pick(loss, packets, picked, dropped)
            {MapSet.put(dropped, {side, elem(packet, 0)}), [packet | picked]}
          end)

        dropped
      end)

    fn side, n, _packet -> MapSet.member?(dropped, {side, n}) end
  end

  defp picks({:nth_data, _side, _n, count}), do: count
  defp picks({:tail, _side, count}), do: count
  defp picks({:resends, _side, _n, count}), do: count

  # The next packet a loss picks, from those `{index, seq, length}` with
  # data, given those it picked already, latest first.
  defp pick({:nth_data, _side, n, _count}, packets, [], _dropped), do: Enum.at(packets, n - 1)

  defp pick({:nth_data, _side, _n, _count}, packets, [{last, _seq, _length} | _picked], _dropped),
    do: Enum.find(packets, fn {index, _seq, _length} -> index > last end)

  defp pick(
         {:tail, _side, _count},
         [{_index, first, _length} | _rest] = packets,
         picked,
         _dropped
       ) do
    ends =
      for {_index, seq, length} <- picked, do: Integer.mod(seq - first, 0x1_0000_0000) + length

    packets
    |> Enum.map(fn {index, seq, length} = packet ->
      {Integer.mod(seq - first, 0x1_0000_0000) + length, -index, packet}
    end)
    |> Enum.reject(fn {end_, _index, _packet} -> end_ in ends end)
    |> Enum.max()
    |> elem(2)
  end

  defp pick({:resends, _side, n, _count}, packets, [], _dropped), do: Enum.at(packets, n - 1)

  defp pick({:resends, side, _n, _count}, packets, picked, dropped) do
    {_index, seq, _length} = List.last(picked)

    Enum.find(packets, fn {index, s, _length} ->
      s == seq and not MapSet.member?(dropped, {side, index})
    end)
  end

  # Each segment with data or a FIN `side` sent in a run losing `dropped`,
  # as `{index, seq, length}`, `index` counting every packet the side sent
  # and `length` a FIN as one.
  defp data_packets(backend, options, dropped, side) do
    drop = fn from, n, _packet -> MapSet.member?(dropped, {from, n}) end
    result = LinkSim.run([backend: backend, drop: drop] ++ options)

    for(%{stack: ^side} = entry <- result.entries, packet <- entry.emitted, do: packet)
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {packet, index} ->
      case segment(packet) do
        {seq, length} when length > 0 -> [{index, seq, length}]
        _other -> []
      end
    end)
  end

  # A TCP segment's sequence number and payload length.
  defp segment(
         <<4::4, ihl::4, _::binary-size(1), total::16, _::binary-size(5), 6, _::binary>> = packet
       ) do
    tcp_segment(binary_part(packet, ihl * 4, total - ihl * 4))
  end

  defp segment(<<6::4, _::28, _length::16, 6, _hop, _addresses::binary-32, tcp::binary>>),
    do: tcp_segment(tcp)

  defp segment(_packet), do: nil

  defp tcp_segment(<<_ports::32, seq::32, _ack::32, offset::4, _::4, flags, _::binary>> = tcp),
    do: {seq, byte_size(tcp) - offset * 4 + Bitwise.band(flags, 1)}

  @doc """
  UDP sockets: the named sequences of `UdpCalls.scripted/0`, then 30 of
  its random ones with a fixed seed, each on a stack of its own.
  """
  def udp_sockets(backend) do
    random =
      StreamData.bind(StreamData.boolean(), fn two_addresses ->
        StreamData.map(UdpCalls.sequence(two_addresses), &{two_addresses, &1})
      end)
      |> StreamData.seeded(173)
      |> Enum.take(30)

    scripted =
      for {_description, two_addresses, calls} <- UdpCalls.scripted(), do: {two_addresses, calls}

    entries =
      (scripted ++ random)
      |> Enum.with_index(1)
      |> Enum.flat_map(fn {{stack, calls}, n} ->
        {:ok, driver} = Driver.start(:"udp_#{n}", backend, UdpCalls.options(stack), 0)

        driver |> UdpCalls.run(calls) |> Driver.trace()
      end)

    Trace.new("udp_sockets", "UDP socket calls and datagrams, named and random.", entries)
  end

  @local4 <<192, 0, 2, 1>>
  @peer4 <<192, 0, 2, 9>>
  @local6 <<0xFD00::16, 0::96, 1::16>>
  @peer6 <<0xFD00::16, 0::96, 9::16>>

  @doc """
  What a stack answers by itself: echo replies, Port and Protocol
  Unreachable, Parameter Problem, a RST for a closed port, silence for an
  IPv4 fragment, and a datagram for a bound UDP socket.
  """
  def ingress_replies(backend) do
    options = [
      addresses: [{{192, 0, 2, 1}, 24}, {{0xFD00, 0, 0, 0, 0, 0, 0, 1}, 64}],
      test_determinism: %{random_seed: 1, timestamp_origin: 0, ephemeral_port_start: 50_000}
    ]

    {:ok, driver} = Driver.start(:stack, backend, options, 0)
    {{:ok, udp}, driver} = Driver.call(driver, :udp_open, [:inet], 0)
    bound = %{address: [192, 0, 2, 1], port: 7, scope_id: 0}
    {{:ok, :ok}, driver} = Driver.call(driver, :udp_bind, [udp, bound], 0)

    syn = %{source: 40_000, destination: 81, seq: 1_000, flags: [:syn], window: 1_000}

    packets = [
      Packets.ipv4(@peer4, @local4, 1, Packets.icmpv4_echo(1, 1, "ping")),
      Packets.ipv6(@peer6, @local6, 58, Packets.icmpv6_echo(@peer6, @local6, 1, 1, "ping")),
      Packets.ipv4(@peer4, @local4, 17, Packets.udp(@peer4, @local4, 40_000, 7, "to a socket")),
      Packets.ipv4(@peer4, @local4, 17, Packets.udp(@peer4, @local4, 40_000, 9, "closed")),
      Packets.ipv6(@peer6, @local6, 17, Packets.udp(@peer6, @local6, 40_000, 9, "closed")),
      Packets.ipv4(@peer4, @local4, 6, Packets.tcp(@peer4, @local4, syn)),
      Packets.ipv6(@peer6, @local6, 6, Packets.tcp(@peer6, @local6, syn)),
      Packets.ipv4(@peer4, @local4, 253, "unknown protocol"),
      Packets.ipv6(@peer6, @local6, 253, "unknown protocol"),
      Packets.ipv6(
        @peer6,
        @local6,
        0,
        Packets.ipv6_options(58, Packets.icmpv6_echo(@peer6, @local6, 2, 1, "hop by hop"))
      ),
      Packets.ipv6(
        @peer6,
        @local6,
        60,
        Packets.ipv6_options(58, Packets.icmpv6_echo(@peer6, @local6, 3, 1, "destination"))
      ),
      Packets.ipv4(@peer4, @local4, 1, Packets.icmpv4_echo(4, 1, "fragment"), flags: 0b001)
    ]

    driver =
      packets
      |> Enum.with_index(1)
      |> Enum.reduce(driver, fn {packet, at}, driver ->
        {_result, driver} = Driver.call(driver, :stack_ingress, [packet], at)
        driver
      end)

    {ref, driver} = Driver.ref(driver)

    {{:ok, {:ok, _source, _destination, "to a socket", false}}, driver} =
      Driver.call(driver, :udp_recvfrom, [udp, 100, ref], 20)

    Trace.new(
      "ingress_replies",
      "What a stack answers by itself to IPv4 and IPv6 packets.",
      Driver.trace(driver)
    )
  end

  @broadcast4 <<192, 0, 2, 255>>
  @all_nodes <<0xFF02::16, 0::96, 1::16>>
  @link_local_peer <<0xFE80::16, 0::96, 9::16>>
  # Not the stack's, but shares its last two bytes with fd00::1.
  @last_two_bytes <<0x2001::16, 0xDB8::16, 0::64, 0x77::16, 1::16>>

  @doc """
  What a stack with no sockets answers by itself, the IP layer's corners
  included: named cases first, then packets from
  `StackPackets.packet/0` with a fixed seed. No call needs a socket, so
  this is the trace the Elixir backend replays before it has any.
  """
  def stack_replies(backend) do
    {:ok, driver} = Driver.start(:stack, backend, StackPackets.config(), 0)
    random = StackPackets.packet() |> StreamData.seeded(173) |> Enum.take(300)

    driver =
      (named_packets() ++ random)
      |> Enum.with_index(1)
      |> Enum.reduce(driver, fn {packet, at}, driver ->
        {_result, driver} = Driver.call(driver, :stack_ingress, [packet], at)
        driver
      end)

    {_snapshot, driver} = Driver.call(driver, :stack_poll, [], 1_000)

    Trace.new(
      "stack_replies",
      "What a stack with no sockets answers, the IP layer's corners included.",
      Driver.trace(driver)
    )
  end

  defp named_packets do
    echo4 = Packets.icmpv4_echo(7, 1, "ping")
    echo6 = &Packets.icmpv6_echo(&1, &2, 7, 1, "ping")
    syn = %{source: 40_000, destination: 81, seq: 1_000, flags: [:syn], window: 1_000}

    [
      # Echo to the subnet's broadcast address, answered from the first
      # IPv4 address, and to 224.0.0.1, not answered.
      Packets.ipv4(@peer4, @broadcast4, 1, echo4),
      Packets.ipv4(@peer4, <<224, 0, 0, 1>>, 1, echo4),
      # A SYN to the broadcast address draws a RST from it.
      Packets.ipv4(@peer4, @broadcast4, 6, Packets.tcp(@peer4, @broadcast4, syn)),
      # A SYN with data, and a segment with neither SYN nor ACK.
      Packets.ipv4(
        @peer4,
        @local4,
        6,
        Packets.tcp(@peer4, @local4, Map.put(syn, :payload, "data"))
      ),
      Packets.ipv4(
        @peer4,
        @local4,
        6,
        Packets.tcp(@peer4, @local4, %{syn | flags: [:psh], seq: 7})
      ),
      # Port Unreachable quoting a long datagram, truncated to fit 576 and
      # 1280 bytes.
      Packets.ipv4(
        @peer4,
        @local4,
        17,
        Packets.udp(@peer4, @local4, 40_000, 9, :binary.copy("x", 1_000))
      ),
      Packets.ipv6(
        @peer6,
        @local6,
        17,
        Packets.udp(@peer6, @local6, 40_000, 9, :binary.copy("x", 1_300))
      ),
      # Echo to all nodes, answered from the chosen source address, for a
      # unique-local and a link-local peer.
      Packets.ipv6(@peer6, @all_nodes, 58, echo6.(@peer6, @all_nodes)),
      Packets.ipv6(@link_local_peer, @all_nodes, 58, echo6.(@link_local_peer, @all_nodes)),
      # Echo to ::1, and to an address taken as a solicited-node group.
      Packets.ipv6(@peer6, <<0::120, 1>>, 58, echo6.(@peer6, <<0::120, 1>>)),
      Packets.ipv6(@peer6, @last_two_bytes, 58, echo6.(@peer6, @last_two_bytes)),
      # Hop-by-Hop: an unrecognised option that asks for an error; one that
      # asks for an error only to unicast, sent to all nodes; the fifth
      # option, which is never read; and a second Hop-by-Hop header.
      Packets.ipv6(
        @peer6,
        @local6,
        0,
        hop_by_hop(58, <<0x9E, 2, 0, 0>>, echo6.(@peer6, @local6))
      ),
      Packets.ipv6(
        @peer6,
        @all_nodes,
        0,
        hop_by_hop(58, <<0xDE, 2, 0, 0>>, echo6.(@peer6, @all_nodes))
      ),
      Packets.ipv6(
        @peer6,
        @local6,
        0,
        hop_by_hop(58, <<0, 0, 0, 0, 0x9E, 0, 0, 0>>, echo6.(@peer6, @local6))
      ),
      Packets.ipv6(
        @peer6,
        @local6,
        0,
        hop_by_hop(0, <<1, 2, 0, 0>>, hop_by_hop(58, <<1, 2, 0, 0>>, echo6.(@peer6, @local6)))
      )
    ]
  end

  # A Hop-by-Hop header of `options` (padded to 6 bytes with PadN), then
  # `payload`.
  defp hop_by_hop(next_header, options, payload) do
    padding = 6 - rem(byte_size(options), 8)

    padded =
      case padding do
        0 -> options
        1 -> options <> <<0>>
        n when n > 1 -> options <> <<1, n - 2>> <> :binary.copy(<<0>>, n - 2)
        n -> options <> <<1, n + 6>> <> :binary.copy(<<0>>, n + 6)
      end

    <<next_header, div(byte_size(padded) + 2, 8) - 1, padded::binary, payload::binary>>
  end
end
