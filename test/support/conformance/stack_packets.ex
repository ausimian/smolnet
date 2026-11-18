defmodule SmolNet.Conformance.StackPackets do
  @moduledoc false

  # StreamData generators of packets that reach the IP layer's corners,
  # for comparing the stack's own replies across backends: every source
  # and destination class the interface treats differently, IPv4 headers
  # with options, TOS, identification and either DF, IPv6 headers with a
  # traffic class and flow label, Hop-by-Hop headers with any options,
  # ICMP errors that report a path MTU, and payloads long enough that an
  # ICMP error must truncate its quote. Most are well formed with correct
  # checksums; some are damaged, as `Generators.packet/2` damages them.
  #
  # `config/0` is the interface the addresses are chosen against.

  import ExUnitProperties, only: [gen: 2]
  import StreamData

  alias SmolNet.Conformance.Generators
  alias SmolNet.Conformance.Packets
  alias SmolNet.Core.Wire.Hbh
  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Ipv6

  @local4 <<192, 0, 2, 1>>
  @peer4 <<192, 0, 2, 9>>
  @second4 <<198, 51, 100, 7>>
  @local6 <<0xFD00::16, 0::96, 1::16>>
  @link_local6 <<0xFE80::16, 0::96, 1::16>>
  @peer6 <<0xFD00::16, 0::96, 9::16>>

  @doc "The addresses and routes of the stacks the packets are sent to."
  def config do
    [
      addresses: [
        {{192, 0, 2, 1}, 24},
        {{198, 51, 100, 7}, 31},
        {{0xFD00, 0, 0, 0, 0, 0, 0, 1}, 64},
        {{0xFE80, 0, 0, 0, 0, 0, 0, 1}, 64}
      ],
      routes: [{{0, 0, 0, 0}, 0, {192, 0, 2, 254}}]
    ]
  end

  @doc "An IPv4 or IPv6 packet for, or near, the stack of `config/0`."
  def packet do
    tuple({one_of([ipv4(), ipv6()]), damage()})
    |> map(fn {packet, damage} -> damage.(packet) end)
  end

  defp ipv4_source do
    member_of([
      @peer4,
      <<203, 0, 113, 5>>,
      <<0, 0, 0, 0>>,
      <<255, 255, 255, 255>>,
      <<192, 0, 2, 255>>,
      <<224, 0, 0, 5>>,
      @local4
    ])
  end

  defp ipv4_destination do
    frequency([
      {6, constant(@local4)},
      {1, constant(@second4)},
      {1, constant(<<192, 0, 2, 255>>)},
      {1, constant(<<255, 255, 255, 255>>)},
      {1, constant(<<224, 0, 0, 1>>)},
      {1, constant(<<224, 0, 0, 2>>)},
      {1, constant(<<192, 0, 2, 77>>)},
      {1, constant(<<0, 0, 0, 0>>)}
    ])
  end

  defp ipv6_source do
    member_of([
      @peer6,
      <<0xFE80::16, 0::96, 9::16>>,
      <<0x2001::16, 0xDB8::16, 0::80, 5::16>>,
      <<0::128>>,
      <<0xFF02::16, 0::96, 1::16>>,
      @local6
    ])
  end

  defp ipv6_destination do
    frequency([
      {6, constant(@local6)},
      {1, constant(@link_local6)},
      {1, constant(<<0xFF02::16, 0::96, 1::16>>)},
      {1, constant(<<0xFF05::16, 0::96, 1::16>>)},
      {1, constant(<<0::120, 1>>)},
      # Shares only its last two bytes with a local address.
      {1, constant(<<0x2001::16, 0xDB8::16, 0::64, 0x77::16, 1::16>>)},
      {1, constant(<<0xFD00::16, 0::96, 0x42::16>>)}
    ])
  end

  defp ipv4 do
    gen all(
          src <- ipv4_source(),
          dst <- ipv4_destination(),
          {protocol, payload} <- transport(src, dst),
          dscp <- frequency([{3, constant(0)}, {1, integer(0..63)}]),
          ident <- frequency([{3, constant(0)}, {1, integer(0..0xFFFF)}]),
          dont_frag <- frequency([{3, constant(true)}, {1, constant(false)}]),
          ttl <- integer(0..255),
          options <-
            frequency([
              {4, constant(<<>>)},
              {1, map(integer(1..10), &:binary.copy(<<1>>, &1 * 4))}
            ])
        ) do
      Ipv4.build(%{
        dscp: dscp,
        ident: ident,
        dont_frag: dont_frag,
        hop_limit: ttl,
        protocol: protocol,
        src: src,
        dst: dst,
        options: options,
        payload: payload
      })
    end
  end

  defp ipv6 do
    gen all(
          src <- ipv6_source(),
          dst <- ipv6_destination(),
          {next_header, payload} <- transport(src, dst),
          hop_by_hop <- frequency([{3, constant(nil)}, {1, hop_by_hop()}]),
          traffic_class <- frequency([{3, constant(0)}, {1, integer(0..255)}]),
          flow_label <- frequency([{3, constant(0)}, {1, integer(0..0xFFFFF)}]),
          hop_limit <- integer(0..255)
        ) do
      {next_header, payload} =
        case hop_by_hop do
          nil -> {next_header, payload}
          options -> {0, hop_by_hop_header(next_header, options) <> payload}
        end

      Ipv6.build(%{
        traffic_class: traffic_class,
        flow_label: flow_label,
        next_header: next_header,
        hop_limit: hop_limit,
        src: src,
        dst: dst,
        payload: payload
      })
    end
  end

  # Up to six options, so some headers carry more than smoltcp's four.
  defp hop_by_hop do
    list_of(
      one_of([
        constant(:pad1),
        map(integer(0..6), &{:padn, &1}),
        map(integer(0..2), &{:router_alert, &1}),
        gen all(
              action <- member_of([0x00, 0x40, 0x80, 0xC0]),
              low <- integer(2..0x1F),
              data <- binary(max_length: 6)
            ) do
          {:unknown, action + low, byte_size(data), data}
        end
      ]),
      max_length: 6
    )
  end

  defp hop_by_hop_header(next_header, options) do
    data = Hbh.emit(options)

    padding =
      case rem(8 - rem(byte_size(data) + 2, 8), 8) do
        0 -> <<>>
        1 -> Hbh.emit([:pad1])
        n -> Hbh.emit([{:padn, n - 2}])
      end

    data = data <> padding
    <<next_header, div(byte_size(data) + 2, 8) - 1, data::binary>>
  end

  defp transport(src, dst) do
    icmp = if byte_size(src) == 4, do: 1, else: 58

    frequency([
      {3, map(Generators.tcp(src, dst), &{6, &1})},
      {3, map(udp(src, dst), &{17, &1})},
      {3, map(Generators.icmp(src, dst), &{icmp, &1})},
      {2, map(path_mtu_error(src, dst), &{icmp, &1})},
      {1, map(big_payload(), &{17, Packets.udp(src, dst, 40_000, 9, &1)})},
      {1, tuple({member_of([0, 2, 41, 43, 44, 50, 59, 60, 132, 253]), binary(max_length: 64)})}
    ])
  end

  defp udp(src, dst) do
    gen all(
          datagram <- Generators.udp(src, dst),
          zero <- frequency([{4, constant(false)}, {1, constant(true)}])
        ) do
      if zero do
        <<ports::32, length::16, _checksum::16, rest::binary>> = datagram
        <<ports::32, length::16, 0::16, rest::binary>>
      else
        datagram
      end
    end
  end

  defp big_payload, do: map(integer(500..1_300), &:binary.copy(<<0xA5>>, &1))

  # A Fragmentation Needed or Packet Too Big error quoting a TCP segment
  # from the stack (or not), with any MTU.
  defp path_mtu_error(<<_::32>> = peer, local) do
    gen all(
          next_hop_mtu <- one_of([constant(0), integer(68..1_600)]),
          total_len <- integer(40..2_000),
          from_us <- boolean(),
          quoted_tcp <- binary(min_length: 0, max_length: 12),
          checksum_ok <- frequency([{4, constant(true)}, {1, constant(false)}])
        ) do
      src = if from_us, do: local, else: <<203, 0, 113, 5>>

      quoted =
        Ipv4.build(%{
          total_len: total_len,
          hop_limit: 64,
          protocol: 6,
          dont_frag: true,
          src: src,
          dst: peer,
          payload: quoted_tcp
        })

      message = fn checksum ->
        <<3, 4, checksum::16, 0::16, next_hop_mtu::16, quoted::binary>>
      end

      checksum = if checksum_ok, do: Packets.checksum(message.(0)), else: 0x1234
      message.(checksum)
    end
  end

  defp path_mtu_error(peer, local) do
    gen all(
          mtu <- integer(0..2_000),
          payload_len <- integer(0..1_600),
          next_header <- member_of([6, 6, 17]),
          from_us <- boolean(),
          quoted_tcp <- binary(min_length: 0, max_length: 12)
        ) do
      src = if from_us, do: local, else: <<0x2001::16, 0xDB8::16, 0::80, 5::16>>

      header =
        Ipv6.build(%{
          payload_len: payload_len,
          next_header: next_header,
          hop_limit: 64,
          src: src,
          dst: peer
        })

      message = fn checksum ->
        <<2, 0, checksum::16, mtu::32, header::binary, quoted_tcp::binary>>
      end

      length = byte_size(message.(0))
      pseudo = <<peer::binary, local::binary, length::32, 0::24, 58>>
      message.(Packets.checksum(pseudo <> message.(0)))
    end
  end

  defp damage do
    frequency([
      {6, constant(& &1)},
      {1, map(integer(0..1_000), fn cut -> &binary_part(&1, 0, min(cut, byte_size(&1))) end)},
      {1,
       map(tuple({integer(0..1_000), integer(0..255)}), fn {at, byte} ->
         fn packet ->
           at = rem(at, byte_size(packet))
           head = binary_part(packet, 0, at)
           tail = binary_part(packet, at + 1, byte_size(packet) - at - 1)
           <<head::binary, byte, tail::binary>>
         end
       end)}
    ])
  end
end
