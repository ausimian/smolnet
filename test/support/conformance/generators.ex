defmodule SmolNet.Conformance.Generators do
  @moduledoc false

  # StreamData generators of what a stack may be sent: IPv4 and IPv6 packets
  # carrying TCP, UDP, ICMP or an unknown protocol, with arbitrary header
  # fields and payloads. Most are well formed with correct checksums, so
  # they reach the protocol code; `packet/0` also damages some, by
  # truncating them or overwriting bytes, so they exercise the parsers'
  # rejections too.

  import StreamData

  alias SmolNet.Conformance.Packets

  @doc "A packet from `source` to `destination`, both 4- or 16-byte binaries."
  def packet(source, destination) do
    tuple({well_formed(source, destination), damage()})
    |> bind(fn {packet, damage} -> constant(damage.(packet)) end)
  end

  @doc "A well-formed packet with correct checksums."
  def well_formed(source, destination) do
    bind(transport(source, destination), fn {protocol, payload} ->
      bind(ip_options(), fn options ->
        constant(ip(source, destination, protocol, payload, options))
      end)
    end)
  end

  defp ip(source, destination, protocol, payload, options) when byte_size(source) == 4 do
    protocol = if protocol == :icmp, do: 1, else: protocol
    Packets.ipv4(source, destination, protocol, payload, options)
  end

  defp ip(source, destination, protocol, payload, options) do
    protocol = if protocol == :icmp, do: 58, else: protocol
    options = Keyword.take(options, [:hop_limit])
    Packets.ipv6(source, destination, protocol, payload, options)
  end

  defp ip_options do
    fixed_list([integer(0..255), member_of([0b010, 0b000]), integer(0..0xFFFF)])
    |> map(fn [ttl, flags, identification] ->
      [ttl: ttl, hop_limit: ttl, flags: flags, identification: identification]
    end)
  end

  defp transport(source, destination) do
    one_of([
      map(tcp(source, destination), &{6, &1}),
      map(udp(source, destination), &{17, &1}),
      map(icmp(source, destination), &{:icmp, &1}),
      tuple({member_of([0, 2, 41, 43, 44, 50, 59, 60, 132, 253]), binary(max_length: 64)})
    ])
  end

  @doc "A TCP segment's bytes, with any flags and options."
  def tcp(source, destination) do
    fixed_map(%{
      source: integer(0..0xFFFF),
      destination: one_of([constant(80), integer(0..0xFFFF)]),
      seq: integer(0..0xFFFF_FFFF),
      ack: integer(0..0xFFFF_FFFF),
      flags: list_of(member_of([:fin, :syn, :rst, :psh, :ack]), max_length: 5),
      window: integer(0..0xFFFF),
      options: tcp_options(),
      payload: binary(max_length: 64)
    })
    |> map(&Packets.tcp(source, destination, &1))
  end

  defp tcp_options do
    option =
      one_of([
        map(integer(0..0xFFFF), &<<2, 4, &1::16>>),
        map(integer(0..20), &<<3, 3, &1>>),
        constant(<<4, 2>>),
        map(integer(0..0xFFFF_FFFF), &<<8, 10, &1::32, 0::32>>),
        map(list_of(integer(0..0xFFFF_FFFF), length: 2), fn [l, r] -> <<5, 10, l::32, r::32>> end),
        constant(<<1>>),
        binary(max_length: 6)
      ])

    # TCP options take at most 40 bytes.
    map(list_of(option, max_length: 4), fn options ->
      options = IO.iodata_to_binary(options)
      binary_part(options, 0, min(byte_size(options), 40))
    end)
  end

  @doc "A UDP datagram's bytes."
  def udp(source, destination) do
    tuple({integer(0..0xFFFF), one_of([constant(7), integer(0..0xFFFF)]), binary(max_length: 64)})
    |> map(fn {from, to, payload} -> Packets.udp(source, destination, from, to, payload) end)
  end

  @doc "An ICMP or ICMPv6 message's bytes: an echo request, or any type and code."
  def icmp(source, destination) do
    one_of([
      map(tuple({integer(0..0xFFFF), integer(0..0xFFFF), binary(max_length: 64)}), fn {id, seq,
                                                                                       data} ->
        if byte_size(source) == 4,
          do: Packets.icmpv4_echo(id, seq, data),
          else: Packets.icmpv6_echo(source, destination, id, seq, data)
      end),
      map(tuple({integer(0..255), integer(0..255), binary(max_length: 64)}), fn {type, code, rest} ->
        <<type, code, 0::16, rest::binary>>
      end)
    ])
  end

  # Leaves a packet as it is, or truncates it, or overwrites a byte.
  defp damage do
    frequency([
      {3, constant(& &1)},
      {1, map(integer(0..1_000), fn cut -> &binary_part(&1, 0, min(cut, byte_size(&1))) end)},
      {1,
       map(tuple({integer(0..1_000), integer(0..255)}), fn {at, byte} ->
         fn packet ->
           at = rem(at, byte_size(packet))
           <<head::binary-size(at), _old, tail::binary>> = packet
           <<head::binary, byte, tail::binary>>
         end
       end)}
    ])
  end
end
