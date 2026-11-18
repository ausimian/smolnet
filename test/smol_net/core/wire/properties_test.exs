defmodule SmolNet.Core.Wire.PropertiesTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # What fuzzing smoltcp's parsers established, for the Elixir ones: they
  # never raise, whatever the bytes, and what they emit parses back to
  # what was emitted.

  alias SmolNet.Core.Wire.Hbh
  alias SmolNet.Core.Wire.Icmpv4
  alias SmolNet.Core.Wire.Icmpv6
  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Ipv6
  alias SmolNet.Core.Wire.Tcp
  alias SmolNet.Core.Wire.Udp

  require Ipv4
  require Ipv6
  require Tcp

  @runs 300

  describe "parsers never raise" do
    property "on any bytes" do
      check all(
              bytes <- binary(max_length: 128),
              src4 <- binary(length: 4),
              dst4 <- binary(length: 4),
              src6 <- binary(length: 16),
              dst6 <- binary(length: 16),
              max_runs: @runs
            ) do
        for parse <- [
              fn -> Ipv4.parse(bytes) end,
              fn -> Ipv4.fields(bytes) end,
              fn -> Ipv6.parse(bytes) end,
              fn -> Ipv6.fields(bytes) end,
              fn -> Hbh.parse_ext_header(bytes) end,
              fn -> Hbh.parse(bytes) end,
              fn -> Hbh.parse_options(bytes) end,
              fn -> Icmpv4.parse(bytes) end,
              fn -> Icmpv4.fields(bytes) end,
              fn -> Icmpv6.parse(src6, dst6, bytes) end,
              fn -> Icmpv6.fields(bytes) end,
              fn -> Tcp.parse(bytes, src4, dst4) end,
              fn -> Tcp.parse(bytes, src6, dst6) end,
              fn -> Tcp.fields(bytes) end,
              fn -> Udp.parse(bytes, src4, dst4) end,
              fn -> Udp.parse(bytes, src6, dst6) end,
              fn -> Udp.fields(bytes) end
            ] do
          result = parse.()
          assert result == :error or is_list(result) or elem(result, 0) == :ok
        end
      end
    end

    property "on emitted packets with a byte changed or the end cut off" do
      check all(
              packet <- one_of([ipv4_packet(), ipv6_packet()]),
              damage <- damage(),
              max_runs: @runs
            ) do
        packet = damage.(packet)

        case packet do
          <<4::4, _rest::bitstring>> -> walk_ipv4(packet)
          <<6::4, _rest::bitstring>> -> walk_ipv6(packet)
          _other -> :ok
        end
      end
    end
  end

  describe "emitted headers parse back" do
    property "IPv4" do
      check all(repr <- ipv4_repr(), payload <- binary(max_length: 64), max_runs: @runs) do
        repr = Ipv4.ipv4(repr, payload_len: byte_size(payload))
        packet = Ipv4.emit(repr, payload)
        assert Ipv4.parse(packet) == {:ok, repr, payload}
        assert {:ok, fields} = Ipv4.fields(packet)
        assert Ipv4.build(fields) == packet
      end
    end

    property "IPv6" do
      check all(repr <- ipv6_repr(), payload <- binary(max_length: 64), max_runs: @runs) do
        repr = Ipv6.ipv6(repr, payload_len: byte_size(payload))
        packet = Ipv6.emit(repr, payload)
        assert Ipv6.parse(packet) == {:ok, repr, payload}
        assert {:ok, fields} = Ipv6.fields(packet)
        assert Ipv6.build(fields) == packet
      end
    end

    property "Hop-by-Hop options" do
      check all(options <- list_of(hbh_option(), min_length: 1, max_length: 4), max_runs: @runs) do
        assert Hbh.parse(Hbh.emit(options)) == {:ok, options}
        assert byte_size(Hbh.emit(options)) == Hbh.buffer_len(options)
      end
    end

    property "ICMPv4" do
      check all(message <- icmpv4_message(), max_runs: @runs) do
        bytes = Icmpv4.emit(message)
        assert byte_size(bytes) == Icmpv4.buffer_len(message)
        assert Icmpv4.parse(bytes) == {:ok, message}
        assert {:ok, fields} = Icmpv4.fields(bytes)
        assert Icmpv4.build(fields) == bytes
      end
    end

    property "ICMPv6" do
      check all(
              message <- icmpv6_message(),
              src <- binary(length: 16),
              dst <- binary(length: 16),
              max_runs: @runs
            ) do
        bytes = Icmpv6.emit(src, dst, message)
        assert byte_size(bytes) == Icmpv6.buffer_len(message)
        assert Icmpv6.parse(src, dst, bytes) == {:ok, message}
        assert {:ok, fields} = Icmpv6.fields(bytes)
        assert Icmpv6.build(src, dst, fields) == bytes
      end
    end

    property "TCP" do
      check all(
              {src, dst} <- addresses(),
              repr <- tcp_repr(),
              max_runs: @runs
            ) do
        bytes = Tcp.emit(repr, src, dst)
        assert byte_size(bytes) == Tcp.buffer_len(repr)
        assert Tcp.parse(bytes, src, dst) == {:ok, repr}
        assert {:ok, fields} = Tcp.fields(bytes)
        assert Tcp.build(fields, src, dst) == bytes
      end
    end

    property "UDP" do
      check all(
              {src, dst} <- addresses(),
              src_port <- integer(0..0xFFFF),
              dst_port <- integer(1..0xFFFF),
              payload <- binary(max_length: 64),
              max_runs: @runs
            ) do
        bytes = Udp.emit(src_port, dst_port, payload, src, dst)
        assert Udp.parse(bytes, src, dst) == {:ok, {src_port, dst_port}, payload}
        assert {:ok, fields} = Udp.fields(bytes)
        assert Udp.build(fields, src, dst) == bytes
      end
    end
  end

  # Parses as the interface does, layer by layer, so that damage deep in a
  # packet reaches the parser of its layer.
  defp walk_ipv4(packet) do
    with {:ok, repr, payload} <- Ipv4.parse(packet) do
      src = Ipv4.ipv4(repr, :src)
      dst = Ipv4.ipv4(repr, :dst)

      case Ipv4.ipv4(repr, :protocol) do
        1 -> Icmpv4.parse(payload)
        6 -> Tcp.parse(payload, src, dst)
        17 -> Udp.parse(payload, src, dst)
        _other -> :ok
      end
    end
  end

  defp walk_ipv6(packet) do
    with {:ok, repr, payload} <- Ipv6.parse(packet) do
      src = Ipv6.ipv6(repr, :src)
      dst = Ipv6.ipv6(repr, :dst)

      case Ipv6.ipv6(repr, :next_header) do
        0 ->
          walk_hop_by_hop(payload)

        58 ->
          Icmpv6.parse(src, dst, payload)

        6 ->
          Tcp.parse(payload, src, dst)

        17 ->
          Udp.parse(payload, src, dst)

        _other ->
          :ok
      end
    end
  end

  defp walk_hop_by_hop(payload) do
    with {:ok, {_next, _length, data}, _rest} <- Hbh.parse_ext_header(payload),
         do: Hbh.parse(data)
  end

  ## Generators

  defp addresses do
    one_of([
      tuple({binary(length: 4), binary(length: 4)}),
      tuple({binary(length: 16), binary(length: 16)})
    ])
  end

  defp ipv4_repr do
    gen all(
          src <- binary(length: 4),
          dst <- binary(length: 4),
          protocol <- integer(0..255),
          hop_limit <- integer(0..255)
        ) do
      Ipv4.ipv4(src: src, dst: dst, protocol: protocol, hop_limit: hop_limit)
    end
  end

  defp ipv6_repr do
    gen all(
          src <- binary(length: 16),
          dst <- binary(length: 16),
          next_header <- integer(0..255),
          hop_limit <- integer(0..255)
        ) do
      Ipv6.ipv6(src: src, dst: dst, next_header: next_header, hop_limit: hop_limit)
    end
  end

  defp ipv4_packet do
    gen all(
          repr <- ipv4_repr(),
          protocol <- member_of([1, 6, 17, 253]),
          payload <- transport(protocol, Ipv4.ipv4(repr, :src), Ipv4.ipv4(repr, :dst))
        ) do
      Ipv4.emit(Ipv4.ipv4(repr, protocol: protocol, payload_len: byte_size(payload)), payload)
    end
  end

  defp ipv6_packet do
    gen all(
          repr <- ipv6_repr(),
          next_header <- member_of([0, 6, 17, 58, 253]),
          payload <- transport(next_header, Ipv6.ipv6(repr, :src), Ipv6.ipv6(repr, :dst))
        ) do
      Ipv6.emit(
        Ipv6.ipv6(repr, next_header: next_header, payload_len: byte_size(payload)),
        payload
      )
    end
  end

  defp transport(0, _src, _dst) do
    map(list_of(hbh_option(), max_length: 6), fn options ->
      data = Hbh.emit(options)
      padding = rem(8 - rem(byte_size(data) + 2, 8), 8)
      data = data <> Hbh.emit(if padding == 0, do: [], else: [padn(padding)])
      <<59, div(byte_size(data) + 2, 8) - 1, data::binary>>
    end)
  end

  defp transport(1, _src, _dst), do: map(icmpv4_message(), &Icmpv4.emit/1)

  defp transport(58, src, dst),
    do: map(icmpv6_message(), &Icmpv6.emit(src, dst, &1))

  defp transport(6, src, dst), do: map(tcp_repr(), &Tcp.emit(&1, src, dst))

  defp transport(17, src, dst) do
    gen all(
          ports <- tuple({integer(0..0xFFFF), integer(0..0xFFFF)}),
          payload <- binary(max_length: 32)
        ) do
      Udp.emit(elem(ports, 0), elem(ports, 1), payload, src, dst)
    end
  end

  defp transport(_protocol, _src, _dst), do: binary(max_length: 32)

  defp padn(1), do: :pad1
  defp padn(padding), do: {:padn, padding - 2}

  defp hbh_option do
    one_of([
      constant(:pad1),
      map(integer(0..8), &{:padn, &1}),
      map(integer(0..0xFFFF), &{:router_alert, &1}),
      gen all(
            type <- filter(integer(2..255), &(&1 != 5)),
            data <- binary(max_length: 8)
          ) do
        {:unknown, type, byte_size(data), data}
      end
    ])
  end

  defp echo do
    gen all(
          kind <- member_of([:echo_request, :echo_reply]),
          ident <- integer(0..0xFFFF),
          seq_no <- integer(0..0xFFFF),
          data <- binary(max_length: 32)
        ) do
      {kind, ident, seq_no, data}
    end
  end

  # An error quoting a whole packet, as smoltcp requires to parse one.
  defp icmpv4_message do
    one_of([
      echo(),
      gen all(
            kind <- member_of([:dst_unreachable, :time_exceeded]),
            code <- integer(0..255),
            header <- ipv4_repr(),
            data <- binary(min_length: 8, max_length: 32)
          ) do
        {kind, code, Ipv4.ipv4(header, payload_len: byte_size(data)), data}
      end
    ])
  end

  defp icmpv6_message do
    one_of([
      echo(),
      gen all(
            kind <- member_of([:dst_unreachable, :time_exceeded]),
            code <- integer(0..255),
            header <- ipv6_repr(),
            payload_len <- integer(0..0xFFFF),
            data <- binary(max_length: 32)
          ) do
        {kind, code, Ipv6.ipv6(header, payload_len: payload_len), data}
      end,
      gen all(
            mtu <- integer(0..0xFFFF_FFFF),
            header <- ipv6_repr(),
            data <- binary(max_length: 32)
          ) do
        {:pkt_too_big, mtu, header, data}
      end,
      gen all(
            code <- integer(0..255),
            pointer <- integer(0..0xFFFF_FFFF),
            header <- ipv6_repr(),
            data <- binary(max_length: 32)
          ) do
        {:param_problem, code, pointer, header, data}
      end
    ])
  end

  # Segments whose options parse back as they were: the SACK blocks go
  # only on an ACK without SACK-permitted, as `emit/3` writes them, and the
  # options fit the header.
  defp tcp_repr do
    gen all(
          ports <- tuple({integer(1..0xFFFF), integer(1..0xFFFF)}),
          control <- member_of([:none, :psh, :syn, :fin, :rst]),
          seq <- integer(0..0xFFFF_FFFF),
          ack <- one_of([constant(nil), integer(0..0xFFFF_FFFF)]),
          window <- integer(0..0xFFFF),
          window_scale <- one_of([constant(nil), integer(0..14)]),
          mss <- one_of([constant(nil), integer(0..0xFFFF)]),
          sack_permitted <- boolean(),
          sack_ranges <-
            list_of(tuple({integer(0..0xFFFF_FFFF), integer(0..0xFFFF_FFFF)}), max_length: 3),
          timestamp <-
            one_of([constant(nil), tuple({integer(0..0xFFFF_FFFF), integer(0..0xFFFF_FFFF)})]),
          payload <- binary(max_length: 32)
        ) do
      sack_ranges = if ack == nil or sack_permitted, do: [], else: sack_ranges

      Tcp.tcp(
        src_port: elem(ports, 0),
        dst_port: elem(ports, 1),
        control: control,
        seq: seq,
        ack: ack,
        window: window,
        window_scale: window_scale,
        mss: mss,
        sack_permitted: sack_permitted,
        sack_ranges: sack_ranges,
        timestamp: timestamp,
        payload: payload
      )
    end
    # The data offset's four bits allow at most 40 bytes of options.
    |> filter(&(Tcp.header_len(&1) <= 60))
  end

  # Leaves a packet as it is, or overwrites a byte, or truncates it.
  defp damage do
    frequency([
      {1, constant(& &1)},
      {3,
       map(tuple({integer(0..1_000), integer(0..255)}), fn {at, byte} ->
         fn packet ->
           at = rem(at, byte_size(packet))
           head = binary_part(packet, 0, at)
           tail = binary_part(packet, at + 1, byte_size(packet) - at - 1)
           <<head::binary, byte, tail::binary>>
         end
       end)},
      {1, map(integer(0..1_000), fn cut -> &binary_part(&1, 0, min(cut, byte_size(&1))) end)}
    ])
  end
end
