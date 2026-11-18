defmodule SmolNet.Core.Wire.TcpTest do
  # Ported from smoltcp's `wire::tcp` unit tests
  # (native/vendor/smoltcp/src/wire/tcp.rs).
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Tcp

  require Tcp

  @tcp 6

  @src_addr <<192, 168, 1, 1>>
  @dst_addr <<192, 168, 1, 2>>

  @packet_bytes <<0xBF, 0x00, 0x00, 0x50, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x60,
                  0x35, 0x01, 0x23, 0x01, 0xB6, 0x02, 0x01, 0x03, 0x03, 0x0C, 0x01, 0xAA, 0x00,
                  0x00, 0xFF>>

  @option_bytes <<0x03, 0x03, 0x0C, 0x01>>

  @payload_bytes <<0xAA, 0x00, 0x00, 0xFF>>

  @syn_packet_bytes <<0xBF, 0x00, 0x00, 0x50, 0x01, 0x23, 0x45, 0x67, 0x00, 0x00, 0x00, 0x00,
                      0x50, 0x02, 0x01, 0x23, 0x7A, 0x8D, 0x00, 0x00, 0xAA, 0x00, 0x00, 0xFF>>

  defp packet_repr do
    Tcp.tcp(
      src_port: 48_896,
      dst_port: 80,
      seq: 0x01234567,
      ack: nil,
      window: 0x0123,
      window_scale: nil,
      control: :syn,
      mss: nil,
      sack_permitted: false,
      sack_ranges: [],
      timestamp: nil,
      payload: @payload_bytes
    )
  end

  # smoltcp's `assert_option_parses!`: the option parses from `data` with
  # nothing left over, and emits back to `data`.
  defp assert_option_parses(option, data) do
    assert Tcp.parse_option(data) == {:ok, option, <<>>}
    assert Tcp.emit_option(option) == data
  end

  test "deconstruct" do
    assert {:ok, packet} = Tcp.fields(@packet_bytes)
    assert packet.src_port == 48_896
    assert packet.dst_port == 80
    assert packet.seq == 0x01234567
    assert packet.ack == 0x89ABCDEF
    assert packet.header_len == 24
    assert packet.fin
    refute packet.syn
    assert packet.rst
    refute packet.psh
    assert packet.ack_flag
    assert packet.urg
    assert packet.window == 0x0123
    assert packet.urgent == 0x0201
    assert packet.checksum == 0x01B6
    assert packet.options == @option_bytes
    assert packet.payload == @payload_bytes

    # `verify_checksum`: FIN with RST is a forbidden combination, so
    # `parse/3` would refuse the segment; check the sum directly.
    assert Checksum.valid?(
             @packet_bytes,
             Checksum.pseudo_header(@src_addr, @dst_addr, @tcp, byte_size(@packet_bytes))
           )
  end

  test "construct" do
    # The Rust test sets checksum 0xEEEE and then fills it; a nil
    # `:checksum` is the fill.
    fields = %{
      src_port: 48_896,
      dst_port: 80,
      seq: 0x01234567,
      ack: 0x89ABCDEF,
      header_len: 24,
      fin: true,
      syn: false,
      rst: true,
      psh: false,
      ack_flag: true,
      urg: true,
      window: 0x0123,
      urgent: 0x0201,
      options: @option_bytes,
      payload: @payload_bytes
    }

    assert Tcp.build(fields, @src_addr, @dst_addr) == @packet_bytes
  end

  test "truncated" do
    truncated = binary_part(@packet_bytes, 0, 23)

    # `check_len`: the data offset (24) runs past the buffer.
    assert {:ok, %{header_len: 24, options: nil, payload: nil}} = Tcp.fields(truncated)
    assert Tcp.parse(truncated, @src_addr, @dst_addr) == :error
  end

  test "impossible_len" do
    # `set_header_len(10)` writes a data offset of 2 words, 8 bytes.
    bytes = Tcp.build(%{header_len: 10, checksum: 0}, @src_addr, @dst_addr)
    assert byte_size(bytes) == 20

    assert {:ok, %{header_len: 8, options: nil, payload: nil}} = Tcp.fields(bytes)
    assert Tcp.parse(bytes, @src_addr, @dst_addr) == :error
  end

  test "parse" do
    assert Tcp.parse(@syn_packet_bytes, @src_addr, @dst_addr) == {:ok, packet_repr()}
  end

  test "emit" do
    repr = packet_repr()
    assert Tcp.buffer_len(repr) == byte_size(@syn_packet_bytes)
    assert Tcp.emit(repr, @src_addr, @dst_addr) == @syn_packet_bytes
  end

  test "header_len_multiple_of_4" do
    # The window scale option needs 3 bytes.
    repr = Tcp.tcp(packet_repr(), window_scale: 0)
    # Should e.g. be 28 instead of 27.
    assert rem(Tcp.header_len(repr), 4) == 0
  end

  test "tcp_options" do
    assert_option_parses(:end_of_list, <<0x00>>)
    assert_option_parses(:nop, <<0x01>>)
    assert_option_parses({:mss, 1500}, <<0x02, 0x04, 0x05, 0xDC>>)
    assert_option_parses({:window_scale, 12}, <<0x03, 0x03, 0x0C>>)
    assert_option_parses(:sack_permitted, <<0x4, 0x02>>)

    assert_option_parses(
      {:sack_range, [{500, 1500}]},
      <<0x05, 0x0A, 0x00, 0x00, 0x01, 0xF4, 0x00, 0x00, 0x05, 0xDC>>
    )

    assert_option_parses(
      {:sack_range, [{875, 1225}, {1500, 2500}]},
      <<0x05, 0x12, 0x00, 0x00, 0x03, 0x6B, 0x00, 0x00, 0x04, 0xC9, 0x00, 0x00, 0x05, 0xDC, 0x00,
        0x00, 0x09, 0xC4>>
    )

    assert_option_parses(
      {:sack_range, [{875_000, 1_225_000}, {1_500_000, 2_500_000}, {876_543_210, 876_654_320}]},
      <<0x05, 0x1A, 0x00, 0x0D, 0x59, 0xF8, 0x00, 0x12, 0xB1, 0x28, 0x00, 0x16, 0xE3, 0x60, 0x00,
        0x26, 0x25, 0xA0, 0x34, 0x3E, 0xFC, 0xEA, 0x34, 0x40, 0xAE, 0xF0>>
    )

    assert_option_parses(
      {:timestamp, 5_000_000, 7_000_000},
      # kind, length, tsval, tsecr
      <<0x08, 0x0A, 0x00, 0x4C, 0x4B, 0x40, 0x00, 0x6A, 0xCF, 0xC0>>
    )

    assert_option_parses({:unknown, 12, <<1, 2, 3>>}, <<0x0C, 0x05, 0x01, 0x02, 0x03>>)
  end

  test "malformed_tcp_options" do
    assert Tcp.parse_option(<<>>) == :error
    assert Tcp.parse_option(<<0xC>>) == :error
    assert Tcp.parse_option(<<0xC, 0x05, 0x01, 0x02>>) == :error
    assert Tcp.parse_option(<<0xC, 0x01>>) == :error
    assert Tcp.parse_option(<<0x2, 0x02>>) == :error
    assert Tcp.parse_option(<<0x3, 0x02>>) == :error
  end

  test "tcp_options_summary" do
    # SmolNet has no `TcpOptionSummary`; the record's option fields are its
    # counterpart, so the options go through `parse/3`. The Rust buffer has
    # zero ports and checksum, which `parse/3` refuses, so this segment
    # carries ports and a filled checksum around the same 32-byte header.
    #
    # MSS=1460 (4b: 02 04 05 b4), WS=7 (3b: 03 03 07), SACK_PERM (2b: 04 02),
    # NOP (1b: 01), END (1b: 00)
    options_bytes =
      <<0x02, 0x04, 0x05, 0xB4, 0x03, 0x03, 0x07, 0x04, 0x02, 0x01, 0x00, 0x00>>

    segment =
      Tcp.build(
        %{src_port: 48_896, dst_port: 80, header_len: 32, options: options_bytes},
        @src_addr,
        @dst_addr
      )

    assert byte_size(segment) == 32
    assert {:ok, repr} = Tcp.parse(segment, @src_addr, @dst_addr)
    assert Tcp.tcp(repr, :mss) == 1460
    assert Tcp.tcp(repr, :window_scale) == 7
    assert Tcp.tcp(repr, :sack_permitted)
    assert Tcp.tcp(repr, :sack_ranges) == []
  end
end
