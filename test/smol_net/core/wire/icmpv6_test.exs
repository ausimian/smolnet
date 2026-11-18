defmodule SmolNet.Core.Wire.Icmpv6Test do
  # Ported from smoltcp's `wire::icmpv6` unit tests
  # (native/vendor/smoltcp/src/wire/icmpv6.rs).
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Icmpv6
  alias SmolNet.Core.Wire.Ipv6

  require Ipv6

  @mock_ip_addr_1 <<0xFE80::16, 0::96, 1::16>>
  @mock_ip_addr_2 <<0xFE80::16, 0::96, 2::16>>
  @unspecified <<0::128>>

  @udp 17
  @tcp 6

  @echo_request 128
  @pkt_too_big 2

  # field::HEADER_END and field::UNUSED.end in smoltcp.
  @header_end 8
  @unused_end 8
  # MAX_ERROR_PACKET_LEN: IPV6_MIN_MTU - IPV6_HEADER_LEN.
  @max_error_packet_len 1280 - 40

  @echo_packet_bytes <<0x80, 0x00, 0x19, 0xB3, 0x12, 0x34, 0xAB, 0xCD, 0xAA, 0x00, 0x00, 0xFF>>

  @echo_packet_payload <<0xAA, 0x00, 0x00, 0xFF>>

  @pkt_too_big_bytes <<0x02, 0x00, 0x0F, 0xC9, 0x00, 0x00, 0x05, 0xDC, 0x60, 0x00, 0x00, 0x00,
                       0x00, 0x0C, 0x11, 0x40, 0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFE, 0x80, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
                       0xBF, 0x00, 0x00, 0x35, 0x00, 0x0C, 0x12, 0x4D, 0xAA, 0x00, 0x00, 0xFF>>

  @pkt_too_big_ip_payload <<0x60, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x11, 0x40, 0xFE, 0x80, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x01, 0xFE, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xBF, 0x00, 0x00, 0x35,
                            0x00, 0x0C, 0x12, 0x4D, 0xAA, 0x00, 0x00, 0xFF>>

  @pkt_too_big_udp_payload <<0xBF, 0x00, 0x00, 0x35, 0x00, 0x0C, 0x12, 0x4D, 0xAA, 0x00, 0x00,
                             0xFF>>

  defp echo_packet_repr, do: {:echo_request, 0x1234, 0xABCD, @echo_packet_payload}

  defp too_big_packet_repr do
    {:pkt_too_big, 1500,
     Ipv6.ipv6(
       src: <<0xFE80::16, 0::96, 1::16>>,
       dst: <<0xFE80::16, 0::96, 2::16>>,
       next_header: @udp,
       payload_len: 12,
       hop_limit: 0x40
     ), @pkt_too_big_udp_payload}
  end

  # Packet::verify_checksum.
  defp verify_checksum(packet, src, dst),
    do: Checksum.valid?(packet, Checksum.pseudo_header(src, dst, 58, byte_size(packet)))

  test "echo_deconstruct" do
    assert {:ok, fields} = Icmpv6.fields(@echo_packet_bytes)
    assert fields.msg_type == @echo_request
    assert fields.msg_code == 0
    assert fields.checksum == 0x19B3
    assert fields.echo_ident == 0x1234
    assert fields.echo_seq_no == 0xABCD
    assert fields.payload == @echo_packet_payload
    assert verify_checksum(@echo_packet_bytes, @mock_ip_addr_1, @mock_ip_addr_2)
    # Message::is_error has no counterpart; an echo request is not an error.
    refute fields.msg_type in [1, 2, 3, 4]
  end

  test "echo_construct" do
    packet =
      Icmpv6.build(@mock_ip_addr_1, @mock_ip_addr_2, %{
        msg_type: @echo_request,
        msg_code: 0,
        echo_ident: 0x1234,
        echo_seq_no: 0xABCD,
        payload: @echo_packet_payload
      })

    assert packet == @echo_packet_bytes
  end

  test "echo_repr_parse" do
    assert Icmpv6.parse(@mock_ip_addr_1, @mock_ip_addr_2, @echo_packet_bytes) ==
             {:ok, echo_packet_repr()}
  end

  test "echo_emit" do
    repr = echo_packet_repr()
    packet = Icmpv6.emit(@mock_ip_addr_1, @mock_ip_addr_2, repr)
    assert byte_size(packet) == Icmpv6.buffer_len(repr)
    assert packet == @echo_packet_bytes
  end

  test "too_big_deconstruct" do
    assert {:ok, fields} = Icmpv6.fields(@pkt_too_big_bytes)
    assert fields.msg_type == @pkt_too_big
    assert fields.msg_code == 0
    assert fields.checksum == 0x0FC9
    # Packet::pkt_too_big_mtu
    assert fields.word == 1500
    assert fields.payload == @pkt_too_big_ip_payload
    assert verify_checksum(@pkt_too_big_bytes, @mock_ip_addr_1, @mock_ip_addr_2)
    # Message::is_error has no counterpart; Packet Too Big is an error.
    assert fields.msg_type in [1, 2, 3, 4]
  end

  test "too_big_construct" do
    packet =
      Icmpv6.build(@mock_ip_addr_1, @mock_ip_addr_2, %{
        msg_type: @pkt_too_big,
        msg_code: 0,
        word: 1500,
        payload: @pkt_too_big_ip_payload
      })

    assert packet == @pkt_too_big_bytes
  end

  test "too_big_repr_parse" do
    assert Icmpv6.parse(@mock_ip_addr_1, @mock_ip_addr_2, @pkt_too_big_bytes) ==
             {:ok, too_big_packet_repr()}
  end

  test "too_big_emit" do
    repr = too_big_packet_repr()
    packet = Icmpv6.emit(@mock_ip_addr_1, @mock_ip_addr_2, repr)
    assert byte_size(packet) == Icmpv6.buffer_len(repr)
    assert packet == @pkt_too_big_bytes
  end

  test "buffer_length_is_truncated_to_mtu" do
    repr =
      {:pkt_too_big, 1280,
       Ipv6.ipv6(
         src: @unspecified,
         dst: @unspecified,
         next_header: @tcp,
         hop_limit: 64,
         payload_len: 1280
       ), :binary.copy(<<0>>, 9999)}

    assert Icmpv6.buffer_len(repr) == 1280 - Ipv6.header_len()
  end

  test "mtu_truncated_payload_roundtrip" do
    ip_packet_repr =
      Ipv6.ipv6(
        src: @unspecified,
        dst: @unspecified,
        next_header: @tcp,
        hop_limit: 64,
        payload_len: Ipv6.min_mtu() - Ipv6.header_len()
      )

    ip_packet =
      Ipv6.emit(ip_packet_repr, :binary.copy(<<0>>, Ipv6.min_mtu() - Ipv6.header_len()))

    assert byte_size(ip_packet) == Ipv6.min_mtu()

    repr1 =
      {:pkt_too_big, Ipv6.min_mtu(), ip_packet_repr,
       binary_part(ip_packet, Ipv6.header_len(), byte_size(ip_packet) - Ipv6.header_len())}

    # this is needed to make sure roundtrip gives the same value
    # it is not needed for ensuring the correct bytes get emitted
    data_end = Icmpv6.buffer_len(repr1) - @unused_end

    repr1 =
      {:pkt_too_big, Ipv6.min_mtu(), ip_packet_repr,
       binary_part(ip_packet, Ipv6.header_len(), data_end - Ipv6.header_len())}

    data = Icmpv6.emit(@mock_ip_addr_1, @mock_ip_addr_2, repr1)
    assert byte_size(data) == @max_error_packet_len

    assert {:ok, repr2} = Icmpv6.parse(@mock_ip_addr_1, @mock_ip_addr_2, data)
    assert repr1 == repr2
  end

  test "truncated_payload_ipv6_header_parse_fails" do
    repr = too_big_packet_repr()
    bytes = Icmpv6.emit(@mock_ip_addr_1, @mock_ip_addr_2, repr)
    packet = binary_part(bytes, 0, @header_end + Ipv6.header_len() - 1)
    assert Icmpv6.parse(@mock_ip_addr_1, @mock_ip_addr_2, packet) == :error

    # smoltcp parses with ChecksumCapabilities::ignored(), but parse/3
    # always verifies the checksum, so the truncated packet above fails on
    # its checksum. Refill the checksum so it fails on the short quote alone.
    {:ok, fields} = Icmpv6.fields(packet)
    packet = Icmpv6.build(@mock_ip_addr_1, @mock_ip_addr_2, %{fields | checksum: nil})
    assert byte_size(packet) == @header_end + Ipv6.header_len() - 1
    assert verify_checksum(packet, @mock_ip_addr_1, @mock_ip_addr_2)
    assert Icmpv6.parse(@mock_ip_addr_1, @mock_ip_addr_2, packet) == :error
  end
end
