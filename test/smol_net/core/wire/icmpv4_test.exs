defmodule SmolNet.Core.Wire.Icmpv4Test do
  # Ported from smoltcp's `wire::icmpv4` unit tests
  # (native/vendor/smoltcp/src/wire/icmpv4.rs).
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Icmpv4

  @echo_request 8

  @echo_packet_bytes <<0x08, 0x00, 0x8E, 0xFE, 0x12, 0x34, 0xAB, 0xCD, 0xAA, 0x00, 0x00, 0xFF>>

  @echo_data_bytes <<0xAA, 0x00, 0x00, 0xFF>>

  defp echo_packet_repr, do: {:echo_request, 0x1234, 0xABCD, @echo_data_bytes}

  test "echo_deconstruct" do
    assert {:ok, packet} = Icmpv4.fields(@echo_packet_bytes)
    assert packet.msg_type == @echo_request
    assert packet.msg_code == 0
    assert packet.checksum == 0x8EFE
    assert packet.echo_ident == 0x1234
    assert packet.echo_seq_no == 0xABCD
    assert packet.data == @echo_data_bytes
    assert Checksum.valid?(@echo_packet_bytes)
  end

  test "echo_construct" do
    packet =
      Icmpv4.build(%{
        msg_type: @echo_request,
        msg_code: 0,
        echo_ident: 0x1234,
        echo_seq_no: 0xABCD,
        data: @echo_data_bytes,
        checksum: nil
      })

    assert packet == @echo_packet_bytes
  end

  test "echo_parse" do
    assert Icmpv4.parse(@echo_packet_bytes) == {:ok, echo_packet_repr()}
  end

  test "echo_emit" do
    repr = echo_packet_repr()

    assert Icmpv4.buffer_len(repr) == byte_size(@echo_packet_bytes)
    assert Icmpv4.emit(repr) == @echo_packet_bytes
  end

  test "check_len" do
    bytes = <<0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>

    assert Icmpv4.fields(<<>>) == :error
    assert Icmpv4.fields(binary_part(bytes, 0, 4)) == :error
    assert {:ok, _packet} = Icmpv4.fields(bytes)
  end
end
