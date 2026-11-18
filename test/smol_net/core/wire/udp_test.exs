defmodule SmolNet.Core.Wire.UdpTest do
  # Ported from smoltcp's `wire::udp` unit tests
  # (native/vendor/smoltcp/src/wire/udp.rs).
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Udp

  @udp 17

  @src_addr <<192, 168, 1, 1>>
  @dst_addr <<192, 168, 1, 2>>

  @packet_bytes <<0xBF, 0x00, 0x00, 0x35, 0x00, 0x0C, 0x12, 0x4D, 0xAA, 0x00, 0x00, 0xFF>>

  @no_checksum_packet <<0xBF, 0x00, 0x00, 0x35, 0x00, 0x0C, 0x00, 0x00, 0xAA, 0x00, 0x00, 0xFF>>

  @payload_bytes <<0xAA, 0x00, 0x00, 0xFF>>

  # smoltcp's `packet_repr()`, `Repr { src_port: 48896, dst_port: 53 }`.
  @ports {48_896, 53}

  test "deconstruct" do
    assert {:ok, packet} = Udp.fields(@packet_bytes)
    assert packet.src_port == 48_896
    assert packet.dst_port == 53
    assert packet.length == 12
    assert packet.checksum == 0x124D
    assert packet.payload == @payload_bytes

    assert Checksum.valid?(
             @packet_bytes,
             Checksum.pseudo_header(@src_addr, @dst_addr, @udp, byte_size(@packet_bytes))
           )
  end

  test "construct" do
    # The Rust test sets checksum 0xffff and then fills it; a nil
    # `:checksum` is the fill.
    fields = %{src_port: 48_896, dst_port: 53, length: 12, payload: @payload_bytes}
    assert Udp.build(fields, @src_addr, @dst_addr) == @packet_bytes
  end

  test "impossible_len" do
    bytes = Udp.build(%{length: 4, checksum: 0, payload: <<0, 0, 0, 0>>}, @src_addr, @dst_addr)
    assert bytes == <<0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0>>

    # `check_len`: a length field under the header's 8 bytes.
    assert {:ok, %{length: 4, payload: nil}} = Udp.fields(bytes)
    assert Udp.parse(bytes, @src_addr, @dst_addr) == :error
  end

  test "zero_checksum" do
    bytes = Udp.build(%{src_port: 1, dst_port: 31_881, length: 8}, @src_addr, @dst_addr)
    assert {:ok, %{checksum: 0xFFFF}} = Udp.fields(bytes)
  end

  test "no_checksum" do
    bytes =
      Udp.build(%{src_port: 1, dst_port: 31_881, length: 8, checksum: 0}, @src_addr, @dst_addr)

    # `verify_checksum` accepts a zero field; `parse/3` holds that rule.
    assert Udp.parse(bytes, @src_addr, @dst_addr) == {:ok, {1, 31_881}, <<>>}
  end

  test "parse" do
    assert Udp.parse(@packet_bytes, @src_addr, @dst_addr) == {:ok, @ports, @payload_bytes}
  end

  test "emit" do
    {src_port, dst_port} = @ports

    assert Udp.emit(src_port, dst_port, @payload_bytes, @src_addr, @dst_addr) ==
             @packet_bytes
  end

  test "checksum_omitted" do
    assert Udp.parse(@no_checksum_packet, @src_addr, @dst_addr) ==
             {:ok, @ports, @payload_bytes}
  end
end
