defmodule SmolNet.Core.Wire.ChecksumTest do
  # Direct tests of the RFC 1071 Internet checksum, checked against the
  # RFC's worked example and a naive 16-bit-word sum written here.
  use ExUnit.Case, async: true

  alias SmolNet.Core.Wire.Checksum

  # RFC 1071 section 3's example words, 0001 f203 f4f5 f6f7.
  @rfc_bytes <<0x00, 0x01, 0xF2, 0x03, 0xF4, 0xF5, 0xF6, 0xF7>>

  # One's complement sum of 16-bit words with end-around carry at every
  # step, an odd final byte padded with zero.
  defp naive_sum(bytes) do
    padded = if rem(byte_size(bytes), 2) == 1, do: bytes <> <<0>>, else: bytes

    for <<word::16 <- padded>>, reduce: 0 do
      acc ->
        sum = acc + word
        if sum > 0xFFFF, do: sum - 0xFFFF, else: sum
    end
  end

  # A deterministic buffer of `n` varied bytes.
  defp buffer(n), do: for(i <- 0..(n - 1)//1, into: <<>>, do: <<rem(i * 37 + 11, 256)>>)

  test "RFC 1071 example" do
    assert naive_sum(@rfc_bytes) == 0xDDF2
    assert Checksum.data(@rfc_bytes) == 0xDDF2
    assert Checksum.fold(Checksum.sum(@rfc_bytes, 0)) == 0xDDF2
    assert Checksum.checksum(@rfc_bytes) == 0x220D
    assert Checksum.valid?(@rfc_bytes <> <<0x220D::16>>)
    refute Checksum.valid?(@rfc_bytes <> <<0x220C::16>>)
  end

  test "odd-length buffer pads its last byte with zero" do
    assert Checksum.data(<<0xFF>>) == 0xFF00
    assert Checksum.data(<<0x01, 0x02, 0x03>>) == 0x0402
    assert Checksum.data(<<0x01, 0x02, 0x03>>) == Checksum.data(<<0x01, 0x02, 0x03, 0x00>>)

    # Longer than the eight-word step and odd, so the fast path and every
    # tail clause run.
    odd = buffer(101)
    assert Checksum.data(odd) == naive_sum(odd)
    assert Checksum.valid?(odd <> <<0>> <> <<Checksum.checksum(odd)::16>>)
  end

  test "matches the naive sum at every length up to 80 bytes" do
    for n <- 0..80 do
      bytes = buffer(n)
      assert Checksum.data(bytes) == naive_sum(bytes), "length #{n}"
    end

    all_ones = :binary.copy(<<0xFF>>, 70)
    assert Checksum.data(all_ones) == naive_sum(all_ones)
  end

  test "IPv4 pseudo-header sum" do
    cases = [
      {<<192, 168, 1, 1>>, <<192, 168, 1, 2>>, 6, 28},
      {<<10, 0, 0, 1>>, <<10, 0, 0, 2>>, 17, 12},
      {<<255, 255, 255, 255>>, <<255, 255, 255, 254>>, 255, 0xFFFF}
    ]

    for {src, dst, protocol, length} <- cases do
      # RFC 793 / 768: source, destination, zero, protocol, length.
      pseudo = <<src::binary, dst::binary, 0, protocol, length::16>>
      assert Checksum.pseudo_header(src, dst, protocol, length) == naive_sum(pseudo)
    end
  end

  test "IPv6 pseudo-header sum" do
    link_local = <<0xFE80::16, 0::48, 0x0211_22FF_FE33_4455::64>>
    all_nodes = <<0xFF02::16, 0::96, 1::16>>
    all_ones = :binary.copy(<<0xFF>>, 16)

    cases = [
      {link_local, all_nodes, 58, 32},
      {all_nodes, link_local, 6, 1440},
      {all_ones, all_ones, 17, 0xFFFF}
    ]

    for {src, dst, next_header, length} <- cases do
      # RFC 8200 section 8.1: source, destination, 32-bit length, three
      # zero bytes, next header.
      pseudo = <<src::binary, dst::binary, length::32, 0, 0, 0, next_header>>
      assert Checksum.pseudo_header(src, dst, next_header, length) == naive_sum(pseudo)
    end
  end

  test "checksum with a pseudo-header validates the filled message" do
    src = <<192, 168, 1, 1>>
    dst = <<192, 168, 1, 2>>
    body = buffer(33)
    message = <<0xBF00::16, 0x0035::16, byte_size(body) + 8::16, 0::16, body::binary>>
    pseudo = Checksum.pseudo_header(src, dst, 17, byte_size(message))

    checksum = Checksum.checksum(message, pseudo)
    <<head::binary-6, 0::16, tail::binary>> = message
    filled = <<head::binary, checksum::16, tail::binary>>

    assert Checksum.valid?(filled, pseudo)
    refute Checksum.valid?(filled, Checksum.pseudo_header(src, dst, 6, byte_size(message)))
    assert Checksum.combine([pseudo, Checksum.data(filled)]) == 0xFFFF
  end

  test "fold carries end-around" do
    assert Checksum.fold(0) == 0
    assert Checksum.fold(0xFFFF) == 0xFFFF
    assert Checksum.fold(0x1_0000) == 0x0001
    assert Checksum.fold(0x1_FFFE) == 0xFFFF
    assert Checksum.fold(0xFFFF_FFFF) == 0xFFFF
    assert Checksum.combine([0xFFFF, 0x0001]) == 0x0001
  end
end
