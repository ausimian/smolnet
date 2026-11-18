defmodule SmolNet.Core.Wire.Checksum do
  @moduledoc false

  # The Internet checksum (RFC 1071), as smoltcp's `wire::ip::checksum`
  # computes it. `data/1` is the folded one's complement sum of a buffer,
  # without the final complement; a buffer whose checksum field is right
  # sums to 0xFFFF. Sums of 32-bit words fold to the same 16-bit sum, since
  # 2^16 is 1 modulo 0xFFFF, so the hot loop takes eight words a step, as
  # the phase 0 spike measured (ADR 0016).

  import Bitwise

  @typedoc "A 4-byte IPv4 or 16-byte IPv6 address."
  @type address :: <<_::32>> | <<_::128>>

  @doc "The folded sum of `data`, odd bytes padded with zero."
  @spec data(binary()) :: 0..0xFFFF
  def data(data), do: data |> sum(0) |> fold()

  @doc "The sum of several folded sums."
  @spec combine([0..0xFFFF]) :: 0..0xFFFF
  def combine(sums), do: sums |> Enum.sum() |> fold()

  @doc """
  The folded sum of the IPv4 or IPv6 pseudo-header for a `protocol`
  message of `length` bytes between two addresses of the same family.
  """
  @spec pseudo_header(address(), address(), 0..255, non_neg_integer()) :: 0..0xFFFF
  def pseudo_header(<<s1::16, s2::16>>, <<d1::16, d2::16>>, protocol, length),
    do: fold(s1 + s2 + d1 + d2 + protocol + band(length, 0xFFFF))

  def pseudo_header(<<_::128>> = source, <<_::128>> = destination, protocol, length),
    do: fold(sum(source, sum(destination, protocol + band(length, 0xFFFF))))

  @doc """
  The checksum field for `data` after the pseudo-header sum `pseudo`, the
  complement of their sum.
  """
  @spec checksum(binary(), non_neg_integer()) :: 0..0xFFFF
  def checksum(data, pseudo \\ 0), do: data |> sum(pseudo) |> fold() |> bxor(0xFFFF)

  @doc "Whether `data` sums, after the pseudo-header sum `pseudo`, to 0xFFFF."
  @spec valid?(binary(), non_neg_integer()) :: boolean()
  def valid?(data, pseudo \\ 0), do: fold(sum(data, pseudo)) == 0xFFFF

  @doc "Adds `data` to an unfolded sum, a word at a time."
  @spec sum(binary(), non_neg_integer()) :: non_neg_integer()
  def sum(<<a::32, b::32, c::32, d::32, e::32, f::32, g::32, h::32, rest::binary>>, acc),
    do: sum(rest, acc + a + b + c + d + e + f + g + h)

  def sum(<<a::32, rest::binary>>, acc), do: sum(rest, acc + a)
  def sum(<<a::16, rest::binary>>, acc), do: sum(rest, acc + a)
  def sum(<<a::8>>, acc), do: acc + (a <<< 8)
  def sum(<<>>, acc), do: acc

  @doc "Folds an unfolded sum to 16 bits, end-around carry included."
  @spec fold(non_neg_integer()) :: 0..0xFFFF
  def fold(sum) when sum > 0xFFFF, do: fold(band(sum, 0xFFFF) + (sum >>> 16))
  def fold(sum), do: sum
end
