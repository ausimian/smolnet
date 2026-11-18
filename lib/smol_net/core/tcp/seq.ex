defmodule SmolNet.Core.Tcp.Seq do
  @moduledoc false

  # TCP sequence numbers, as smoltcp's `TcpSeqNumber` treats them: 32-bit
  # values held here as 0..2^32-1, ordered by their wrapping difference read
  # as a signed 32-bit integer, so that `a < b` when `b` lies less than 2^31
  # ahead of `a`. Adding or subtracting a length wraps; the distance from one
  # to a later one is a length, and asking for it the other way round is a
  # bug, as in smoltcp, where it panics.

  import Bitwise

  @mask 0xFFFF_FFFF

  @compile {:inline,
            add: 2, sub: 2, signed: 1, lt?: 2, le?: 2, gt?: 2, ge?: 2, max: 2, min: 2, diff: 2}

  @type t :: 0..0xFFFF_FFFF

  @doc "`seq + len`, wrapping."
  @spec add(t(), non_neg_integer()) :: t()
  def add(seq, len), do: band(seq + len, @mask)

  @doc "`seq - len`, wrapping."
  @spec sub(t(), non_neg_integer()) :: t()
  def sub(seq, len), do: band(seq - len, @mask)

  @doc "The signed distance from `b` to `a`."
  @spec signed(integer()) :: integer()
  def signed(value) do
    value = band(value, @mask)
    if value >= 0x8000_0000, do: value - 0x1_0000_0000, else: value
  end

  @doc "How far `a` lies past `b`; raises if `a` lies before `b`, as smoltcp panics."
  @spec diff(t(), t()) :: non_neg_integer()
  def diff(a, b) do
    case signed(a - b) do
      distance when distance >= 0 -> distance
      _negative -> raise ArithmeticError, "attempt to subtract sequence numbers with underflow"
    end
  end

  def lt?(a, b), do: signed(a - b) < 0
  def le?(a, b), do: signed(a - b) <= 0
  def gt?(a, b), do: signed(a - b) > 0
  def ge?(a, b), do: signed(a - b) >= 0

  @doc "The later of two, the first when they are equal or incomparable."
  def max(a, b), do: if(gt?(a, b), do: a, else: b)

  @doc "The earlier of two."
  def min(a, b), do: if(lt?(a, b), do: a, else: b)
end
