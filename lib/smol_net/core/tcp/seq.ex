defmodule SmolNet.Core.Tcp.Seq do
  @moduledoc false

  # TCP sequence numbers, as smoltcp's `TcpSeqNumber` treats them: 32-bit
  # values held here as 0..2^32-1, ordered by their wrapping difference read
  # as a signed 32-bit integer, so that `a < b` when `b` lies less than 2^31
  # ahead of `a`. Adding or subtracting a length wraps; the distance from one
  # to a later one is a length, and asking for it the other way round is a
  # bug, as in smoltcp, where it panics.
  #
  # Every operation but `signed/1` is a macro, so that the connection's hot
  # path does this arithmetic inline rather than through a call to another
  # module for each one; callers `require` the module. Each argument is
  # evaluated once, left to right, as a function's would be. The wrapping
  # difference `d = (a - b) mod 2^32` reads as negative from 2^31 up, so
  # `a < b` is `d >= 2^31`, `a <= b` is `d == 0 or d >= 2^31`, and so on.

  @type t :: 0..0xFFFF_FFFF

  @mask 0xFFFF_FFFF
  @half 0x8000_0000

  @doc "`seq + len`, wrapping."
  defmacro add(seq, len), do: quote(do: :erlang.band(unquote(seq) + unquote(len), unquote(@mask)))

  @doc "`seq - len`, wrapping."
  defmacro sub(seq, len), do: quote(do: :erlang.band(unquote(seq) - unquote(len), unquote(@mask)))

  @doc "The signed distance from `b` to `a`, given `a - b`."
  @spec signed(integer()) :: integer()
  def signed(value) do
    value = :erlang.band(value, @mask)
    if value >= @half, do: value - 0x1_0000_0000, else: value
  end

  @doc "How far `a` lies past `b`; raises if `a` lies before `b`, as smoltcp panics."
  defmacro diff(a, b) do
    quote do
      case :erlang.band(unquote(a) - unquote(b), unquote(@mask)) do
        distance when distance < unquote(@half) ->
          distance

        _negative ->
          raise ArithmeticError, "attempt to subtract sequence numbers with underflow"
      end
    end
  end

  defmacro lt?(a, b),
    do: quote(do: :erlang.band(unquote(a) - unquote(b), unquote(@mask)) >= unquote(@half))

  defmacro ge?(a, b),
    do: quote(do: :erlang.band(unquote(a) - unquote(b), unquote(@mask)) < unquote(@half))

  defmacro le?(a, b) do
    quote do
      case :erlang.band(unquote(a) - unquote(b), unquote(@mask)) do
        0 -> true
        distance -> distance >= unquote(@half)
      end
    end
  end

  defmacro gt?(a, b) do
    quote do
      case :erlang.band(unquote(a) - unquote(b), unquote(@mask)) do
        0 -> false
        distance -> distance < unquote(@half)
      end
    end
  end

  @doc "The later of two, the first when they are equal or incomparable."
  defmacro max(a, b) do
    quote do
      first = unquote(a)
      second = unquote(b)

      case :erlang.band(first - second, unquote(@mask)) do
        0 -> second
        distance when distance < unquote(@half) -> first
        _behind -> second
      end
    end
  end

  @doc "The earlier of two."
  defmacro min(a, b) do
    quote do
      first = unquote(a)
      second = unquote(b)

      if :erlang.band(first - second, unquote(@mask)) >= unquote(@half),
        do: first,
        else: second
    end
  end
end
