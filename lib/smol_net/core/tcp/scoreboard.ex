defmodule SmolNet.Core.Tcp.Scoreboard do
  @moduledoc false

  # The SACK scoreboard of RFC 6675, as SmolNet's smoltcp keeps it: the
  # octets above the cumulative ACK that the peer has reported in SACK
  # blocks, as `{left, right}` sequence ranges, ascending, disjoint and not
  # adjacent. At most 32 are held; past that the lowest is forgotten, which
  # can cost a needless resend but never withholds a needed one.

  alias SmolNet.Core.Tcp.Seq

  require SmolNet.Core.Tcp.Seq

  @size 32
  @dup_thresh 3

  @type range :: {Seq.t(), Seq.t()}
  @type t :: [range()]

  def new, do: []

  @doc "Records `[left, right)`; returns whether any of it is new, and the board."
  @spec add(t(), Seq.t(), Seq.t()) :: {boolean(), t()}
  def add(board, left, right) do
    if Seq.ge?(left, right) or Enum.any?(board, &covers?(&1, left, right)) do
      {false, board}
    else
      {merged, range, inserted} = Enum.reduce(board, {[], {left, right}, false}, &merge/2)
      merged = if inserted, do: merged, else: [range | merged]
      merged = :lists.reverse(merged)
      {true, Enum.drop(merged, max(length(merged) - @size, 0))}
    end
  end

  defp covers?({l, r}, left, right), do: Seq.le?(l, left) and Seq.le?(right, r)

  # Builds the board, reversed, with the new range absorbing those it
  # overlaps or touches.
  defp merge({l, r}, {acc, {left, right}, inserted}) do
    cond do
      Seq.lt?(r, left) -> {[{l, r} | acc], {left, right}, inserted}
      Seq.lt?(right, l) and inserted -> {[{l, r} | acc], {left, right}, true}
      Seq.lt?(right, l) -> {[{l, r}, {left, right} | acc], {left, right}, true}
      true -> {acc, {Seq.min(left, l), Seq.max(right, r)}, inserted}
    end
  end

  @doc "Forgets the octets below `ack`."
  @spec advance(t(), Seq.t()) :: t()
  def advance(board, ack) do
    case Enum.drop_while(board, fn {_l, r} -> Seq.le?(r, ack) end) do
      [{l, r} | rest] -> if Seq.lt?(l, ack), do: [{ack, r} | rest], else: [{l, r} | rest]
      [] -> []
    end
  end

  @doc "How many recorded octets lie in `[from, to)`."
  @spec sacked_between(t(), Seq.t(), Seq.t()) :: non_neg_integer()
  def sacked_between(board, from, to) do
    Enum.reduce(board, 0, fn {l, r}, sum ->
      {l, r} = {Seq.max(l, from), Seq.min(r, to)}
      if Seq.lt?(l, r), do: sum + Seq.diff(r, l), else: sum
    end)
  end

  @doc """
  RFC 6675 `IsLost` as a boundary: an unreported octet below it is lost,
  since `DupThresh` ranges, or more than `(DupThresh - 1) * mss` reported
  octets, lie above it.
  """
  @spec lost_below(t(), non_neg_integer()) :: Seq.t() | nil
  def lost_below(board, mss) do
    board
    |> :lists.reverse()
    |> Enum.with_index(1)
    |> Enum.reduce_while(0, fn {{l, r}, count}, sacked ->
      sacked = sacked + Seq.diff(r, l)

      if count >= @dup_thresh or sacked > (@dup_thresh - 1) * mss,
        do: {:halt, {:lost, l}},
        else: {:cont, sacked}
    end)
    |> case do
      {:lost, l} -> l
      _sacked -> nil
    end
  end

  @doc "The lowest gap at or above `from` below a recorded range."
  @spec next_hole(t(), Seq.t()) :: range() | nil
  def next_hole(board, from) do
    Enum.reduce_while(board, from, fn {l, r}, start ->
      if Seq.lt?(start, l), do: {:halt, {:hole, {start, l}}}, else: {:cont, Seq.max(start, r)}
    end)
    |> case do
      {:hole, hole} -> hole
      _start -> nil
    end
  end

  @doc "The lowest run of `[from, to)` that no recorded range covers."
  @spec unsacked_run(t(), Seq.t(), Seq.t()) :: range() | nil
  def unsacked_run(board, from, to) do
    Enum.reduce_while(board, from, fn {l, r}, start ->
      cond do
        Seq.ge?(start, to) or Seq.ge?(l, to) -> {:halt, start}
        Seq.lt?(start, l) -> {:halt, {:run, {start, l}}}
        true -> {:cont, Seq.max(start, r)}
      end
    end)
    |> case do
      {:run, run} -> run
      start -> if Seq.lt?(start, to), do: {start, to}, else: nil
    end
  end
end
