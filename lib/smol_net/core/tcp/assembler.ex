defmodule SmolNet.Core.Tcp.Assembler do
  @moduledoc false

  # Out-of-order data, as smoltcp's `storage::Assembler` tracks it for a
  # TCP receiver: the ranges of octets held beyond the next one expected,
  # as `[left, right)` offsets from it, ascending, disjoint and not
  # adjacent. At most `capacity` ranges are held, however many segments
  # make them up (32 in SmolNet's build, 4 in smoltcp's test build).
  #
  # Adding a range merges it with any it overlaps or touches. A range that
  # would make one more than the capacity is refused, unless it starts at
  # offset 0 (`add_then_remove_front/3`): the next expected data is never
  # refused, since that could stall the connection.

  defstruct capacity: 32, ranges: []

  @type t :: %__MODULE__{capacity: pos_integer(), ranges: [{non_neg_integer(), pos_integer()}]}

  @spec new(pos_integer()) :: t()
  def new(capacity \\ 32), do: %__MODULE__{capacity: capacity}

  @spec clear(t()) :: t()
  def clear(assembler), do: %{assembler | ranges: []}

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{ranges: ranges}), do: ranges == []

  @doc "The ranges held, as `{left, right}` offsets from the next octet expected."
  @spec ranges(t()) :: [{non_neg_integer(), pos_integer()}]
  def ranges(%__MODULE__{ranges: ranges}), do: ranges

  @doc "Adds `size` octets at `offset`, or `:error` past the capacity."
  @spec add(t(), non_neg_integer(), non_neg_integer()) :: {:ok, t()} | :error
  def add(assembler, _offset, 0), do: {:ok, assembler}

  def add(assembler, offset, size) do
    ranges = merge(assembler.ranges, offset, offset + size)
    if length(ranges) > assembler.capacity, do: :error, else: {:ok, %{assembler | ranges: ranges}}
  end

  defp merge(ranges, left, right) do
    {before, rest} = Enum.split_while(ranges, fn {_l, r} -> r < left end)
    {touching, after_} = Enum.split_while(rest, fn {l, _r} -> l <= right end)

    merged =
      Enum.reduce(touching, {left, right}, fn {l, r}, {left, right} ->
        {Kernel.min(l, left), Kernel.max(r, right)}
      end)

    before ++ [merged | after_]
  end

  @doc "Takes away the contiguous run at the front, returning its length."
  @spec remove_front(t()) :: {non_neg_integer(), t()}
  def remove_front(%__MODULE__{ranges: [{0, right} | rest]} = assembler),
    do: {right, %{assembler | ranges: for({l, r} <- rest, do: {l - right, r - right})}}

  def remove_front(assembler), do: {0, assembler}

  @doc """
  Adds `size` octets at `offset`, then takes away the contiguous run at
  the front, if any. Returns `{:ok, run_length, assembler}`, or `:error`
  when the range would exceed the capacity, which one at offset 0 never
  does.
  """
  @spec add_then_remove_front(t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer(), t()} | :error
  def add_then_remove_front(assembler, 0, size) when size > 0 do
    {run, assembler} = remove_front(%{assembler | ranges: merge(assembler.ranges, 0, size)})
    {:ok, run, assembler}
  end

  def add_then_remove_front(assembler, offset, size) do
    with {:ok, assembler} <- add(assembler, offset, size) do
      {run, assembler} = remove_front(assembler)
      {:ok, run, assembler}
    end
  end
end
