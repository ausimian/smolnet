defmodule SmolNet.Core.Tcp.RecvBuffer do
  @moduledoc false

  # A TCP receive buffer with smoltcp's `RingBuffer` accounting, holding
  # references to the binaries that arrived rather than copying octets into
  # a ring:
  #
  #   * the data received in order and not yet read, `len` octets, as a
  #     queue of binaries;
  #   * the data received out of order, beyond it, as `{offset, binary}`
  #     pieces, ascending and disjoint, at offsets from the end of the
  #     in-order data. Which of these octets are wanted is the assembler's
  #     business; the buffer only keeps them, a later write over an earlier
  #     one winning, as in a ring.
  #
  # The window is the capacity less the in-order data. `read_at` is where
  # smoltcp's ring would read next. Nothing here depends on it except the
  # largest contiguous read a ring allows, `contiguous/1`, which
  # `Connection.recv/3` reproduces because a caller sees it.

  defstruct capacity: 0, len: 0, read_at: 0, queue: nil, pieces: []

  @type t :: %__MODULE__{
          capacity: non_neg_integer(),
          len: non_neg_integer(),
          read_at: non_neg_integer(),
          queue: :queue.queue(binary()),
          pieces: [{non_neg_integer(), binary()}]
        }

  @spec new(non_neg_integer()) :: t()
  def new(capacity), do: %__MODULE__{capacity: capacity, queue: :queue.new()}

  @doc "An empty buffer of the same capacity, as `RingBuffer::clear`."
  @spec clear(t()) :: t()
  def clear(buffer), do: new(buffer.capacity)

  @doc "A buffer holding `data` in order, read from the start of the ring."
  @spec from_data(non_neg_integer(), binary()) :: t()
  def from_data(capacity, data) do
    %{new(capacity) | len: byte_size(data), queue: queue_in(:queue.new(), data)}
  end

  def len(buffer), do: buffer.len
  def capacity(buffer), do: buffer.capacity
  def window(buffer), do: buffer.capacity - buffer.len
  def empty?(buffer), do: buffer.len == 0

  @doc """
  Keeps `data` at `offset` beyond the in-order data, cut to the window as
  `RingBuffer::write_unallocated` cuts it. Returns the octets kept.
  """
  @spec write(t(), non_neg_integer(), binary()) :: {non_neg_integer(), t()}
  def write(buffer, offset, data) do
    window = window(buffer)

    if offset > window do
      {0, buffer}
    else
      size = Kernel.min(byte_size(data), window - offset)

      if size == 0 do
        {0, buffer}
      else
        data = binary_part(data, 0, size)
        {size, %{buffer | pieces: overwrite(buffer.pieces, offset, data)}}
      end
    end
  end

  # Inserts `data` at `offset`, cutting whatever it covers out of the
  # pieces already held.
  defp overwrite(pieces, offset, data) do
    right = offset + byte_size(data)

    {before, rest} =
      Enum.split_while(pieces, fn {start, piece} -> start + byte_size(piece) <= offset end)

    {covered, after_} = Enum.split_while(rest, fn {start, _piece} -> start < right end)

    head =
      case covered do
        [{start, piece} | _] when start < offset ->
          [{start, binary_part(piece, 0, offset - start)}]

        _ ->
          []
      end

    tail =
      case List.last(covered) do
        {start, piece} when start + byte_size(piece) > right ->
          cut = right - start
          [{right, binary_part(piece, cut, byte_size(piece) - cut)}]

        _ ->
          []
      end

    before ++ head ++ [{offset, data}] ++ tail ++ after_
  end

  @doc """
  Moves the first `count` octets beyond the in-order data into it, as
  `RingBuffer::enqueue_unallocated`; they must have been written.
  """
  @spec enqueue(t(), non_neg_integer()) :: t()
  def enqueue(buffer, 0), do: buffer

  def enqueue(buffer, count) do
    true = count <= window(buffer)
    {taken, rest} = take_pieces(buffer.pieces, count, [])
    queue = Enum.reduce(taken, buffer.queue, &queue_in(&2, &1))
    rest = for {start, piece} <- rest, do: {start - count, piece}
    %{buffer | len: buffer.len + count, queue: queue, pieces: rest}
  end

  # The pieces run contiguously from 0 up to `count`: the assembler said so.
  defp take_pieces([], _count, taken), do: {:lists.reverse(taken), []}

  defp take_pieces([{start, piece} | rest] = pieces, count, taken) do
    size = byte_size(piece)

    cond do
      start >= count ->
        {:lists.reverse(taken), pieces}

      start + size <= count ->
        take_pieces(rest, count, [piece | taken])

      true ->
        cut = count - start

        {:lists.reverse([binary_part(piece, 0, cut) | taken]),
         [{count, binary_part(piece, cut, size - cut)} | rest]}
    end
  end

  @doc "Up to `size` octets of the in-order data, without taking them."
  @spec peek(t(), non_neg_integer()) :: binary()
  def peek(buffer, size) do
    {data, _queue} = queue_take(buffer.queue, Kernel.min(size, buffer.len), [])
    data
  end

  @doc """
  The most a ring would hand over in one piece, from `read_at` to the end
  of its storage, as `RingBuffer::dequeue_many_with` does.
  """
  @spec contiguous(t()) :: non_neg_integer()
  def contiguous(buffer), do: Kernel.min(buffer.len, buffer.capacity - buffer.read_at)

  @doc "Takes up to `size` octets of the in-order data."
  @spec dequeue(t(), non_neg_integer()) :: {binary(), t()}
  def dequeue(buffer, size) do
    size = Kernel.min(size, buffer.len)
    {data, queue} = queue_take(buffer.queue, size, [])

    read_at =
      if buffer.capacity > 0, do: rem(buffer.read_at + size, buffer.capacity), else: 0

    {data, %{buffer | len: buffer.len - size, queue: queue, read_at: read_at}}
  end

  @doc "The in-order data, all of it."
  @spec data(t()) :: binary()
  def data(buffer), do: IO.iodata_to_binary(:queue.to_list(buffer.queue))

  defp queue_in(queue, <<>>), do: queue
  defp queue_in(queue, data), do: :queue.in(data, queue)

  # One chunk is handed over as it is.
  defp queue_take(queue, 0, [chunk]), do: {chunk, queue}
  defp queue_take(queue, 0, acc), do: {IO.iodata_to_binary(:lists.reverse(acc)), queue}

  defp queue_take(queue, size, acc) do
    {{:value, chunk}, rest} = :queue.out(queue)

    if byte_size(chunk) <= size do
      queue_take(rest, size - byte_size(chunk), [chunk | acc])
    else
      {IO.iodata_to_binary(:lists.reverse([binary_part(chunk, 0, size) | acc])),
       :queue.in_r(binary_part(chunk, size, byte_size(chunk) - size), rest)}
    end
  end
end
