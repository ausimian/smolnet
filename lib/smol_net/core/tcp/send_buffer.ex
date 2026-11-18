defmodule SmolNet.Core.Tcp.SendBuffer do
  @moduledoc false

  # A TCP send buffer with smoltcp's `RingBuffer` accounting, holding the
  # binaries written rather than copying their octets into a ring: `len`
  # octets in all, from the oldest unacknowledged octet, which lies at
  # `start` in the stream of everything ever written. The binaries are in
  # a tree keyed by where each starts in that stream, negated, so that the
  # one holding any octet is the first at or after its negated position,
  # and a slice from anywhere in the buffer is found without walking the
  # ones before it.
  #
  # `read_at` is where smoltcp's ring would hold that octet. A segment is
  # cut where the ring's storage wraps, as `RingBuffer::get_allocated`
  # cuts it, so the segments sent match smoltcp's however long the
  # connection runs; that is all it is for.

  defstruct capacity: 0, len: 0, read_at: 0, start: 0, chunks: nil

  @type t :: %__MODULE__{
          capacity: non_neg_integer(),
          len: non_neg_integer(),
          read_at: non_neg_integer(),
          start: non_neg_integer(),
          chunks: :gb_trees.tree(non_neg_integer(), binary())
        }

  @spec new(non_neg_integer()) :: t()
  def new(capacity), do: %__MODULE__{capacity: capacity, chunks: :gb_trees.empty()}

  @spec clear(t()) :: t()
  def clear(buffer), do: new(buffer.capacity)

  @doc "A buffer holding `data`, written into an empty ring."
  @spec from_data(non_neg_integer(), binary()) :: t()
  def from_data(capacity, data), do: enqueue(new(capacity), data) |> elem(1)

  def len(buffer), do: buffer.len
  def capacity(buffer), do: buffer.capacity
  def window(buffer), do: buffer.capacity - buffer.len
  def empty?(buffer), do: buffer.len == 0
  def full?(buffer), do: buffer.len == buffer.capacity

  @doc """
  Appends as much of `data` as fits, returning how much did. An empty
  ring starts again from the beginning of its storage, as smoltcp's does.
  """
  @spec enqueue(t(), binary()) :: {non_neg_integer(), t()}
  def enqueue(buffer, data) do
    size = Kernel.min(byte_size(data), window(buffer))
    read_at = if buffer.len == 0, do: 0, else: buffer.read_at

    if size == 0 do
      {0, %{buffer | read_at: read_at}}
    else
      chunk = if size == byte_size(data), do: data, else: binary_part(data, 0, size)
      chunks = :gb_trees.insert(-(buffer.start + buffer.len), chunk, buffer.chunks)
      {size, %{buffer | len: buffer.len + size, read_at: read_at, chunks: chunks}}
    end
  end

  @doc "Drops the first `count` octets, which the peer has acknowledged."
  @spec dequeue(t(), non_neg_integer()) :: t()
  def dequeue(buffer, 0), do: buffer

  def dequeue(buffer, count) do
    true = count <= buffer.len

    %{
      buffer
      | len: buffer.len - count,
        start: buffer.start + count,
        chunks: drop(buffer.chunks, count),
        read_at: rem(buffer.read_at + count, buffer.capacity)
    }
  end

  defp drop(chunks, 0), do: chunks

  defp drop(chunks, count) do
    {key, chunk, rest} = :gb_trees.take_largest(chunks)
    size = byte_size(chunk)

    if size <= count,
      do: drop(rest, count - size),
      else: :gb_trees.insert(key - count, binary_part(chunk, count, size - count), rest)
  end

  @doc """
  Up to `size` octets from `offset`, stopping at the end of what is held
  and where the ring's storage wraps, as `RingBuffer::get_allocated`.
  """
  @spec get(t(), non_neg_integer(), non_neg_integer()) :: binary()
  def get(buffer, offset, size) do
    if offset > buffer.len or buffer.capacity == 0 do
      <<>>
    else
      start_at = rem(buffer.read_at + offset, buffer.capacity)
      size = size |> Kernel.min(buffer.len - offset) |> Kernel.min(buffer.capacity - start_at)
      slice(buffer.chunks, buffer.start + offset, size)
    end
  end

  defp slice(_chunks, _at, 0), do: <<>>

  # The chunk holding octet `at` is the last to start at or before it.
  defp slice(chunks, at, size) do
    {key, chunk, _iterator} = :gb_trees.next(:gb_trees.iterator_from(-at, chunks))
    chunk_at = -key
    offset = at - chunk_at

    if offset + size <= byte_size(chunk),
      do: binary_part(chunk, offset, size),
      else: IO.iodata_to_binary(collect(chunks, chunk_at, chunk, offset, size))
  end

  # The chunks after one are found where it ends.
  defp collect(chunks, chunk_at, chunk, offset, size) do
    take = Kernel.min(byte_size(chunk) - offset, size)
    part = binary_part(chunk, offset, take)

    if take == size do
      [part]
    else
      next_at = chunk_at + byte_size(chunk)
      [part | collect(chunks, next_at, :gb_trees.get(-next_at, chunks), 0, size - take)]
    end
  end

  @doc "Everything held, in order."
  @spec data(t()) :: binary()
  def data(buffer), do: IO.iodata_to_binary(:lists.reverse(:gb_trees.values(buffer.chunks)))
end
