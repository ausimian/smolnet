defmodule SmolNet.Core.Tcp.SendBuffer do
  @moduledoc false

  # A TCP send buffer with smoltcp's `RingBuffer` accounting, holding the
  # binaries written rather than copying their octets into a ring: a queue
  # of binaries, `len` octets in all, from the oldest unacknowledged octet.
  #
  # `read_at` is where smoltcp's ring would hold that octet. A segment is
  # cut where the ring's storage wraps, as `RingBuffer::get_allocated`
  # cuts it, so the segments sent match smoltcp's however long the
  # connection runs; that is all it is for.

  defstruct capacity: 0, len: 0, read_at: 0, queue: nil

  @type t :: %__MODULE__{
          capacity: non_neg_integer(),
          len: non_neg_integer(),
          read_at: non_neg_integer(),
          queue: :queue.queue(binary())
        }

  @spec new(non_neg_integer()) :: t()
  def new(capacity), do: %__MODULE__{capacity: capacity, queue: :queue.new()}

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

      {size,
       %{buffer | len: buffer.len + size, read_at: read_at, queue: :queue.in(chunk, buffer.queue)}}
    end
  end

  @doc "Drops the first `count` octets, which the peer has acknowledged."
  @spec dequeue(t(), non_neg_integer()) :: t()
  def dequeue(buffer, 0), do: buffer

  def dequeue(buffer, count) do
    true = count <= buffer.len
    queue = drop(buffer.queue, count)

    %{
      buffer
      | len: buffer.len - count,
        queue: queue,
        read_at: rem(buffer.read_at + count, buffer.capacity)
    }
  end

  defp drop(queue, 0), do: queue

  defp drop(queue, count) do
    {{:value, chunk}, rest} = :queue.out(queue)
    size = byte_size(chunk)

    if size <= count,
      do: drop(rest, count - size),
      else: :queue.in_r(binary_part(chunk, count, size - count), rest)
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
      slice(:queue.to_list(buffer.queue), offset, size)
    end
  end

  defp slice(_chunks, _offset, 0), do: <<>>

  defp slice([chunk | rest], offset, size) do
    chunk_size = byte_size(chunk)

    cond do
      offset >= chunk_size ->
        slice(rest, offset - chunk_size, size)

      offset + size <= chunk_size ->
        binary_part(chunk, offset, size)

      true ->
        IO.iodata_to_binary(collect([chunk | rest], offset, size, []))
    end
  end

  defp collect(_chunks, _offset, 0, acc), do: :lists.reverse(acc)

  defp collect([chunk | rest], offset, size, acc) do
    take = Kernel.min(byte_size(chunk) - offset, size)
    collect(rest, 0, size - take, [binary_part(chunk, offset, take) | acc])
  end

  @doc "Everything held, in order."
  @spec data(t()) :: binary()
  def data(buffer), do: IO.iodata_to_binary(:queue.to_list(buffer.queue))
end
