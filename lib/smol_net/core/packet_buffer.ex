defmodule SmolNet.Core.PacketBuffer do
  @moduledoc false

  # A ring of whole packets, as smoltcp's `storage::PacketBuffer` keeps
  # them: at most `slots` packets and `bytes` bytes of payload, each packet
  # contiguous in the payload ring. A packet that does not fit before the
  # end of the ring wraps to its start behind a padding entry, which takes
  # a slot of its own; so does smoltcp's, and a padding entry can be left
  # at the tail when the packet behind it finds the slots full. Only the
  # accounting is kept: the payloads are the binaries themselves.
  #
  # `enqueue/3` returns `:full` exactly when smoltcp's `enqueue` refuses the
  # packet, so a ring drops what the NIF's drops.

  defstruct slots: 16, bytes: 16_384, used_slots: 0, read_at: 0, length: 0, entries: :queue.new()

  @type t :: %__MODULE__{}

  @doc "An empty ring of `slots` packets and `bytes` bytes of payload."
  @spec new(pos_integer(), pos_integer()) :: t()
  def new(slots, bytes), do: %__MODULE__{slots: slots, bytes: bytes}

  @doc """
  Whether the ring holds no entry, padding included (smoltcp's
  `is_empty`, which a socket's `can_recv` reads).
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{used_slots: used_slots}), do: used_slots == 0

  @doc """
  Appends a packet with its metadata, or refuses it as smoltcp would. A
  refusal can still change the ring, by the padding it left.
  """
  @spec enqueue(t(), binary(), term()) :: {:ok | :full, t()}
  def enqueue(%__MODULE__{} = buffer, payload, meta) do
    size = byte_size(payload)

    if buffer.bytes < size or buffer.used_slots == buffer.slots do
      {:full, buffer}
    else
      # An empty payload ring starts again at its beginning.
      buffer = if buffer.length == 0, do: %{buffer | read_at: 0}, else: buffer
      window = buffer.bytes - buffer.length
      contiguous = min(window, buffer.bytes - rem(buffer.read_at + buffer.length, buffer.bytes))

      cond do
        window < size -> {:full, buffer}
        contiguous >= size -> {:ok, push(buffer, {:packet, payload, meta}, size)}
        window - contiguous < size -> {:full, buffer}
        true -> wrap(push(buffer, {:padding, contiguous}, contiguous), payload, meta, size)
      end
    end
  end

  # The padding is kept even when the packet behind it then finds no slot.
  defp wrap(buffer, payload, meta, size) do
    if buffer.used_slots == buffer.slots,
      do: {:full, buffer},
      else: {:ok, push(buffer, {:packet, payload, meta}, size)}
  end

  defp push(buffer, entry, size) do
    %{
      buffer
      | entries: :queue.in(entry, buffer.entries),
        used_slots: buffer.used_slots + 1,
        length: buffer.length + size
    }
  end

  @doc """
  Takes the oldest packet, dropping a padding entry before it, as smoltcp's
  `dequeue` does even when no packet follows.
  """
  @spec dequeue(t()) :: {:ok, binary(), term(), t()} | {:empty, t()}
  def dequeue(%__MODULE__{} = buffer) do
    buffer = drop_padding(buffer)

    case :queue.out(buffer.entries) do
      {{:value, {:packet, payload, meta}}, entries} ->
        {:ok, payload, meta, pop(%{buffer | entries: entries}, byte_size(payload))}

      {:empty, _entries} ->
        {:empty, buffer}
    end
  end

  defp drop_padding(buffer) do
    case :queue.peek(buffer.entries) do
      {:value, {:padding, size}} -> pop(%{buffer | entries: :queue.drop(buffer.entries)}, size)
      _packet_or_empty -> buffer
    end
  end

  defp pop(buffer, size) do
    %{
      buffer
      | used_slots: buffer.used_slots - 1,
        read_at: rem(buffer.read_at + size, buffer.bytes),
        length: buffer.length - size
    }
  end
end
