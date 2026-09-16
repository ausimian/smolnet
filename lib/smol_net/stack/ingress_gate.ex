defmodule SmolNet.Stack.IngressGate do
  @moduledoc false

  @packets 1
  @bytes 2
  @rejected 3
  @status 4

  @up 1
  @down 2
  @closed 3

  @type t :: %__MODULE__{
          atomics: :atomics.atomics_ref(),
          packet_limit: pos_integer(),
          byte_limit: pos_integer()
        }

  @enforce_keys [:atomics, :packet_limit, :byte_limit]
  defstruct [:atomics, :packet_limit, :byte_limit]

  @spec new(pos_integer(), pos_integer(), :up | :down) :: t()
  def new(packet_limit, byte_limit, status) do
    atomics = :atomics.new(4, signed: false)
    :ok = :atomics.put(atomics, @status, encode_status(status))

    %__MODULE__{
      atomics: atomics,
      packet_limit: packet_limit,
      byte_limit: byte_limit
    }
  end

  @spec reserve(t(), pos_integer()) :: :ok | {:error, :link_down | :closed | :queue_full}
  def reserve(%__MODULE__{} = gate, bytes) do
    with :ok <- reservable(gate),
         :ok <- increment_below(gate.atomics, @packets, gate.packet_limit),
         :ok <- reserve_bytes(gate, bytes),
         :ok <- still_up(gate, bytes) do
      :ok
    else
      {:error, reason} ->
        reject(gate)
        {:error, reason}
    end
  end

  @spec release(t(), non_neg_integer()) :: :ok
  def release(%__MODULE__{atomics: atomics}, bytes) do
    _previous_packets = :atomics.sub_get(atomics, @packets, 1)
    _previous_bytes = :atomics.sub_get(atomics, @bytes, bytes)
    :ok
  end

  @spec reject(t()) :: :ok
  def reject(%__MODULE__{atomics: atomics}) do
    _count = :atomics.add_get(atomics, @rejected, 1)
    :ok
  end

  @spec mark_down(t()) :: :ok
  def mark_down(%__MODULE__{atomics: atomics}) do
    :ok = :atomics.put(atomics, @status, @down)
  end

  @spec close(t()) :: :ok
  def close(%__MODULE__{atomics: atomics}) do
    :ok = :atomics.put(atomics, @status, @closed)
  end

  @spec snapshot(t()) :: map()
  def snapshot(%__MODULE__{atomics: atomics} = gate) do
    %{
      packets: :atomics.get(atomics, @packets),
      packet_limit: gate.packet_limit,
      bytes: :atomics.get(atomics, @bytes),
      byte_limit: gate.byte_limit,
      rejected: :atomics.get(atomics, @rejected),
      status: decode_status(:atomics.get(atomics, @status))
    }
  end

  defp reservable(%__MODULE__{atomics: atomics}) do
    case :atomics.get(atomics, @status) do
      @up -> :ok
      @down -> {:error, :link_down}
      @closed -> {:error, :closed}
    end
  end

  defp reserve_bytes(gate, bytes) do
    case increment_below(gate.atomics, @bytes, gate.byte_limit, bytes) do
      :ok ->
        :ok

      {:error, :queue_full} = error ->
        _remaining = :atomics.sub_get(gate.atomics, @packets, 1)
        error
    end
  end

  defp still_up(gate, bytes) do
    case reservable(gate) do
      :ok ->
        :ok

      {:error, _reason} = error ->
        release(gate, bytes)
        error
    end
  end

  defp increment_below(atomics, index, limit, increment \\ 1) do
    current = :atomics.get(atomics, index)

    if increment > limit - min(current, limit) do
      {:error, :queue_full}
    else
      case :atomics.compare_exchange(atomics, index, current, current + increment) do
        :ok -> :ok
        _actual -> increment_below(atomics, index, limit, increment)
      end
    end
  end

  defp encode_status(:up), do: @up
  defp encode_status(:down), do: @down

  defp decode_status(@up), do: :up
  defp decode_status(@down), do: :down
  defp decode_status(@closed), do: :closed
end
