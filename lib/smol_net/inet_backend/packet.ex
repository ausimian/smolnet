defmodule SmolNet.InetBackend.Packet do
  @moduledoc false

  @spec encode(iodata(), SmolNet.InetBackend.Options.packet(), pos_integer()) ::
          {:ok, binary()} | {:error, :einval | :emsgsize}
  def encode(data, packet, packet_size) do
    with {:ok, payload} <- to_binary(data),
         :ok <- validate_size(payload, packet, packet_size) do
      frame(payload, packet)
    end
  end

  @spec extract(binary(), SmolNet.InetBackend.Options.packet(), non_neg_integer(), pos_integer()) ::
          {:ok, binary(), binary()} | :more | {:error, :emsgsize}
  def extract(buffer, :raw, 0, _packet_size) do
    if buffer == <<>>, do: :more, else: {:ok, buffer, <<>>}
  end

  def extract(buffer, :raw, length, _packet_size) when byte_size(buffer) >= length do
    <<packet::binary-size(^length), rest::binary>> = buffer
    {:ok, packet, rest}
  end

  def extract(_buffer, :raw, _length, _packet_size), do: :more

  def extract(buffer, :line, _length, packet_size) do
    case :binary.match(buffer, "\n") do
      {offset, 1} when offset + 1 <= packet_size ->
        size = offset + 1
        <<packet::binary-size(^size), rest::binary>> = buffer
        {:ok, packet, rest}

      {_offset, 1} ->
        {:error, :emsgsize}

      :nomatch when byte_size(buffer) >= packet_size ->
        {:error, :emsgsize}

      :nomatch ->
        :more
    end
  end

  def extract(buffer, width, _length, packet_size) when width in [1, 2, 4] do
    if byte_size(buffer) < width do
      :more
    else
      <<size::unsigned-big-integer-size(^width)-unit(8), rest::binary>> = buffer

      cond do
        size > packet_size ->
          {:error, :emsgsize}

        byte_size(rest) < size ->
          :more

        true ->
          <<packet::binary-size(^size), tail::binary>> = rest
          {:ok, packet, tail}
      end
    end
  end

  @spec represent(binary(), :binary | :list) :: binary() | list()
  def represent(packet, :binary), do: packet
  def represent(packet, :list), do: :binary.bin_to_list(packet)

  defp to_binary(data) do
    {:ok, IO.iodata_to_binary(data)}
  rescue
    ArgumentError -> {:error, :einval}
  end

  defp validate_size(payload, packet, packet_size) do
    size = byte_size(payload)

    cond do
      size > packet_size -> {:error, :emsgsize}
      packet == 1 and size > 255 -> {:error, :emsgsize}
      packet == 2 and size > 65_535 -> {:error, :emsgsize}
      true -> :ok
    end
  end

  defp frame(payload, packet) when packet in [:raw, :line], do: {:ok, payload}
  defp frame(payload, 1), do: {:ok, <<byte_size(payload)::8, payload::binary>>}
  defp frame(payload, 2), do: {:ok, <<byte_size(payload)::16, payload::binary>>}
  defp frame(payload, 4), do: {:ok, <<byte_size(payload)::32, payload::binary>>}
end
