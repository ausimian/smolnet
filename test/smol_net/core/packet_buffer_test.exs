defmodule SmolNet.Core.PacketBufferTest do
  use ExUnit.Case, async: true

  # smoltcp's `storage::packet_buffer` tests, on its buffer of four packets
  # and 16 bytes. `test_peek`, `test_dequeue_with` and `clear` are not
  # ported: this buffer has no peek, no fallible dequeue and no reset.

  alias SmolNet.Core.PacketBuffer

  defp buffer, do: PacketBuffer.new(4, 16)

  defp enqueue!(buffer, payload) do
    assert {:ok, buffer} = PacketBuffer.enqueue(buffer, payload, :meta)
    buffer
  end

  defp dequeue!(buffer) do
    assert {:ok, payload, :meta, buffer} = PacketBuffer.dequeue(buffer)
    {payload, buffer}
  end

  defp bytes(size), do: :binary.copy("x", size)

  test "test_simple" do
    buffer = enqueue!(buffer(), "abcdef")
    assert {:full, buffer} = PacketBuffer.enqueue(buffer, bytes(16), :meta)
    assert buffer.used_slots == 1
    assert {"abcdef", buffer} = dequeue!(buffer)
    assert {:empty, _buffer} = PacketBuffer.dequeue(buffer)
  end

  test "test_padding" do
    buffer = buffer() |> enqueue!(bytes(6)) |> enqueue!(bytes(8))
    {_payload, buffer} = dequeue!(buffer)
    buffer = enqueue!(buffer, "abcd")
    assert buffer.used_slots == 3
    {_payload, buffer} = dequeue!(buffer)
    assert {"abcd", buffer} = dequeue!(buffer)
    assert buffer.used_slots == 0
  end

  test "test_padding_with_large_payload" do
    buffer = enqueue!(buffer(), bytes(12))
    {_payload, buffer} = dequeue!(buffer)
    enqueue!(buffer, "abcdefghijkl")
  end

  test "test_metadata_full_empty" do
    buffer = buffer()
    assert PacketBuffer.empty?(buffer)
    buffer = buffer |> enqueue!("a") |> enqueue!("a") |> enqueue!("a")
    refute PacketBuffer.empty?(buffer)
    buffer = enqueue!(buffer, "a")
    assert buffer.used_slots == 4
    assert {:full, _buffer} = PacketBuffer.enqueue(buffer, "a", :meta)
  end

  test "test_window_too_small" do
    buffer = buffer() |> enqueue!(bytes(4)) |> enqueue!(bytes(8))
    {_payload, buffer} = dequeue!(buffer)
    assert {:full, buffer} = PacketBuffer.enqueue(buffer, bytes(16), :meta)
    assert buffer.used_slots == 1
  end

  test "test_contiguous_window_too_small" do
    buffer = buffer() |> enqueue!(bytes(4)) |> enqueue!(bytes(8))
    {_payload, buffer} = dequeue!(buffer)
    assert {:full, buffer} = PacketBuffer.enqueue(buffer, bytes(8), :meta)
    assert buffer.used_slots == 1
  end

  test "test_contiguous_window_wrap" do
    buffer = enqueue!(buffer(), bytes(15))
    {_payload, buffer} = dequeue!(buffer)
    enqueue!(buffer, bytes(16))
  end

  test "test_capacity_too_small" do
    assert {:full, _buffer} = PacketBuffer.enqueue(buffer(), bytes(32), :meta)
  end

  test "test_contig_window_prioritized" do
    buffer = enqueue!(buffer(), bytes(4))
    {_payload, buffer} = dequeue!(buffer)
    enqueue!(buffer, bytes(5))
  end

  # From `test_enqueue_fallible_full_metadata_fn_not_called`, with
  # `enqueue`: the padding takes the last slot, the packet is refused, and
  # the padding stays until a dequeue reaches it.
  test "a refused packet can leave its padding behind" do
    buffer = buffer() |> enqueue!(bytes(12)) |> enqueue!("a") |> enqueue!("b") |> enqueue!("c")
    {payload, buffer} = dequeue!(buffer)
    assert byte_size(payload) == 12

    assert {:full, buffer} = PacketBuffer.enqueue(buffer, bytes(5), :meta)
    assert buffer.used_slots == 4

    {"a", buffer} = dequeue!(buffer)
    {"b", buffer} = dequeue!(buffer)
    {"c", buffer} = dequeue!(buffer)
    refute PacketBuffer.empty?(buffer)
    assert {:empty, buffer} = PacketBuffer.dequeue(buffer)
    assert PacketBuffer.empty?(buffer)
  end
end
