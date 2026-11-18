defmodule SmolNet.Core.Tcp.BuffersTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  # The send and receive buffers against models: the send buffer against a
  # literal ring of octets that does what smoltcp's `RingBuffer` does, so
  # that segments cut where the ring wraps match, and the receive buffer
  # against a map of octets by offset.

  alias SmolNet.Core.Tcp.RecvBuffer
  alias SmolNet.Core.Tcp.SendBuffer

  describe "the send buffer" do
    property "holds, drops and slices what a ring would" do
      check all(
              capacity <- integer(1..64),
              ops <- list_of(send_op(), max_length: 60),
              max_runs: 300
            ) do
        Enum.reduce(ops, {SendBuffer.new(capacity), ring(capacity)}, fn op, {buffer, ring} ->
          {buffer, ring} = apply_send(op, buffer, ring)
          assert SendBuffer.len(buffer) == ring.length

          assert SendBuffer.data(buffer) ==
                   ring_get(ring, 0, ring.length) <> ring_get2(ring, 0, ring.length)

          {buffer, ring}
        end)
      end
    end

    test "cuts a slice where the ring's storage wraps" do
      buffer = SendBuffer.new(9)
      {6, buffer} = SendBuffer.enqueue(buffer, "xxxyyy")
      buffer = SendBuffer.dequeue(buffer, 3)
      {6, buffer} = SendBuffer.enqueue(buffer, "abcdef")
      assert SendBuffer.get(buffer, 0, 9) == "yyyabc"
      assert SendBuffer.get(buffer, 6, 9) == "def"
    end

    test "starts an empty ring again from the beginning" do
      buffer = SendBuffer.new(4)
      {3, buffer} = SendBuffer.enqueue(buffer, "abc")
      buffer = SendBuffer.dequeue(buffer, 3)
      {4, buffer} = SendBuffer.enqueue(buffer, "wxyz")
      assert SendBuffer.get(buffer, 0, 4) == "wxyz"
    end
  end

  defp send_op do
    one_of([
      tuple({constant(:enqueue), binary(max_length: 70)}),
      tuple({constant(:dequeue), integer(0..70)}),
      tuple({constant(:get), integer(0..70), integer(0..70)})
    ])
  end

  defp apply_send({:enqueue, data}, buffer, ring) do
    {size, buffer} = SendBuffer.enqueue(buffer, data)
    {expected, ring} = ring_enqueue(ring, data)
    assert size == expected
    {buffer, ring}
  end

  defp apply_send({:dequeue, count}, buffer, ring) do
    count = min(count, ring.length)
    {SendBuffer.dequeue(buffer, count), ring_dequeue(ring, count)}
  end

  defp apply_send({:get, offset, size}, buffer, ring) do
    assert SendBuffer.get(buffer, offset, size) == ring_get(ring, offset, size)
    {buffer, ring}
  end

  # smoltcp's `RingBuffer<u8>`, octet for octet.
  defp ring(capacity),
    do: %{storage: :array.new(capacity, default: 0), read_at: 0, length: 0, capacity: capacity}

  defp ring_enqueue(ring, data) do
    {size1, ring, rest} = enqueue_many(ring, data)
    {size2, ring, _rest} = enqueue_many(ring, rest)
    {size1 + size2, ring}
  end

  defp enqueue_many(ring, data) do
    ring = if ring.length == 0, do: %{ring | read_at: 0}, else: ring
    write_at = rem(ring.read_at + ring.length, ring.capacity)
    window = ring.capacity - ring.length
    contiguous = min(window, ring.capacity - write_at)
    size = min(contiguous, byte_size(data))

    storage =
      data
      |> binary_part(0, size)
      |> :binary.bin_to_list()
      |> Enum.with_index()
      |> Enum.reduce(ring.storage, fn {byte, i}, storage ->
        :array.set(write_at + i, byte, storage)
      end)

    {size, %{ring | storage: storage, length: ring.length + size},
     binary_part(data, size, byte_size(data) - size)}
  end

  defp ring_dequeue(ring, count),
    do: %{ring | length: ring.length - count, read_at: rem(ring.read_at + count, ring.capacity)}

  defp ring_get(ring, offset, size) do
    if offset > ring.length do
      <<>>
    else
      start_at = rem(ring.read_at + offset, ring.capacity)
      size = size |> min(ring.length - offset) |> min(ring.capacity - start_at)
      for i <- 0..(size - 1)//1, into: <<>>, do: <<:array.get(start_at + i, ring.storage)>>
    end
  end

  # What follows the first contiguous piece, past the wrap.
  defp ring_get2(ring, offset, size) do
    first = byte_size(ring_get(ring, offset, size))

    if first < min(size, ring.length - offset),
      do: ring_get(ring, offset + first, size - first),
      else: <<>>
  end

  describe "the receive buffer" do
    property "keeps out-of-order data, the later write winning, and reads it in order" do
      check all(
              capacity <- integer(1..64),
              ops <- list_of(recv_op(), max_length: 60),
              max_runs: 300
            ) do
        model = %{capacity: capacity, queue: <<>>, ooo: %{}, read_at: 0}

        Enum.reduce(ops, {RecvBuffer.new(capacity), model}, fn op, {buffer, model} ->
          {buffer, model} = apply_recv(op, buffer, model)
          assert RecvBuffer.len(buffer) == byte_size(model.queue)
          assert RecvBuffer.data(buffer) == model.queue
          assert RecvBuffer.window(buffer) == capacity - byte_size(model.queue)

          assert RecvBuffer.contiguous(buffer) ==
                   min(byte_size(model.queue), capacity - model.read_at)

          {buffer, model}
        end)
      end
    end
  end

  defp recv_op do
    one_of([
      tuple({constant(:write), integer(0..70), binary(max_length: 40)}),
      tuple({constant(:enqueue), integer(0..70)}),
      tuple({constant(:dequeue), integer(0..70)}),
      tuple({constant(:peek), integer(0..70)})
    ])
  end

  defp apply_recv({:write, offset, data}, buffer, model) do
    window = model.capacity - byte_size(model.queue)
    {size, buffer} = RecvBuffer.write(buffer, offset, data)
    expected = if offset > window, do: 0, else: min(byte_size(data), window - offset)
    assert size == expected

    ooo =
      data
      |> binary_part(0, expected)
      |> :binary.bin_to_list()
      |> Enum.with_index(offset)
      |> Enum.reduce(model.ooo, fn {byte, at}, ooo -> Map.put(ooo, at, byte) end)

    {buffer, %{model | ooo: ooo}}
  end

  # Only octets that were written can be enqueued, as the assembler sees to.
  defp apply_recv({:enqueue, count}, buffer, model) do
    count = Enum.find(0..count, &(not Map.has_key?(model.ooo, &1))) || count
    count = min(count, model.capacity - byte_size(model.queue))
    taken = for i <- 0..(count - 1)//1, into: <<>>, do: <<Map.fetch!(model.ooo, i)>>

    ooo =
      for {at, byte} <- model.ooo, at >= count, into: %{}, do: {at - count, byte}

    {RecvBuffer.enqueue(buffer, count), %{model | queue: model.queue <> taken, ooo: ooo}}
  end

  defp apply_recv({:dequeue, size}, buffer, model) do
    {data, buffer} = RecvBuffer.dequeue(buffer, size)
    size = min(size, byte_size(model.queue))
    assert data == binary_part(model.queue, 0, size)
    rest = binary_part(model.queue, size, byte_size(model.queue) - size)
    {buffer, %{model | queue: rest, read_at: rem(model.read_at + size, model.capacity)}}
  end

  defp apply_recv({:peek, size}, buffer, model) do
    assert RecvBuffer.peek(buffer, size) ==
             binary_part(model.queue, 0, min(size, byte_size(model.queue)))

    {buffer, model}
  end
end
