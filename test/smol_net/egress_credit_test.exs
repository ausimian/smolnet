defmodule SmolNet.EgressCreditTest do
  use ExUnit.Case, async: false

  alias SmolNet.Test.CreditLink
  alias SmolNet.Test.Timing

  @server {192, 0, 2, 1}
  @client {192, 0, 2, 2}
  @port 41_200
  @mtu 1_280

  @wait_5s Timing.liveness(5_000)
  @wait_10s Timing.liveness(10_000)
  @idle_50ms Timing.quiescence(50)

  setup do
    on_exit(&stop_all_stacks/0)
  end

  # Eight streams can have eight send buffers of data in flight, far more than
  # the link's buffer holds. With credit, the surplus waits in the senders'
  # sockets instead of overrunning the link.
  test "parallel TCP streams through a bounded link never overrun it" do
    {server, client, link} = credit_stacks({16, 16 * @mtu})
    streams = 8
    stream_bytes = 128 * 1024

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: server)
    :ok = SmolNet.bind(listener, endpoint(@server, @port))
    :ok = SmolNet.listen(listener, streams)

    receiver =
      Task.async(fn ->
        for _stream <- 1..streams do
          {:ok, child} = SmolNet.accept(listener, @wait_5s)
          Task.async(fn -> read_exactly(child, stream_bytes, []) end)
        end
        |> Task.await_many(@wait_10s)
      end)

    # Connect one at a time: the listener's native pool admits only a few
    # simultaneous handshakes.
    sockets =
      for _stream <- 1..streams do
        {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: client)
        :ok = SmolNet.connect(socket, endpoint(@server, @port), @wait_5s)
        socket
      end

    payloads = for _stream <- 1..streams, do: :crypto.strong_rand_bytes(stream_bytes)

    sockets
    |> Enum.zip(payloads)
    |> Enum.map(fn {socket, payload} ->
      Task.async(fn -> :ok = SmolNet.send(socket, payload, @wait_10s) end)
    end)
    |> Task.await_many(@wait_10s)

    assert Enum.sort(Task.await(receiver, @wait_10s)) == Enum.sort(payloads)

    assert %{overruns: 0, refused: 0, saturated: saturated} = CreditLink.stats(link)
    assert saturated > 0
  end

  test "a stack holds egress without credit and does no work until granted" do
    {:ok, stack} =
      SmolNet.start_stack(
        egress: {self(), :held},
        addresses: [{@server, 24}],
        egress_credit: {0, 0}
      )

    {:ok, socket} = SmolNet.open(:inet, :dgram, :udp, stack: stack)
    :ok = SmolNet.bind(socket, endpoint(@server, @port))
    :ok = SmolNet.sendto(socket, "first", endpoint(@client, @port), :nowait)
    :ok = SmolNet.sendto(socket, "second", endpoint(@client, @port), :nowait)

    refute_receive {:smol_stack, :held, :egress, _packets}, @idle_50ms
    idle = native_counters(stack)
    refute_receive {:smol_stack, :held, :egress, _packets}, @idle_50ms
    assert native_counters(stack) == idle

    # A 33-byte datagram needs one packet and 33 bytes of credit.
    :ok = SmolNet.grant_egress(stack, 1, 10)
    refute_receive {:smol_stack, :held, :egress, _packets}, @idle_50ms

    :ok = SmolNet.grant_egress(stack, 0, 23)
    assert_receive {:smol_stack, :held, :egress, [first]}
    assert udp_payload(first) == "first"
    assert egress_credit(stack) == %{packets: 0, bytes: 0}
    refute_receive {:smol_stack, :held, :egress, _packets}, @idle_50ms

    :ok = SmolNet.grant_egress(stack, 5, 1_000)
    assert_receive {:smol_stack, :held, :egress, [second]}
    assert udp_payload(second) == "second"
    assert egress_credit(stack) == %{packets: 4, bytes: 1_000 - byte_size(second)}
  end

  test "UDP senders see a full transmit ring instead of losing datagrams" do
    {:ok, stack} =
      SmolNet.start_stack(
        egress: {self(), :ring},
        addresses: [{@server, 24}],
        egress_credit: {0, 0}
      )

    {:ok, socket} = SmolNet.open(:inet, :dgram, :udp, stack: stack)
    :ok = SmolNet.bind(socket, endpoint(@server, @port))
    accepted = fill_ring(socket, 0)
    assert accepted > 0

    :ok = SmolNet.grant_egress(stack, 1_000, 1_000_000)
    payloads = socket |> receive_datagrams(accepted, []) |> Enum.map(&udp_payload/1)
    assert payloads == Enum.map(0..(accepted - 1), &Integer.to_string/1)
    refute_receive {:smol_stack, :ring, :egress, _packets}, @idle_50ms
  end

  test "a loopback link grants back the credit it forwards" do
    {:ok, _link, stack} =
      SmolNet.Loopback.start_link(addresses: [{@server, 8}], egress_credit: {2, 2 * 1_500})

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    :ok = SmolNet.bind(listener, endpoint(@server, @port))
    :ok = SmolNet.listen(listener, 1)
    accept = Task.async(fn -> SmolNet.accept(listener, @wait_5s) end)

    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    :ok = SmolNet.connect(socket, endpoint(@server, @port), @wait_5s)
    {:ok, child} = Task.await(accept, @wait_5s)

    payload = :crypto.strong_rand_bytes(256 * 1024)
    reader = Task.async(fn -> read_exactly(child, byte_size(payload), []) end)
    assert :ok = SmolNet.send(socket, payload, @wait_5s)
    assert Task.await(reader, @wait_5s) == payload
  end

  test "egress credit options and grants are validated" do
    for credit <- [5, {-1, 0}, {0, -1}, {1.0, 1}, {1, 0x1_0000_0000}, {1, 2, 3}] do
      assert {:error, :invalid_egress_credit} = SmolNet.start_stack(egress_credit: credit)
    end

    {:ok, unlimited} = SmolNet.start_stack(egress_credit: :infinity)
    assert {:error, :egress_credit_disabled} = SmolNet.grant_egress(unlimited, 1, 1)
    assert egress_credit(unlimited) == nil

    {:ok, credited} = SmolNet.start_stack(egress_credit: {3, 4_000})
    assert egress_credit(credited) == %{packets: 3, bytes: 4_000}
    assert {:error, :invalid_egress_credit} = SmolNet.grant_egress(credited, -1, 0)
    assert {:error, :invalid_egress_credit} = SmolNet.grant_egress(credited, 0, :lots)
    assert {:error, :invalid_egress_credit} = SmolNet.grant_egress(credited, 0x1_0000_0000, 0)
    assert {:error, :invalid_egress_credit} = SmolNet.grant_egress(:not_a_stack, 1, 1)
    assert :ok = SmolNet.grant_egress(credited, 0, 0)
    assert :ok = SmolNet.grant_egress(credited, 2, 1_000)
    assert egress_credit(credited) == %{packets: 5, bytes: 5_000}

    :ok = SmolNet.stop_stack(credited)
    assert {:error, :closed} = SmolNet.grant_egress(credited, 1, 1)
  end

  defp credit_stacks(credit) do
    {:ok, link} = CreditLink.start_link(credit: credit, mtu: @mtu)

    {:ok, server} =
      SmolNet.start_stack(
        egress: {link, :server},
        mtu: @mtu,
        addresses: [{@server, 24}],
        egress_credit: credit
      )

    {:ok, client} =
      SmolNet.start_stack(
        egress: {link, :client},
        mtu: @mtu,
        addresses: [{@client, 24}],
        egress_credit: credit
      )

    :ok = CreditLink.connect(link, :server, server, client)
    :ok = CreditLink.connect(link, :client, client, server)
    {server, client, link}
  end

  defp fill_ring(socket, sent) do
    case SmolNet.sendto(socket, Integer.to_string(sent), endpoint(@client, @port), :nowait) do
      :ok ->
        fill_ring(socket, sent + 1)

      {:select, select_info} ->
        :ok = SmolNet.cancel(socket, select_info)
        sent
    end
  end

  defp receive_datagrams(_socket, 0, packets), do: Enum.reverse(packets)

  defp receive_datagrams(socket, remaining, packets) do
    assert_receive {:smol_stack, :ring, :egress, batch}, @wait_5s
    receive_datagrams(socket, remaining - length(batch), Enum.reverse(batch, packets))
  end

  defp read_exactly(_socket, 0, chunks), do: chunks |> Enum.reverse() |> IO.iodata_to_binary()

  defp read_exactly(socket, remaining, chunks) do
    {:ok, chunk} = SmolNet.recv(socket, 0, @wait_5s)
    read_exactly(socket, remaining - byte_size(chunk), [chunk | chunks])
  end

  defp udp_payload(<<4::4, ihl::4, _rest::binary>> = packet) do
    offset = ihl * 4 + 8
    binary_part(packet, offset, byte_size(packet) - offset)
  end

  defp native_counters(stack) do
    {:ok, %{native: %{result: %{counters: counters}}}} = SmolNet.stack_info(stack)
    Map.take(counters, [:native_calls, :poll_calls, :emitted_packets])
  end

  defp egress_credit(stack) do
    {:ok, %{native: %{result: %{egress_credit: credit}}}} = SmolNet.stack_info(stack)
    credit
  end

  defp endpoint(address, port), do: %{family: :inet, addr: address, port: port}

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end
end
