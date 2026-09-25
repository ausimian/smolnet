defmodule SmolNet.TcpLossRecoveryTest do
  use ExUnit.Case, async: false

  alias SmolNet.Test.LossyLink
  alias SmolNet.Test.Timing

  @server {192, 0, 2, 1}
  @client {192, 0, 2, 2}
  @port 41_100

  @wait_5s Timing.liveness(5_000)

  setup do
    on_exit(&stop_all_stacks/0)
  end

  # A burst loss leaves the fast retransmission of its first segment answered
  # by a partial ACK. Without fast recovery (RFC 6582) the sender then waits
  # out smoltcp's 1 s minimum retransmission timeout and resends every segment
  # after the hole; with it, the sender resends each lost segment exactly once.
  test "a burst of lost segments is resent segment by segment, not after a timeout" do
    {server, client, link} = lossy_stacks(every: 40, burst: 3)

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: server)
    :ok = SmolNet.bind(listener, endpoint(@server))
    :ok = SmolNet.listen(listener, 1)
    accept = Task.async(fn -> SmolNet.accept(listener, @wait_5s) end)

    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: client)
    :ok = SmolNet.connect(socket, endpoint(@server), @wait_5s)
    {:ok, child} = Task.await(accept, @wait_5s)

    payload = :crypto.strong_rand_bytes(256 * 1024)
    reader = Task.async(fn -> read_exactly(child, byte_size(payload), []) end)
    assert :ok = SmolNet.send(socket, payload, @wait_5s)
    assert Task.await(reader, @wait_5s) == payload

    assert %{dropped: dropped, retransmitted: retransmitted} = LossyLink.stats(link)
    assert dropped > 0
    assert retransmitted == dropped
  end

  defp lossy_stacks(pattern) do
    {:ok, link} = LossyLink.start_link([lossy: :client] ++ pattern)
    {:ok, server} = SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 24}])
    {:ok, client} = SmolNet.start_stack(egress: {link, :client}, addresses: [{@client, 24}])
    :ok = LossyLink.connect(link, :server, client)
    :ok = LossyLink.connect(link, :client, server)
    {server, client, link}
  end

  defp read_exactly(_socket, 0, chunks), do: chunks |> Enum.reverse() |> IO.iodata_to_binary()

  defp read_exactly(socket, remaining, chunks) do
    {:ok, chunk} = SmolNet.recv(socket, 0, @wait_5s)
    read_exactly(socket, remaining - byte_size(chunk), [chunk | chunks])
  end

  defp endpoint(address), do: %{family: :inet, addr: address, port: @port}

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end
end
