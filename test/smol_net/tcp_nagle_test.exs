defmodule SmolNet.TcpNagleTest do
  use ExUnit.Case, async: false

  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Timing

  @server {192, 0, 2, 1}
  @client {192, 0, 2, 2}
  @port 41_200

  # Longer than one segment at the default MTU of 1500, whose MSS is 1460.
  @write_bytes 2_000

  @wait_5s Timing.liveness(5_000)

  setup do
    on_exit(&stop_all_stacks/0)
  end

  # Nagle's algorithm must not hold back the tail of a write longer than an
  # MSS until the peer acknowledges the write's first segment. That cost an
  # extra round trip, plus the peer's delayed ACK, on every such write, such
  # as a TLS 1.3 ClientHello carrying a post-quantum key share (#102). The
  # link holds every packet, so no ACK reaches the sender: the whole write
  # going out proves that the sender did not wait for one.
  test "a write longer than an MSS is sent whole without waiting for an ACK" do
    {server, client, link} = stacks()

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: server)
    :ok = SmolNet.bind(listener, endpoint(@server))
    :ok = SmolNet.listen(listener, 1)
    accept = Task.async(fn -> SmolNet.accept(listener, @wait_5s) end)

    {:ok, socket} = SmolNet.open(:inet, :stream, :tcp, stack: client)
    :ok = SmolNet.connect(socket, endpoint(@server), @wait_5s)
    {:ok, child} = Task.await(accept, @wait_5s)

    :ok = RawIpLink.fault(link, :hold)
    flush_egress()

    payload = :crypto.strong_rand_bytes(@write_bytes)
    assert :ok = SmolNet.send(socket, payload, @wait_5s)

    segments = await_client_data(%{})
    assert map_size(segments) > 1
    assert segments |> Enum.sort() |> Enum.map_join(&elem(&1, 1)) == payload

    :ok = RawIpLink.fault(link, :pass)
    :ok = RawIpLink.release(link)
    assert {:ok, ^payload} = SmolNet.recv(child, @write_bytes, @wait_5s)
  end

  # Collects the data segments the client sends, by sequence number, until
  # they carry the whole write. A retransmission replaces its original.
  defp await_client_data(segments) do
    if segments |> Map.values() |> Enum.map(&byte_size/1) |> Enum.sum() >= @write_bytes do
      segments
    else
      receive do
        {:test_link_egress, :client, packet} ->
          case tcp_data(packet) do
            {sequence, data} -> await_client_data(Map.put(segments, sequence, data))
            nil -> await_client_data(segments)
          end
      after
        @wait_5s ->
          sent = Enum.map(segments, fn {sequence, data} -> {sequence, byte_size(data)} end)
          flunk("the client sent only these segments, as {sequence, bytes}: #{inspect(sent)}")
      end
    end
  end

  defp tcp_data(<<4::4, ihl::4, _tos, total::16, _rest::binary>> = packet) do
    header_bytes = ihl * 4
    tcp_bytes = total - header_bytes
    <<_ip::binary-size(^header_bytes), tcp::binary-size(^tcp_bytes), _padding::binary>> = packet
    <<_ports::32, sequence::32, _ack::32, offset::4, _bits::12, _tail::binary>> = tcp

    case binary_part(tcp, offset * 4, byte_size(tcp) - offset * 4) do
      <<>> -> nil
      data -> {sequence, data}
    end
  end

  defp flush_egress do
    receive do
      {:test_link_egress, _link_ref, _packet} -> flush_egress()
    after
      0 -> :ok
    end
  end

  defp stacks do
    {:ok, link} = RawIpLink.start_link(self())
    {:ok, server} = SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 24}])
    {:ok, client} = SmolNet.start_stack(egress: {link, :client}, addresses: [{@client, 24}])
    :ok = RawIpLink.connect(link, :server, client)
    :ok = RawIpLink.connect(link, :client, server)
    {server, client, link}
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
