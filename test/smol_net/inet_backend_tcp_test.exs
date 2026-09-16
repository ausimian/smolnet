defmodule SmolNet.InetBackendTcpTest do
  use ExUnit.Case, async: false

  alias SmolNet.InetBackend.Tcp
  alias SmolNet.Stack.Ref
  alias SmolNet.Test.IPv6TcpPeer

  @client {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @peer {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    on_exit(&stop_all_stacks/0)
    :ok
  end

  test "public gen_tcp and inet entry points drive a passive IPv6 client" do
    {stack, peer} = stack_and_peer()

    assert {:ok, socket = {:"$inet", Tcp, adapter}} =
             :gen_tcp.connect(@peer, 443, client_options(stack), 1_000)

    assert Process.alive?(adapter)
    assert :ok = :gen_tcp.send(socket, ["hel", "lo"])
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).received == "hello" end)

    assert :ok = IPv6TcpPeer.send_data(peer, "world")
    assert {:ok, "world"} = :gen_tcp.recv(socket, 5, 1_000)
    assert {:ok, {@peer, 443}} = :inet.peername(socket)
    assert {:ok, {@client, local_port}} = :inet.sockname(socket)
    assert local_port in 49_152..50_175

    assert {:ok,
            [
              active: false,
              mode: :binary,
              packet: :raw,
              packet_size: 65_536
            ]} = :inet.getopts(socket, [:active, :mode, :packet, :packet_size])

    assert %{owner: owner, read_pending: false, write_pending: false} = :inet.info(socket)
    assert owner == self()
    assert {:ok, stats} = :inet.getstat(socket)
    assert Keyword.keys(stats) == :inet.stats()
    assert Enum.all?(stats, fn {_name, value} -> value == 0 end)

    assert :ok = :gen_tcp.close(socket)
    refute Process.alive?(adapter)
    assert Process.alive?(Ref.pids(stack).stack)
  end

  test "binary and list passive clients time out without blocking opposite-direction work" do
    {stack, peer} = stack_and_peer()
    {:ok, socket} = :gen_tcp.connect(@peer, 443, client_options(stack, [:list]), 1_000)

    receiver = Task.async(fn -> :gen_tcp.recv(socket, 4, 100) end)

    assert_eventually(fn -> Tcp.info(socket).read_pending end)
    assert :ok = :gen_tcp.send(socket, "ping")
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).received == "ping" end)
    assert {:error, :busy} = :gen_tcp.recv(socket, 1, 0)
    assert {:error, :timeout} = Task.await(receiver)

    assert :ok = IPv6TcpPeer.send_data(peer, "pong")
    assert {:ok, ~c"pong"} = :gen_tcp.recv(socket, 4, 1_000)
  end

  test "packet modes span arbitrary native chunk boundaries and frame sends" do
    cases = [
      {:line, ["hello", "\n"], "hello\n", "world"},
      {1, [<<5>>, "he", "llo"], "hello", <<5, "world">>},
      {2, [<<0>>, <<5, "he">>, "llo"], "hello", <<0, 5, "world">>},
      {4, [<<0, 0>>, <<0>>, <<5, "he">>, "llo"], "hello", <<0, 0, 0, 5, "world">>}
    ]

    for {packet, incoming_chunks, expected, framed_outgoing} <- cases do
      {stack, peer} = stack_and_peer()

      {:ok, socket} =
        :gen_tcp.connect(
          @peer,
          443,
          client_options(stack, packet: packet, packet_size: 32),
          1_000
        )

      Enum.each(incoming_chunks, fn chunk ->
        assert :ok = IPv6TcpPeer.send_data(peer, chunk)
      end)

      assert {:ok, ^expected} = :gen_tcp.recv(socket, 999, 1_000)
      assert :ok = :gen_tcp.send(socket, "world")

      assert_eventually(fn ->
        IPv6TcpPeer.stats(peer).received == framed_outgoing
      end)

      :ok = :gen_tcp.close(socket)
      :ok = SmolNet.stop_stack(stack)
    end
  end

  test "packet_size rejects oversized outbound and declared inbound frames" do
    {stack, peer} = stack_and_peer()

    {:ok, socket = {:"$inet", Tcp, adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(stack, packet: 2, packet_size: 4), 1_000)

    assert {:error, :emsgsize} = :gen_tcp.send(socket, "12345")
    assert :ok = IPv6TcpPeer.send_data(peer, <<0, 5>>)
    assert {:error, :emsgsize} = :gen_tcp.recv(socket, 0, 1_000)
    assert_eventually(fn -> not Process.alive?(adapter) end)
  end

  test "active once and counted active modes count logical packets" do
    {stack, peer} = stack_and_peer()

    {:ok, socket} =
      :gen_tcp.connect(@peer, 443, client_options(stack, packet: 1), 1_000)

    assert :ok = :inet.setopts(socket, active: :once)
    assert :ok = IPv6TcpPeer.send_data(peer, <<3, "one", 3, "two">>)
    assert_receive {:tcp, ^socket, "one"}, 1_000
    assert {:ok, [active: false]} = :inet.getopts(socket, [:active])
    refute_receive {:tcp, ^socket, _data}, 20

    assert :ok = :inet.setopts(socket, active: 2)
    assert_receive {:tcp, ^socket, "two"}, 1_000
    assert :ok = IPv6TcpPeer.send_data(peer, <<5, "three">>)
    assert_receive {:tcp, ^socket, "three"}, 1_000
    assert_receive {:tcp_passive, ^socket}, 1_000
    assert {:ok, [active: false]} = :inet.getopts(socket, [:active])
  end

  test "active true delivers framed packets and remote close exactly once" do
    {stack, peer} = stack_and_peer()

    {:ok, socket} =
      :gen_tcp.connect(
        @peer,
        443,
        client_options(stack, packet: 1, active: true),
        1_000
      )

    assert :ok = IPv6TcpPeer.send_data(peer, <<1, "a", 1, "b", 1, "c">>)
    assert_receive {:tcp, ^socket, "a"}, 1_000
    assert_receive {:tcp, ^socket, "b"}, 1_000
    assert_receive {:tcp, ^socket, "c"}, 1_000

    assert :ok = IPv6TcpPeer.finish(peer)
    assert_receive {:tcp_closed, ^socket}, 1_000
    refute_receive {:tcp_closed, ^socket}, 20
  end

  test "sustained active input is drained in bounded logical batches" do
    {stack, _peer} = stack_and_peer()

    {:ok, socket = {:"$inet", Tcp, adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(stack, packet: 1), 1_000)

    frames =
      for index <- 0..39, into: <<>> do
        <<1, ?a + rem(index, 26)>>
      end

    assert :ok = :gen_tcp.unrecv(socket, frames)
    assert :ok = :inet.setopts(socket, active: true)
    assert {:ok, [active: true]} = :inet.getopts(socket, [:active])

    packets = receive_packets(socket, 40, [])
    assert length(packets) == 40
    assert Process.alive?(adapter)
  end

  test "controlling_process atomically forwards queued data and redirects later data" do
    {stack, peer} = stack_and_peer()

    {:ok, socket} =
      :gen_tcp.connect(
        @peer,
        443,
        client_options(stack, packet: 1, active: true),
        1_000
      )

    parent = self()

    new_owner =
      spawn(fn ->
        send(parent, :new_owner_ready)
        forward_to_test(parent)
      end)

    assert_receive :new_owner_ready
    assert :ok = IPv6TcpPeer.send_data(peer, <<3, "old">>)
    assert_eventually(fn -> socket_message_queued?(socket) end)

    assert :ok = :gen_tcp.controlling_process(socket, new_owner)
    assert_receive {:new_owner, {:tcp, ^socket, "old"}}, 1_000

    assert :ok = IPv6TcpPeer.send_data(peer, <<3, "new">>)
    assert_receive {:new_owner, {:tcp, ^socket, "new"}}, 1_000
    refute_receive {:tcp, ^socket, _data}, 20

    send(new_owner, :stop)
  end

  test "owner death closes its adapter and killing an adapter preserves siblings" do
    {stack, peer} = stack_and_peer()
    parent = self()

    owner =
      spawn(fn ->
        result = :gen_tcp.connect(@peer, 443, client_options(stack), 1_000)
        send(parent, {:owned_socket, result})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:owned_socket, {:ok, {:"$inet", Tcp, owned_adapter}}}, 1_000
    Process.exit(owner, :kill)
    assert_eventually(fn -> not Process.alive?(owned_adapter) end)
    assert_eventually(fn -> native_socket_count(stack) == 0 end)
    assert Process.alive?(Ref.pids(stack).stack)

    {:ok, victim = {:"$inet", Tcp, victim_adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(stack), 1_000)

    {:ok, sibling = {:"$inet", Tcp, sibling_adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(stack), 1_000)

    Process.exit(victim_adapter, :kill)
    assert_eventually(fn -> not Process.alive?(victim_adapter) end)
    assert_eventually(fn -> native_socket_count(stack) == 1 end)
    assert Process.alive?(Ref.pids(stack).stack)
    assert Process.alive?(sibling_adapter)
    assert {:error, :closed} = :gen_tcp.send(victim, "closed")
    assert :ok = :gen_tcp.send(sibling, "alive")
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).received == "alive" end)
  end

  test "connect timeout cancels its waiter and releases the adapter socket" do
    {stack, _peer} = stack_and_peer(:ignore)

    assert {:error, :timeout} = :gen_tcp.connect(@peer, 443, client_options(stack), 10)
    assert_eventually(fn -> adapter_pids(stack) == [] end)
    assert_eventually(fn -> native_socket_count(stack) == 0 end)
  end

  test "owner death aborts an in-progress connect and releases its socket" do
    {stack, _peer} = stack_and_peer(:ignore)
    parent = self()

    owner =
      spawn(fn ->
        send(parent, :connect_started)
        result = :gen_tcp.connect(@peer, 443, client_options(stack), :infinity)
        send(parent, {:connect_result, result})
      end)

    assert_receive :connect_started
    assert_eventually(fn -> length(adapter_pids(stack)) == 1 end)
    [adapter] = adapter_pids(stack)

    Process.exit(owner, :kill)
    assert_eventually(fn -> not Process.alive?(adapter) end)
    assert_eventually(fn -> native_socket_count(stack) == 0 end)
    refute_receive {:connect_result, _result}, 20
  end

  test "independent read and write continuations coexist while competing operations are busy" do
    {stack, peer} = stack_and_peer()
    {:ok, socket} = :gen_tcp.connect(@peer, 443, client_options(stack), 1_000)
    assert :ok = IPv6TcpPeer.hold_acks(peer, true)

    receiver = Task.async(fn -> :gen_tcp.recv(socket, 4, 2_000) end)
    assert_eventually(fn -> Tcp.info(socket).read_pending end)
    assert {:error, :busy} = :inet.setopts(socket, packet: 1)
    assert :ok = :inet.setopts(socket, send_timeout: 2_000)

    payload = :binary.copy("send", 2_000)
    sender = Task.async(fn -> :gen_tcp.send(socket, payload) end)
    assert_eventually(fn -> Tcp.info(socket).write_pending end)

    assert {:error, :busy} = :gen_tcp.recv(socket, 1, 0)
    assert {:error, :busy} = :gen_tcp.send(socket, "competing")

    assert :ok = IPv6TcpPeer.send_data(peer, "read")
    assert {:ok, "read"} = Task.await(receiver)
    assert :ok = IPv6TcpPeer.release_acks(peer)
    assert :ok = Task.await(sender, 2_000)
  end

  test "send_timeout_close returns the unsent suffix and terminates the adapter" do
    {stack, peer} = stack_and_peer()

    {:ok, socket = {:"$inet", Tcp, adapter}} =
      :gen_tcp.connect(
        @peer,
        443,
        client_options(stack, send_timeout: 0, send_timeout_close: true),
        1_000
      )

    assert :ok = IPv6TcpPeer.hold_acks(peer, true)
    payload = :binary.copy("timeout", 2_000)
    assert {:error, {:timeout, remainder}} = :gen_tcp.send(socket, payload)
    assert byte_size(remainder) in 1..(byte_size(payload) - 1)
    assert_eventually(fn -> not Process.alive?(adapter) end)
    assert_eventually(fn -> native_socket_count(stack) == 0 end)
  end

  test "peer reset is terminal for passive reads and pending writes" do
    {read_stack, read_peer} = stack_and_peer()

    {:ok, read_socket = {:"$inet", Tcp, read_adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(read_stack), 1_000)

    assert :ok = IPv6TcpPeer.reset(read_peer)
    assert {:error, :econnreset} = :gen_tcp.recv(read_socket, 1, 1_000)
    assert_eventually(fn -> not Process.alive?(read_adapter) end)
    assert_eventually(fn -> native_socket_count(read_stack) == 0 end)
    assert {:error, :closed} = :gen_tcp.recv(read_socket, 1, 0)

    {write_stack, write_peer} = stack_and_peer()

    {:ok, write_socket = {:"$inet", Tcp, write_adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(write_stack), 1_000)

    assert :ok = IPv6TcpPeer.hold_acks(write_peer, true)
    payload = :binary.copy("reset", 3_000)
    sender = Task.async(fn -> :gen_tcp.send(write_socket, payload) end)
    assert_eventually(fn -> Tcp.info(write_socket).write_pending end)
    assert :ok = IPv6TcpPeer.reset(write_peer)
    assert {:error, {:econnreset, remainder}} = Task.await(sender, 1_000)
    assert byte_size(remainder) in 1..(byte_size(payload) - 1)
    assert_eventually(fn -> not Process.alive?(write_adapter) end)
    assert_eventually(fn -> native_socket_count(write_stack) == 0 end)
  end

  test "stack failure removes only that stack's temporary adapters" do
    {first_stack, first_peer} = stack_and_peer()
    {second_stack, second_peer} = stack_and_peer()

    {:ok, first = {:"$inet", Tcp, first_adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(first_stack), 1_000)

    {:ok, second = {:"$inet", Tcp, second_adapter}} =
      :gen_tcp.connect(@peer, 443, client_options(second_stack), 1_000)

    receiver = Task.async(fn -> :gen_tcp.recv(first, 1, :infinity) end)
    assert_eventually(fn -> Tcp.info(first).read_pending end)

    first_monitor = Process.monitor(first_adapter)
    Process.exit(Ref.pids(first_stack).stack, :kill)
    assert_receive {:DOWN, ^first_monitor, :process, ^first_adapter, _reason}, 1_000
    assert {:error, :enetdown} = Task.await(receiver)
    refute Process.alive?(first_adapter)
    assert Process.alive?(second_adapter)
    assert {:error, :closed} = :gen_tcp.recv(first, 1, 0)

    assert :ok = :gen_tcp.send(second, "alive")
    assert_eventually(fn -> IPv6TcpPeer.stats(second_peer).received == "alive" end)
    assert IPv6TcpPeer.stats(first_peer).received == ""
  end

  test "shutdown and unsupported family paths return stable errors" do
    {stack, _peer} = stack_and_peer()
    {:ok, socket} = :gen_tcp.connect(@peer, 443, client_options(stack), 1_000)

    assert :ok = :gen_tcp.shutdown(socket, :write)
    assert {:error, :closed} = :gen_tcp.send(socket, "later")

    assert {:error, :eafnosupport} =
             :gen_tcp.connect({127, 0, 0, 1}, 443, client_options(stack), 10)

    assert {:ok, listener} = :gen_tcp.listen(0, client_options(stack))
    assert :ok = :gen_tcp.close(listener)
  end

  test "invalid options and socket calls fail explicitly" do
    {stack, _peer} = stack_and_peer()

    assert catch_exit(
             :gen_tcp.connect(@peer, 443, [
               {:tcp_module, Tcp},
               {:smolnet_stack, stack},
               {:packet, :http}
             ])
           ) == :badarg

    closed = {:"$inet", Tcp, spawn(fn -> :ok end)}
    assert_eventually(fn -> match?({:error, :closed}, :gen_tcp.recv(closed, 0, 0)) end)
  end

  defp stack_and_peer(mode \\ :accept) do
    {:ok, peer} = IPv6TcpPeer.start_link(self(), mode)

    {:ok, stack} =
      SmolNet.start_stack(egress: {peer, :tcp_client}, addresses: [{@client, 64}])

    :ok = IPv6TcpPeer.attach(peer, stack)
    {stack, peer}
  end

  defp client_options(stack, extra \\ []) do
    [
      {:tcp_module, Tcp},
      {:smolnet_stack, stack},
      :inet6,
      :binary,
      {:active, false}
      | extra
    ]
  end

  defp forward_to_test(test) do
    receive do
      :stop ->
        :ok

      message ->
        send(test, {:new_owner, message})
        forward_to_test(test)
    end
  end

  defp socket_message_queued?(socket) do
    {:messages, messages} = Process.info(self(), :messages)

    Enum.any?(messages, fn
      {:tcp, ^socket, _data} -> true
      _message -> false
    end)
  end

  defp native_socket_count(stack) do
    {:ok, info} = SmolNet.stack_info(stack)
    info.native.result.socket_count
  end

  defp adapter_pids(stack) do
    stack
    |> Ref.pids()
    |> Map.fetch!(:inet_backends)
    |> DynamicSupervisor.which_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, [Tcp]} when is_pid(pid) -> [pid]
      _child -> []
    end)
  end

  defp receive_packets(_socket, 0, packets), do: Enum.reverse(packets)

  defp receive_packets(socket, remaining, packets) do
    receive do
      {:tcp, ^socket, packet} -> receive_packets(socket, remaining - 1, [packet | packets])
    after
      1_000 -> flunk("did not receive all active packets")
    end
  end

  defp assert_eventually(check, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(check, deadline)
  end

  defp do_assert_eventually(check, deadline) do
    if check.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true")
      else
        Process.sleep(5)
        do_assert_eventually(check, deadline)
      end
    end
  end

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _result = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end
end
