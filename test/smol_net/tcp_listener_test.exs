defmodule SmolNet.TcpListenerTest do
  use ExUnit.Case, async: false

  alias SmolNet.Socket
  alias SmolNet.Stack
  alias SmolNet.Test.IPv6Link
  alias SmolNet.Test.ManualClock

  @server {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @client {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    previous_clock = Application.get_env(:smolnet, :clock_module)
    previous_manual_clock = Application.get_env(:smolnet, :manual_clock)

    on_exit(fn ->
      stop_all_stacks()
      restore_env(:clock_module, previous_clock)
      restore_env(:manual_clock, previous_manual_clock)
    end)
  end

  test "a bounded listener accepts sequential clients with independent identities" do
    {server_stack, client_stack} = linked_stacks()
    listener = listener(server_stack, 4)

    assert {:ok, %{native: %{result: native}}} = SmolNet.stack_info(server_stack)
    assert native.tcp_listener_count == 1
    assert native.listener_pool_socket_count == 4
    assert native.listener_pool_target_count == 4
    assert native.listener_backlog_capacity == 4

    assert {:select, select_info} = SmolNet.accept(listener, :nowait)
    assert {:select_info, :accept, _reference} = select_info
    assert :ok = SmolNet.cancel(listener, select_info)

    first_accept = Task.async(fn -> SmolNet.accept(listener, 1_000) end)
    first_client = connect(client_stack, 40_001)
    assert {:ok, first_server} = Task.await(first_accept)
    refute Socket.identity(first_server) == Socket.identity(listener)

    assert {:ok, %{addr: @server, port: 40_001}} = SmolNet.sockname(first_server)
    assert {:ok, %{addr: @client}} = SmolNet.peername(first_server)
    assert :ok = SmolNet.send(first_client, "first", 1_000)
    assert {:ok, "first"} = SmolNet.recv(first_server, 5, 1_000)
    assert :ok = SmolNet.send(first_server, "reply", 1_000)
    assert {:ok, "reply"} = SmolNet.recv(first_client, 5, 1_000)

    second_client = connect(client_stack, 40_001)
    assert {:ok, second_server} = SmolNet.accept(listener, 1_000)
    refute Socket.identity(second_server) == Socket.identity(first_server)

    assert :ok = SmolNet.close(first_client)
    assert :ok = SmolNet.close(first_server)
    assert :ok = SmolNet.close(second_client)
    assert :ok = SmolNet.close(second_server)
    assert :ok = SmolNet.close(listener)
  end

  test "accept timeout cancellation does not consume the next child" do
    {server_stack, client_stack} = linked_stacks()
    listener = listener(server_stack, 2, 40_002)

    assert {:error, :timeout} = SmolNet.accept(listener, 10)
    client = connect(client_stack, 40_002)
    assert {:ok, child} = SmolNet.accept(listener, 1_000)
    assert :ok = SmolNet.send(client, "ok", 1_000)
    assert {:ok, "ok"} = SmolNet.recv(child, 2, 1_000)
  end

  test "the accepted queue holds concurrent clients up to the application backlog" do
    {server_stack, client_stack} = linked_stacks()
    listener = listener(server_stack, 3, 40_006)
    clients = Enum.map(1..3, fn _index -> connect(client_stack, 40_006) end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      info.native.result.accepted_queue_count == 3
    end)

    children = Enum.map(1..3, fn _index -> elem(SmolNet.accept(listener, 1_000), 1) end)
    identities = Enum.map(children, &Socket.identity/1)
    assert length(Enum.uniq(identities)) == 3

    Enum.zip(clients, children)
    |> Enum.with_index(1)
    |> Enum.each(fn {{client, child}, index} ->
      message = "client-#{index}"
      assert :ok = SmolNet.send(client, message, 1_000)
      assert {:ok, ^message} = SmolNet.recv(child, byte_size(message), 1_000)
    end)
  end

  test "listener scanning and replenishment obey a one-unit maintenance budget" do
    {server_stack, client_stack} = linked_stacks(limits: %{maintenance_work: 1})
    listener = listener(server_stack, 4, 40_007)
    client = connect(client_stack, 40_007)
    assert {:ok, child} = SmolNet.accept(listener, 1_000)
    assert :ok = SmolNet.send(client, "fair", 1_000)
    assert {:ok, "fair"} = SmolNet.recv(child, 4, 1_000)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      native = info.native.result

      native.listener_pool_socket_count == native.listener_pool_target_count and
        native.counters.max_maintenance_work <= 1
    end)
  end

  test "closing a listener releases queued children and rejects their late traffic" do
    {server_stack, client_stack} = linked_stacks()
    listener = listener(server_stack, 2, 40_008)
    client = connect(client_stack, 40_008)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      info.native.result.accepted_queue_count == 1
    end)

    assert :ok = SmolNet.close(listener)
    assert :ok = SmolNet.send(client, "late", 1_000)
    assert {:error, :connection_reset} = SmolNet.recv(client, 0, 1_000)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      native = info.native.result
      native.socket_count == 0 and native.native_socket_count == 0
    end)
  end

  test "closing a listener removes half-open pool members before late handshake packets" do
    {:ok, client_link} = IPv6Link.start_link(self())
    {:ok, server_link} = IPv6Link.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {server_link, :server}, addresses: [{@server, 64}])

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {client_link, :client}, addresses: [{@client, 64}])

    :ok = IPv6Link.connect(client_link, :client, server_stack)
    :ok = IPv6Link.connect(server_link, :server, client_stack)
    :ok = IPv6Link.fault(server_link, :hold)
    listener = listener(server_stack, 2, 40_010)

    connect =
      Task.async(fn ->
        {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: client_stack)
        {SmolNet.connect(socket, endpoint(@server, 40_010), 100), socket}
      end)

    assert_receive {:test_link_egress, :server, _held_syn_ack}, 1_000
    assert :ok = SmolNet.close(listener)
    :ok = IPv6Link.release(server_link)
    {_result, _socket} = Task.await(connect, 1_000)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      native = info.native.result
      native.socket_count == 0 and native.native_socket_count == 0
    end)
  end

  test "a half-open handshake expires and replenishes its listener slot" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)
    {:ok, client_link} = IPv6Link.start_link(self())
    {:ok, server_link} = IPv6Link.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {server_link, :server}, addresses: [{@server, 64}])

    {:ok, first_client_stack} =
      SmolNet.start_stack(egress: {client_link, :client}, addresses: [{@client, 64}])

    :ok = IPv6Link.connect(client_link, :client, server_stack)
    :ok = IPv6Link.connect(server_link, :server, first_client_stack)
    :ok = IPv6Link.fault(server_link, :hold)
    listener = listener(server_stack, 1, 40_018)

    first_connect =
      Task.async(fn ->
        {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: first_client_stack)
        SmolNet.connect(socket, endpoint(@server, 40_018), 10)
      end)

    assert_receive {:test_link_egress, :server, _held_syn_ack}, 1_000
    assert {:error, :timeout} = Task.await(first_connect)
    assert :ok = SmolNet.stop_stack(first_client_stack)

    :ok = ManualClock.advance(clock, 30_001)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      native = info.native.result

      native.listener_pool_socket_count == 1 and native.counters.listener_refills >= 1
    end)

    :ok = IPv6Link.release(server_link)
    :ok = IPv6Link.fault(server_link, :pass)

    {:ok, second_client_stack} =
      SmolNet.start_stack(egress: {client_link, :client}, addresses: [{@client, 64}])

    :ok = IPv6Link.connect(server_link, :server, second_client_stack)
    client = connect(second_client_stack, 40_018)
    assert {:ok, child} = SmolNet.accept(listener, 1_000)
    assert :ok = SmolNet.send(client, "refilled", 1_000)
    assert {:ok, "refilled"} = SmolNet.recv(child, 8, 1_000)
  end

  test "closing a listener aborts a pending accept and releases its pool" do
    {server_stack, _client_stack} = linked_stacks()
    listener = listener(server_stack, 3, 40_003)
    accept = Task.async(fn -> SmolNet.accept(listener, :infinity) end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      info.native.result.read_waiter_count == 1
    end)

    assert :ok = SmolNet.close(listener)
    assert {:error, :closed} = Task.await(accept)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      native = info.native.result
      native.tcp_listener_count == 0 and native.listener_pool_socket_count == 0
    end)
  end

  test "an accepted child remains owned during adapter handoff" do
    {server_stack, client_stack} = linked_stacks()
    listener = listener(server_stack, 2, 40_020)
    client = connect(client_stack, 40_020)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      info.native.result.accepted_queue_count == 1
    end)

    parent = self()

    temporary_owner =
      spawn(fn ->
        send(parent, {:handoff_child, Stack.socket_accept(listener, self())})
        Process.sleep(:infinity)
      end)

    assert_receive {:handoff_child, {:ok, %Socket{}}}, 1_000
    monitor = Process.monitor(temporary_owner)
    Process.exit(temporary_owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^temporary_owner, :killed}, 1_000

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      info.native.result.socket_count == 1
    end)

    assert {:error, :closed} = SmolNet.recv(client, 0, 1_000)
  end

  test "explicit close releases a temporary handoff owner monitor" do
    {server_stack, client_stack} = linked_stacks()
    listener = listener(server_stack, 2, 40_021)
    _client = connect(client_stack, 40_021)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      info.native.result.accepted_queue_count == 1
    end)

    assert {:ok, child} = Stack.socket_accept(listener, self())
    stack_state = :sys.get_state(server_stack.stack)
    assert map_size(stack_state.socket_owner_monitors) == 1
    assert map_size(stack_state.socket_owner_monitors_by_identity) == 1

    assert :ok = SmolNet.close(child)
    stack_state = :sys.get_state(server_stack.stack)
    assert stack_state.socket_owner_monitors == %{}
    assert stack_state.socket_owner_monitors_by_identity == %{}
  end

  test "backlog saturation is bounded and drops the newest established child" do
    {server_stack, client_stack} = linked_stacks()
    listener = listener(server_stack, 1, 40_004)
    first = connect(client_stack, 40_004)

    second = connect(client_stack, 40_004)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      native = info.native.result

      native.accepted_queue_count == 1 and
        native.counters.listener_overflow_drops >= 1 and
        native.listener_pool_socket_count == 1
    end)

    assert :ok = SmolNet.send(second, "dropped", 1_000)
    assert {:error, :connection_reset} = SmolNet.recv(second, 0, 1_000)

    assert {:ok, child} = SmolNet.accept(listener, 1_000)
    assert :ok = SmolNet.send(first, "kept", 1_000)
    assert {:ok, "kept"} = SmolNet.recv(child, 4, 1_000)
  end

  test "public socket-table saturation drops a promoted child and refills the pool" do
    {server_stack, client_stack} = linked_stacks(limits: %{ready_events: 1})
    _listener = listener(server_stack, 1, 40_019)
    client = connect(client_stack, 40_019)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      native = info.native.result

      native.socket_count == 1 and native.accepted_queue_count == 0 and
        native.listener_pool_socket_count == 1 and
        native.counters.listener_overflow_drops >= 1
    end)

    assert :ok = SmolNet.send(client, "overflow", 1_000)
    assert {:error, :connection_reset} = SmolNet.recv(client, 0, 1_000)
  end

  test "listener validation and operation classes are explicit" do
    {server_stack, _client_stack} = linked_stacks()
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: server_stack)

    assert {:error, :not_bound} = SmolNet.listen(socket, 1)
    assert {:error, :invalid_backlog} = SmolNet.listen(socket, 0)
    assert :ok = SmolNet.bind(socket, endpoint(@server, 40_005))
    assert :ok = SmolNet.listen(socket, 2)
    assert {:error, :invalid_socket_state} = SmolNet.send(socket, "no", :nowait)
    assert {:error, :not_connected} = SmolNet.peername(socket)
    assert {:select, select_info} = SmolNet.accept(socket, :nowait)
    assert {:error, :invalid_socket_state} = SmolNet.recv(socket, 0, :nowait)
    assert :ok = SmolNet.cancel(socket, select_info)
    assert {:error, :invalid_socket_state} = SmolNet.listen(socket, 2)
  end

  test "a dropped SYN retransmits on the native timer and still reaches accept" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)
    {server_stack, client_stack, link} = linked_stacks_with_link()
    listener = listener(server_stack, 2, 40_009)
    :ok = IPv6Link.fault(link, :drop)

    connect = Task.async(fn -> connect(client_stack, 40_009) end)
    assert_receive {:test_link_egress, :client, _dropped_syn}, 1_000

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(client_stack)
      info.poll_at == 1_000
    end)

    :ok = IPv6Link.fault(link, :pass)
    :ok = ManualClock.advance(clock, 1_000)

    client = Task.await(connect, 1_000)
    assert {:ok, child} = SmolNet.accept(listener, 1_000)
    assert :ok = SmolNet.send(client, "retry", 1_000)
    assert {:ok, "retry"} = SmolNet.recv(child, 5, 1_000)
  end

  defp linked_stacks(server_options \\ []) do
    {server_stack, client_stack, _link} = linked_stacks_with_link(server_options)
    {server_stack, client_stack}
  end

  defp linked_stacks_with_link(server_options \\ []) do
    {:ok, link} = IPv6Link.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(
        Keyword.merge([egress: {link, :server}, addresses: [{@server, 64}]], server_options)
      )

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client, 64}])

    :ok = IPv6Link.connect(link, :server, client_stack)
    :ok = IPv6Link.connect(link, :client, server_stack)
    {server_stack, client_stack, link}
  end

  defp listener(stack, backlog, port \\ 40_001) do
    {:ok, listener} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    :ok = SmolNet.bind(listener, endpoint(@server, port))
    :ok = SmolNet.listen(listener, backlog)
    listener
  end

  defp connect(stack, port) do
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    :ok = SmolNet.connect(socket, endpoint(@server, port), 1_000)
    socket
  end

  defp endpoint(address, port), do: %{family: :inet6, addr: address, port: port}

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
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:smolnet, key)
  defp restore_env(key, value), do: Application.put_env(:smolnet, key, value)
end
