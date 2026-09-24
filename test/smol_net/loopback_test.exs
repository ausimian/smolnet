defmodule SmolNet.LoopbackTest do
  use ExUnit.Case, async: false

  alias SmolNet.Loopback
  alias SmolNet.Stack.Ref
  alias SmolNet.Test.Timing

  # Liveness budgets: bounds on how long a healthy run may take to make
  # progress, not properties under test. See `SmolNet.Test.Timing`.
  @wait_1s Timing.liveness(1_000)
  @wait_2s Timing.liveness(2_000)

  # `assert_eventually/2` polls every 10 ms, so its attempt count is the same
  # one second liveness bound expressed in polls.
  @poll_attempts div(@wait_1s, 10)

  @localhost4 {127, 0, 0, 1}
  @localhost6 {0, 0, 0, 0, 0, 0, 0, 1}
  @addresses [{@localhost6, 128}, {@localhost4, 8}]

  setup do
    on_exit(&stop_all_stacks/0)
  end

  test "completes an IPv4 connection between two sockets on one stack" do
    stack = loopback_stack()
    server = endpoint4(41_101)

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    :ok = SmolNet.bind(listener, server)
    :ok = SmolNet.listen(listener, 1)

    accept = Task.async(fn -> SmolNet.accept(listener, @wait_1s) end)

    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    assert :ok = SmolNet.connect(client, server, @wait_1s)
    assert {:ok, accepted} = Task.await(accept, @wait_2s)

    assert {:ok, %{addr: @localhost4, port: 41_101}} = SmolNet.peername(client)
    assert {:ok, %{addr: @localhost4}} = SmolNet.peername(accepted)

    assert :ok = SmolNet.send(client, "request")
    assert {:ok, "request"} = SmolNet.recv(accepted, 7, @wait_1s)
    assert :ok = SmolNet.send(accepted, "response")
    assert {:ok, "response"} = SmolNet.recv(client, 8, @wait_1s)
  end

  test "completes an IPv6 connection between two sockets on one stack" do
    stack = loopback_stack()
    server = endpoint6(41_102)

    {:ok, listener} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    :ok = SmolNet.bind(listener, server)
    :ok = SmolNet.listen(listener, 1)

    accept = Task.async(fn -> SmolNet.accept(listener, @wait_1s) end)

    {:ok, client} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    assert :ok = SmolNet.connect(client, server, @wait_1s)
    assert {:ok, accepted} = Task.await(accept, @wait_2s)

    assert {:ok, %{addr: @localhost6, port: 41_102}} = SmolNet.peername(client)

    assert :ok = SmolNet.send(client, "over the loop")
    assert {:ok, "over the loop"} = SmolNet.recv(accepted, 13, @wait_1s)
  end

  test "carries UDP datagrams back to the stack that sent them" do
    stack = loopback_stack()
    server = endpoint4(41_103)

    {:ok, receiver} = SmolNet.open(:inet, :dgram, :udp, stack: stack)
    :ok = SmolNet.bind(receiver, server)

    {:ok, sender} = SmolNet.open(:inet, :dgram, :udp, stack: stack)
    :ok = SmolNet.bind(sender, endpoint4(0))
    assert :ok = SmolNet.sendto(sender, "datagram", server)

    assert {:ok, datagram} = SmolNet.recvfrom(receiver, 0, @wait_1s)
    assert datagram.data == "datagram"
    assert datagram.source.addr == @localhost4
  end

  test "loops a connection refused by the stack's own closed port" do
    stack = loopback_stack()

    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: stack)

    assert {:error, :connection_refused} =
             SmolNet.connect(client, endpoint4(41_104), @wait_1s)
  end

  test "reaches any address the stack holds, not only conventional localhost" do
    {:ok, _link, stack} = Loopback.start_link(addresses: [{{192, 0, 2, 1}, 24}])
    server = %{family: :inet, addr: {192, 0, 2, 1}, port: 41_105}

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    :ok = SmolNet.bind(listener, server)
    :ok = SmolNet.listen(listener, 1)

    accept = Task.async(fn -> SmolNet.accept(listener, @wait_1s) end)

    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    assert :ok = SmolNet.connect(client, server, @wait_1s)
    assert {:ok, _accepted} = Task.await(accept, @wait_2s)
  end

  test "rejects an explicit egress because the link is the stack's egress" do
    Process.flag(:trap_exit, true)

    assert {:error, :invalid_options} =
             Loopback.start_link(addresses: @addresses, egress: {self(), :mine})
  end

  test "reports an invalid stack option rather than starting a link" do
    Process.flag(:trap_exit, true)

    assert {:error, :invalid_mtu} = Loopback.start_link(mtu: 1)
  end

  test "stopping the link stops the stack it loops" do
    {:ok, link, stack} = Loopback.start_link(addresses: @addresses)

    assert {:ok, _info} = SmolNet.stack_info(stack)

    Process.flag(:trap_exit, true)
    Process.exit(link, :shutdown)

    assert_eventually(fn -> SmolNet.stack_info(stack) == {:error, :closed} end)
  end

  test "stopping the stack stops the link that loops it" do
    {:ok, link, stack} = Loopback.start_link(addresses: @addresses)
    monitor = Process.monitor(link)

    assert :ok = SmolNet.stop_stack(stack)

    assert_receive {:DOWN, ^monitor, :process, ^link, :normal}, @wait_1s
  end

  test "a stack crash stops the link that loops it" do
    {:ok, link, stack} = Loopback.start_link(addresses: @addresses)
    monitor = Process.monitor(link)
    %{stack: stack_pid} = Ref.pids(stack)

    Process.exit(stack_pid, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^link, :normal}, @wait_1s
  end

  test "accepts a registered name for the link process" do
    {:ok, link, stack} =
      Loopback.start_link(name: :loopback_test_link, addresses: @addresses)

    assert Process.whereis(:loopback_test_link) == link
    assert Loopback.stack(:loopback_test_link) == stack
    assert {:ok, _info} = SmolNet.stack_info(stack)
  end

  test "returns the stack as child information when supervised" do
    supervisor = start_supervised!({DynamicSupervisor, strategy: :one_for_one})

    assert {:ok, link, stack} =
             DynamicSupervisor.start_child(supervisor, {Loopback, addresses: @addresses})

    assert Loopback.stack(link) == stack
    assert {:ok, _info} = SmolNet.stack_info(stack)
  end

  defp loopback_stack do
    {:ok, _link, stack} = Loopback.start_link(addresses: @addresses)
    stack
  end

  defp endpoint4(port), do: %{family: :inet, addr: @localhost4, port: port}
  defp endpoint6(port), do: %{family: :inet6, addr: @localhost6, port: port}

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <-
            DynamicSupervisor.which_children(SmolNet.Supervisor) do
        DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end

  defp assert_eventually(assertion, attempts \\ @poll_attempts)
  defp assert_eventually(assertion, 0), do: assert(assertion.())

  defp assert_eventually(assertion, attempts) do
    if assertion.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(assertion, attempts - 1)
    end
  end
end
