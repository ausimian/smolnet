defmodule SmolNet.LoopbackTest do
  use ExUnit.Case, async: false

  alias SmolNet.Loopback

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

    accept = Task.async(fn -> SmolNet.accept(listener, 1_000) end)

    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    assert :ok = SmolNet.connect(client, server, 1_000)
    assert {:ok, accepted} = Task.await(accept, 2_000)

    assert {:ok, %{addr: @localhost4, port: 41_101}} = SmolNet.peername(client)
    assert {:ok, %{addr: @localhost4}} = SmolNet.peername(accepted)

    assert :ok = SmolNet.send(client, "request")
    assert {:ok, "request"} = SmolNet.recv(accepted, 7, 1_000)
    assert :ok = SmolNet.send(accepted, "response")
    assert {:ok, "response"} = SmolNet.recv(client, 8, 1_000)
  end

  test "completes an IPv6 connection between two sockets on one stack" do
    stack = loopback_stack()
    server = endpoint6(41_102)

    {:ok, listener} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    :ok = SmolNet.bind(listener, server)
    :ok = SmolNet.listen(listener, 1)

    accept = Task.async(fn -> SmolNet.accept(listener, 1_000) end)

    {:ok, client} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    assert :ok = SmolNet.connect(client, server, 1_000)
    assert {:ok, accepted} = Task.await(accept, 2_000)

    assert {:ok, %{addr: @localhost6, port: 41_102}} = SmolNet.peername(client)

    assert :ok = SmolNet.send(client, "over the loop")
    assert {:ok, "over the loop"} = SmolNet.recv(accepted, 13, 1_000)
  end

  test "carries UDP datagrams back to the stack that sent them" do
    stack = loopback_stack()
    server = endpoint4(41_103)

    {:ok, receiver} = SmolNet.open(:inet, :dgram, :udp, stack: stack)
    :ok = SmolNet.bind(receiver, server)

    {:ok, sender} = SmolNet.open(:inet, :dgram, :udp, stack: stack)
    :ok = SmolNet.bind(sender, endpoint4(0))
    assert :ok = SmolNet.sendto(sender, "datagram", server)

    assert {:ok, datagram} = SmolNet.recvfrom(receiver, 0, 1_000)
    assert datagram.data == "datagram"
    assert datagram.source.addr == @localhost4
  end

  test "loops a connection refused by the stack's own closed port" do
    stack = loopback_stack()

    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: stack)

    assert {:error, :connection_refused} =
             SmolNet.connect(client, endpoint4(41_104), 1_000)
  end

  test "reaches any address the stack holds, not only conventional localhost" do
    {:ok, link} = Loopback.start_link(addresses: [{{192, 0, 2, 1}, 24}])
    stack = Loopback.stack(link)
    server = %{family: :inet, addr: {192, 0, 2, 1}, port: 41_105}

    {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    :ok = SmolNet.bind(listener, server)
    :ok = SmolNet.listen(listener, 1)

    accept = Task.async(fn -> SmolNet.accept(listener, 1_000) end)

    {:ok, client} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    assert :ok = SmolNet.connect(client, server, 1_000)
    assert {:ok, _accepted} = Task.await(accept, 2_000)
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
    {:ok, link} = Loopback.start_link(addresses: @addresses)
    stack = Loopback.stack(link)

    assert {:ok, _info} = SmolNet.stack_info(stack)

    Process.flag(:trap_exit, true)
    Process.exit(link, :shutdown)

    assert_eventually(fn -> SmolNet.stack_info(stack) == {:error, :closed} end)
  end

  test "stopping the stack stops the link that loops it" do
    {:ok, link} = Loopback.start_link(addresses: @addresses)
    stack = Loopback.stack(link)
    monitor = Process.monitor(link)

    assert :ok = SmolNet.stop_stack(stack)

    assert_receive {:DOWN, ^monitor, :process, ^link, :normal}, 1_000
  end

  test "accepts a registered name for the link process" do
    {:ok, link} = Loopback.start_link(name: :loopback_test_link, addresses: @addresses)

    assert Process.whereis(:loopback_test_link) == link
    assert {:ok, _info} = :loopback_test_link |> Loopback.stack() |> SmolNet.stack_info()
  end

  defp loopback_stack do
    {:ok, link} = Loopback.start_link(addresses: @addresses)
    Loopback.stack(link)
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

  defp assert_eventually(assertion, attempts \\ 100)
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
