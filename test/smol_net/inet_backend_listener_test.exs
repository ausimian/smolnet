defmodule SmolNet.InetBackendListenerTest do
  use ExUnit.Case, async: false

  alias SmolNet.InetBackend.Tcp
  alias SmolNet.Stack.Ref
  alias SmolNet.Test.IPv6Link

  @server {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @client {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    on_exit(&stop_all_stacks/0)
  end

  test "public gen_tcp listen and accept inherit supported IPv6 options" do
    {server_stack, client_stack} = linked_stacks()

    assert {:ok, listener} =
             :gen_tcp.listen(
               0,
               server_options(server_stack,
                 backlog: 4,
                 packet: 2,
                 packet_size: 64,
                 buffer: 128,
                 recbuf: 16_384,
                 sndbuf: 32_768,
                 send_timeout: 321,
                 send_timeout_close: true
               )
             )

    assert {:ok, {{0, 0, 0, 0, 0, 0, 0, 0}, port}} = :inet.sockname(listener)
    assert port in 49_152..50_175
    assert {:ok, listener_stats} = :inet.getstat(listener)

    assert Keyword.keys(listener_stats) ==
             ~w(recv_oct recv_cnt recv_max recv_avg recv_dvi send_oct send_cnt send_max send_avg send_pend)a

    accept = accept_for_parent(listener)

    assert {:ok, client} =
             :gen_tcp.connect(
               @server,
               port,
               client_options(client_stack, packet: 2, packet_size: 64, buffer: 128),
               1_000
             )

    assert {:ok, server} = Task.await(accept)
    refute server == listener

    assert {:ok,
            [
              active: false,
              mode: :binary,
              packet: 2,
              packet_size: 64,
              buffer: 16_384,
              recbuf: 16_384,
              sndbuf: 32_768,
              send_timeout: 321,
              send_timeout_close: true
            ]} =
             :inet.getopts(server, [
               :active,
               :mode,
               :packet,
               :packet_size,
               :buffer,
               :recbuf,
               :sndbuf,
               :send_timeout,
               :send_timeout_close
             ])

    assert {:error, :einval} = :inet.setopts(listener, recbuf: 65_536)
    assert {:error, :einval} = :inet.setopts(server, sndbuf: 65_536)

    assert {:ok, info} = SmolNet.stack_info(server_stack)
    assert %{sockets: server_buffers} = info.native.result.tcp_buffer_bytes
    assert length(server_buffers) == 2
    assert Enum.all?(server_buffers, &match?(%{rcvbuf: 16_384, sndbuf: 32_768}, &1))

    assert {:ok, {@server, ^port}} = :inet.sockname(server)
    assert {:ok, {@client, _client_port}} = :inet.peername(server)
    assert :ok = :gen_tcp.send(client, "hello")
    assert {:ok, "hello"} = :gen_tcp.recv(server, 0, 1_000)
    assert :ok = :gen_tcp.send(server, "world")
    assert {:ok, "world"} = :gen_tcp.recv(client, 0, 1_000)

    assert :ok = :gen_tcp.close(server)
    assert :ok = :gen_tcp.close(client)
    assert :ok = :gen_tcp.close(listener)
  end

  test "a passive raw read of an explicit length beyond the receive bound completes" do
    {server_stack, client_stack} = linked_stacks()
    {:ok, listener} = :gen_tcp.listen(40_021, server_options(server_stack, backlog: 1))
    accept = accept_for_parent(listener)
    {:ok, client} = :gen_tcp.connect(@server, 40_021, client_options(client_stack), 1_000)
    {:ok, server} = Task.await(accept)

    # 200 KB, well past the 64 KiB default receive buffer, read back in one call.
    payload = :crypto.strong_rand_bytes(200_000)
    sender = Task.async(fn -> :gen_tcp.send(server, payload) end)
    assert {:ok, ^payload} = :gen_tcp.recv(client, byte_size(payload), 10_000)
    assert :ok = Task.await(sender, 10_000)

    # A chunk read and framed reads keep the bound.
    assert {:error, :timeout} = :gen_tcp.recv(client, 0, 10)
    assert :ok = :inet.setopts(client, packet: 4)
    assert {:error, :emsgsize} = :gen_tcp.recv(client, 2_000_000, 10)

    :ok = :gen_tcp.close(client)
    :ok = :gen_tcp.close(server)
    :ok = :gen_tcp.close(listener)
  end

  test "a reusable gen_tcp listener accepts concurrent clients sequentially" do
    {server_stack, client_stack} = linked_stacks()
    {:ok, listener} = :gen_tcp.listen(40_011, server_options(server_stack, backlog: 3))

    first_accept = accept_for_parent(listener)
    {:ok, first_client} = :gen_tcp.connect(@server, 40_011, client_options(client_stack), 1_000)
    {:ok, first_server} = Task.await(first_accept)

    second_accept = accept_for_parent(listener)

    {:ok, second_client} =
      :gen_tcp.connect(@server, 40_011, client_options(client_stack), 1_000)

    {:ok, second_server} = Task.await(second_accept)
    refute first_server == second_server

    assert :ok = :gen_tcp.send(first_client, "one")
    assert :ok = :gen_tcp.send(second_client, "two")
    assert {:ok, "one"} = :gen_tcp.recv(first_server, 3, 1_000)
    assert {:ok, "two"} = :gen_tcp.recv(second_server, 3, 1_000)
  end

  test "accept timeout and listener close races produce one stable outcome" do
    {server_stack, client_stack} = linked_stacks()
    {:ok, listener} = :gen_tcp.listen(40_012, server_options(server_stack, backlog: 2))

    assert {:error, :timeout} = :gen_tcp.accept(listener, 10)
    accept = accept_for_parent(listener)
    {:ok, client} = :gen_tcp.connect(@server, 40_012, client_options(client_stack), 1_000)
    assert {:ok, child} = Task.await(accept)

    pending = Task.async(fn -> :gen_tcp.accept(listener, :infinity) end)
    Process.sleep(10)
    assert :ok = :gen_tcp.close(listener)
    assert {:error, :closed} = Task.await(pending)

    assert :ok = :gen_tcp.send(client, "still open")
    assert {:ok, "still open"} = :gen_tcp.recv(child, 10, 1_000)
  end

  test "accepted sockets inherit active mode without sharing listener state" do
    {server_stack, client_stack} = linked_stacks()

    {:ok, listener} =
      :gen_tcp.listen(40_013, server_options(server_stack, active: :once, packet: :line))

    accept = accept_for_parent(listener)

    {:ok, client} =
      :gen_tcp.connect(
        @server,
        40_013,
        client_options(client_stack, packet: :line),
        1_000
      )

    {:ok, child} = Task.await(accept)
    assert :ok = :gen_tcp.send(client, "first\nsecond\n")
    assert_receive {:tcp, ^child, "first\n"}, 1_000
    refute_receive {:tcp, ^child, _packet}, 50
    assert {:ok, [active: false]} = :inet.getopts(child, [:active])
    assert {:ok, "second\n"} = :gen_tcp.recv(child, 0, 1_000)
    assert {:ok, [active: :once]} = :inet.getopts(listener, [:active])
  end

  test "hard-killing a listener releases only its pool and leaves accepted children usable" do
    {server_stack, client_stack} = linked_stacks()
    {:ok, listener} = :gen_tcp.listen(40_014, server_options(server_stack, backlog: 2))
    accept = accept_for_parent(listener)
    {:ok, client} = :gen_tcp.connect(@server, 40_014, client_options(client_stack), 1_000)
    {:ok, child} = Task.await(accept)
    {:"$inet", Tcp, listener_pid} = listener
    monitor = Process.monitor(listener_pid)

    Process.exit(listener_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^listener_pid, :killed}, 1_000

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(server_stack)
      info.native.result.tcp_listener_count == 0
    end)

    assert :ok = :gen_tcp.send(client, "survives")
    assert {:ok, "survives"} = :gen_tcp.recv(child, 8, 1_000)
    assert {:ok, replacement} = :gen_tcp.listen(40_014, server_options(server_stack, backlog: 1))

    replacement_accept = accept_for_parent(replacement)

    assert {:ok, second_client} =
             :gen_tcp.connect(@server, 40_014, client_options(client_stack), 1_000)

    assert {:ok, second_child} = Task.await(replacement_accept)
    assert :ok = :gen_tcp.send(second_client, "replacement")
    assert {:ok, "replacement"} = :gen_tcp.recv(second_child, 11, 1_000)
    assert :ok = :gen_tcp.send(client, "old child")
    assert {:ok, "old child"} = :gen_tcp.recv(child, 9, 1_000)
    assert :ok = :gen_tcp.close(replacement)
  end

  test "stack failure resolves a pending accept as enetdown" do
    {server_stack, _client_stack} = linked_stacks()
    {:ok, listener} = :gen_tcp.listen(40_015, server_options(server_stack, backlog: 2))
    {:"$inet", Tcp, listener_pid} = listener
    listener_monitor = Process.monitor(listener_pid)
    pending = Task.async(fn -> :gen_tcp.accept(listener, :infinity) end)

    assert_eventually(fn -> Tcp.info(listener).accept_pending end)
    Process.exit(Ref.pids(server_stack).stack, :kill)

    assert {:error, :enetdown} = Task.await(pending)
    assert_receive {:DOWN, ^listener_monitor, :process, ^listener_pid, _reason}, 1_000
  end

  test "listener bind, family, and backlog errors remain explicit" do
    {server_stack, _client_stack} = linked_stacks()
    options = server_options(server_stack, backlog: 2)
    {:ok, listener} = :gen_tcp.listen(40_016, options)

    assert {:error, :eaddrinuse} = :gen_tcp.listen(40_016, options)
    assert {:error, :eafnosupport} = Tcp.listen(40_017, [:inet | options])

    assert {:error, :einval} =
             Tcp.listen(40_017, [
               {:backlog, 129} | Enum.reject(options, &match?({:backlog, _value}, &1))
             ])

    assert :ok = :gen_tcp.close(listener)
  end

  defp accept_for_parent(listener) do
    parent = self()

    Task.async(fn ->
      with {:ok, socket} <- :gen_tcp.accept(listener, 1_000),
           :ok <- :gen_tcp.controlling_process(socket, parent) do
        {:ok, socket}
      end
    end)
  end

  defp linked_stacks do
    {:ok, link} = IPv6Link.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 64}])

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client, 64}])

    :ok = IPv6Link.connect(link, :server, client_stack)
    :ok = IPv6Link.connect(link, :client, server_stack)
    {server_stack, client_stack}
  end

  defp server_options(stack, extra) do
    options = [
      {:tcp_module, Tcp},
      {:smolnet_stack, stack},
      :inet6,
      :binary,
      {:active, Keyword.get(extra, :active, false)},
      {:backlog, Keyword.get(extra, :backlog, 5)},
      {:packet, Keyword.get(extra, :packet, :raw)},
      {:packet_size, Keyword.get(extra, :packet_size, 65_536)},
      {:buffer, Keyword.get(extra, :buffer, 65_536)},
      {:send_timeout, Keyword.get(extra, :send_timeout, :infinity)},
      {:send_timeout_close, Keyword.get(extra, :send_timeout_close, false)}
    ]

    options ++ Keyword.take(extra, [:recbuf, :sndbuf])
  end

  defp client_options(stack, extra \\ []) do
    options = [
      {:tcp_module, Tcp},
      {:smolnet_stack, stack},
      :inet6,
      :binary,
      {:active, false},
      {:packet, Keyword.get(extra, :packet, :raw)},
      {:packet_size, Keyword.get(extra, :packet_size, 65_536)},
      {:buffer, Keyword.get(extra, :buffer, 65_536)}
    ]

    options ++ Keyword.take(extra, [:recbuf, :sndbuf])
  end

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <- DynamicSupervisor.which_children(SmolNet.Supervisor) do
        _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
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
end
