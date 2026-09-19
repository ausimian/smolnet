defmodule SmolNet.UdpTest do
  use ExUnit.Case, async: false

  alias SmolNet.Inet6.Udp
  alias SmolNet.Test.RawIpLink

  @server {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @client {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    on_exit(&stop_all_stacks/0)
  end

  test "low-level UDP preserves datagrams and endpoint metadata" do
    {server_stack, client_stack, _link} = stacks()
    port = 42_001

    {:ok, server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(server, endpoint(@server, port))
    {:ok, client} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(client, endpoint(@client, 0))

    assert {:select, select} = SmolNet.recvfrom(server, 0, :nowait)
    assert :ok = SmolNet.cancel(server, select)

    assert :ok = SmolNet.sendto(client, "hello", endpoint(@server, port), 1_000)

    assert {:ok,
            %{
              source: %{family: :inet6, addr: @client, port: client_port},
              destination: %{family: :inet6, addr: @server, port: ^port},
              data: "hello",
              truncated: false
            }} = SmolNet.recvfrom(server, 0, 1_000)

    assert client_port in 49_152..50_175
    assert {:ok, %{addr: @client, port: ^client_port}} = SmolNet.sockname(client)
    assert {:error, :not_connected} = SmolNet.peername(client)

    assert :ok = SmolNet.sendto(client, "abcdef", endpoint(@server, port), 1_000)
    assert {:ok, %{data: "abc", truncated: true}} = SmolNet.recvfrom(server, 3, 1_000)

    assert :ok = SmolNet.sendto(client, <<>>, endpoint(@server, port), 1_000)
    assert {:ok, %{data: <<>>, truncated: false}} = SmolNet.recvfrom(server, 0, 1_000)

    assert {:error, :message_too_large} =
             SmolNet.sendto(client, :binary.copy("x", 1_453), endpoint(@server, port), :nowait)

    maximum = :binary.copy("m", 1_452)
    assert :ok = SmolNet.sendto(client, maximum, endpoint(@server, port), 1_000)
    assert {:ok, %{data: ^maximum, truncated: false}} = SmolNet.recvfrom(server, 0, 1_000)

    {:ok, info} = SmolNet.stack_info(server_stack)
    native = info.native.result
    assert native.udp_socket_count == 1
    assert native.udp_packet_capacity == 16
    assert native.udp_payload_bytes == 16_384
    assert native.udp_max_datagram_bytes == 1_452
    assert native.udp_ipv4_max_datagram_bytes == 1_472
    assert native.counters.max_bytes_copied <= native.limits.bytes_copied
  end

  test "UDP waiters share cancel, timeout, retry, and close semantics" do
    {server_stack, client_stack, _link} = stacks()
    port = 42_002

    {:ok, server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(server, endpoint(@server, port))
    {:ok, client} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(client, endpoint(@client, 0))

    assert {:error, :timeout} = SmolNet.recvfrom(server, 0, 10)
    assert {:select, cancelled} = SmolNet.recvfrom(server, 0, :nowait)
    assert :ok = SmolNet.cancel(server, cancelled)
    refute_receive {:"$smol_socket", _, :select, _}, 20

    assert {:select, select} = SmolNet.recvfrom(server, 0, :nowait)
    assert :ok = SmolNet.sendto(client, "once", endpoint(@server, port), 1_000)
    assert_receive {:"$smol_socket", identity, :select, reference}, 1_000
    assert identity == SmolNet.Socket.identity(server)
    assert {:select_info, :recvfrom, reference} == select
    assert {:ok, %{data: "once"}} = SmolNet.recvfrom(server, 0, :nowait)
    refute_receive {:"$smol_socket", ^identity, :select, ^reference}, 20

    assert {:select, closing} = SmolNet.recvfrom(server, 0, :nowait)
    {:select_info, :recvfrom, closing_reference} = closing
    assert :ok = SmolNet.close(server)

    assert_receive {:"$smol_socket", ^identity, :abort, ^closing_reference, :closed}, 1_000
    assert {:error, :invalid_socket} = SmolNet.recvfrom(server, 0, :nowait)
  end

  test "a full transmit ring retries the whole datagram exactly once" do
    {:ok, link} = RawIpLink.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 64}])

    {:ok, client_stack} =
      SmolNet.start_stack(
        egress: {link, :client},
        mtu: 1_280,
        addresses: [{@client, 64}],
        limits: %{bytes_copied: 1_280, output_packets: 1}
      )

    on_exit(fn ->
      resume_if_alive(client_stack.stack)
    end)

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)

    port = 42_007
    {:ok, server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(server, endpoint(@server, port))
    {:ok, client} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(client, endpoint(@client, 0))
    destination = endpoint(@server, port)

    collector =
      Task.async(fn ->
        for _index <- 1..18 do
          {:ok, %{data: <<sequence::16, _padding::binary>>, truncated: false}} =
            SmolNet.recvfrom(server, 0, 5_000)

          sequence
        end
      end)

    :ok = :sys.suspend(client_stack.stack)

    sends =
      for sequence <- 1..18 do
        Task.async(fn ->
          payload = <<sequence::16, 0::size(998 * 8)>>

          case SmolNet.sendto(client, payload, destination, :nowait) do
            :ok ->
              {sequence, :immediate}

            {:select, {:select_info, :sendto, reference}} ->
              receive do
                {:"$smol_socket", identity, :select, ^reference} ->
                  assert identity == SmolNet.Socket.identity(client)
              after
                5_000 -> flunk("full UDP transmit ring never became writable")
              end

              assert :ok = SmolNet.sendto(client, payload, destination, 5_000)
              {sequence, :retried}
          end
        end)
      end

    assert_eventually(fn ->
      {:message_queue_len, queued} = Process.info(client_stack.stack, :message_queue_len)
      queued >= 18
    end)

    :ok = :sys.resume(client_stack.stack)
    send_results = Enum.map(sends, &Task.await(&1, 6_000))
    received = Task.await(collector, 6_000)

    assert Enum.count(send_results, fn {_sequence, mode} -> mode == :retried end) == 1
    assert Enum.sort(received) == Enum.to_list(1..18)
  end

  test "arming an unrelated receive preserves pending UDP egress" do
    {:ok, link} = RawIpLink.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 64}])

    {:ok, client_stack} =
      SmolNet.start_stack(
        egress: {link, :client},
        mtu: 1_280,
        addresses: [{@client, 64}],
        limits: %{bytes_copied: 1_280, output_packets: 1}
      )

    on_exit(fn ->
      resume_if_alive(client_stack.stack)
    end)

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)

    port = 42_008
    {:ok, server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(server, endpoint(@server, port))
    {:ok, sender} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(sender, endpoint(@client, 0))
    {:ok, idle} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(idle, endpoint(@client, 0))

    :ok = :sys.suspend(client_stack.stack)

    send_task =
      Task.async(fn ->
        SmolNet.sendto(sender, :binary.copy("x", 1_000), endpoint(@server, port), 1_000)
      end)

    assert_eventually(fn -> call_queued?(client_stack.stack, send_task.pid) end)

    recv_task = Task.async(fn -> SmolNet.recvfrom(idle, 0, :nowait) end)
    assert_eventually(fn -> call_queued?(client_stack.stack, recv_task.pid) end)

    :ok = :sys.resume(client_stack.stack)

    assert :ok = Task.await(send_task, 2_000)
    assert {:select, _select_info} = Task.await(recv_task, 2_000)
    assert {:ok, %{data: data}} = SmolNet.recvfrom(server, 0, 2_000)
    assert data == :binary.copy("x", 1_000)
  end

  test "connected UDP filters peers and rejects mismatched destinations" do
    {server_stack, client_stack, _link} = stacks()
    port = 42_003

    {:ok, server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(server, endpoint(@server, port))
    {:ok, expected} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(expected, endpoint(@client, 0))
    {:ok, %{port: expected_port}} = SmolNet.sockname(expected)
    {:ok, other} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(other, endpoint(@client, 0))

    assert :ok = SmolNet.connect(server, endpoint(@client, expected_port))
    assert :ok = SmolNet.connect(expected, endpoint(@server, port))
    assert {:ok, %{addr: @client, port: ^expected_port}} = SmolNet.peername(server)

    assert :ok = SmolNet.sendto(other, "discard", endpoint(@server, port), 1_000)
    assert :ok = SmolNet.sendto(expected, "keep", endpoint(@server, port), 1_000)

    assert {:ok, %{data: "keep", source: %{port: ^expected_port}}} =
             SmolNet.recvfrom(server, 0, 1_000)

    assert {:error, :invalid_socket_state} =
             SmolNet.sendto(expected, "wrong", endpoint(@client, 9), :nowait)
  end

  test "bad IPv6 UDP checksums are discarded and no-route errors are explicit" do
    {server_stack, client_stack, link} = stacks()
    port = 42_004

    {:ok, server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(server, endpoint(@server, port))
    {:ok, client} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(client, endpoint(@client, 0))

    assert {:error, :network_unreachable} =
             SmolNet.sendto(
               client,
               "lost",
               endpoint({0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}, port),
               :nowait
             )

    :ok = RawIpLink.fault(link, :drop)
    assert :ok = SmolNet.sendto(client, "checksum", endpoint(@server, port), 1_000)
    assert_receive {:test_link_egress, :client, packet}, 1_000

    <<prefix::binary-size(46), checksum::16, suffix::binary>> = packet
    bad_checksum = if checksum == 1, do: 2, else: 1
    corrupted = <<prefix::binary, bad_checksum::16, suffix::binary>>
    assert :ok = SmolNet.ingress(server_stack, corrupted)
    assert {:error, :timeout} = SmolNet.recvfrom(server, 0, 20)

    assert :ok = SmolNet.ingress(server_stack, packet)
    assert {:ok, %{data: "checksum"}} = SmolNet.recvfrom(server, 0, 1_000)
  end

  test "gen_udp callback supports passive, connected, and active delivery" do
    {server_stack, client_stack, _link} = stacks()

    assert {:ok, server = {:"$inet", Udp, _server_pid}} =
             :gen_udp.open(0, options(server_stack))

    assert {:ok, {{0, 0, 0, 0, 0, 0, 0, 0}, server_port}} = :inet.sockname(server)

    assert {:ok, client = {:"$inet", Udp, _client_pid}} =
             :gen_udp.open(0, options(client_stack))

    assert :ok = :gen_udp.send(client, @server, server_port, "passive")
    assert {:ok, {@client, client_port, "passive"}} = :gen_udp.recv(server, 0, 1_000)

    assert :ok = :gen_udp.send(client, {@server, server_port}, "destination")
    assert {:ok, {@client, ^client_port, "destination"}} = :gen_udp.recv(server, 0, 1_000)

    sockaddr = %{family: :inet6, addr: @server, port: server_port}
    assert :ok = :gen_udp.send(client, sockaddr, [], "sockaddr")
    assert {:ok, {@client, ^client_port, "sockaddr"}} = :gen_udp.recv(server, 0, 1_000)

    assert :ok = :gen_udp.send(client, {@server, server_port}, 0, "legacy")
    assert {:ok, {@client, ^client_port, "legacy"}} = :gen_udp.recv(server, 0, 1_000)

    assert :ok = :gen_udp.send(client, @server, server_port, [], "ancillary-free")
    assert {:ok, {@client, ^client_port, "ancillary-free"}} = :gen_udp.recv(server, 0, 1_000)

    assert :ok = :gen_udp.connect(client, @server, server_port)
    assert {:ok, {@server, ^server_port}} = :inet.peername(client)
    assert :ok = :gen_udp.send(client, "connected")
    assert {:ok, {@client, ^client_port, "connected"}} = :gen_udp.recv(server, 0, 1_000)

    assert :ok = :inet.setopts(server, active: 2)
    assert {:ok, [active: 2, mode: :binary]} = :inet.getopts(server, [:active, :mode])
    assert :ok = :gen_udp.send(client, "one")
    assert :ok = :gen_udp.send(client, "two")
    assert_receive {:udp, ^server, @client, ^client_port, "one"}, 1_000
    assert_receive {:udp, ^server, @client, ^client_port, "two"}, 1_000
    assert_receive {:udp_passive, ^server}, 1_000

    assert :ok = :gen_udp.close(server)
    assert :ok = :gen_udp.close(client)
  end

  test "gen_udp active once and true preserve datagram counting and yield" do
    {server_stack, client_stack, _link} = stacks()

    {:ok, server} = :gen_udp.open(0, options(server_stack))
    {:ok, {{0, 0, 0, 0, 0, 0, 0, 0}, server_port}} = :inet.sockname(server)
    {:ok, client} = :gen_udp.open(0, options(client_stack))
    {:ok, {{0, 0, 0, 0, 0, 0, 0, 0}, client_port}} = :inet.sockname(client)

    assert :ok = :inet.setopts(server, active: :once)
    assert :ok = :gen_udp.send(client, @server, server_port, "once")
    assert :ok = :gen_udp.send(client, @server, server_port, "passive")
    assert_receive {:udp, ^server, @client, ^client_port, "once"}, 1_000
    refute_receive {:udp, ^server, @client, ^client_port, "passive"}, 20
    assert {:ok, {@client, ^client_port, "passive"}} = :gen_udp.recv(server, 0, 1_000)

    assert :ok = :inet.setopts(server, active: true)

    for sequence <- 1..20 do
      assert :ok = :gen_udp.send(client, @server, server_port, <<sequence>>)
    end

    assert Enum.sort(receive_active(server, 20, [])) == Enum.map(1..20, &<<&1>>)
    assert :ok = :inet.setopts(server, active: false)
    assert :ok = :gen_udp.close(server)
    assert :ok = :gen_udp.close(client)
  end

  test "gen_udp receive buffer truncates one whole datagram without leaking a remainder" do
    {server_stack, client_stack, _link} = stacks()

    {:ok, server} = :gen_udp.open(0, options(server_stack, buffer: 3))
    {:ok, {_any, server_port}} = :inet.sockname(server)
    {:ok, client} = :gen_udp.open(0, options(client_stack))

    assert :ok = :gen_udp.send(client, @server, server_port, "abcdef")
    assert {:ok, {@client, _port, "abc"}} = :gen_udp.recv(server, 0, 1_000)
    assert {:error, :timeout} = :gen_udp.recv(server, 0, 10)

    assert :ok = :inet.setopts(server, active: :once)
    assert :ok = :gen_udp.send(client, @server, server_port, "ghijkl")
    assert_receive {:udp, ^server, @client, _port, "ghi"}, 1_000

    assert :ok = :gen_udp.close(server)
    assert :ok = :gen_udp.close(client)
  end

  test "gen_udp ownership transfer moves queued and future active messages" do
    {server_stack, client_stack, _link} = stacks()
    parent = self()

    {:ok, server} = :gen_udp.open(0, options(server_stack))
    {:ok, {_any, server_port}} = :inet.sockname(server)
    {:ok, client} = :gen_udp.open(0, options(client_stack))
    assert :ok = :inet.setopts(server, active: true)

    assert :ok = :gen_udp.send(client, @server, server_port, "queued")
    assert_eventually(fn -> socket_message_queued?(server) end)

    new_owner = spawn(fn -> forward_to_test(parent) end)
    assert :ok = :gen_udp.controlling_process(server, new_owner)
    assert_receive {:new_owner, {:udp, ^server, @client, _port, "queued"}}, 1_000

    assert :ok = :gen_udp.send(client, @server, server_port, "future")
    assert_receive {:new_owner, {:udp, ^server, @client, _port, "future"}}, 1_000
    refute_receive {:udp, ^server, _address, _port, _packet}, 20

    send(new_owner, :stop)
    assert_eventually(fn -> Udp.info(server) == {:error, :closed} end)
    assert :ok = :gen_udp.close(client)
  end

  test "TCP and UDP coexist on one IPv6 stack without readiness cross-talk" do
    {server_stack, client_stack, _link} = stacks()
    port = 42_005

    {:ok, listener} = SmolNet.open(:inet6, :stream, :tcp, stack: server_stack)
    :ok = SmolNet.bind(listener, endpoint(@server, port))
    :ok = SmolNet.listen(listener, 2)
    accept = Task.async(fn -> SmolNet.accept(listener, 1_000) end)

    {:ok, tcp_client} = SmolNet.open(:inet6, :stream, :tcp, stack: client_stack)
    assert :ok = SmolNet.connect(tcp_client, endpoint(@server, port), 1_000)
    assert {:ok, tcp_server} = Task.await(accept)

    {:ok, udp_server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    assert :ok = SmolNet.bind(udp_server, endpoint(@server, port))
    {:ok, udp_client} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    assert :ok = SmolNet.bind(udp_client, endpoint(@client, 0))

    tcp_read = Task.async(fn -> SmolNet.recv(tcp_server, 3, 1_000) end)
    udp_read = Task.async(fn -> SmolNet.recvfrom(udp_server, 0, 1_000) end)
    Process.sleep(10)

    assert :ok = SmolNet.sendto(udp_client, "udp", endpoint(@server, port), 1_000)
    assert :ok = SmolNet.send(tcp_client, "tcp", 1_000)
    assert {:ok, "tcp"} = Task.await(tcp_read)
    assert {:ok, %{data: "udp"}} = Task.await(udp_read)

    assert :ok = SmolNet.close(udp_client)
    assert :ok = SmolNet.close(udp_server)
    assert :ok = SmolNet.close(tcp_client)
    assert :ok = SmolNet.close(tcp_server)
    assert :ok = SmolNet.close(listener)
  end

  test "non-driving UDP connect preserves a pending TCP timer" do
    {server_stack, client_stack, link} = stacks()
    :ok = RawIpLink.fault(link, :drop)

    {:ok, tcp} = SmolNet.open(:inet6, :stream, :tcp, stack: client_stack)
    assert {:select, tcp_select} = SmolNet.connect(tcp, endpoint(@server, 42_006), :nowait)
    {:ok, before} = SmolNet.stack_info(client_stack)
    assert is_integer(before.poll_at)

    {:ok, udp} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    assert :ok = SmolNet.bind(udp, endpoint(@client, 0))
    assert :ok = SmolNet.connect(udp, endpoint(@server, 42_006))
    {:ok, after_connect} = SmolNet.stack_info(client_stack)
    assert after_connect.poll_at == before.poll_at

    assert :ok = SmolNet.cancel(tcp, tcp_select)
    assert :ok = SmolNet.close(udp)
    assert :ok = SmolNet.close(tcp)
    assert {:ok, _info} = SmolNet.stack_info(server_stack)
  end

  test "UDP options, wrong-kind calls, and adapter errors are explicit" do
    {server_stack, _client_stack, _link} = stacks()

    assert {:error, :eaddrnotavail} =
             Udp.open(0, [
               :inet,
               {:smolnet_stack, server_stack},
               :binary,
               {:active, false}
             ])

    assert {:error, :eafnosupport} =
             Udp.open(0, [
               :inet,
               :inet6,
               {:smolnet_stack, server_stack},
               :binary,
               {:active, false}
             ])

    assert {:error, :einval} = :gen_udp.open(0, options(server_stack, packet: 2))

    {:ok, udp} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    assert {:error, :invalid_socket_state} = SmolNet.listen(udp, 1)
    assert {:error, :invalid_socket_state} = SmolNet.send(udp, "stream", :nowait)
    assert {:error, :invalid_socket_state} = SmolNet.recv(udp, 0, :nowait)
    assert {:error, :invalid_socket_state} = SmolNet.shutdown(udp, :read)

    {:ok, tcp} = SmolNet.open(:inet6, :stream, :tcp, stack: server_stack)

    assert {:error, :invalid_socket_state} =
             SmolNet.sendto(tcp, "datagram", endpoint(@server, 9), :nowait)

    assert {:error, :invalid_socket_state} = SmolNet.recvfrom(tcp, 0, :nowait)
    assert :ok = SmolNet.close(tcp)
    assert :ok = SmolNet.close(udp)

    {:ok, socket} = :gen_udp.open(0, options(server_stack))
    assert {:error, :einval} = :inet.setopts(socket, packet: 2)
    assert {:error, :einval} = :inet.setopts(socket, send_timeout: 10)
    assert {:error, :einval} = :inet.setopts(socket, ipv6_v6only: true)
    assert {:error, :einval} = :inet.getopts(socket, [:packet])
    assert {:error, :emsgsize} = :gen_udp.send(socket, @client, 9, :binary.copy("x", 1_453))
    assert :ok = :gen_udp.close(socket)
  end

  defp stacks do
    {:ok, link} = RawIpLink.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 64}])

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client, 64}])

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)
    {server_stack, client_stack, link}
  end

  defp options(stack, extra \\ []) do
    [{:udp_module, Udp}] ++
      extra ++
      [
        {:smolnet_stack, stack},
        :inet6,
        :binary,
        {:active, false}
      ]
  end

  defp endpoint(address, port) do
    %{family: :inet6, addr: address, port: port, flowinfo: 0, scope_id: 0}
  end

  defp receive_active(_socket, 0, packets), do: packets

  defp receive_active(socket, remaining, packets) do
    receive do
      {:udp, ^socket, @client, _port, packet} ->
        receive_active(socket, remaining - 1, [packet | packets])
    after
      1_000 -> flunk("did not receive every active UDP datagram")
    end
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
      {:udp, ^socket, _address, _port, _packet} -> true
      _message -> false
    end)
  end

  defp assert_eventually(check, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(check, deadline)
  end

  defp call_queued?(stack, caller) do
    {:messages, messages} = Process.info(stack, :messages)

    Enum.any?(messages, fn
      {:"$gen_call", {^caller, _tag}, _request} -> true
      _message -> false
    end)
  end

  defp resume_if_alive(stack) do
    :sys.resume(stack)
  catch
    :exit, _reason -> :ok
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
