defmodule SmolNet.TcpConnectTest do
  use ExUnit.Case, async: false

  alias SmolNet.Socket
  alias SmolNet.Test.IPv6TcpPeer
  alias SmolNet.Test.ManualClock

  import Bitwise

  @client {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @peer {0xFD00, 0, 0, 0, 0, 0, 0, 2}
  @unreachable {0xFD01, 0, 0, 0, 0, 0, 0, 2}
  @link_local {0xFE80, 0, 0, 0, 0, 0, 0, 1}
  @link_local_peer {0xFE80, 0, 0, 0, 0, 0, 0, 2}

  setup do
    previous_clock = Application.get_env(:smolnet, :clock_module)
    previous_manual_clock = Application.get_env(:smolnet, :manual_clock)

    on_exit(fn ->
      stop_all_stacks()

      case previous_clock do
        nil -> Application.delete_env(:smolnet, :clock_module)
        value -> Application.put_env(:smolnet, :clock_module, value)
      end

      case previous_manual_clock do
        nil -> Application.delete_env(:smolnet, :manual_clock)
        value -> Application.put_env(:smolnet, :manual_clock, value)
      end
    end)
  end

  test "an immediate IPv6 handshake completes through nowait and retry" do
    {stack, peer} = start_peer_stack(:accept)
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert {:error, :not_bound} = SmolNet.sockname(socket)
    assert {:error, :not_connected} = SmolNet.peername(socket)
    assert {:select, select_info} = SmolNet.connect(socket, endpoint(@peer, 443), :nowait)
    assert {:select_info, :connect, _reference} = select_info
    assert_select(socket, select_info)
    assert :ok = SmolNet.connect(socket, endpoint(@peer, 443), :nowait)
    assert {:error, :already_connected} = SmolNet.connect(socket, endpoint(@peer, 443), :nowait)

    assert {:error, :already_connected} =
             SmolNet.connect(socket, endpoint(@unreachable, 443), :nowait)

    assert {:ok, %{addr: @client, port: port}} = SmolNet.sockname(socket)
    assert port in 49_152..50_175
    assert {:ok, %{addr: @peer, port: 443}} = SmolNet.peername(socket)

    assert_eventually(fn ->
      %{packets: packets} = IPv6TcpPeer.stats(peer)

      Enum.any?(packets, &flag?(&1.flags, 0x02)) and
        Enum.any?(packets, &(flag?(&1.flags, 0x10) and not flag?(&1.flags, 0x02)))
    end)

    assert :ok = SmolNet.close(socket)
    assert {:error, :invalid_socket} = SmolNet.sockname(socket)
    assert {:error, :invalid_socket} = SmolNet.peername(socket)
    assert {:error, :invalid_socket} = SmolNet.close(socket)
  end

  test "a delayed SYN/ACK wakes once and finalizes on retry" do
    {stack, peer} = start_peer_stack({:delay, :accept})
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert {:select, select_info} = SmolNet.connect(socket, endpoint(@peer, 80), :nowait)
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).held == 1 end)
    refute_select(socket, select_info)

    {:ok, before_release} = SmolNet.stack_info(stack)
    assert is_integer(before_release.poll_at)
    assert {:ok, %{addr: @client, port: port}} = SmolNet.sockname(socket)
    assert port in 49_152..50_175
    assert {:ok, %{addr: @peer, port: 80}} = SmolNet.peername(socket)
    :ok = IPv6TcpPeer.release(peer)
    assert_select(socket, select_info)
    assert :ok = SmolNet.connect(socket, endpoint(@peer, 80), :nowait)

    {:ok, after_release} = SmolNet.stack_info(stack)
    assert after_release.timer_generation > before_release.timer_generation
  end

  test "duplicate connects are busy and cancellation permits a new waiter" do
    {stack, _peer} = start_peer_stack(:ignore)
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert {:select, first} = SmolNet.connect(socket, endpoint(@peer, 80), :nowait)
    assert {:error, :busy} = SmolNet.connect(socket, endpoint(@peer, 80), :nowait)

    assert {:error, :invalid_socket_state} =
             SmolNet.connect(socket, endpoint(@peer, 81), :nowait)

    assert {:error, :invalid_socket_state} =
             SmolNet.connect(socket, endpoint(@unreachable, 81), :nowait)

    assert :ok = SmolNet.cancel(socket, first)
    assert {:select, second} = SmolNet.connect(socket, endpoint(@peer, 80), :nowait)
    assert {:select_info, :connect, first_reference} = first
    assert {:select_info, :connect, second_reference} = second
    refute first_reference == second_reference
  end

  test "refused and reset handshakes have distinct stable errors" do
    {refused_stack, _peer} = start_peer_stack(:refuse)
    {:ok, refused} = SmolNet.open(:inet6, :stream, :tcp, stack: refused_stack)
    assert {:select, refused_select} = SmolNet.connect(refused, endpoint(@peer, 81), :nowait)
    assert_select(refused, refused_select)

    assert {:error, :connection_refused} =
             SmolNet.connect(refused, endpoint(@peer, 81), :nowait)

    assert {:ok, %{addr: @client}} = SmolNet.sockname(refused)
    assert {:error, :not_connected} = SmolNet.peername(refused)

    {reset_stack, _peer} = start_peer_stack(:reset)
    {:ok, reset} = SmolNet.open(:inet6, :stream, :tcp, stack: reset_stack)
    assert {:select, reset_select} = SmolNet.connect(reset, endpoint(@peer, 82), :nowait)
    assert_select(reset, reset_select)
    assert_receive {:tcp_peer_egress, flags, _packet} when band(flags, 0x10) != 0

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(reset_stack)
      info.processed_ingress == 2
    end)

    assert {:error, :connection_reset} = SmolNet.connect(reset, endpoint(@peer, 82), :nowait)
  end

  test "hop-by-hop reset packets retain refused and reset classifications" do
    {refused_stack, _peer} = start_peer_stack({:hop_by_hop, :refuse})
    {:ok, refused} = SmolNet.open(:inet6, :stream, :tcp, stack: refused_stack)
    assert {:select, refused_select} = SmolNet.connect(refused, endpoint(@peer, 85), :nowait)
    assert_select(refused, refused_select)

    assert {:error, :connection_refused} =
             SmolNet.connect(refused, endpoint(@peer, 85), :nowait)

    {reset_stack, _peer} = start_peer_stack({:hop_by_hop, :reset})
    {:ok, reset} = SmolNet.open(:inet6, :stream, :tcp, stack: reset_stack)
    assert {:select, reset_select} = SmolNet.connect(reset, endpoint(@peer, 86), :nowait)
    assert_select(reset, reset_select)
    assert_receive {:tcp_peer_egress, flags, _packet} when band(flags, 0x10) != 0

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(reset_stack)
      info.processed_ingress == 2
    end)

    assert {:error, :connection_reset} = SmolNet.connect(reset, endpoint(@peer, 86), :nowait)
  end

  test "an ignored handshake retransmits and ends in the native timeout" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)

    {stack, peer} = start_peer_stack(:ignore)
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    assert {:select, select_info} = SmolNet.connect(socket, endpoint(@peer, 83), :nowait)

    {:ok, before} = SmolNet.stack_info(stack)
    assert before.poll_at == 1_000
    :ok = ManualClock.advance(clock, 1_000)

    assert_eventually(fn ->
      %{packets: packets} = IPv6TcpPeer.stats(peer)
      Enum.count(packets, &flag?(&1.flags, 0x02)) >= 2
    end)

    :ok = ManualClock.advance(clock, 29_001)
    assert_select(socket, select_info)
    assert {:error, :connection_timeout} = SmolNet.connect(socket, endpoint(@peer, 83), :nowait)

    %{packets: packets} = IPv6TcpPeer.stats(peer)
    assert Enum.count(packets, &flag?(&1.flags, 0x02)) >= 2
  end

  test "close during handshake aborts once and a late SYN/ACK cannot revive the handle" do
    {stack, peer} = start_peer_stack({:delay, :accept})
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    assert {:select, select_info} = SmolNet.connect(socket, endpoint(@peer, 84), :nowait)
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).held == 1 end)

    assert :ok = SmolNet.close(socket)
    assert_abort(socket, select_info, :closed)
    refute_socket_message(socket)

    :ok = IPv6TcpPeer.release(peer)
    refute_socket_message(socket)
    assert {:error, :invalid_socket} = SmolNet.connect(socket, endpoint(@peer, 84), :nowait)
  end

  test "a finalized connection survives beyond the connect timeout" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)

    {stack, peer} = start_peer_stack(:accept)
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    assert {:select, select_info} = SmolNet.connect(socket, endpoint(@peer, 87), :nowait)
    assert_select(socket, select_info)
    assert :ok = SmolNet.connect(socket, endpoint(@peer, 87), :nowait)

    {:ok, finalized} = SmolNet.stack_info(stack)
    assert finalized.poll_at == nil

    :ok = ManualClock.advance(clock, 30_001)
    Process.sleep(50)

    assert {:ok, %{addr: @client}} = SmolNet.sockname(socket)
    assert {:ok, %{addr: @peer, port: 87}} = SmolNet.peername(socket)
    assert {:error, :already_connected} = SmolNet.connect(socket, endpoint(@peer, 87), :nowait)
    refute Enum.any?(IPv6TcpPeer.stats(peer).packets, &flag?(&1.flags, 0x04))
  end

  test "an explicitly bound link-local socket connects with a scope" do
    {stack, _peer} = start_peer_stack(:accept, addresses: [{@link_local, 64}])
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert :ok = SmolNet.bind(socket, endpoint(@link_local, 49_999, scope_id: 7))

    assert {:select, select_info} =
             SmolNet.connect(socket, endpoint(@link_local_peer, 443, scope_id: 7), :nowait)

    assert_select(socket, select_info)
    assert :ok = SmolNet.connect(socket, endpoint(@link_local_peer, 443, scope_id: 7), :nowait)

    assert {:ok, %{addr: @link_local, port: 49_999, scope_id: 7}} = SmolNet.sockname(socket)
    assert {:ok, %{addr: @link_local_peer, port: 443, scope_id: 7}} = SmolNet.peername(socket)
  end

  test "bind detects collisions and allocates bounded ephemeral ports" do
    {:ok, stack} = SmolNet.start_stack(addresses: [{@client, 64}])
    {:ok, first} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    {:ok, second} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    {:ok, third} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert :ok = SmolNet.bind(first, endpoint(@client, 50_000))
    assert {:error, :address_in_use} = SmolNet.bind(second, endpoint(@client, 50_000))
    assert :ok = SmolNet.bind(second, endpoint(@client, 0))
    assert :ok = SmolNet.bind(third, endpoint(@client, 0))
    assert {:ok, %{port: second_port}} = SmolNet.sockname(second)
    assert {:ok, %{port: third_port}} = SmolNet.sockname(third)
    refute second_port == third_port
    assert {:error, :invalid_socket_state} = SmolNet.bind(first, endpoint(@client, 50_001))
  end

  test "ephemeral allocation reports deterministic exhaustion" do
    {:ok, stack} =
      SmolNet.start_stack(addresses: [{@client, 64}], limits: %{ready_events: 1_025})

    for port <- 49_152..50_175 do
      {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
      assert :ok = SmolNet.bind(socket, endpoint(@client, port))
    end

    {:ok, exhausted} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
    assert {:error, :ephemeral_ports_exhausted} = SmolNet.bind(exhausted, endpoint(@client, 0))
  end

  test "address, port, scope, family, route, and timeout validation is explicit" do
    {:ok, stack} = SmolNet.start_stack(addresses: [{@client, 64}, {@link_local, 64}])
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert {:error, :unsupported_family} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
    assert {:error, :unsupported_socket} = SmolNet.open(:inet6, :dgram, :udp, stack: stack)
    assert {:error, :invalid_options} = SmolNet.open(:inet6, :stream, :tcp, [])

    assert {:error, :invalid_address} =
             SmolNet.bind(socket, %{family: :inet6, addr: {1}, port: 1})

    assert {:error, :invalid_address} =
             SmolNet.bind(socket, endpoint(@client, 1) |> Map.put(:flowinfo, 0.0))

    ipv4_endpoint = %{family: :inet, addr: {192, 0, 2, 1}, port: 80}
    assert {:error, :unsupported_family} = SmolNet.bind(socket, ipv4_endpoint)
    assert {:error, :unsupported_family} = SmolNet.connect(socket, ipv4_endpoint, :nowait)

    assert {:error, :invalid_port} = SmolNet.bind(socket, endpoint(@client, 65_536))
    assert {:error, :scope_required} = SmolNet.bind(socket, endpoint(@link_local, 1))

    assert {:error, :invalid_scope} =
             SmolNet.bind(socket, endpoint(@link_local, 1, scope_id: -1))

    assert {:error, :invalid_scope} =
             SmolNet.bind(socket, endpoint(@link_local, 1, scope_id: 0.0))

    assert {:error, :invalid_scope} =
             SmolNet.bind(socket, endpoint(@link_local, 1, scope_id: 4_294_967_296))

    assert :ok =
             SmolNet.bind(socket, endpoint(@link_local, 1, scope_id: 7))

    {:ok, other} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert {:error, :scope_required} =
             SmolNet.connect(other, endpoint(@link_local_peer, 80), :nowait)

    assert {:error, :invalid_scope} =
             SmolNet.connect(other, endpoint(@link_local_peer, 80, scope_id: -1), :nowait)

    assert {:error, :invalid_scope} =
             SmolNet.connect(other, endpoint(@link_local_peer, 80, scope_id: 0.0), :nowait)

    assert {:error, :invalid_scope} =
             SmolNet.connect(
               other,
               endpoint(@link_local_peer, 80, scope_id: 4_294_967_296),
               :nowait
             )

    assert {:error, :invalid_scope} =
             SmolNet.connect(other, endpoint(@peer, 80, scope_id: 7), :nowait)

    assert {:error, :invalid_address} =
             SmolNet.connect(other, endpoint({0, 0, 0, 0, 0, 0, 0, 0}, 80), :nowait)

    assert {:error, :network_unreachable} =
             SmolNet.connect(other, endpoint(@unreachable, 80), :nowait)

    assert {:error, :invalid_timeout} =
             SmolNet.connect(other, endpoint(@peer, 80), -1)
  end

  test "many pending sockets stay bounded and separate stacks progress independently" do
    {blocked_stack, _blocked_peer} = start_peer_stack(:ignore, limits: %{ready_events: 32})

    blocked =
      for port <- 10_000..10_031 do
        {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: blocked_stack)
        assert {:select, _select_info} = SmolNet.connect(socket, endpoint(@peer, port), :nowait)
        socket
      end

    assert {:error, :system_limit} =
             SmolNet.open(:inet6, :stream, :tcp, stack: blocked_stack)

    {:ok, blocked_info} = SmolNet.stack_info(blocked_stack)
    assert blocked_info.native.result.socket_count == 32
    assert blocked_info.native.result.waiter_count == 32
    assert blocked_info.native.result.tcp_buffer_bytes == 4_096

    {active_stack, _active_peer} = start_peer_stack(:accept)
    {:ok, active} = SmolNet.open(:inet6, :stream, :tcp, stack: active_stack)
    assert {:select, active_select} = SmolNet.connect(active, endpoint(@peer, 443), :nowait)
    assert_select(active, active_select)
    assert :ok = SmolNet.connect(active, endpoint(@peer, 443), :nowait)

    Enum.each(blocked, &SmolNet.close/1)
  end

  test "concurrent connects on one stack are serialized and all complete" do
    {stack, _peer} = start_peer_stack(:accept)

    sockets =
      for port <- 20_000..20_015 do
        {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
        {socket, port}
      end

    tasks =
      Enum.map(sockets, fn {socket, port} ->
        Task.async(fn ->
          %Socket{id: id, generation: generation} = socket

          assert {:select, {:select_info, :connect, reference}} =
                   SmolNet.connect(socket, endpoint(@peer, port), :nowait)

          assert_receive {:"$smol_socket", {^id, ^generation}, :select, ^reference}, 1_000
          assert :ok = SmolNet.connect(socket, endpoint(@peer, port), :nowait)
          {socket, port}
        end)
      end)

    for task <- tasks do
      {socket, port} = Task.await(task, 2_000)
      assert {:ok, %{addr: @peer, port: ^port}} = SmolNet.peername(socket)
    end
  end

  defp start_peer_stack(mode, options \\ []) do
    {:ok, peer} = IPv6TcpPeer.start_link(self(), mode)

    {:ok, stack} =
      SmolNet.start_stack(
        Keyword.merge(
          [egress: {peer, :tcp_client}, addresses: [{@client, 64}]],
          options
        )
      )

    :ok = IPv6TcpPeer.attach(peer, stack)
    {stack, peer}
  end

  defp endpoint(address, port, options \\ []) do
    %{
      family: :inet6,
      addr: address,
      port: port,
      flowinfo: 0,
      scope_id: Keyword.get(options, :scope_id, 0)
    }
  end

  defp assert_select(socket, select_info) do
    %Socket{id: id, generation: generation} = socket
    {:select_info, _operation, reference} = select_info
    assert_receive {:"$smol_socket", {^id, ^generation}, :select, ^reference}, 1_000
  end

  defp refute_select(socket, select_info) do
    %Socket{id: id, generation: generation} = socket
    {:select_info, _operation, reference} = select_info
    refute_receive {:"$smol_socket", {^id, ^generation}, :select, ^reference}, 50
  end

  defp assert_abort(socket, select_info, reason) do
    %Socket{id: id, generation: generation} = socket
    {:select_info, _operation, reference} = select_info

    assert_receive {:"$smol_socket", {^id, ^generation}, :abort, ^reference, ^reason},
                   1_000
  end

  defp refute_socket_message(socket) do
    %Socket{id: id, generation: generation} = socket
    refute_receive {:"$smol_socket", {^id, ^generation}, _, _}, 50
    refute_receive {:"$smol_socket", {^id, ^generation}, _, _, _}, 50
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      SmolNet.Supervisor
      |> DynamicSupervisor.which_children()
      |> Enum.each(fn {_id, pid, _type, _modules} ->
        DynamicSupervisor.terminate_child(SmolNet.Supervisor, pid)
      end)
    end
  end

  defp flag?(flags, flag), do: (flags &&& flag) != 0
end
