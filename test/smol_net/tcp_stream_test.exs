defmodule SmolNet.TcpStreamTest do
  use ExUnit.Case, async: false

  alias SmolNet.Socket
  alias SmolNet.Test.IPv6TcpPeer
  alias SmolNet.Test.ManualClock

  import Bitwise

  @client {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @peer {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    previous_clock = Application.get_env(:smolnet, :clock_module)
    previous_manual_clock = Application.get_env(:smolnet, :manual_clock)

    on_exit(fn ->
      stop_all_stacks()

      restore_env(:clock_module, previous_clock)
      restore_env(:manual_clock, previous_manual_clock)
    end)
  end

  test "small nowait sends and receives complete with byte-stream data" do
    {stack, peer, socket} = connected_socket()

    assert :ok = SmolNet.send(socket, ["hel", "lo"], :nowait)
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).received == "hello" end)

    assert :ok = IPv6TcpPeer.send_data(peer, "world")
    assert {:ok, "world"} = SmolNet.recv(socket, 5, 1_000)

    assert {:ok, %{native: %{result: native}}} = SmolNet.stack_info(stack)
    assert native.counters.max_bytes_copied <= native.limits.bytes_copied
  end

  test "large synchronous sends retry bounded native chunks in order" do
    {_stack, peer, socket} = connected_socket()
    payload = :binary.copy(<<0, 1, 2, 3, 4, 5, 6, 7>>, 2_500)

    assert :ok = SmolNet.send(socket, payload, 2_000)
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).received == payload end, 2_000)
  end

  test "graceful closing records retain bounded logical socket capacity" do
    {stack, peer, socket} = connected_socket(limits: %{ready_events: 1})
    :ok = IPv6TcpPeer.hold_acks(peer, true)

    assert :ok = SmolNet.close(socket)
    assert {:error, :system_limit} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    {:ok, info} = SmolNet.stack_info(stack)
    assert info.native.result.socket_count == 0
    assert info.native.result.closing_tcp_socket_count == 1

    assert info.native.result.socket_count + info.native.result.closing_tcp_socket_count <=
             info.native.result.limits.ready_events
  end

  test "exact receives accumulate only in the caller across bounded native reads" do
    {_stack, peer, socket} = connected_socket(limits: %{bytes_copied: 1_280}, mtu: 1_280)
    payload = :binary.copy("abc", 1_000)

    assert :ok = IPv6TcpPeer.send_data(peer, payload)
    assert {:ok, ^payload} = SmolNet.recv(socket, byte_size(payload), 1_000)
  end

  test "zero-length receive returns one bounded currently available chunk" do
    {_stack, peer, socket} = connected_socket(limits: %{bytes_copied: 1_280}, mtu: 1_280)
    payload = :binary.copy("z", 2_000)

    assert :ok = IPv6TcpPeer.send_data(peer, payload)
    assert {:ok, first} = SmolNet.recv(socket, 0, 1_000)
    assert byte_size(first) in 1..1_280
    assert binary_part(payload, 0, byte_size(first)) == first
    assert {:ok, second} = SmolNet.recv(socket, byte_size(payload) - byte_size(first), 1_000)
    assert first <> second == payload
  end

  test "zero-length receive waits when empty and reports EOF as closed" do
    {_stack, peer, socket} = connected_socket()

    assert {:select, select_info} = SmolNet.recv(socket, 0, :nowait)
    assert :ok = SmolNet.cancel(socket, select_info)
    assert {:error, :timeout} = SmolNet.recv(socket, 0, 0)

    assert :ok = IPv6TcpPeer.finish(peer)
    assert {:error, :closed} = SmolNet.recv(socket, 0, 1_000)
  end

  test "send timeout returns only the caller-owned unsent remainder" do
    {_stack, peer, socket} = connected_socket()
    :ok = IPv6TcpPeer.hold_acks(peer, true)
    payload = :binary.copy("send", 2_000)

    assert {:error, {:timeout, remainder}} = SmolNet.send(socket, payload, 0)
    assert byte_size(remainder) > 0
    assert byte_size(remainder) < byte_size(payload)
    refute remainder == payload

    assert_eventually(fn ->
      received = IPv6TcpPeer.stats(peer).received

      byte_size(received) > 0 and
        byte_size(received) <= byte_size(payload) - byte_size(remainder) and
        binary_part(payload, 0, byte_size(received)) == received
    end)
  end

  test "nonblocking partial operations use independent read and write waiters" do
    {stack, peer, socket} = connected_socket()
    assert {:select, read_select} = SmolNet.recv(socket, 10, :nowait)

    :ok = IPv6TcpPeer.hold_acks(peer, true)
    payload = :binary.copy("wait", 2_000)
    assert {:select, {write_select, remainder}} = SmolNet.send(socket, payload, :nowait)
    assert byte_size(remainder) in 1..(byte_size(payload) - 1)

    assert {:ok, info} = SmolNet.stack_info(stack)
    assert info.native.result.read_waiter_count == 1
    assert info.native.result.write_waiter_count == 1
    assert :ok = SmolNet.cancel(socket, read_select)
    assert :ok = SmolNet.cancel(socket, write_select)
  end

  test "waiter exhaustion never consumes send or receive bytes" do
    {stack, peer, socket} = connected_socket(limits: %{ready_events: 1})
    assert {:select, read_select} = SmolNet.recv(socket, 1, :nowait)
    assert :ok = IPv6TcpPeer.hold_acks(peer, true)

    assert {:error, :system_limit} =
             SmolNet.send(socket, :binary.copy("send", 2_000), :nowait)

    Process.sleep(20)
    assert IPv6TcpPeer.stats(peer).received == ""
    assert :ok = SmolNet.cancel(socket, read_select)

    payload = :binary.copy("wait", 5_000)
    assert {:select, {write_select, remainder}} = SmolNet.send(socket, payload, :nowait)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.native.result.write_waiter_count == 1
    end)

    assert :ok = IPv6TcpPeer.send_data(peer, "abc")
    assert_select(socket, write_select)

    assert {:select, {new_write_select, _remainder}} =
             SmolNet.send(socket, remainder, :nowait)

    assert {:error, :system_limit} = SmolNet.recv(socket, 10, :nowait)
    assert :ok = SmolNet.cancel(socket, new_write_select)
    assert {:select, {read_select, "abc"}} = SmolNet.recv(socket, 10, :nowait)
    assert :ok = SmolNet.cancel(socket, read_select)
  end

  test "one receive deadline is not restarted by partial progress" do
    {stack, peer, socket} = connected_socket()
    assert :ok = IPv6TcpPeer.send_data(peer, "first")

    sender =
      Task.async(fn ->
        assert_eventually(fn ->
          {:ok, info} = SmolNet.stack_info(stack)
          info.native.result.read_waiter_count == 1
        end)

        Process.sleep(300)
        :ok = IPv6TcpPeer.send_data(peer, "second")
        System.monotonic_time(:millisecond)
      end)

    started_at = System.monotonic_time(:millisecond)
    assert {:error, {:timeout, "firstsecond"}} = SmolNet.recv(socket, 20, 1_000)
    finished_at = System.monotonic_time(:millisecond)
    second_sent_at = Task.await(sender)

    assert finished_at - started_at >= 850
    assert finished_at - second_sent_at < 850
  end

  test "EOF returns buffered data before closed and reset stays distinct" do
    {_stack, peer, socket} = connected_socket()
    assert :ok = IPv6TcpPeer.send_data(peer, "final")
    assert :ok = IPv6TcpPeer.finish(peer)
    assert {:ok, "final"} = SmolNet.recv(socket, 100, 1_000)
    assert {:error, :closed} = SmolNet.recv(socket, 1, :nowait)

    {_reset_stack, reset_peer, reset_socket} = connected_socket()
    assert :ok = IPv6TcpPeer.reset(reset_peer)

    assert_eventually(fn ->
      SmolNet.recv(reset_socket, 1, :nowait) == {:error, :connection_reset}
    end)
  end

  test "exact receive returns earlier chunks when FIN arrives separately" do
    {stack, peer, socket} = connected_socket()
    assert :ok = IPv6TcpPeer.send_data(peer, "abc")

    task = Task.async(fn -> SmolNet.recv(socket, 10, 1_000) end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.native.result.read_waiter_count == 1
    end)

    assert :ok = IPv6TcpPeer.finish(peer)
    assert {:ok, "abc"} = Task.await(task)
    assert {:error, :closed} = SmolNet.recv(socket, 1, :nowait)
  end

  test "stack loss after partial receive remains an error with partial data" do
    {stack, peer, socket} = connected_socket()
    assert :ok = IPv6TcpPeer.send_data(peer, "abc")

    task = Task.async(fn -> SmolNet.recv(socket, 10, :infinity) end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.native.result.read_waiter_count == 1
    end)

    assert true = :erlang.suspend_process(task.pid)

    try do
      assert :ok = IPv6TcpPeer.send_data(peer, "d")

      assert_eventually(fn ->
        {:messages, messages} = Process.info(task.pid, :messages)

        Enum.any?(messages, fn
          {:"$smol_socket", _, :select, _reference} -> true
          _message -> false
        end)
      end)

      assert :ok = SmolNet.stop_stack(stack)
    after
      assert true = :erlang.resume_process(task.pid)
    end

    assert {:error, {:closed, "abc"}} = Task.await(task)
  end

  test "write shutdown sends FIN, rejects sends, and preserves reads" do
    {_stack, peer, socket} = connected_socket()
    assert :ok = SmolNet.shutdown(socket, :write)
    assert {:error, :closed} = SmolNet.send(socket, "late", :nowait)

    assert_eventually(fn ->
      Enum.any?(IPv6TcpPeer.stats(peer).packets, &flag?(&1.flags, 0x01))
    end)

    assert :ok = IPv6TcpPeer.send_data(peer, "still readable")
    assert {:ok, "still readable"} = SmolNet.recv(socket, 14, 1_000)
    assert :ok = SmolNet.shutdown(socket, :write)
  end

  test "read shutdown aborts its waiter without closing the write half" do
    {stack, peer, socket} = connected_socket()
    task = Task.async(fn -> SmolNet.recv(socket, 1, :infinity) end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.native.result.read_waiter_count == 1
    end)

    assert :ok = SmolNet.shutdown(socket, :read)
    assert {:error, :closed} = Task.await(task)
    assert {:error, :closed} = SmolNet.recv(socket, 1, :nowait)
    assert :ok = SmolNet.send(socket, "write remains", 1_000)
    assert_eventually(fn -> IPv6TcpPeer.stats(peer).received == "write remains" end)
  end

  test "graceful close invalidates the handle while bounded native closing state is driven" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)
    {stack, peer, socket} = connected_socket()

    assert :ok = SmolNet.close(socket)
    assert {:error, :invalid_socket} = SmolNet.send(socket, "closed", :nowait)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.native.result.closing_tcp_socket_count == 1
    end)

    assert_eventually(fn ->
      Enum.any?(IPv6TcpPeer.stats(peer).packets, &flag?(&1.flags, 0x01))
    end)

    assert :ok = IPv6TcpPeer.finish(peer)

    for _step <- 1..3 do
      :ok = ManualClock.advance(clock, 60_001)
      Process.sleep(10)
    end

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)

      info.native.result.closing_tcp_socket_count == 0 and
        info.native.result.native_socket_count == 0
    end)
  end

  test "graceful close deadline starts at close after an idle connection" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)
    {_stack, peer, socket} = connected_socket()

    assert :ok = ManualClock.advance(clock, 60_001)
    Process.sleep(10)
    packet_count = length(IPv6TcpPeer.stats(peer).packets)

    assert :ok = SmolNet.close(socket)

    assert_eventually(fn ->
      packets = IPv6TcpPeer.stats(peer).packets |> Enum.drop(packet_count)

      Enum.any?(packets, &flag?(&1.flags, 0x01)) and
        Enum.all?(packets, &(not flag?(&1.flags, 0x04)))
    end)
  end

  test "unresponsive graceful close expires with minimum maintenance work" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)
    {stack, peer, socket} = connected_socket(limits: %{maintenance_work: 1})

    assert :ok = SmolNet.close(socket)
    assert :ok = ManualClock.advance(clock, 30_001)

    for _step <- 1..8 do
      Process.sleep(10)
      assert :ok = ManualClock.advance(clock, 0)
    end

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)

      info.native.result.closing_tcp_socket_count == 0 and
        info.native.result.native_socket_count == 0
    end)

    assert Enum.any?(IPv6TcpPeer.stats(peer).packets, &flag?(&1.flags, 0x04))
  end

  test "close timeout retains an aborted socket until its RST is dispatched" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)

    {stack, peer, busy_socket} =
      connected_socket(limits: %{maintenance_work: 1, output_packets: 1})

    {:ok, closing_socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert {:select, connect_info} =
             SmolNet.connect(closing_socket, endpoint(@peer, 443), :nowait)

    assert_select(closing_socket, connect_info)
    assert :ok = SmolNet.connect(closing_socket, endpoint(@peer, 443), :nowait)
    assert {:ok, %{port: closing_port}} = SmolNet.sockname(closing_socket)

    assert :ok = IPv6TcpPeer.hold_acks(peer, true)

    assert {:select, {busy_select, _remainder}} =
             SmolNet.send(busy_socket, :binary.copy("busy", 5_000), :nowait)

    assert :ok = SmolNet.close(closing_socket)

    assert_eventually(fn ->
      IPv6TcpPeer.stats(peer).packets
      |> Enum.any?(&(&1.source_port == closing_port and flag?(&1.flags, 0x01)))
    end)

    assert :ok = ManualClock.advance(clock, 30_001)

    for _step <- 1..40 do
      Process.sleep(5)
      assert :ok = ManualClock.advance(clock, 0)
    end

    assert_eventually(fn ->
      IPv6TcpPeer.stats(peer).packets
      |> Enum.any?(&(&1.source_port == closing_port and flag?(&1.flags, 0x04)))
    end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)

      info.native.result.closing_tcp_socket_count == 0 and
        info.native.result.native_socket_count == 1
    end)

    assert :ok = SmolNet.cancel(busy_socket, busy_select)
  end

  test "a blocked large send cannot monopolize another stack" do
    {blocked_stack, blocked_peer, blocked_socket} = connected_socket()
    assert :ok = IPv6TcpPeer.hold_acks(blocked_peer, true)

    blocked =
      Task.async(fn ->
        SmolNet.send(blocked_socket, :binary.copy("blocked", 4_000), :infinity)
      end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(blocked_stack)
      info.native.result.write_waiter_count == 1
    end)

    {_free_stack, free_peer, free_socket} = connected_socket()
    assert :ok = SmolNet.send(free_socket, "free", 1_000)
    assert_eventually(fn -> IPv6TcpPeer.stats(free_peer).received == "free" end)
    assert :ok = IPv6TcpPeer.send_data(free_peer, "reply")
    assert {:ok, "reply"} = SmolNet.recv(free_socket, 5, 1_000)

    assert :ok = SmolNet.close(blocked_socket)
    assert {:error, {:closed, remainder}} = Task.await(blocked)
    assert byte_size(remainder) > 0
  end

  test "a timed-out synchronous receive cannot consume future readiness" do
    {_stack, peer, socket} = connected_socket()

    assert {:error, :timeout} = SmolNet.recv(socket, 1, 0)
    refute_receive {:"$smol_socket", _, _, _}, 20

    assert :ok = IPv6TcpPeer.send_data(peer, "x")
    assert {:ok, "x"} = SmolNet.recv(socket, 1, 1_000)
    refute_receive {:"$smol_socket", _, _, _}, 20
  end

  test "a synchronous waiter returns closed when its stack exits" do
    {stack, _peer, socket} = connected_socket()
    task = Task.async(fn -> SmolNet.recv(socket, 1, :infinity) end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.native.result.read_waiter_count == 1
    end)

    assert :ok = SmolNet.stop_stack(stack)
    assert {:error, :closed} = Task.await(task)
  end

  test "validation and synchronous connect errors are stable" do
    {:ok, stack} = SmolNet.start_stack(addresses: [{@client, 64}])
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    assert {:error, :invalid_data} = SmolNet.send(socket, {:not, :iodata}, :nowait)
    assert {:error, :invalid_length} = SmolNet.recv(socket, -1, :nowait)
    assert {:error, :invalid_timeout} = SmolNet.recv(socket, 1, -1)

    assert {:error, :invalid_timeout} =
             SmolNet.connect(socket, endpoint(@peer, 443), 4_294_967_296)

    assert {:error, :invalid_timeout} = SmolNet.send(socket, "data", 4_294_967_296)
    assert {:error, :invalid_timeout} = SmolNet.recv(socket, 1, 4_294_967_296)
    assert {:error, :invalid_how} = SmolNet.shutdown(socket, :sideways)
    assert {:error, :not_connected} = SmolNet.send(socket, "data", :nowait)
    assert {:error, :not_connected} = SmolNet.recv(socket, 1, :nowait)

    {connected_stack, _peer, connected} = connected_socket(connect_timeout: 1_000)
    assert Process.alive?(connected_stack.stack)
    assert {:ok, %{addr: @peer}} = SmolNet.peername(connected)
  end

  defp connected_socket(options \\ []) do
    connect_timeout = Keyword.get(options, :connect_timeout, :nowait)
    stack_options = Keyword.drop(options, [:connect_timeout])
    {:ok, peer} = IPv6TcpPeer.start_link(self(), :accept)

    {:ok, stack} =
      SmolNet.start_stack(
        Keyword.merge([egress: {peer, :tcp_client}, addresses: [{@client, 64}]], stack_options)
      )

    :ok = IPv6TcpPeer.attach(peer, stack)
    {:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)

    case connect_timeout do
      :nowait ->
        assert {:select, select_info} = SmolNet.connect(socket, endpoint(@peer, 443), :nowait)
        assert_select(socket, select_info)
        assert :ok = SmolNet.connect(socket, endpoint(@peer, 443), :nowait)

      timeout ->
        assert :ok = SmolNet.connect(socket, endpoint(@peer, 443), timeout)
    end

    {stack, peer, socket}
  end

  defp endpoint(address, port), do: %{family: :inet6, addr: address, port: port}

  defp assert_select(%Socket{id: id, generation: generation}, {:select_info, _op, reference}) do
    assert_receive {:"$smol_socket", {^id, ^generation}, :select, ^reference}, 1_000
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

  defp flag?(flags, flag), do: band(flags, flag) != 0

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
