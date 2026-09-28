defmodule SmolNet.StackLossTest do
  # A gen_tcp or gen_udp adapter learns that its stack is gone in one of two
  # ways: the stack's DOWN, or the shutdown its supervisor sends once the
  # stack's death has stopped the bundle. The two come from different
  # processes, so either can arrive first. The stack-failure tests elsewhere
  # see whichever the scheduler delivers first, usually the DOWN. These force
  # the shutdown to arrive first, by having the adapter stop watching the
  # stack before it dies, and check that pending calls still see `:enetdown`.
  #
  # A stack that stops on its own, because its link died under
  # `link_down: :stop` or because it failed, wakes its waiters itself before
  # it exits. Those wakeups carry the loss, so pending calls see `:enetdown`
  # there too (#136).
  use ExUnit.Case, async: false

  alias SmolNet.Inet6.Tcp
  alias SmolNet.Inet6.Udp
  alias SmolNet.Stack.Ref
  alias SmolNet.Test.IPv6TcpPeer
  alias SmolNet.Test.Monitoring
  alias SmolNet.Test.Timing

  @client {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @peer {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  # Liveness budgets scale with the host. See `SmolNet.Test.Timing`.
  @wait_1s Timing.liveness(1_000)

  setup do
    on_exit(&stop_all_stacks/0)
  end

  describe "gen_tcp, when the supervisor's shutdown overtakes the stack's DOWN" do
    test "a pending passive recv returns enetdown" do
      {stack, _peer} = stack_and_peer()
      {:ok, socket = {:"$inet", Tcp, adapter}} = tcp_connect(stack)

      receiver = Task.async(fn -> :gen_tcp.recv(socket, 1, :infinity) end)
      assert_eventually(fn -> Tcp.info(socket).read_pending end)

      monitor = kill_stack_behind_shutdown(stack, adapter)
      assert {:error, :enetdown} = Task.await(receiver, @wait_1s)
      assert_shut_down(monitor, adapter)
    end

    test "a pending send returns enetdown" do
      {stack, peer} = stack_and_peer()
      {:ok, socket = {:"$inet", Tcp, adapter}} = tcp_connect(stack, sndbuf: 4_096)

      assert :ok = IPv6TcpPeer.hold_acks(peer, true)
      sender = Task.async(fn -> :gen_tcp.send(socket, :binary.copy("lost", 4_096)) end)
      assert_eventually(fn -> Tcp.info(socket).write_pending end)

      monitor = kill_stack_behind_shutdown(stack, adapter)
      assert {:error, {:enetdown, unsent}} = Task.await(sender, @wait_1s)
      assert is_binary(unsent)
      assert_shut_down(monitor, adapter)
    end

    test "a pending accept returns enetdown" do
      {stack, _peer} = stack_and_peer()
      {:ok, listener = {:"$inet", Tcp, adapter}} = :gen_tcp.listen(0, tcp_options(stack))

      acceptor = Task.async(fn -> :gen_tcp.accept(listener, :infinity) end)
      assert_eventually(fn -> Tcp.info(listener).accept_pending end)

      monitor = kill_stack_behind_shutdown(stack, adapter)
      assert {:error, :enetdown} = Task.await(acceptor, @wait_1s)
      assert_shut_down(monitor, adapter)
    end

    test "a pending connect returns enetdown" do
      {stack, _peer} = stack_and_peer(:ignore)

      connector =
        Task.async(fn -> :gen_tcp.connect(@peer, 443, tcp_options(stack), :infinity) end)

      assert_eventually(fn -> length(adapter_pids(stack)) == 1 end)
      [adapter] = adapter_pids(stack)

      assert_eventually(fn ->
        match?({:connecting, %{connect_from: {_pid, _tag}}}, :sys.get_state(adapter))
      end)

      monitor = kill_stack_behind_shutdown(stack, adapter)
      assert {:error, :enetdown} = Task.await(connector, @wait_1s)
      assert_shut_down(monitor, adapter)
    end

    test "an active owner gets tcp_error enetdown, then tcp_closed" do
      {stack, _peer} = stack_and_peer()
      {:ok, socket = {:"$inet", Tcp, adapter}} = tcp_connect(stack, active: true)

      monitor = kill_stack_behind_shutdown(stack, adapter)
      assert_receive {:tcp_error, ^socket, :enetdown}, @wait_1s
      assert_receive {:tcp_closed, ^socket}, @wait_1s
      assert_shut_down(monitor, adapter)
    end
  end

  describe "gen_udp, when the supervisor's shutdown overtakes the stack's DOWN" do
    test "a pending passive recv returns enetdown" do
      {stack, _peer} = stack_and_peer()
      {:ok, socket = {:"$inet", Udp, adapter}} = :gen_udp.open(0, udp_options(stack))

      receiver = Task.async(fn -> :gen_udp.recv(socket, 0, :infinity) end)
      assert_eventually(fn -> Udp.info(socket).read_pending end)

      monitor = kill_stack_behind_shutdown(stack, adapter)
      assert {:error, :enetdown} = Task.await(receiver, @wait_1s)
      assert_shut_down(monitor, adapter)
    end

    test "an active owner gets udp_error enetdown" do
      {stack, _peer} = stack_and_peer()

      {:ok, socket = {:"$inet", Udp, adapter}} =
        :gen_udp.open(0, udp_options(stack, active: true))

      monitor = kill_stack_behind_shutdown(stack, adapter)
      assert_receive {:udp_error, ^socket, :enetdown}, @wait_1s
      assert_shut_down(monitor, adapter)
    end
  end

  # An explicit stop, or any other supervisor shutdown while the stack still
  # runs, is not a lost stack, so it keeps closing pending calls as `:closed`.
  describe "a supervisor shutdown while the stack is still alive" do
    test "closes a pending gen_tcp recv" do
      {stack, _peer} = stack_and_peer()
      {:ok, socket = {:"$inet", Tcp, adapter}} = tcp_connect(stack)

      receiver = Task.async(fn -> :gen_tcp.recv(socket, 1, :infinity) end)
      assert_eventually(fn -> Tcp.info(socket).read_pending end)

      assert :ok = DynamicSupervisor.terminate_child(Ref.pids(stack).inet_backends, adapter)
      assert {:error, :closed} = Task.await(receiver, @wait_1s)
      assert Process.alive?(Ref.pids(stack).stack)
    end

    test "closes a pending gen_udp recv" do
      {stack, _peer} = stack_and_peer()
      {:ok, socket = {:"$inet", Udp, adapter}} = :gen_udp.open(0, udp_options(stack))

      receiver = Task.async(fn -> :gen_udp.recv(socket, 0, :infinity) end)
      assert_eventually(fn -> Udp.info(socket).read_pending end)

      assert :ok = DynamicSupervisor.terminate_child(Ref.pids(stack).inet_backends, adapter)
      assert {:error, :closed} = Task.await(receiver, @wait_1s)
      assert Process.alive?(Ref.pids(stack).stack)
    end

    test "stop_stack closes pending gen_tcp and gen_udp recvs" do
      {stack, _peer} = stack_and_peer()
      {:ok, tcp} = tcp_connect(stack)
      {:ok, udp} = :gen_udp.open(0, udp_options(stack))

      tcp_receiver = Task.async(fn -> :gen_tcp.recv(tcp, 1, :infinity) end)
      udp_receiver = Task.async(fn -> :gen_udp.recv(udp, 0, :infinity) end)
      assert_eventually(fn -> Tcp.info(tcp).read_pending and Udp.info(udp).read_pending end)

      assert :ok = SmolNet.stop_stack(stack)
      assert {:error, :closed} = Task.await(tcp_receiver, @wait_1s)
      assert {:error, :closed} = Task.await(udp_receiver, @wait_1s)
    end

    test "stop_stack ends an active gen_tcp owner's stream with a bare tcp_closed" do
      {stack, _peer} = stack_and_peer()
      {:ok, socket} = tcp_connect(stack, active: true)
      assert_eventually(fn -> Tcp.info(socket).read_pending end)

      assert :ok = SmolNet.stop_stack(stack)
      assert_receive {:tcp_closed, ^socket}, @wait_1s
      refute_received {:tcp_error, ^socket, _reason}
    end
  end

  describe "gen_tcp, when the link dies under link_down: :stop" do
    test "a pending passive recv returns enetdown" do
      {stack, peer} = stack_and_peer(:accept, link_down: :stop)
      {:ok, socket} = tcp_connect(stack)

      receiver = Task.async(fn -> :gen_tcp.recv(socket, 1, :infinity) end)
      assert_eventually(fn -> Tcp.info(socket).read_pending end)

      monitor = kill_link(stack, peer)
      assert {:error, :enetdown} = Task.await(receiver, @wait_1s)
      assert_link_down(monitor)
    end

    # The send had queued part of its data, so it reports the rest unsent.
    test "a pending send returns enetdown" do
      {stack, peer} = stack_and_peer(:accept, link_down: :stop)
      {:ok, socket} = tcp_connect(stack, sndbuf: 4_096)

      assert :ok = IPv6TcpPeer.hold_acks(peer, true)
      sender = Task.async(fn -> :gen_tcp.send(socket, :binary.copy("lost", 4_096)) end)
      assert_eventually(fn -> Tcp.info(socket).write_pending end)

      monitor = kill_link(stack, peer)
      assert {:error, {:enetdown, unsent}} = Task.await(sender, @wait_1s)
      assert is_binary(unsent)
      assert_link_down(monitor)
    end

    # The stack aborts the read and the write waiter one at a time; the
    # first must not stop the adapter before the send has its answer.
    test "an active owner's pending send returns enetdown too" do
      {stack, peer} = stack_and_peer(:accept, link_down: :stop)
      {:ok, socket} = tcp_connect(stack, active: true, sndbuf: 4_096)

      assert :ok = IPv6TcpPeer.hold_acks(peer, true)
      sender = Task.async(fn -> :gen_tcp.send(socket, :binary.copy("lost", 4_096)) end)
      assert_eventually(fn -> Tcp.info(socket).write_pending end)

      monitor = kill_link(stack, peer)
      assert {:error, {:enetdown, unsent}} = Task.await(sender, @wait_1s)
      assert is_binary(unsent)
      assert_receive {:tcp_error, ^socket, :enetdown}, @wait_1s
      assert_receive {:tcp_closed, ^socket}, @wait_1s
      assert_link_down(monitor)
    end

    test "a pending accept returns enetdown" do
      {stack, peer} = stack_and_peer(:accept, link_down: :stop)
      {:ok, listener} = :gen_tcp.listen(0, tcp_options(stack))

      acceptor = Task.async(fn -> :gen_tcp.accept(listener, :infinity) end)
      assert_eventually(fn -> Tcp.info(listener).accept_pending end)

      monitor = kill_link(stack, peer)
      assert {:error, :enetdown} = Task.await(acceptor, @wait_1s)
      assert_link_down(monitor)
    end

    test "a pending connect returns enetdown" do
      {stack, peer} = stack_and_peer(:ignore, link_down: :stop)

      connector =
        Task.async(fn -> :gen_tcp.connect(@peer, 443, tcp_options(stack), :infinity) end)

      assert_eventually(fn -> length(adapter_pids(stack)) == 1 end)
      [adapter] = adapter_pids(stack)

      assert_eventually(fn ->
        match?({:connecting, %{connect_select: {:select_info, _, _}}}, :sys.get_state(adapter))
      end)

      monitor = kill_link(stack, peer)
      assert {:error, :enetdown} = Task.await(connector, @wait_1s)
      assert_link_down(monitor)
    end

    test "an active owner gets tcp_error enetdown, then tcp_closed" do
      {stack, peer} = stack_and_peer(:accept, link_down: :stop)
      {:ok, socket} = tcp_connect(stack, active: true)
      assert_eventually(fn -> Tcp.info(socket).read_pending end)

      monitor = kill_link(stack, peer)
      assert_receive {:tcp_error, ^socket, :enetdown}, @wait_1s
      assert_receive {:tcp_closed, ^socket}, @wait_1s
      assert_link_down(monitor)
    end
  end

  describe "gen_udp, when the link dies under link_down: :stop" do
    test "a pending passive recv returns enetdown" do
      {stack, link} = blackhole_stack(link_down: :stop)
      {:ok, socket} = :gen_udp.open(0, udp_options(stack))

      receiver = Task.async(fn -> :gen_udp.recv(socket, 0, :infinity) end)
      assert_eventually(fn -> Udp.info(socket).read_pending end)

      monitor = kill_link(stack, link)
      assert {:error, :enetdown} = Task.await(receiver, @wait_1s)
      assert_link_down(monitor)
    end

    # Credit for one datagram leaves the rest in the transmit ring, so a send
    # blocks once the ring is full.
    test "a pending send returns enetdown" do
      {stack, link} = blackhole_stack(link_down: :stop, egress_credit: {1, 1_280})
      {:ok, socket} = :gen_udp.open(0, udp_options(stack))
      payload = :binary.copy(<<0>>, 1_000)

      sender = Task.async(fn -> send_until_error(socket, payload) end)
      assert_eventually(fn -> Udp.info(socket).write_pending end)

      monitor = kill_link(stack, link)
      assert {:error, :enetdown} = Task.await(sender, @wait_1s)
      assert_link_down(monitor)
    end

    test "an active owner gets udp_error enetdown" do
      {stack, link} = blackhole_stack(link_down: :stop)
      {:ok, socket} = :gen_udp.open(0, udp_options(stack, active: true))
      assert_eventually(fn -> Udp.info(socket).read_pending end)

      monitor = kill_link(stack, link)
      assert_receive {:udp_error, ^socket, :enetdown}, @wait_1s
      assert_link_down(monitor)
    end
  end

  # The other policies keep the stack running without its link, so nothing
  # pending fails until the stack is stopped, and stop_stack/1 closes it.
  describe "when the link dies under a policy that keeps the stack" do
    for policy <- [:mark_down, :notify] do
      test "#{policy}: pending calls wait, and stop_stack closes them" do
        link_down = if unquote(policy) == :notify, do: {:notify, self()}, else: :mark_down
        {stack, peer} = stack_and_peer(:accept, link_down: link_down)
        {:ok, tcp} = tcp_connect(stack)
        {:ok, udp} = :gen_udp.open(0, udp_options(stack))

        tcp_receiver = Task.async(fn -> :gen_tcp.recv(tcp, 1, :infinity) end)
        udp_receiver = Task.async(fn -> :gen_udp.recv(udp, 0, :infinity) end)
        assert_eventually(fn -> Tcp.info(tcp).read_pending and Udp.info(udp).read_pending end)

        monitor = kill_link(stack, peer)

        assert_eventually(fn ->
          match?({:ok, %{link_status: :down}}, SmolNet.stack_info(stack))
        end)

        refute_received {:DOWN, ^monitor, :process, _pid, _reason}
        assert Task.yield(tcp_receiver, 0) == nil
        assert Task.yield(udp_receiver, 0) == nil

        assert :ok = SmolNet.stop_stack(stack)
        assert {:error, :closed} = Task.await(tcp_receiver, @wait_1s)
        assert {:error, :closed} = Task.await(udp_receiver, @wait_1s)
      end
    end
  end

  describe "a stack that stops abnormally on its own" do
    # `:sys.terminate/2` runs the stack's terminate/2 with the given reason,
    # as a crash in one of its callbacks does.
    @tag capture_log: true
    test "wakes pending gen_tcp and gen_udp calls with enetdown" do
      {stack, _peer} = stack_and_peer()
      {:ok, tcp} = tcp_connect(stack)
      {:ok, udp} = :gen_udp.open(0, udp_options(stack))

      tcp_receiver = Task.async(fn -> :gen_tcp.recv(tcp, 1, :infinity) end)
      udp_receiver = Task.async(fn -> :gen_udp.recv(udp, 0, :infinity) end)
      assert_eventually(fn -> Tcp.info(tcp).read_pending and Udp.info(udp).read_pending end)

      stack_pid = Ref.pids(stack).stack
      monitor = Monitoring.monitor_in_place(stack_pid)
      assert :ok = :sys.terminate(stack_pid, :crashed)
      assert {:error, :enetdown} = Task.await(tcp_receiver, @wait_1s)
      assert {:error, :enetdown} = Task.await(udp_receiver, @wait_1s)
      assert_receive {:DOWN, ^monitor, :process, ^stack_pid, :crashed}, @wait_1s
    end
  end

  # Kills the stack after `adapter` has stopped watching it, so the adapter
  # learns of the loss only from its supervisor's shutdown, as it does when
  # that shutdown overtakes the stack's DOWN. Returns a monitor on `adapter`.
  #
  # The monitor is taken before the replace_state call, whose reply shows it
  # is in place. Taken after the call, just before the kill, it has been seen
  # to report `:noproc` on a loaded host (see #109).
  defp kill_stack_behind_shutdown(stack, adapter) do
    monitor = Process.monitor(adapter)

    _state =
      :sys.replace_state(adapter, fn {state_name, data} ->
        Process.demonitor(data.stack_monitor, [:flush])
        {state_name, %{data | stack_monitor: make_ref()}}
      end)

    Process.exit(Ref.pids(stack).stack, :kill)
    monitor
  end

  # An adapter that saw the stack's DOWN stops with `{:shutdown, :stack_down}`;
  # plain `:shutdown` shows that the supervisor's shutdown reached it instead.
  defp assert_shut_down(monitor, adapter) do
    assert_receive {:DOWN, ^monitor, :process, ^adapter, :shutdown}, @wait_1s
  end

  # Kills the stack's link, as a transport that fails would, and returns a
  # monitor on the stack, taken in place first (see #109).
  defp kill_link(stack, link) do
    monitor = Monitoring.monitor_in_place(Ref.pids(stack).stack)
    Process.unlink(link)
    Process.exit(link, :kill)
    monitor
  end

  defp assert_link_down(monitor) do
    assert_receive {:DOWN, ^monitor, :process, _stack, {:shutdown, {:link_down, :killed}}},
                   @wait_1s
  end

  # A link that discards everything the stack sends it.
  defp blackhole_stack(extra) do
    link = spawn(fn -> discard() end)
    options = [egress: {link, :blackhole}, addresses: [{@client, 64}]] ++ extra
    {:ok, stack} = SmolNet.start_stack(options)
    {stack, link}
  end

  defp discard do
    receive do
      _message -> discard()
    end
  end

  defp send_until_error(socket, payload) do
    case :gen_udp.send(socket, @peer, 9, payload) do
      :ok -> send_until_error(socket, payload)
      error -> error
    end
  end

  defp stack_and_peer(mode \\ :accept, extra \\ []) do
    {:ok, peer} = IPv6TcpPeer.start_link(self(), mode)
    options = [egress: {peer, :tcp_client}, addresses: [{@client, 64}]] ++ extra
    {:ok, stack} = SmolNet.start_stack(options)

    :ok = IPv6TcpPeer.attach(peer, stack)
    {stack, peer}
  end

  defp tcp_connect(stack, extra \\ []) do
    :gen_tcp.connect(@peer, 443, tcp_options(stack, extra), @wait_1s)
  end

  defp tcp_options(stack, extra \\ []) do
    [{:tcp_module, Tcp}, {:smolnet_stack, stack}, :inet6, :binary, {:active, false} | extra]
  end

  defp udp_options(stack, extra \\ []) do
    [{:udp_module, Udp}, {:smolnet_stack, stack}, :inet6, :binary, {:active, false} | extra]
  end

  defp adapter_pids(stack) do
    stack
    |> Ref.pids()
    |> Map.fetch!(:inet_backends)
    |> DynamicSupervisor.which_children()
    |> Enum.flat_map(fn
      {_id, pid, :worker, _modules} when is_pid(pid) -> [pid]
      _child -> []
    end)
  end

  defp assert_eventually(check, timeout \\ @wait_1s) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(check, deadline)
  end

  defp do_assert_eventually(check, deadline) do
    cond do
      check.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition did not become true")

      true ->
        Process.sleep(5)
        do_assert_eventually(check, deadline)
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
