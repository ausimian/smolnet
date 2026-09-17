defmodule SmolNet.SocketReadinessTest do
  use ExUnit.Case, async: false

  @moduletag :debug_nif

  alias SmolNet.Native
  alias SmolNet.Socket
  alias SmolNet.Stack
  alias SmolNet.Stack.Ref
  alias SmolNet.Test.Readiness

  @max_small_integer 576_460_752_303_423_487

  setup do
    on_exit(fn -> stop_all_stacks() end)
  end

  test "socket values validate stable identities and malformed select info crashes" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)
    reference = make_ref()
    select_info = {:select_info, :recv, reference}

    assert %Socket{stack: stack_pid, id: id, generation: generation} = socket
    assert is_pid(stack_pid)
    assert id in 1..@max_small_integer
    assert generation in 1..@max_small_integer
    assert Socket.valid?(socket)
    assert Socket.identity(socket) == {id, generation}
    assert select_info == {:select_info, :recv, reference}
    refute Socket.valid?(%{socket | generation: 0})

    assert_raise FunctionClauseError, fn ->
      SmolNet.cancel(socket, {:select_info, :unknown, reference})
    end
  end

  test "try-and-arm cannot lose readiness at any instrumented point" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)

    assert :ready =
             Readiness.wait(socket, :read, :recv,
               wake: :before_try,
               completed: true
             )

    refute_receive {:"$smol_socket", _, _, _}

    for arm_point <- [:before_try, :between_try_and_arm, :after_arm] do
      assert {:select, select_info} =
               Readiness.wait(socket, :read, :recv, wake: arm_point)

      assert_select(socket, select_info)
      refute_select(socket, select_info)
    end
  end

  test "spurious and repeated wakes produce one retry hint and permit re-arm" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)

    assert {:select, first} =
             Readiness.wait(socket, :read, :recv,
               wake: :after_arm,
               wake_count: 8
             )

    assert_select(socket, first)
    refute_select(socket, first)

    assert {:select, second} = Readiness.wait(socket, :read, :recv)
    assert {:select_info, :recv, first_reference} = first
    assert {:select_info, :recv, second_reference} = second
    refute first_reference == second_reference
    refute_select(socket, second)

    assert :ok = Readiness.ready(socket, :read)
    assert_select(socket, second)

    {:ok, info} = SmolNet.stack_info(stack)
    assert info.native.result.readiness.coalesced >= 7
    assert info.native.result.counters.notifications_delivered == 2
  end

  test "one read and one write waiter coexist while competitors receive busy" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)

    assert {:select, read_select} = Readiness.wait(socket, :read, :recv)
    assert {:error, :busy} = Readiness.wait(socket, :read, :accept)
    assert {:select, write_select} = Readiness.wait(socket, :write, :send)
    assert {:error, :busy} = Readiness.wait(socket, :write, :connect)

    assert :ok = Readiness.ready_many([{socket, :read}, {socket, :write}])
    assert_select(socket, read_select)
    assert_select(socket, write_select)

    {:ok, info} = SmolNet.stack_info(stack)
    assert info.native.result.waiter_count == 0
    assert info.native.result.read_waiter_count == 0
    assert info.native.result.write_waiter_count == 0
  end

  test "cancel-before-ready cannot notify the cancelled or a later operation" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)
    assert {:select, cancelled} = Readiness.wait(socket, :read, :recv)

    assert :ok = SmolNet.cancel(socket, cancelled)
    assert :ok = Readiness.ready(socket, :read)
    refute_select(socket, cancelled)

    assert {:select, later} = Readiness.wait(socket, :read, :recv)
    refute_select(socket, later)
    assert :ok = Readiness.ready(socket, :read)
    assert_select(socket, later)

    assert :not_found = SmolNet.cancel(socket, cancelled)
  end

  test "ready-before-cancel reports already_sent" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)

    assert {:select, select_info} =
             Readiness.wait(socket, :write, :send, wake: :after_arm)

    assert :already_sent = SmolNet.cancel(socket, select_info)
    assert_select(socket, select_info)
    refute_select(socket, select_info)
  end

  test "simultaneous cancel and ready has one stable race outcome" do
    {:ok, stack} = SmolNet.start_stack()

    for internal_handle <- 1..25 do
      socket = Readiness.open(stack, internal_handle)
      assert {:select, select_info} = Readiness.wait(socket, :read, :recv)

      ready_task = Task.async(fn -> Readiness.ready(socket, :read) end)
      cancel_task = Task.async(fn -> SmolNet.cancel(socket, select_info) end)

      assert Task.await(ready_task) == :ok

      case Task.await(cancel_task) do
        :ok ->
          refute_select(socket, select_info)

        :already_sent ->
          assert_select(socket, select_info)
          refute_select(socket, select_info)
      end

      assert {:select, later} = Readiness.wait(socket, :read, :recv)
      assert :ok = Readiness.ready(socket, :read)
      assert_select(socket, later)
      assert :ok = Readiness.close(socket)
    end
  end

  test "close aborts both directions once and drops late readiness" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)
    assert {:select, read_select} = Readiness.wait(socket, :read, :recv)
    assert {:select, write_select} = Readiness.wait(socket, :write, :send)

    assert :ok = Readiness.close(socket, wake: :read)
    assert_abort(socket, read_select, :closed)
    assert_abort(socket, write_select, :closed)
    refute_socket_message(socket)

    assert {:error, :invalid_socket} = Readiness.close(socket)
    assert {:error, :invalid_socket} = SmolNet.cancel(socket, read_select)
  end

  test "orderly stack shutdown aborts every pending waiter once" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)
    assert {:select, read_select} = Readiness.wait(socket, :read, :recv)
    assert {:select, write_select} = Readiness.wait(socket, :write, :send)

    assert :ok = SmolNet.stop_stack(stack)
    assert_abort(socket, read_select, :closed)
    assert_abort(socket, write_select, :closed)
    refute_socket_message(socket)
  end

  @tag :debug_nif
  test "stack termination drains retained aborts at maximum waiter capacity" do
    {:ok, stack} = SmolNet.start_stack()

    sockets_and_selects =
      Enum.map(1..div(Stack.default_limits().ready_events, 2), fn internal_handle ->
        socket = Readiness.open(stack, internal_handle)
        assert {:select, read_select} = Readiness.wait(socket, :read, :recv)
        assert {:select, write_select} = Readiness.wait(socket, :write, :send)
        {socket, [read_select, write_select]}
      end)

    %{stack: stack_pid} = Ref.pids(stack)
    %{native: native} = :sys.get_state(stack_pid)
    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(native, 1)

    assert :ok = GenServer.stop(stack_pid, :normal)

    Enum.each(sockets_and_selects, fn {socket, selects} ->
      Enum.each(selects, &assert_abort(socket, &1, :closed))
    end)
  end

  test "shutdown rejects every later socket operation" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)
    assert {:select, select_info} = Readiness.wait(socket, :read, :recv)

    assert :ok = Stack.shutdown_waiters(socket.stack)
    assert_abort(socket, select_info, :closed)

    assert {:error, :closed} = Readiness.wait(socket, :read, :recv)
    assert {:error, :closed} = Readiness.ready(socket, :read)
    assert {:error, :closed} = Readiness.close(socket)
    assert {:error, :closed} = SmolNet.cancel(socket, select_info)
    assert {:error, :closed} = Readiness.open(stack, 2)
    refute_socket_message(socket)
  end

  test "live socket entries are bounded and close releases capacity" do
    {:ok, stack} = SmolNet.start_stack(limits: %{ready_events: 2})
    first = Readiness.open(stack, 1)
    _second = Readiness.open(stack, 2)

    assert {:error, :system_limit} = Readiness.open(stack, 3)
    assert :ok = Readiness.close(first)
    replacement = Readiness.open(stack, 3)
    refute Socket.identity(first) == Socket.identity(replacement)

    {:ok, info} = SmolNet.stack_info(stack)
    assert info.native.result.socket_count == 2
    assert info.native.result.socket_count <= info.native.result.limits.ready_events
  end

  test "recycled internal handles cannot revive an old logical socket" do
    {:ok, stack} = SmolNet.start_stack()
    old_socket = Readiness.open(stack, 42)
    assert :ok = Readiness.close(old_socket)
    new_socket = Readiness.open(stack, 42)

    refute Socket.identity(old_socket) == Socket.identity(new_socket)
    assert {:error, :invalid_socket} = Readiness.ready(old_socket, :read)

    assert {:select, select_info} = Readiness.wait(new_socket, :read, :recv)
    assert :ok = Readiness.ready(new_socket, :read)
    assert_select(new_socket, select_info)
  end

  test "ready-queue overflow falls back to a bounded sweep without sleeping callers" do
    {:ok, stack} =
      SmolNet.start_stack(limits: %{ready_events: 4, maintenance_work: 1})

    first = Readiness.open(stack, 1)
    second = Readiness.open(stack, 2)

    waiters = [
      {first, :read, :recv},
      {first, :write, :send},
      {second, :read, :recv},
      {second, :write, :send}
    ]

    selects =
      Enum.map(waiters, fn {socket, direction, operation} ->
        {:select, select_info} = Readiness.wait(socket, direction, operation)
        {socket, direction, select_info}
      end)

    keys = Enum.map(selects, fn {socket, direction, _select_info} -> {socket, direction} end)
    assert :ok = Readiness.ready_many(keys)

    Enum.each(selects, fn {socket, _direction, select_info} ->
      assert_select(socket, select_info)
    end)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)

      info.native.result.waiter_count == 0 and
        info.native.result.readiness.overflows > 0 and
        info.native.result.counters.max_ready_events <= 4 and
        info.native.result.counters.max_maintenance_work <= 1
    end)
  end

  test "invalid direction and stale generation are rejected before waiter mutation" do
    {:ok, stack} = SmolNet.start_stack()
    socket = Readiness.open(stack)

    assert {:error, :invalid_operation} = Readiness.wait(socket, :read, :send)

    stale = %{socket | generation: socket.generation + 1}
    assert {:error, :invalid_socket} = Readiness.wait(stale, :read, :recv)

    {:ok, info} = SmolNet.stack_info(stack)
    assert info.native.result.waiter_count == 0
  end

  defp assert_select(socket, select_info) do
    identity = Socket.identity(socket)
    {:select_info, _operation, reference} = select_info
    assert_receive {:"$smol_socket", ^identity, :select, ^reference}
  end

  defp assert_abort(socket, select_info, reason) do
    identity = Socket.identity(socket)
    {:select_info, _operation, reference} = select_info
    assert_receive {:"$smol_socket", ^identity, :abort, ^reference, ^reason}
  end

  defp refute_select(socket, select_info) do
    identity = Socket.identity(socket)
    {:select_info, _operation, reference} = select_info
    refute_receive {:"$smol_socket", ^identity, :select, ^reference}
  end

  defp refute_socket_message(socket) do
    identity = Socket.identity(socket)
    refute_receive {:"$smol_socket", ^identity, _, _}
    refute_receive {:"$smol_socket", ^identity, _, _, _}
  end

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
