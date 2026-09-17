defmodule SmolNet.StackSupervisorTest do
  use ExUnit.Case, async: false

  alias SmolNet.Stack
  alias SmolNet.Stack.Ref
  alias SmolNet.StackSupervisor
  alias SmolNet.Test.FixtureAdapter
  alias SmolNet.Test.NativeDouble

  setup do
    previous_native = Application.get_env(:smolnet, :native_module)
    previous_process = Application.get_env(:smolnet, :native_test_process)
    previous_result = Application.get_env(:smolnet, :native_test_result)
    previous_shutdown_result = Application.get_env(:smolnet, :native_shutdown_result)

    on_exit(fn ->
      restore_env(:native_module, previous_native)
      restore_env(:native_test_process, previous_process)
      restore_env(:native_test_result, previous_result)
      restore_env(:native_shutdown_result, previous_shutdown_result)
      stop_all_stacks()
    end)
  end

  test "resolves one opaque reference after the stack readiness acknowledgement" do
    assert {:ok, %Ref{} = ref} = SmolNet.start_stack()
    pids = Ref.pids(ref)

    assert Enum.all?(pids, fn {_name, pid} -> Process.alive?(pid) end)
    assert inspect(ref) == "#SmolNet.Stack.Ref<:running>"

    assert Supervisor.which_children(pids.bundle) == [
             {:inet_backends, pids.inet_backends, :supervisor, [DynamicSupervisor]},
             {Stack, pids.stack, :worker, [Stack]}
           ]
  end

  for reason <- [:normal, :shutdown, :abnormal] do
    test "stack termination with #{inspect(reason)} tears down the complete bundle" do
      {:ok, ref} = SmolNet.start_stack()
      pids = Ref.pids(ref)
      monitors = monitor_all(pids)

      GenServer.stop(pids.stack, unquote(reason))

      assert_all_down(monitors)
      assert DynamicSupervisor.which_children(SmolNet.Supervisor) == []
    end
  end

  test "unexpected inet supervisor termination tears down stack and bundle" do
    {:ok, ref} = SmolNet.start_stack()
    pids = Ref.pids(ref)
    monitors = monitor_all(pids)

    Process.exit(pids.inet_backends, :kill)

    assert_all_down(monitors)
  end

  test "an individual temporary adapter is neither restarted nor tears down siblings" do
    {:ok, ref} = SmolNet.start_stack()
    pids = Ref.pids(ref)

    {:ok, first} = start_adapter(ref, pids.stack)
    assert_receive {:adapter_initialized, ^first}
    {:ok, second} = start_adapter(ref, pids.stack)
    assert_receive {:adapter_initialized, ^second}

    first_monitor = Process.monitor(first)
    GenServer.stop(first, :normal)

    assert_receive {:DOWN, ^first_monitor, :process, ^first, :normal}
    assert Process.alive?(second)
    assert Process.alive?(pids.stack)
    assert Process.alive?(pids.bundle)

    assert [{:undefined, ^second, :worker, [FixtureAdapter]}] =
             DynamicSupervisor.which_children(pids.inet_backends)
  end

  test "manual bundle shutdown terminates adapters before the live stack" do
    {:ok, ref} = SmolNet.start_stack()
    pids = Ref.pids(ref)
    {:ok, adapter} = start_adapter(ref, pids.stack)
    assert_receive {:adapter_initialized, ^adapter}
    stack_monitor = Process.monitor(pids.stack)

    assert :ok = SmolNet.stop_stack(ref)

    assert_receive {:adapter_terminated, ^adapter, :shutdown, true}
    assert_receive {:DOWN, ^stack_monitor, :process, _, :shutdown}
  end

  test "shutdown errors still terminate the complete bundle" do
    configure_native_double({:ok, empty_envelope(make_ref())})
    {:ok, ref} = SmolNet.start_stack()
    pids = Ref.pids(ref)
    monitors = monitor_all(pids)
    shutdown_calls = :atomics.new(1, [])

    Application.put_env(
      :smolnet,
      :native_shutdown_result,
      {:counted, shutdown_calls, {:error, :native_panic}}
    )

    assert :ok = SmolNet.stop_stack(ref)
    assert_all_down(monitors)
    assert :atomics.get(shutdown_calls, 1) == 1
  end

  test "termination retries one transient shutdown lock collision" do
    configure_native_double({:ok, empty_envelope(make_ref())})
    {:ok, ref} = SmolNet.start_stack()
    pids = Ref.pids(ref)
    monitors = monitor_all(pids)
    shutdown_calls = :atomics.new(1, [])

    Application.put_env(
      :smolnet,
      :native_shutdown_result,
      {:fail_once, shutdown_calls, {:error, :ownership_invariant_violation},
       {:ok, empty_envelope(:ok)}}
    )

    assert :ok = SmolNet.stop_stack(ref)
    assert_all_down(monitors)
    assert :atomics.get(shutdown_calls, 1) == 2
  end

  test "forced stack loss removes every adapter" do
    {:ok, ref} = SmolNet.start_stack()
    pids = Ref.pids(ref)
    {:ok, adapter} = start_adapter(ref, pids.stack)
    assert_receive {:adapter_initialized, ^adapter}
    adapter_monitor = Process.monitor(adapter)
    bundle_monitor = Process.monitor(pids.bundle)

    Process.exit(pids.stack, :kill)

    assert_receive {:DOWN, ^adapter_monitor, :process, ^adapter, :shutdown}
    assert_receive {:DOWN, ^bundle_monitor, :process, _, :shutdown}
  end

  test "all init callbacks are cheap and native allocation starts in handle_continue" do
    configure_native_double({:error, :not_called_during_init})
    ready_ref = make_ref()
    options = [starter: self(), ready_ref: ready_ref, limits: Stack.default_limits()]

    assert {:ok, state, {:continue, :create_native_stack}} = Stack.init(options)
    refute_received {:native_stack_new, _pid}

    assert {:ok, {_flags, children}} = StackSupervisor.init(options)
    assert length(children) == 2
    refute_received {:native_stack_new, _pid}

    assert {:noreply, _state} =
             Stack.handle_continue(:create_native_stack, state)

    assert_receive {:native_stack_new, _pid}
    assert_receive {:smolnet_stack_error, ^ready_ref, :not_called_during_init}
  end

  test "failed handle_continue initialization leaves no child behind" do
    configure_native_double({:error, :allocation_failed})

    assert SmolNet.start_stack() == {:error, :allocation_failed}
    assert_eventually(fn -> DynamicSupervisor.which_children(SmolNet.Supervisor) == [] end)
  end

  test "caller death during initialization cannot orphan a bundle" do
    configure_native_double(:wait)
    parent = self()

    caller =
      spawn(fn ->
        send(parent, {:caller_started, self()})
        SmolNet.start_stack()
      end)

    assert_receive {:caller_started, ^caller}
    assert_receive {:native_stack_new, stack}
    caller_monitor = Process.monitor(caller)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^caller_monitor, :process, ^caller, :killed}

    send(stack, {:native_test_reply, {:ok, empty_envelope(make_ref())}})

    assert_eventually(fn -> DynamicSupervisor.which_children(SmolNet.Supervisor) == [] end)
  end

  defp configure_native_double(result) do
    Application.put_env(:smolnet, :native_module, NativeDouble)
    Application.put_env(:smolnet, :native_test_process, self())
    Application.put_env(:smolnet, :native_test_result, result)
  end

  defp empty_envelope(resource) do
    %{result: resource, output: [], poll_at: nil, more: false}
  end

  defp start_adapter(ref, stack) do
    StackSupervisor.start_inet_backend(ref, {FixtureAdapter, test: self(), stack: stack})
  end

  defp monitor_all(pids) do
    Map.new(pids, fn {name, pid} -> {name, {pid, Process.monitor(pid)}} end)
  end

  defp assert_all_down(monitors) do
    for {_name, {pid, monitor}} <- monitors do
      assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}
    end
  end

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <-
            DynamicSupervisor.which_children(SmolNet.Supervisor) do
        DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:smolnet, key)
  defp restore_env(key, value), do: Application.put_env(:smolnet, key, value)

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
