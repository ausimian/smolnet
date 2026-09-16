defmodule SmolNet.NativeTest do
  use ExUnit.Case, async: false

  alias SmolNet.Native
  alias SmolNet.Stack
  alias SmolNet.Stack.Ref

  setup do
    on_exit(fn -> stop_all_stacks() end)
  end

  test "creates two independent empty native stacks with a common envelope" do
    {:ok, first} = SmolNet.start_stack()
    {:ok, second} = SmolNet.start_stack()

    first_snapshot = snapshot(first)
    second_snapshot = snapshot(second)

    assert first_snapshot.output == []
    assert first_snapshot.poll_at == nil
    assert first_snapshot.more == false
    assert first_snapshot.result.lifecycle == :running
    assert first_snapshot.result.socket_count == 0
    assert first_snapshot.result.native_socket_count == 0
    assert first_snapshot.result.ready_count == 0
    assert first_snapshot.result.id != second_snapshot.result.id
  end

  test "returns immediately when the defensive mutex is already held" do
    {:ok, ref} = SmolNet.start_stack()
    %{stack: stack} = Ref.pids(ref)

    task = Task.async(fn -> Stack.test_contention(stack) end)

    assert Task.yield(task, 1_000) ==
             {:ok, {:error, :ownership_invariant_violation}}
  end

  test "bounds every ABI work dimension and reports a continuation" do
    limits = %{
      bytes_copied: 1_500,
      output_packets: 2,
      ready_events: 3,
      maintenance_work: 4
    }

    {:ok, ref} = SmolNet.start_stack(limits: limits)
    %{stack: stack} = Ref.pids(ref)

    assert {:ok,
            %{
              result: ^limits,
              output: [],
              poll_at: nil,
              more: true
            }} =
             Stack.test_bounded_work(stack, %{
               bytes_copied: 10_000,
               output_packets: 100,
               ready_events: 100,
               maintenance_work: 100
             })

    assert {:ok, %{result: %{counters: counters}}} = Stack.native_snapshot(stack)
    assert counters.max_bytes_copied == 1_500
    assert counters.max_output_packets == 2
    assert counters.max_ready_events == 3
    assert counters.max_maintenance_work == 4
  end

  test "handles zero, large, expired, negative, and overflowing monotonic times" do
    assert Native.stack_time_until(0, 0) == {:ok, 0}
    assert Native.stack_time_until(10, 9) == {:ok, 0}
    assert Native.stack_time_until(-10, 10) == {:ok, 20}

    assert Native.stack_time_until(0, 9_223_372_036_854_775_807) ==
             {:ok, 9_223_372_036_854_775_807}

    assert Native.stack_time_until(
             -9_223_372_036_854_775_808,
             9_223_372_036_854_775_807
           ) == {:error, :time_overflow}

    valid_millis = div(9_223_372_036_854_775_807, 1_000)
    config = %{mtu: 1_500, addresses: [], routes: []}
    assert {:ok, _envelope} = Native.stack_new(Stack.default_limits(), config, valid_millis)

    assert Native.stack_new(Stack.default_limits(), config, valid_millis + 1) ==
             {:error, :time_overflow}
  end

  test "malformed native input is contained without destabilizing the VM" do
    assert Native.stack_new(%{}, %{mtu: 1_500, addresses: [], routes: []}, 0) ==
             {:error, :invalid_limits}

    assert Native.health() == :ok
  end

  test "native TCP endpoint decoding returns stable validation errors" do
    global = [0xFD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
    link_local = [0xFE, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
    config = %{mtu: 1_280, addresses: [%{address: global, prefix_length: 64}], routes: []}
    {:ok, %{result: resource}} = Native.stack_new(Stack.default_limits(), config, 0)
    {:ok, %{result: identity}} = Native.tcp_open(resource)
    huge_integer = 1_267_650_600_228_229_401_496_703_205_376

    assert Native.tcp_bind(resource, identity, %{}) == {:error, :invalid_address}

    assert Native.tcp_bind(resource, identity, %{address: global, scope_id: 0}) ==
             {:error, :invalid_port}

    assert Native.tcp_bind(resource, identity, %{address: global, port: 80}) ==
             {:error, :invalid_scope}

    assert Native.tcp_bind(resource, identity, %{
             address: Enum.take(global, 15),
             port: 80,
             scope_id: 0
           }) == {:error, :invalid_address}

    assert Native.tcp_bind(resource, identity, %{address: [256 | global], port: 80, scope_id: 0}) ==
             {:error, :invalid_address}

    assert Native.tcp_bind(resource, identity, %{address: global, port: :http, scope_id: 0}) ==
             {:error, :invalid_port}

    assert Native.tcp_bind(resource, identity, %{
             address: global,
             port: huge_integer,
             scope_id: 0
           }) == {:error, :invalid_port}

    assert Native.tcp_bind(resource, identity, %{
             address: global,
             port: 80,
             scope_id: huge_integer
           }) == {:error, :invalid_scope}

    assert Native.tcp_bind(resource, identity, %{address: global, port: 80, scope_id: 0.0}) ==
             {:error, :invalid_scope}

    assert Native.tcp_bind(resource, identity, %{address: link_local, port: 80, scope_id: 0}) ==
             {:error, :scope_required}

    assert Native.tcp_bind(resource, identity, %{address: link_local, port: 80, scope_id: -1}) ==
             {:error, :invalid_scope}

    assert Native.tcp_connect(
             resource,
             identity,
             %{address: :bad, port: 80, scope_id: 0},
             self(),
             make_ref(),
             0
           ) ==
             {:error, :invalid_address}

    assert Native.health() == :ok
  end

  test "native ingress defensively rejects invalid and non-IPv6 packets" do
    config = %{mtu: 1_280, addresses: [], routes: []}
    {:ok, %{result: resource}} = Native.stack_new(Stack.default_limits(), config, 0)

    assert Native.stack_ingress(resource, <<6::4, 0::308>>, 0) ==
             {:error, :invalid_packet}

    assert Native.stack_ingress(resource, <<4::4, 0::316>>, 0) ==
             {:error, :unsupported_family}

    assert Native.stack_ingress(resource, <<6::4, 0::28, 1::16, 59, 64, 0::256>>, 0) ==
             {:error, :invalid_packet}

    oversized = <<6::4, 0::28, 1_241::16, 59, 64, 0::256, 0::size(1_241 * 8)>>
    assert Native.stack_ingress(resource, oversized, 0) == {:error, :packet_too_large}

    assert {:ok, %{result: %{counters: counters}}} = Native.stack_snapshot(resource)
    assert counters.ingress_packets == 0
    assert counters.rejected_packets == 4
    assert Native.health() == :ok
  end

  test "native configuration rejects multicast interface addresses without panicking" do
    multicast = [0xFF, 2] ++ List.duplicate(0, 14)

    config = %{
      mtu: 1_280,
      addresses: [%{address: multicast, prefix_length: 64}],
      routes: []
    }

    assert Native.stack_new(Stack.default_limits(), config, 0) ==
             {:error, :invalid_stack_config}

    assert Native.health() == :ok
  end

  test "repeated create and destroy cycles release their native resources" do
    baseline = Native.resource_counts().active

    for _index <- 1..25 do
      {:ok, ref} = SmolNet.start_stack()
      assert :ok = SmolNet.stop_stack(ref)
    end

    assert_eventually(fn -> Native.resource_counts().active == baseline end)
    counts = Native.resource_counts()
    assert counts.created >= 25
    assert counts.dropped >= 25
  end

  defp snapshot(ref) do
    %{stack: stack} = Ref.pids(ref)
    {:ok, snapshot} = Stack.native_snapshot(stack)
    snapshot
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
