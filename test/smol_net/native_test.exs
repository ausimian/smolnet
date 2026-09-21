defmodule SmolNet.NativeTest do
  use ExUnit.Case, async: false

  @moduletag :debug_nif

  alias SmolNet.Native
  alias SmolNet.Stack
  alias SmolNet.Stack.Ref
  alias SmolNet.Test.Timing

  @wait_1s Timing.liveness(1_000)

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
    assert first_snapshot.result.call_target_nanoseconds == 1_000_000
    assert first_snapshot.result.work_budget_nanoseconds == 750_000
    assert first_snapshot.result.encoding_headroom_nanoseconds == 250_000
    assert first_snapshot.result.id != second_snapshot.result.id
  end

  test "snapshots do not perturb the native call metrics they report" do
    resource = native_stack()
    assert {:ok, _envelope} = Native.stack_poll(resource, 0)
    assert {:ok, %{result: first}} = Native.stack_snapshot(resource)
    assert {:ok, %{result: second}} = Native.stack_snapshot(resource)

    assert second.counters.native_calls == first.counters.native_calls
    assert second.counters.deadline_yields == first.counters.deadline_yields

    assert second.counters.max_native_work_nanoseconds ==
             first.counters.max_native_work_nanoseconds
  end

  @tag :debug_nif
  test "deadline yields retain zero-copy output for the next continuation" do
    resource = native_stack()
    assert {:ok, %{result: :ok}} = Native.test_prepare_maximum_drop(resource)
    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 0)

    assert {:ok, %{output: [], more: true}} = Native.stack_poll(resource, 0)
    assert {:ok, %{result: yielded}} = Native.stack_snapshot(resource)
    assert yielded.transmit_packets == yielded.limits.output_packets
    assert yielded.counters.deadline_yields >= 1

    assert {:ok, %{output: output, more: false}} = poll_until_complete(resource)
    assert length(output) == yielded.limits.output_packets
    assert Enum.all?(output, &(byte_size(&1) == yielded.mtu))

    assert {:ok, %{result: completed}} = Native.stack_snapshot(resource)
    assert completed.transmit_packets == 0
    assert completed.counters.max_output_packets == completed.limits.output_packets
  end

  @tag :debug_nif
  test "an expired checkpoint without retained work does not request a continuation" do
    resource = native_stack()
    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 0)
    assert {:ok, %{more: false}} = Native.test_socket_ready(resource, [])

    assert {:ok, %{result: snapshot}} = Native.stack_snapshot(resource)
    assert snapshot.counters.deadline_yields >= 1
  end

  @tag :debug_nif
  test "shutdown aborts queued readiness instead of delivering a select" do
    resource = native_stack()
    {:ok, %{result: identity}} = Native.test_socket_open(resource, 1)
    reference = make_ref()

    assert {:ok, %{result: {:select, :recv, ^reference}}} =
             Native.test_socket_wait(resource, identity, %{
               direction: :read,
               operation: :recv,
               pid: self(),
               reference: reference,
               arm_point: :none,
               wake_count: 1,
               completed: false
             })

    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 0)

    assert {:ok, %{more: true}} =
             Native.test_socket_ready(resource, [%{identity: identity, direction: :read}])

    assert {:ok, _shutdown} = Native.stack_shutdown(resource)
    assert {:ok, %{more: false}} = poll_until_complete(resource)
    %{id: id, generation: generation} = identity
    assert_receive {:"$smol_socket", {^id, ^generation}, :abort, ^reference, :closed}
    refute_receive {:"$smol_socket", {^id, ^generation}, :select, ^reference}
  end

  @tag :debug_nif
  test "listener scans and closing cleanup resume from retained cursors" do
    address = [0xFD | List.duplicate(0, 14)] ++ [1]

    resource =
      native_stack(%{mtu: 1_500, addresses: [%{address: address, prefix_length: 64}], routes: []})

    {:ok, %{result: listener}} = Native.tcp_open(resource, :inet6)

    assert {:ok, %{result: :ok}} =
             Native.tcp_bind(resource, listener, %{address: address, port: 40_000, scope_id: 0})

    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 0)
    assert {:ok, %{result: :ok, more: true}} = Native.tcp_listen(resource, listener, 4, 0)
    assert {:ok, %{more: false}} = poll_at_until_complete(resource, 0)

    closing = native_stack()
    assert {:ok, %{result: :ok}} = Native.test_prepare_closing(closing, 64)
    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(closing, 1)
    assert {:ok, %{more: true}} = Native.stack_poll(closing, 30_000)
    assert {:ok, %{result: retained}} = Native.stack_snapshot(closing)
    assert retained.closing_tcp_socket_count == 64

    assert {:ok, %{more: false}} = poll_at_until_complete(closing, 30_000)
    assert {:ok, %{result: completed}} = Native.stack_snapshot(closing)
    assert completed.closing_tcp_socket_count == 0
    assert completed.native_socket_count == 0
    assert completed.counters.deadline_yields >= 1
  end

  @tag :debug_nif
  test "readiness and shutdown make bounded progress across forced continuations" do
    resource = native_stack()

    identities_and_references =
      Enum.map(1..Stack.default_limits().ready_events, fn internal_handle ->
        {:ok, %{result: identity}} = Native.test_socket_open(resource, internal_handle)
        reference = make_ref()

        assert {:ok, %{result: {:select, :recv, ^reference}}} =
                 Native.test_socket_wait(resource, identity, %{
                   direction: :read,
                   operation: :recv,
                   pid: self(),
                   reference: reference,
                   arm_point: :none,
                   wake_count: 1,
                   completed: false
                 })

        {identity, reference}
      end)

    keys =
      Enum.map(identities_and_references, fn {identity, _reference} ->
        %{identity: identity, direction: :read}
      end)

    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 0)
    assert {:ok, %{more: true}} = Native.test_socket_ready(resource, keys)
    refute_receive {:"$smol_socket", _identity, :select, _reference}, 0

    assert {:ok, %{more: false}} = poll_until_complete(resource)

    Enum.each(identities_and_references, fn {identity, reference} ->
      %{id: id, generation: generation} = identity
      assert_receive {:"$smol_socket", {^id, ^generation}, :select, ^reference}
    end)

    shutdown_references =
      Enum.map(identities_and_references, fn {identity, _reference} ->
        reference = make_ref()

        assert {:ok, %{result: {:select, :send, ^reference}}} =
                 Native.test_socket_wait(resource, identity, %{
                   direction: :write,
                   operation: :send,
                   pid: self(),
                   reference: reference,
                   arm_point: :none,
                   wake_count: 0,
                   completed: false
                 })

        {identity, reference}
      end)

    expected_shutdown_messages =
      Enum.map(shutdown_references, fn {%{id: id, generation: generation}, reference} ->
        {:"$smol_socket", {id, generation}, :abort, reference, :closed}
      end)

    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 1)
    assert {:ok, %{more: true}} = Native.stack_shutdown(resource)
    refute_receive {:"$smol_socket", _identity, :abort, _reference, :closed}, 0

    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 1)
    assert {:ok, %{more: true}} = Native.stack_poll(resource, 0)

    assert_receive priority_abort =
                     {:"$smol_socket", _identity, :abort, _reference, :closed}

    assert priority_abort in expected_shutdown_messages

    assert {:ok, %{result: retained_shutdown}} = Native.stack_snapshot(resource)
    assert retained_shutdown.lifecycle == :shutting_down
    assert retained_shutdown.socket_count == Stack.default_limits().ready_events - 1

    assert {:ok, %{more: false}} = poll_until_complete(resource)

    Enum.each(List.delete(expected_shutdown_messages, priority_abort), fn message ->
      assert_receive ^message
    end)

    assert {:ok, %{result: shutdown}} = Native.stack_snapshot(resource)
    assert shutdown.lifecycle == :shutdown
    assert shutdown.socket_count == 0
    assert shutdown.native_socket_count == 0
    assert shutdown.waiter_count == 0
    assert shutdown.counters.deadline_yields >= 2

    packet = <<6::4, 0::28, 0::16, 59, 64, 0::256>>
    assert {:error, :closed} = Native.stack_ingress(resource, packet, 0)
  end

  @tag :debug_nif
  test "a caller whose reduction slice is spent receives a shorter native slice" do
    ready_events = Stack.default_limits().ready_events

    unconstrained = native_stack()
    keys = arm_read_waiters(unconstrained, ready_events)
    # Arming the waiters spends this process's slice. Use a fresh process so
    # the reference call cannot inherit a nearly exhausted reduction slice.
    assert {:ok, _envelope} =
             with_fresh_reduction_slice(fn ->
               Native.test_socket_ready(unconstrained, keys)
             end)

    delivered_unconstrained = await_select_messages()

    starved = native_stack()
    keys = arm_read_waiters(starved, ready_events)

    # The first incremental charge reports the caller's slice as spent, so the
    # call stops after the chunk that charge covers instead of continuing to
    # drain readiness.
    assert {:ok, %{result: :ok}} = Native.test_set_slice_exhaustion(starved, 0)

    assert {:ok, %{more: true}} =
             with_fresh_reduction_slice(fn ->
               Native.test_socket_ready(starved, keys)
             end)

    delivered_starved = await_select_messages()
    assert delivered_starved > 0
    assert delivered_starved < delivered_unconstrained

    assert {:ok, %{result: snapshot}} = Native.stack_snapshot(starved)
    assert snapshot.counters.timeslice_exhaustions >= 1
    assert snapshot.counters.deadline_yields >= 1
    assert snapshot.counters.max_native_work_nanoseconds > 0
    assert snapshot.counters.max_native_work_nanoseconds < snapshot.call_target_nanoseconds

    # Readiness retained by the shortened slice still completes across ordinary
    # continuations rather than being dropped.
    assert {:ok, %{more: false}} = poll_until_complete(starved)
    assert delivered_starved + await_select_messages() == ready_events
  end

  @tag :debug_nif
  test "returns immediately when the defensive mutex is already held" do
    {:ok, ref} = SmolNet.start_stack()
    %{stack: stack} = Ref.pids(ref)

    task = Task.async(fn -> Stack.test_contention(stack) end)

    assert Task.yield(task, 1_000) ==
             {:ok, {:error, :ownership_invariant_violation}}
  end

  @tag :debug_nif
  test "bounds every ABI work dimension and reports a continuation" do
    limits = %{
      bytes_copied: 1_500,
      input_packets: 1,
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
               input_packets: 100,
               output_packets: 100,
               ready_events: 100,
               maintenance_work: 100
             })

    assert {:ok, %{result: %{counters: counters}}} = Stack.native_snapshot(stack)
    assert counters.max_bytes_copied == 1_500
    assert counters.max_input_packets == 1
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
    {:ok, %{result: identity}} = Native.tcp_open(resource, :inet6)
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

  test "native ingress defensively rejects malformed IP packets" do
    config = %{mtu: 1_280, addresses: [], routes: []}
    {:ok, %{result: resource}} = Native.stack_new(Stack.default_limits(), config, 0)

    assert Native.stack_ingress(resource, <<6::4, 0::308>>, 0) ==
             {:error, :invalid_packet}

    assert Native.stack_ingress(resource, <<4::4, 0::316>>, 0) ==
             {:error, :invalid_packet}

    assert Native.stack_ingress(resource, <<6::4, 0::28, 1::16, 59, 64, 0::256>>, 0) ==
             {:error, :invalid_packet}

    oversized = <<6::4, 0::28, 1_241::16, 59, 64, 0::256, 0::size(1_241 * 8)>>
    assert Native.stack_ingress(resource, oversized, 0) == {:error, :packet_too_large}

    assert {:ok, %{result: %{counters: counters}}} = Native.stack_snapshot(resource)
    assert counters.ingress_packets == 0
    assert counters.rejected_packets == 4
    assert Native.health() == :ok
  end

  test "native batch admission is atomic and bounded across continuations" do
    limits = %{Stack.default_limits() | input_packets: 2}
    config = %{mtu: 1_280, addresses: [], routes: []}
    {:ok, %{result: resource}} = Native.stack_new(limits, config, 0)
    first = <<6::4, 1::28, 0::16, 59, 64, 0::256>>
    second = <<6::4, 2::28, 0::16, 59, 64, 0::256>>
    invalid = <<6::4, 0::308>>

    assert Native.stack_ingress_batch(resource, [first, :not_a_packet], 0) ==
             {:error, :invalid_packet}

    assert Native.stack_ingress_batch(resource, [first, invalid], 0) ==
             {:error, :invalid_packet}

    assert {:ok, %{result: rejected}} = Native.stack_snapshot(resource)
    assert rejected.receive_packets == 0
    assert rejected.counters.ingress_packets == 0

    assert {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, 1)

    assert {:ok, %{result: 2, output: [], more: true}} =
             Native.stack_ingress_batch(resource, [first, second], 0)

    assert {:ok, %{result: retained}} = Native.stack_snapshot(resource)
    assert retained.receive_packets == 1
    assert retained.counters.ingress_packets == 2
    assert retained.counters.max_input_packets == 1

    assert {:ok, %{output: [], more: false}} = poll_until_complete(resource)
    assert {:ok, %{result: completed}} = Native.stack_snapshot(resource)
    assert completed.receive_packets == 0
  end

  test "native configuration rejects multicast and broadcast addresses without panicking" do
    multicast = [0xFF, 2] ++ List.duplicate(0, 14)

    config = %{
      mtu: 1_280,
      addresses: [%{address: multicast, prefix_length: 64}],
      routes: []
    }

    assert Native.stack_new(Stack.default_limits(), config, 0) ==
             {:error, :invalid_stack_config}

    broadcast_config = %{
      mtu: 1_280,
      addresses: [%{address: [255, 255, 255, 255], prefix_length: 32}],
      routes: []
    }

    assert Native.stack_new(Stack.default_limits(), broadcast_config, 0) ==
             {:error, :invalid_stack_config}

    assert Native.health() == :ok
  end

  test "native collection decoders reject oversized input before allocating it" do
    limits = Stack.default_limits()
    address = List.duplicate(0, 17)
    too_many_addresses = List.duplicate(%{address: [127, 0, 0, 1], prefix_length: 8}, 9)

    too_many_routes =
      List.duplicate(
        %{destination: [0, 0, 0, 0], prefix_length: 0, gateway: [127, 0, 0, 1]},
        5
      )

    assert Native.stack_new(limits, %{mtu: 1_280, addresses: too_many_addresses, routes: []}, 0) ==
             {:error, :invalid_stack_config}

    assert Native.stack_new(limits, %{mtu: 1_280, addresses: [], routes: too_many_routes}, 0) ==
             {:error, :invalid_stack_config}

    assert Native.stack_new(
             limits,
             %{mtu: 1_280, addresses: [%{address: address, prefix_length: 64}], routes: []},
             0
           ) == {:error, :invalid_stack_config}

    {:ok, %{result: resource}} =
      Native.stack_new(limits, %{mtu: 1_280, addresses: [], routes: []}, 0)

    {:ok, %{result: identity}} = Native.tcp_open(resource, :inet6)

    assert Native.tcp_bind(resource, identity, %{address: address, port: 80, scope_id: 0}) ==
             {:error, :invalid_address}

    assert Native.health() == :ok
  end

  test "native backing sockets have a hard per-stack capacity" do
    config = %{mtu: 1_500, addresses: ipv6_addresses(8), routes: []}
    {:ok, %{result: resource}} = Native.stack_new(Stack.default_limits(), config, 0)

    for index <- 1..8 do
      {:ok, %{result: identity}} = Native.udp_open(resource, :inet6)

      assert {:ok, %{result: :ok}} =
               Native.udp_bind(resource, identity, %{
                 address: List.duplicate(0, 16),
                 port: 40_000 + index,
                 scope_id: 0
               })
    end

    assert {:ok, %{result: snapshot}} = Native.stack_snapshot(resource)
    assert snapshot.native_socket_capacity == 64
    assert snapshot.native_socket_count == snapshot.native_socket_capacity
    assert Native.udp_open(resource, :inet6) == {:error, :system_limit}
  end

  test "rejected wildcard expansion preserves the original UDP socket" do
    addresses = ipv6_addresses(8)
    config = %{mtu: 1_500, addresses: addresses, routes: []}
    {:ok, %{result: resource}} = Native.stack_new(Stack.default_limits(), config, 0)

    for index <- 1..7 do
      {:ok, %{result: identity}} = Native.udp_open(resource, :inet6)

      assert {:ok, %{result: :ok}} =
               Native.udp_bind(resource, identity, %{
                 address: List.duplicate(0, 16),
                 port: 41_000 + index,
                 scope_id: 0
               })
    end

    {:ok, %{result: candidate}} = Native.udp_open(resource, :inet6)
    {:ok, _spare} = Native.udp_open(resource, :inet6)

    assert Native.udp_bind(resource, candidate, %{
             address: List.duplicate(0, 16),
             port: 42_000,
             scope_id: 0
           }) == {:error, :system_limit}

    assert {:ok, %{result: snapshot}} = Native.stack_snapshot(resource)
    assert snapshot.native_socket_count == 58

    assert {:ok, %{result: :ok}} =
             Native.udp_bind(resource, candidate, %{
               address: hd(addresses).address,
               port: 42_000,
               scope_id: 0
             })

    assert {:ok, %{result: preserved}} = Native.stack_snapshot(resource)
    assert preserved.native_socket_count == 58
  end

  test "repeated create and destroy cycles release their native resources" do
    assert_eventually(fn -> Native.resource_counts().active == 0 end)
    baseline = Native.resource_counts().active

    for _index <- 1..25 do
      {:ok, ref} = SmolNet.start_stack()
      assert :ok = SmolNet.stop_stack(ref)
    end

    assert_eventually(fn ->
      :erlang.garbage_collect()
      Native.resource_counts().active == baseline
    end)

    counts = Native.resource_counts()
    assert counts.created >= 25
    assert counts.dropped >= 25
  end

  defp snapshot(ref) do
    %{stack: stack} = Ref.pids(ref)
    {:ok, snapshot} = Stack.native_snapshot(stack)
    snapshot
  end

  defp native_stack(config \\ %{mtu: 1_500, addresses: [], routes: []}) do
    {:ok, %{result: resource}} =
      Native.stack_new(Stack.default_limits(), config, 0)

    resource
  end

  defp arm_read_waiters(resource, count) do
    Enum.map(1..count, fn internal_handle ->
      {:ok, %{result: identity}} = Native.test_socket_open(resource, internal_handle)
      reference = make_ref()

      assert {:ok, %{result: {:select, :recv, ^reference}}} =
               Native.test_socket_wait(resource, identity, %{
                 direction: :read,
                 operation: :recv,
                 pid: self(),
                 reference: reference,
                 arm_point: :none,
                 wake_count: 1,
                 completed: false
               })

      %{identity: identity, direction: :read}
    end)
  end

  defp await_select_messages do
    assert_receive {:"$smol_socket", _identity, :select, _reference}, @wait_1s
    drain_select_messages(1)
  end

  defp with_fresh_reduction_slice(call) do
    call
    |> Task.async()
    |> Task.await(@wait_1s)
  end

  defp drain_select_messages(count) do
    receive do
      {:"$smol_socket", _identity, :select, _reference} -> drain_select_messages(count + 1)
    after
      0 -> count
    end
  end

  defp poll_until_complete(resource, calls \\ 0, output \\ [])

  defp poll_until_complete(_resource, calls, _output) when calls >= 512 do
    flunk("native continuation did not make bounded progress")
  end

  defp poll_until_complete(resource, calls, output) do
    assert {:ok, envelope} = Native.stack_poll(resource, calls)
    output = output ++ envelope.output

    if envelope.more do
      poll_until_complete(resource, calls + 1, output)
    else
      {:ok, %{envelope | output: output}}
    end
  end

  defp poll_at_until_complete(resource, now, calls \\ 0)

  defp poll_at_until_complete(_resource, _now, calls) when calls >= 512 do
    flunk("native maintenance continuation did not make bounded progress")
  end

  defp poll_at_until_complete(resource, now, calls) do
    assert {:ok, envelope} = Native.stack_poll(resource, now)

    if envelope.more do
      poll_at_until_complete(resource, now, calls + 1)
    else
      {:ok, envelope}
    end
  end

  defp ipv6_addresses(count) do
    Enum.map(1..count, fn suffix ->
      %{address: [0xFD | List.duplicate(0, 14)] ++ [suffix], prefix_length: 64}
    end)
  end

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <-
            DynamicSupervisor.which_children(SmolNet.Supervisor) do
        DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end

  defp assert_eventually(assertion, timeout \\ @wait_1s) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(assertion, deadline)
  end

  defp do_assert_eventually(assertion, deadline) do
    if assertion.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        assert assertion.()
      else
        Process.sleep(10)
        do_assert_eventually(assertion, deadline)
      end
    end
  end
end
