defmodule SmolNet.NifBudget do
  alias SmolNet.Native

  @maximum_limits %{
    bytes_copied: 65_575,
    input_packets: 32,
    output_packets: 32,
    ready_events: 128,
    maintenance_work: 128
  }
  @maximum_mtu 65_575
  @maximum_batch_packets 32
  @maximum_batch_packet_bytes 2_048
  @maximum_batch_bytes @maximum_batch_packets * @maximum_batch_packet_bytes
  @native_socket_capacity 64
  @maximum_addresses 8
  @maximum_wildcard_udp_sockets div(@native_socket_capacity, @maximum_addresses)
  @max_wall_nanoseconds 1_000_000
  # Message-heavy NIFs are charged for message delivery as well as the measured
  # native share reported through enif_consume_timeslice/2. Crossing one
  # 2,000-reduction slice is expected to yield the caller; this upper bound
  # guards against accidentally charging more than two complete slices.
  @max_reductions 4_000
  @wall_clock_mode (case System.get_env("SMOLNET_NIF_WALL_CLOCK_MODE") do
                      nil -> :enforce
                      "enforce" -> :enforce
                      "p99" -> :p99
                      "report" -> :report
                      value -> raise "invalid SMOLNET_NIF_WALL_CLOCK_MODE: #{inspect(value)}"
                    end)
  @scenario_iterations if(@wall_clock_mode == :enforce, do: 1, else: 100)

  def run do
    IO.puts("wall-clock budget mode: #{@wall_clock_mode}")

    empty = new_stack(SmolNet.Stack.default_limits(), empty_config())

    repeated("empty poll", 20_000, fn index ->
      {:ok, _envelope} = Native.stack_poll(empty, index)
    end)

    maximum = new_stack(@maximum_limits, empty_config(@maximum_mtu))

    repeated("maximum output encoding", 5, fn _index ->
      {:ok, _envelope} = Native.test_maximum_work(maximum)
    end)

    packet = maximum_ipv6_packet()

    repeated("maximum raw packet on a fresh stack", 100, fn index ->
      ingress = new_stack(@maximum_limits, maximum_ingress_config())
      {:ok, _envelope} = Native.stack_ingress(ingress, packet, index)
    end)

    batch = List.duplicate(maximum_batch_ipv6_packet(), @maximum_batch_packets)

    scenario(
      "maximum batch ingress",
      fn ->
        limits = %{@maximum_limits | bytes_copied: @maximum_batch_bytes}
        new_stack(limits, maximum_ingress_config(@maximum_batch_packet_bytes))
      end,
      fn resource ->
        {:ok, %{result: @maximum_batch_packets}} =
          result = Native.stack_ingress_batch(resource, batch, 0)

        result
      end,
      fn resource, {:ok, envelope} -> continue_native_work(resource, envelope, 0) end
    )

    scenario(
      "maximum closing-socket maintenance",
      fn ->
        resource = new_stack(@maximum_limits, empty_config())
        {:ok, _envelope} = Native.test_prepare_closing(resource, @native_socket_capacity)
        resource
      end,
      fn resource -> Native.stack_poll(resource, 30_000) end,
      fn resource, {:ok, envelope} -> continue_native_work(resource, envelope, 30_000) end
    )

    scenario(
      "maximum readiness delivery",
      fn ->
        resource = new_stack(@maximum_limits, empty_config())
        {keys, _references} = arm_waiters(resource)
        {resource, keys}
      end,
      fn {resource, keys} ->
        {:ok, %{more: true}} = result = Native.test_socket_ready(resource, keys)
        result
      end,
      fn {resource, _keys}, {:ok, envelope} ->
        continue_native_work(resource, envelope, 0)
        drain_messages(@maximum_limits.ready_events)
      end
    )

    scenario(
      "maximum readiness overflow sweep",
      fn ->
        resource = new_stack(@maximum_limits, empty_config())
        {keys, _references} = arm_waiters(resource)
        {:ok, %{more: true}} = Native.test_socket_ready(resource, keys)
        resource
      end,
      fn resource -> Native.stack_poll(resource, 0) end,
      fn resource, {:ok, envelope} ->
        continue_native_work(resource, envelope, 0)
        drain_messages(@maximum_limits.ready_events)
      end
    )

    scenario(
      "combined maximum work",
      fn ->
        resource = new_stack(@maximum_limits, empty_config())
        {:ok, _envelope} = Native.test_prepare_closing(resource, @native_socket_capacity)
        {keys, _references} = arm_waiters(resource, @native_socket_capacity, [:read, :write])
        {resource, keys}
      end,
      fn {resource, keys} -> Native.test_combined_maximum_work(resource, keys, 30_000) end,
      fn {resource, _keys}, {:ok, envelope} ->
        continue_native_work(resource, envelope, 30_000)
        drain_messages(@maximum_limits.ready_events)
      end
    )

    scenario(
      "maximum waiter shutdown",
      fn ->
        resource = new_stack(@maximum_limits, empty_config())
        {_keys, _references} = arm_waiters(resource)
        resource
      end,
      &Native.stack_shutdown/1,
      fn resource, {:ok, envelope} ->
        continue_native_work(resource, envelope, 0)
        drain_messages(@maximum_limits.ready_events)
      end
    )

    scenario(
      "maximum wildcard-UDP bind",
      fn ->
        resource = new_stack(@maximum_limits, maximum_udp_config(@maximum_addresses))
        {:ok, %{result: identity}} = Native.udp_open(resource, :inet6)
        {resource, identity}
      end,
      fn {resource, identity} ->
        {:ok, %{result: :ok}} =
          result =
          Native.udp_bind(resource, identity, %{
            address: List.duplicate(0, 16),
            port: 39_999,
            scope_id: 0
          })

        result
      end,
      fn {resource, _identity}, {:ok, envelope} ->
        continue_native_work(resource, envelope, 0)
      end
    )

    scenario(
      "maximum TCP listener expansion",
      fn ->
        resource = new_stack(@maximum_limits, maximum_udp_config(@maximum_addresses))
        {:ok, %{result: identity}} = Native.tcp_open(resource, :inet6)

        {:ok, %{result: :ok}} =
          Native.tcp_bind(resource, identity, %{
            address: maximum_ipv6_address(1),
            port: 39_998,
            scope_id: 0
          })

        {resource, identity}
      end,
      fn {resource, identity} ->
        {:ok, %{result: :ok}} = result = Native.tcp_listen(resource, identity, 128, 0)
        result
      end,
      fn {resource, _identity}, {:ok, envelope} ->
        continue_native_work(resource, envelope, 0)
      end
    )

    scenario(
      "maximum wildcard-UDP shutdown",
      fn ->
        {resource, identities} = maximum_wildcard_udp_state()
        _references = populate_maximum_waiter_state(resource, identities)
        resource
      end,
      &Native.stack_shutdown/1,
      fn resource, {:ok, envelope} ->
        continue_native_work(resource, envelope, 0)
        drain_messages(@maximum_limits.ready_events)
      end
    )

    scenario(
      "maximum native-allocation shutdown",
      fn ->
        {resource, identities} = maximum_single_address_udp_state()
        _references = populate_maximum_waiter_state(resource, identities)
        resource
      end,
      &Native.stack_shutdown/1,
      fn resource, {:ok, envelope} ->
        continue_native_work(resource, envelope, 0)
        drain_messages(@maximum_limits.ready_events)
      end
    )

    scenario(
      "maximum wildcard-UDP resource destructor",
      fn -> prepare_resource_drop(&maximum_wildcard_udp_state/0) end,
      fn drop -> drop.() end
    )

    scenario(
      "maximum native-allocation resource destructor",
      fn -> prepare_resource_drop(&maximum_single_address_udp_state/0) end,
      fn drop -> drop.() end
    )
  end

  defp repeated(label, iterations, operation) do
    Enum.each(1..min(iterations, 1_000), operation)
    {samples, reductions} = samples(iterations, operation)
    sorted = Enum.sort(samples)
    maximum = List.last(sorted)
    p99 = Enum.at(sorted, div(iterations * 99 + 99, 100) - 1)
    mean = div(Enum.sum(samples), iterations)

    IO.puts("#{label} budget evidence")
    IO.puts("  calls: #{iterations}")
    IO.puts("  mean wall time: #{mean} ns")
    IO.puts("  p99 wall time: #{p99} ns")
    IO.puts("  maximum wall time: #{maximum} ns (budget #{@max_wall_nanoseconds} ns)")
    IO.puts("  caller reductions/call: #{Float.round(reductions, 2)} (budget #{@max_reductions})")

    enforce!(label, maximum, reductions, p99)
  end

  defp scenario(label, prepare, operation, cleanup \\ fn _state, _result -> :ok end) do
    measurements =
      Enum.map(1..@scenario_iterations, fn _index ->
        state = prepare.()
        :erlang.garbage_collect()
        {:reductions, reductions_before} = Process.info(self(), :reductions)
        started_at = System.monotonic_time(:nanosecond)
        result = operation.(state)
        elapsed = System.monotonic_time(:nanosecond) - started_at
        {:reductions, reductions_after} = Process.info(self(), :reductions)
        cleanup.(state, result)
        assert_no_socket_messages!()
        {elapsed, reductions_after - reductions_before}
      end)

    {samples, reductions} = Enum.unzip(measurements)
    sorted = Enum.sort(samples)
    maximum = List.last(sorted)
    p99 = Enum.at(sorted, div(@scenario_iterations * 99 + 99, 100) - 1)
    mean = div(Enum.sum(samples), @scenario_iterations)
    reductions = Enum.sum(reductions) / @scenario_iterations

    IO.puts("#{label} budget evidence")
    IO.puts("  calls: #{@scenario_iterations}")
    IO.puts("  mean wall time: #{mean} ns")
    IO.puts("  p99 wall time: #{p99} ns")
    IO.puts("  maximum wall time: #{maximum} ns (budget #{@max_wall_nanoseconds} ns)")
    IO.puts("  caller reductions/call: #{Float.round(reductions, 2)} (budget #{@max_reductions})")

    enforce!(label, maximum, reductions, p99)
  end

  defp samples(iterations, operation) do
    {:reductions, reductions_before} = Process.info(self(), :reductions)

    samples =
      Enum.map(1..iterations, fn index ->
        started_at = System.monotonic_time(:nanosecond)
        operation.(index)
        System.monotonic_time(:nanosecond) - started_at
      end)

    {:reductions, reductions_after} = Process.info(self(), :reductions)
    {samples, (reductions_after - reductions_before) / iterations}
  end

  defp enforce!(label, maximum, reductions, p99) do
    if reductions > @max_reductions do
      raise "#{label} exceeded the normal-scheduler reduction budget"
    end

    if maximum > @max_wall_nanoseconds do
      enforce_wall_clock!(label, p99)
    end
  end

  defp enforce_wall_clock!(label, p99) do
    case @wall_clock_mode do
      :enforce ->
        raise "#{label} exceeded the normal-scheduler wall-clock budget"

      :p99 when is_integer(p99) and p99 > @max_wall_nanoseconds ->
        raise "#{label} exceeded the normal-scheduler p99 wall-clock budget"

      :p99 ->
        IO.puts("  maximum overrun recorded with a passing p99")

      :report when is_integer(p99) and p99 > @max_wall_nanoseconds ->
        IO.puts("  p99 overrun recorded without failing in report-only mode")

      :report ->
        IO.puts("  maximum overrun recorded with a passing p99")
    end
  end

  defp new_stack(limits, config) do
    {:ok, %{result: resource}} = Native.stack_new(limits, config, 0)
    resource
  end

  defp empty_config(mtu \\ 1_500), do: %{mtu: mtu, addresses: [], routes: []}

  defp maximum_ingress_config(mtu \\ @maximum_mtu) do
    %{mtu: mtu, addresses: [%{address: destination(), prefix_length: 64}], routes: []}
  end

  defp maximum_ipv6_packet do
    source = <<0xFD, 0::112, 1>>
    destination = destination() |> :erlang.list_to_binary()
    payload = :binary.copy(<<0>>, 65_535)
    <<6::4, 0::28, 65_535::16, 59, 64, source::binary, destination::binary, payload::binary>>
  end

  defp maximum_batch_ipv6_packet do
    source = <<0xFD, 0::112, 1>>
    destination = destination() |> :erlang.list_to_binary()
    payload_bytes = @maximum_batch_packet_bytes - 40
    payload = :binary.copy(<<0>>, payload_bytes)

    <<6::4, 0::28, payload_bytes::16, 59, 64, source::binary, destination::binary,
      payload::binary>>
  end

  defp destination, do: [0xFD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]

  defp arm_waiters(
         resource,
         socket_count \\ @maximum_limits.ready_events,
         directions \\ [:read]
       ) do
    Enum.reduce(1..socket_count, {[], []}, fn internal_handle, {keys, references} ->
      {:ok, %{result: identity}} = Native.test_socket_open(resource, internal_handle)

      Enum.reduce(directions, {keys, references}, fn direction, {keys, references} ->
        reference = make_ref()

        {:ok, _envelope} =
          Native.test_socket_wait(resource, identity, %{
            direction: direction,
            operation: waiter_operation(direction),
            pid: self(),
            reference: reference,
            arm_point: :none,
            wake_count: 1,
            completed: false
          })

        {[%{identity: identity, direction: direction} | keys], [reference | references]}
      end)
    end)
  end

  defp waiter_operation(:read), do: :recv
  defp waiter_operation(:write), do: :send

  defp drain_messages(count) do
    Enum.each(1..count, fn _index ->
      receive do
        {:"$smol_socket", _identity, kind, _reference} when kind in [:select, :abort] -> :ok
        {:"$smol_socket", _identity, :abort, _reference, :closed} -> :ok
      after
        5_000 -> raise "timed out draining bounded readiness messages"
      end
    end)
  end

  defp assert_no_socket_messages! do
    receive do
      message
      when is_tuple(message) and tuple_size(message) > 0 and
             elem(message, 0) == :"$smol_socket" ->
        raise "unexpected surplus socket message after benchmark cleanup: #{inspect(message)}"
    after
      0 -> :ok
    end
  end

  defp maximum_wildcard_udp_state do
    resource = new_stack(@maximum_limits, maximum_udp_config(@maximum_addresses))

    identities =
      Enum.map(1..@maximum_wildcard_udp_sockets, fn index ->
        {:ok, %{result: identity}} = Native.udp_open(resource, :inet6)

        {:ok, %{result: :ok}} =
          Native.udp_bind(resource, identity, %{
            address: List.duplicate(0, 16),
            port: 40_000 + index,
            scope_id: 0
          })

        identity
      end)

    assert_native_capacity!(resource)
    {resource, identities}
  end

  defp maximum_single_address_udp_state do
    resource = new_stack(@maximum_limits, maximum_udp_config(1))

    identities =
      Enum.map(1..@native_socket_capacity, fn _index ->
        {:ok, %{result: identity}} = Native.udp_open(resource, :inet6)
        identity
      end)

    assert_native_capacity!(resource)
    {resource, identities}
  end

  defp populate_maximum_waiter_state(resource, native_identities) do
    synthetic_identities =
      Enum.map(
        1..(@maximum_limits.ready_events - length(native_identities)),
        fn internal_handle ->
          {:ok, %{result: identity}} = Native.test_socket_open(resource, internal_handle)
          identity
        end
      )

    identities = native_identities ++ synthetic_identities
    {ready_keys, sent_references} = arm_identity_waiters(resource, identities, :read)
    {:ok, envelope} = Native.test_socket_ready(resource, Enum.reverse(ready_keys))
    continue_native_work(resource, envelope, 0)
    drain_messages(@maximum_limits.ready_events)
    {_active_keys, active_references} = arm_identity_waiters(resource, identities, :write)
    {:ok, %{result: snapshot}} = Native.stack_snapshot(resource)
    expected_waiters = @maximum_limits.ready_events
    ^expected_waiters = snapshot.waiter_count
    ^expected_waiters = snapshot.write_waiter_count
    ^expected_waiters = snapshot.sent_waiter_count
    ^expected_waiters = snapshot.read_sent_waiter_count
    sent_references ++ active_references
  end

  defp arm_identity_waiters(resource, identities, direction) do
    Enum.map_reduce(identities, [], fn identity, references ->
      reference = make_ref()

      {:ok, _envelope} =
        Native.test_socket_wait(resource, identity, %{
          direction: direction,
          operation: waiter_operation(direction),
          pid: self(),
          reference: reference,
          arm_point: :none,
          wake_count: 1,
          completed: false
        })

      {%{identity: identity, direction: direction}, [reference | references]}
    end)
  end

  defp maximum_udp_config(address_count) do
    addresses =
      Enum.map(1..address_count, fn suffix ->
        %{address: maximum_ipv6_address(suffix), prefix_length: 64}
      end)

    %{mtu: @maximum_mtu, addresses: addresses, routes: []}
  end

  defp maximum_ipv6_address(suffix), do: [0xFD | List.duplicate(0, 14)] ++ [suffix]

  defp assert_native_capacity!(resource) do
    {:ok, %{result: snapshot}} = Native.stack_snapshot(resource)
    @native_socket_capacity = snapshot.native_socket_count
    @native_socket_capacity = snapshot.native_socket_capacity
  end

  defp prepare_resource_drop(state_factory) do
    baseline = Native.resource_counts().dropped
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {resource, identities} = state_factory.()
        references = populate_maximum_waiter_state(resource, identities)
        {:ok, _envelope} = Native.test_prepare_maximum_drop(resource)
        {:ok, %{result: snapshot}} = Native.stack_snapshot(resource)
        expected_packets = @maximum_limits.output_packets
        ^expected_packets = snapshot.transmit_packets

        send(parent, {:resource_ready, self(), references})

        receive do
          :drop -> :ok
        end
      end)

    receive do
      {:resource_ready, ^pid, _references} -> :ok
    end

    fn ->
      send(pid, :drop)

      receive do
        {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
      end

      wait_for_drop(baseline, 10_000)
    end
  end

  defp wait_for_drop(baseline, attempts) do
    cond do
      Native.resource_counts().dropped > baseline -> :ok
      attempts == 0 -> raise "maximum resource destructor did not complete"
      true -> wait_for_drop(baseline, attempts - 1)
    end
  end

  defp continue_native_work(_resource, %{more: false}, _now), do: :ok

  defp continue_native_work(resource, %{more: true}, now) do
    continue_native_polls(resource, now, 0)
  end

  defp continue_native_polls(_resource, _now, 512) do
    raise "native continuation did not make bounded progress"
  end

  defp continue_native_polls(resource, now, calls) do
    {:ok, envelope} = Native.stack_poll(resource, now)

    if envelope.more do
      continue_native_polls(resource, now, calls + 1)
    else
      :ok
    end
  end
end

SmolNet.NifBudget.run()
