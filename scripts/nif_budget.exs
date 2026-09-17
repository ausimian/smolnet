defmodule SmolNet.NifBudget do
  alias SmolNet.Native

  @maximum_limits %{
    bytes_copied: 65_575,
    output_packets: 32,
    ready_events: 128,
    maintenance_work: 128
  }
  @maximum_mtu 65_575
  @native_socket_capacity 64
  @maximum_addresses 8
  @maximum_wildcard_udp_sockets div(@native_socket_capacity, @maximum_addresses)
  @max_wall_nanoseconds 1_000_000
  # Message-heavy NIFs are charged a larger caller cost by the BEAM even when
  # their measured wall time is sub-millisecond. Keep the ceiling below the
  # default 2,000-reduction process time slice.
  @max_reductions 1_200
  @wall_clock_mode (case System.get_env("SMOLNET_NIF_WALL_CLOCK_MODE") do
                      nil -> :enforce
                      "enforce" -> :enforce
                      "report" -> :report
                      value -> raise "invalid SMOLNET_NIF_WALL_CLOCK_MODE: #{inspect(value)}"
                    end)

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

    maintenance = new_stack(@maximum_limits, empty_config())
    {:ok, _envelope} = Native.test_prepare_closing(maintenance, @native_socket_capacity)

    once("maximum closing-socket maintenance", fn ->
      {:ok, _envelope} = Native.stack_poll(maintenance, 30_000)
    end)

    ready_resource = new_stack(@maximum_limits, empty_config())
    {ready_keys, _ready_references} = arm_waiters(ready_resource)

    once("maximum readiness delivery", fn ->
      {:ok, _envelope} = Native.test_socket_ready(ready_resource, ready_keys)
    end)

    once("maximum readiness overflow sweep", fn ->
      {:ok, _envelope} = Native.stack_poll(ready_resource, 0)
    end)

    drain_messages(@maximum_limits.ready_events)

    combined = new_stack(@maximum_limits, empty_config())
    {:ok, _envelope} = Native.test_prepare_closing(combined, @native_socket_capacity)

    {combined_keys, _combined_references} =
      arm_waiters(combined, @native_socket_capacity, [:read, :write])

    once("combined maximum work", fn ->
      {:ok, _envelope} = Native.test_combined_maximum_work(combined, combined_keys, 30_000)
    end)

    drain_messages(@maximum_limits.ready_events)

    shutdown_resource = new_stack(@maximum_limits, empty_config())
    {_shutdown_keys, _shutdown_references} = arm_waiters(shutdown_resource)

    once("maximum waiter shutdown", fn ->
      {:ok, _envelope} = Native.stack_shutdown(shutdown_resource)
    end)

    drain_messages(@maximum_limits.ready_events)

    wildcard_bind = new_stack(@maximum_limits, maximum_udp_config(@maximum_addresses))
    {:ok, %{result: wildcard_identity}} = Native.udp_open(wildcard_bind, :inet6)

    once("maximum wildcard-UDP bind", fn ->
      {:ok, %{result: :ok}} =
        Native.udp_bind(wildcard_bind, wildcard_identity, %{
          address: List.duplicate(0, 16),
          port: 39_999,
          scope_id: 0
        })
    end)

    listener_expand = new_stack(@maximum_limits, maximum_udp_config(@maximum_addresses))
    {:ok, %{result: listener_identity}} = Native.tcp_open(listener_expand, :inet6)

    {:ok, %{result: :ok}} =
      Native.tcp_bind(listener_expand, listener_identity, %{
        address: maximum_ipv6_address(1),
        port: 39_998,
        scope_id: 0
      })

    once("maximum TCP listener expansion", fn ->
      {:ok, %{result: :ok}} = Native.tcp_listen(listener_expand, listener_identity, 128, 0)
    end)

    {wildcard_shutdown, wildcard_identities} = maximum_wildcard_udp_state()
    _wildcard_references = populate_maximum_waiter_state(wildcard_shutdown, wildcard_identities)

    once("maximum wildcard-UDP shutdown", fn ->
      {:ok, _envelope} = Native.stack_shutdown(wildcard_shutdown)
    end)

    drain_messages(@maximum_limits.ready_events)

    {allocation_shutdown, allocation_identities} = maximum_single_address_udp_state()

    _allocation_references =
      populate_maximum_waiter_state(allocation_shutdown, allocation_identities)

    once("maximum native-allocation shutdown", fn ->
      {:ok, _envelope} = Native.stack_shutdown(allocation_shutdown)
    end)

    drain_messages(@maximum_limits.ready_events)

    once(
      "maximum wildcard-UDP resource destructor",
      prepare_resource_drop(&maximum_wildcard_udp_state/0)
    )

    once(
      "maximum native-allocation resource destructor",
      prepare_resource_drop(&maximum_single_address_udp_state/0)
    )
  end

  defp repeated(label, iterations, operation) do
    Enum.each(1..min(iterations, 1_000), operation)
    {samples, reductions} = samples(iterations, operation)
    sorted = Enum.sort(samples)
    maximum = List.last(sorted)
    p99 = Enum.at(sorted, div(iterations * 99, 100))
    mean = div(Enum.sum(samples), iterations)

    IO.puts("#{label} budget evidence")
    IO.puts("  calls: #{iterations}")
    IO.puts("  mean wall time: #{mean} ns")
    IO.puts("  p99 wall time: #{p99} ns")
    IO.puts("  maximum wall time: #{maximum} ns (budget #{@max_wall_nanoseconds} ns)")
    IO.puts("  caller reductions/call: #{Float.round(reductions, 2)} (budget #{@max_reductions})")

    enforce!(label, maximum, reductions)
  end

  defp once(label, operation) do
    {samples, reductions} = samples(1, fn _index -> operation.() end)
    maximum = hd(samples)

    IO.puts("#{label} budget evidence")
    IO.puts("  maximum wall time: #{maximum} ns (budget #{@max_wall_nanoseconds} ns)")
    IO.puts("  caller reductions/call: #{Float.round(reductions, 2)} (budget #{@max_reductions})")

    enforce!(label, maximum, reductions)
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

  defp enforce!(label, maximum, reductions) do
    if reductions > @max_reductions do
      raise "#{label} exceeded the normal-scheduler reduction budget"
    end

    if maximum > @max_wall_nanoseconds do
      case @wall_clock_mode do
        :enforce ->
          raise "#{label} exceeded the normal-scheduler wall-clock budget"

        :report ->
          IO.puts("  wall-clock overrun recorded without failing in report-only mode")
      end
    end
  end

  defp new_stack(limits, config) do
    {:ok, %{result: resource}} = Native.stack_new(limits, config, 0)
    resource
  end

  defp empty_config(mtu \\ 1_500), do: %{mtu: mtu, addresses: [], routes: []}

  defp maximum_ingress_config do
    %{mtu: @maximum_mtu, addresses: [%{address: destination(), prefix_length: 64}], routes: []}
  end

  defp maximum_ipv6_packet do
    source = <<0xFD, 0::112, 1>>
    destination = destination() |> :erlang.list_to_binary()
    payload = :binary.copy(<<0>>, 65_535)
    <<6::4, 0::28, 65_535::16, 59, 64, source::binary, destination::binary, payload::binary>>
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
    {:ok, _envelope} = Native.test_socket_ready(resource, Enum.reverse(ready_keys))
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
end

SmolNet.NifBudget.run()
