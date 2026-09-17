defmodule SmolNet.HardeningTest do
  use ExUnit.Case, async: false

  @moduletag :debug_nif

  alias SmolNet.Native
  alias SmolNet.Socket
  alias SmolNet.Test.RawIpLink
  alias SmolNet.Test.Readiness

  @state_machine_seeds [10_471, 81_911, 216_613, 524_287]
  @close_cancel_seed 911_357
  @server {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @client {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    on_exit(&stop_all_stacks/0)
  end

  for seed <- @state_machine_seeds do
    @seed seed

    test "readiness state machine preserves lifecycle invariants (seed #{seed})" do
      :rand.seed(:exsss, {@seed, @seed * 3, @seed * 7})
      {:ok, stack} = SmolNet.start_stack(limits: %{ready_events: 64, maintenance_work: 8})

      sockets =
        Enum.reduce(1..750, %{}, fn step, sockets ->
          run_state_machine_step(stack, sockets, step)
        end)

      Enum.each(sockets, fn {_identity, {socket, pending}} ->
        assert :ok = Readiness.close(socket)
        assert_aborts(socket, pending)
      end)

      {:ok, info} = SmolNet.stack_info(stack)
      assert info.native.result.socket_count == 0
      assert info.native.result.waiter_count == 0
      assert info.native.result.ready_count <= info.native.result.limits.ready_events

      assert info.native.result.counters.max_ready_events <=
               info.native.result.limits.ready_events
    end
  end

  test "deterministic link faults preserve UDP boundaries and bounded queues" do
    {:ok, link} = RawIpLink.start_link(self())

    {:ok, server_stack} =
      SmolNet.start_stack(egress: {link, :server}, addresses: [{@server, 64}])

    {:ok, client_stack} =
      SmolNet.start_stack(egress: {link, :client}, addresses: [{@client, 64}])

    :ok = RawIpLink.connect(link, :server, client_stack)
    :ok = RawIpLink.connect(link, :client, server_stack)

    {:ok, server} = SmolNet.open(:inet6, :dgram, :udp, stack: server_stack)
    :ok = SmolNet.bind(server, endpoint(@server, 45_011))
    {:ok, client} = SmolNet.open(:inet6, :dgram, :udp, stack: client_stack)
    :ok = SmolNet.bind(client, endpoint(@client, 0))
    destination = endpoint(@server, 45_011)

    :ok = RawIpLink.fault(link, :drop)

    for sequence <- 1..32 do
      assert :ok = SmolNet.sendto(client, <<sequence::16>>, destination, 2_000)
    end

    assert {:error, :timeout} = SmolNet.recvfrom(server, 0, 20)

    :ok = RawIpLink.fault(link, :duplicate)
    assert :ok = SmolNet.sendto(client, "duplicate", destination, 2_000)
    assert {:ok, %{data: "duplicate"}} = SmolNet.recvfrom(server, 0, 2_000)
    assert {:ok, %{data: "duplicate"}} = SmolNet.recvfrom(server, 0, 2_000)

    :ok = RawIpLink.fault(link, :hold)

    for sequence <- 1..8 do
      assert :ok = SmolNet.sendto(client, <<sequence>>, destination, 2_000)
    end

    assert {:error, :timeout} = SmolNet.recvfrom(server, 0, 20)
    :ok = RawIpLink.release(link, :reverse)

    assert Enum.map(1..8, fn _index ->
             {:ok, %{data: <<sequence>>}} = SmolNet.recvfrom(server, 0, 2_000)
             sequence
           end) == Enum.to_list(8..1//-1)

    {:ok, info} = SmolNet.stack_info(server_stack)
    assert info.native.result.receive_packets <= 1
    assert info.native.result.transmit_packets <= info.native.result.limits.output_packets
    assert info.native.result.counters.max_bytes_copied <= info.native.result.limits.bytes_copied
  end

  test "one hundred stack restarts return all native resources to baseline" do
    baseline = Native.resource_counts().active

    for _iteration <- 1..100 do
      {:ok, stack} = SmolNet.start_stack()
      assert :ok = SmolNet.stop_stack(stack)
    end

    assert_eventually(fn -> Native.resource_counts().active == baseline end)
  end

  test "concurrent close and cancel have exactly one linearized outcome" do
    :rand.seed(:exsss, {@close_cancel_seed, @close_cancel_seed * 3, @close_cancel_seed * 7})
    {:ok, stack} = SmolNet.start_stack()

    outcomes =
      Enum.map(1..250, fn iteration ->
        socket = Readiness.open(stack, iteration)
        assert {:select, select_info} = Readiness.wait(socket, :read, :recv)
        gate = make_ref()
        parent = self()
        cancel_delay = :rand.uniform(2) - 1
        close_delay = 1 - cancel_delay

        close =
          Task.async(fn ->
            send(parent, {:close_ready, gate})

            receive do
              {:go, ^gate} -> Process.sleep(close_delay)
            end

            Readiness.close(socket)
          end)

        cancel =
          Task.async(fn ->
            send(parent, {:cancel_ready, gate})

            receive do
              {:go, ^gate} -> Process.sleep(cancel_delay)
            end

            SmolNet.cancel(socket, select_info)
          end)

        assert_receive {:close_ready, ^gate}
        assert_receive {:cancel_ready, ^gate}
        send(close.pid, {:go, gate})
        send(cancel.pid, {:go, gate})

        assert Task.await(close, 2_000) == :ok

        outcome =
          case Task.await(cancel, 2_000) do
            :ok ->
              refute_socket_message(socket, select_info)
              :cancel_won

            {:error, :invalid_socket} ->
              assert_abort(socket, select_info)
              :close_won
          end

        refute_socket_message(socket, select_info)
        outcome
      end)

    assert MapSet.new(outcomes) == MapSet.new([:cancel_won, :close_won])
    {:ok, info} = SmolNet.stack_info(stack)
    assert info.native.result.socket_count == 0
    assert info.native.result.waiter_count == 0
  end

  defp run_state_machine_step(stack, sockets, step) do
    case :rand.uniform(6) do
      1 when map_size(sockets) < 32 ->
        socket = Readiness.open(stack, step)
        Map.put(sockets, Socket.identity(socket), {socket, %{read: nil, write: nil}})

      2 when map_size(sockets) > 0 ->
        with_random_socket(sockets, &arm_random_waiter(sockets, &1, &2, &3))

      operation when operation in 3..5 and map_size(sockets) > 0 ->
        resolve_random_waiter(sockets, operation)

      6 when map_size(sockets) > 0 ->
        with_random_socket(sockets, fn identity, socket, pending ->
          assert :ok = Readiness.close(socket)
          assert_aborts(socket, pending)
          Map.delete(sockets, identity)
        end)

      _other ->
        sockets
    end
  end

  defp resolve_random_waiter(sockets, resolution) do
    pending_waiters =
      for {identity, {socket, pending}} <- sockets,
          {direction, select_info} <- pending,
          select_info != nil,
          do: {identity, socket, pending, direction, select_info}

    case random_member(pending_waiters) do
      nil ->
        sockets

      {identity, socket, pending, direction, select_info} ->
        resolve_waiter(socket, direction, select_info, resolution)
        Map.put(sockets, identity, {socket, Map.put(pending, direction, nil)})
    end
  end

  defp arm_random_waiter(sockets, identity, socket, pending) do
    direction = random_direction()

    if pending[direction] do
      sockets
    else
      operation = if direction == :read, do: :recv, else: :send
      assert {:select, select_info} = Readiness.wait(socket, direction, operation)
      Map.put(sockets, identity, {socket, Map.put(pending, direction, select_info)})
    end
  end

  defp resolve_waiter(socket, direction, select_info, 3) do
    assert :ok = Readiness.ready(socket, direction)
    assert_select(socket, select_info)
  end

  defp resolve_waiter(socket, _direction, select_info, 4) do
    assert :ok = SmolNet.cancel(socket, select_info)
    refute_socket_message(socket, select_info)
  end

  defp resolve_waiter(socket, direction, select_info, 5) do
    ready = Task.async(fn -> Readiness.ready(socket, direction) end)
    cancel = Task.async(fn -> SmolNet.cancel(socket, select_info) end)
    assert Task.await(ready, 2_000) == :ok

    case Task.await(cancel, 2_000) do
      :ok -> refute_socket_message(socket, select_info)
      :already_sent -> assert_select(socket, select_info)
    end
  end

  defp with_random_socket(sockets, operation) do
    {identity, {socket, pending}} = sockets |> Enum.to_list() |> random_member()
    operation.(identity, socket, pending)
  end

  defp random_member([]), do: nil
  defp random_member(values), do: Enum.at(values, :rand.uniform(length(values)) - 1)
  defp random_direction, do: if(:rand.uniform(2) == 1, do: :read, else: :write)

  defp assert_aborts(socket, pending) do
    Enum.each(pending, fn
      {_direction, nil} -> :ok
      {_direction, select_info} -> assert_abort(socket, select_info)
    end)
  end

  defp assert_select(socket, {:select_info, _operation, reference}) do
    identity = Socket.identity(socket)
    assert_receive {:"$smol_socket", ^identity, :select, ^reference}, 2_000
  end

  defp assert_abort(socket, {:select_info, _operation, reference}) do
    identity = Socket.identity(socket)
    assert_receive {:"$smol_socket", ^identity, :abort, ^reference, :closed}, 2_000
  end

  defp refute_socket_message(socket, {:select_info, _operation, reference}) do
    identity = Socket.identity(socket)
    refute_receive {:"$smol_socket", ^identity, _, ^reference}, 0
    refute_receive {:"$smol_socket", ^identity, _, ^reference, _reason}, 0
  end

  defp endpoint(address, port), do: %{family: :inet6, addr: address, port: port, scope_id: 0}

  defp stop_all_stacks do
    if Process.whereis(SmolNet.Supervisor) do
      for {_id, bundle, _type, _modules} <-
            DynamicSupervisor.which_children(SmolNet.Supervisor) do
        DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
      end
    end
  end

  defp assert_eventually(assertion, attempts \\ 200)
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
