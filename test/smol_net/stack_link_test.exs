defmodule SmolNet.StackLinkTest do
  use ExUnit.Case, async: false

  alias SmolNet.Socket
  alias SmolNet.Stack.Ref
  alias SmolNet.Test.IPv6Link
  alias SmolNet.Test.ManualClock
  alias SmolNet.Test.NativeDouble

  @address_a {0xFD00, 0, 0, 0, 0, 0, 0, 1}
  @address_b {0xFD00, 0, 0, 0, 0, 0, 0, 2}

  setup do
    previous_env =
      Map.new(
        [
          :native_module,
          :native_test_process,
          :native_test_result,
          :native_ingress_result,
          :native_poll_result,
          :clock_module,
          :manual_clock
        ],
        &{&1, Application.get_env(:smolnet, &1)}
      )

    on_exit(fn ->
      stop_all_stacks()

      manual_clock = Application.get_env(:smolnet, :manual_clock)

      if is_pid(manual_clock) and manual_clock != previous_env.manual_clock and
           Process.alive?(manual_clock) do
        Agent.stop(manual_clock)
      end

      Enum.each(previous_env, fn
        {key, nil} -> Application.delete_env(:smolnet, key)
        {key, value} -> Application.put_env(:smolnet, key, value)
      end)
    end)
  end

  test "accepts raw IPv6 and emits exactly one tagged packet batch" do
    {:ok, stack} =
      SmolNet.start_stack(
        egress: {self(), :direct_link},
        mtu: 1_280,
        addresses: [{@address_a, 64}],
        routes: [{{0, 0, 0, 0, 0, 0, 0, 0}, 0, @address_b}]
      )

    packet = echo_request(@address_b, @address_a, "hello")
    assert :ok = SmolNet.ingress(stack, packet)

    assert_receive {:smol_stack, :direct_link, :egress, [response]}
    assert is_binary(response)
    assert <<6::4, _::bitstring>> = response
    assert ipv6_source(response) == ipv6_binary(@address_a)
    assert ipv6_destination(response) == ipv6_binary(@address_b)
    assert <<129, 0, _checksum::16, 17::16, 23::16, "hello">> = binary_part(response, 40, 13)
    refute_receive {:smol_stack, :direct_link, :egress, _duplicate}, 50

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.processed_ingress == 1 and info.native.result.counters.emitted_packets == 1
    end)
  end

  test "accepts a bounded ingress batch and drives every packet in one native call" do
    {:ok, stack} =
      SmolNet.start_stack(
        egress: {self(), :ingress_batch},
        mtu: 1_280,
        addresses: [{@address_a, 64}],
        routes: [{{0, 0, 0, 0, 0, 0, 0, 0}, 0, @address_b}],
        limits: %{input_packets: 2}
      )

    first = echo_request(@address_b, @address_a, "first")
    second = echo_request(@address_b, @address_a, "second")
    assert {:ok, 2} = SmolNet.ingress(stack, [first, second])

    assert_receive {:smol_stack, :ingress_batch, :egress, responses}
    assert length(responses) == 2
    assert Enum.map(responses, &binary_part(&1, 48, byte_size(&1) - 48)) == ["first", "second"]
    refute_receive {:smol_stack, :ingress_batch, :egress, _duplicate}, 50

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)

      info.processed_ingress == 2 and info.native.result.counters.ingress_packets == 2 and
        info.native.result.counters.max_input_packets == 2 and
        info.native.result.counters.native_calls == 1
    end)
  end

  test "batch ingress validates and admits atomically" do
    {:ok, stack} =
      SmolNet.start_stack(
        egress: {self(), :batch_validation},
        mtu: 1_280,
        addresses: [{@address_a, 64}],
        limits: %{bytes_copied: 1_280, input_packets: 2}
      )

    valid = empty_ipv6_packet()
    large = <<6::4, 0::28, 660::16, 59, 64, 0::256, 0::size(660 * 8)>>

    assert {:ok, 0} = SmolNet.ingress(stack, [])
    assert {:error, :invalid_packet} = SmolNet.ingress(stack, [valid, :not_a_packet])
    assert {:error, :invalid_packet} = SmolNet.ingress(stack, [valid | :improper])
    assert {:error, :batch_too_large} = SmolNet.ingress(stack, [valid, valid, valid])
    assert {:error, :batch_too_large} = SmolNet.ingress(stack, [large, large])

    assert {:ok, info} = SmolNet.stack_info(stack)
    assert info.processed_ingress == 0
    assert info.native.result.counters.ingress_packets == 0
    assert info.ingress.rejected == 4
  end

  test "egress emits one ordered list per native output envelope" do
    first = empty_ipv6_packet(1)
    second = empty_ipv6_packet(2)
    configure_native_double(:default, output: [first, second])

    {:ok, _stack} = SmolNet.start_stack(egress: {self(), :batch_link})

    assert_receive {:smol_stack, :batch_link, :egress, [^first, ^second]}
    refute_receive {:smol_stack, :batch_link, :egress, _other}, 50
  end

  test "egress does not emit empty output envelopes" do
    configure_native_double(:default)

    {:ok, _stack} = SmolNet.start_stack(egress: {self(), :empty_batch})

    refute_receive {:smol_stack, :empty_batch, :egress, _packets}, 50
  end

  test "rejects malformed and oversized packets before native mutation" do
    {:ok, stack} =
      SmolNet.start_stack(
        egress: {self(), :validation},
        mtu: 1_280,
        addresses: [{@address_a, 64}]
      )

    assert SmolNet.ingress(stack, :not_a_binary) == {:error, :invalid_packet}
    assert SmolNet.ingress(stack, <<6::4, 0::308>>) == {:error, :invalid_packet}
    assert SmolNet.ingress(stack, <<4::4, 0::156>>) == {:error, :invalid_packet}

    wrong_declared_length = <<6::4, 0::28, 1::16, 59, 64, 0::256>>
    assert SmolNet.ingress(stack, wrong_declared_length) == {:error, :invalid_packet}

    oversized = <<6::4, 0::28, 1_241::16, 59, 64, 0::256, 0::size(1_241 * 8)>>
    assert SmolNet.ingress(stack, oversized) == {:error, :packet_too_large}

    {:ok, info} = SmolNet.stack_info(stack)
    assert info.processed_ingress == 0
    assert info.native.result.counters.ingress_packets == 0
    assert info.ingress.rejected == 5
  end

  test "continues bounded output before processing the next admitted ingress" do
    {:ok, stack} =
      SmolNet.start_stack(
        egress: {self(), :byte_bound},
        mtu: 1_280,
        addresses: [{@address_a, 64}],
        limits: %{bytes_copied: 1_280}
      )

    packet = echo_request(@address_b, @address_a, :binary.copy(<<1>>, 1_232))
    assert byte_size(packet) == 1_280
    assert :ok = SmolNet.ingress(stack, packet)
    assert :ok = SmolNet.ingress(stack, packet)

    assert_receive {:smol_stack, :byte_bound, :egress, [first_response]}
    assert_receive {:smol_stack, :byte_bound, :egress, [second_response]}
    assert byte_size(first_response) == 1_280
    assert byte_size(second_response) == 1_280

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)

      info.native.result.counters.max_bytes_copied == 1_280 and
        info.native.result.counters.poll_calls >= 2 and info.processed_ingress == 2 and
        info.failed_ingress == 0
    end)
  end

  test "single feeder is acknowledged before bounded native processing" do
    configure_native_double(:wait)
    Application.put_env(:smolnet, :native_poll_result, :wait)
    packet = empty_ipv6_packet()
    test = self()

    feeder =
      spawn(fn ->
        receive do
          {:feed, stack} ->
            send(test, {:first_ingress, SmolNet.ingress(stack, packet)})
            send(test, :second_ingress_started)
            send(test, {:second_ingress, SmolNet.ingress(stack, packet)})

            receive do
              :stop -> :ok
            end
        end
      end)

    feeder_monitor = Process.monitor(feeder)
    {:ok, stack} = SmolNet.start_stack(egress: {feeder, :serialized})
    send(feeder, {:feed, stack})

    assert_receive {:first_ingress, :ok}
    assert_receive {:native_stack_ingress, stack_pid, ^packet}
    assert_receive :second_ingress_started

    refute_receive {:second_ingress, _result}, 50
    refute_receive {:native_stack_ingress, _, _}, 50

    send(stack_pid, {:native_ingress_reply, empty_effects(more: true)})
    assert_receive {:native_stack_poll, ^stack_pid, _now}

    refute_receive {:second_ingress, _result}, 50
    refute_receive {:native_stack_ingress, _, _}, 50

    send(stack_pid, {:native_poll_reply, empty_effects()})
    assert_receive {:second_ingress, :ok}
    assert_receive {:native_stack_ingress, ^stack_pid, ^packet}
    send(stack_pid, {:native_ingress_reply, empty_effects()})

    assert_eventually(fn ->
      {:ok, drained} = SmolNet.stack_info(stack)

      drained.ingress == %{mode: :single_feeder, rejected: 0} and
        drained.processed_ingress == 2
    end)

    send(feeder, :stop)
    assert_receive {:DOWN, ^feeder_monitor, :process, ^feeder, :normal}
  end

  test "batched ingress preserves the pending slot and busy backpressure" do
    configure_native_double(:wait)
    Application.put_env(:smolnet, :native_poll_result, :wait)
    batch = [empty_ipv6_packet(1), empty_ipv6_packet(2)]

    {:ok, stack} =
      SmolNet.start_stack(egress: {self(), :batch_pending}, limits: %{input_packets: 2})

    %{stack: stack_pid} = Ref.pids(stack)

    assert {:ok, 2} = SmolNet.ingress(stack, batch)
    assert_receive {:native_stack_ingress_batch, ^stack_pid, ^batch}

    second = Task.async(fn -> SmolNet.ingress(stack, batch) end)
    third = Task.async(fn -> SmolNet.ingress(stack, batch) end)
    assert_eventually(fn -> message_queue_length(stack_pid) == 2 end)

    send(stack_pid, {:native_ingress_reply, empty_effects(more: true)})
    assert_receive {:native_stack_poll, ^stack_pid, _now}

    pending =
      case {Task.yield(second, 1_000), Task.yield(third, 1_000)} do
        {{:ok, {:error, :busy}}, nil} -> third
        {nil, {:ok, {:error, :busy}}} -> second
      end

    send(stack_pid, {:native_poll_reply, empty_effects()})
    assert Task.await(pending) == {:ok, 2}
    assert_receive {:native_stack_ingress_batch, ^stack_pid, ^batch}
    send(stack_pid, {:native_ingress_reply, empty_effects()})

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.processed_ingress == 4 and info.ingress.rejected == 1
    end)
  end

  test "a stale poll still releases a held ingress packet" do
    configure_native_double(:wait)
    Application.put_env(:smolnet, :native_poll_result, :wait)
    packet = empty_ipv6_packet()
    test = self()

    feeder =
      spawn(fn ->
        receive do
          {:feed, stack} ->
            send(test, {:first_ingress, SmolNet.ingress(stack, packet)})
            send(test, :second_ingress_started)
            send(test, {:second_ingress, SmolNet.ingress(stack, packet)})

            receive do
              :stop -> :ok
            end
        end
      end)

    feeder_monitor = Process.monitor(feeder)
    {:ok, stack} = SmolNet.start_stack(egress: {feeder, :stale_poll})
    %{stack: stack_pid} = Ref.pids(stack)
    send(feeder, {:feed, stack})

    # The stack is blocked inside stack_ingress/3 for the first packet while
    # the feeder's second packet queues behind it.
    assert_receive {:first_ingress, :ok}
    assert_receive {:native_stack_ingress, ^stack_pid, ^packet}
    assert_receive :second_ingress_started
    assert_eventually(fn -> message_queue_length(stack_pid) == 1 end)

    # A socket call queued behind the second packet replaces the poll timer
    # when it completes, which makes the continuation's self-sent poll stale.
    request = :gen_server.send_request(stack_pid, {:socket_shutdown, 1, 1, :write})
    assert_eventually(fn -> message_queue_length(stack_pid) == 2 end)

    send(stack_pid, {:native_ingress_reply, empty_effects(more: true)})
    assert {:reply, :ok} = :gen_server.receive_response(request, 1_000)

    # The stale poll never reaches the native, but the held packet must still
    # be admitted rather than leaving the feeder blocked.
    assert_receive {:second_ingress, :ok}
    assert_receive {:native_stack_ingress, ^stack_pid, ^packet}
    refute_received {:native_stack_poll, _, _}
    send(stack_pid, {:native_ingress_reply, empty_effects()})

    assert_eventually(fn ->
      {:ok, drained} = SmolNet.stack_info(stack)
      drained.processed_ingress == 2 and drained.poll_at == nil
    end)

    send(feeder, :stop)
    assert_receive {:DOWN, ^feeder_monitor, :process, ^feeder, :normal}
  end

  test "continuation poll failure closes without admitting waiting ingress" do
    configure_native_double(:wait)
    Application.put_env(:smolnet, :native_poll_result, :wait)
    packet = empty_ipv6_packet()
    test = self()

    feeder =
      spawn(fn ->
        receive do
          {:feed, stack} ->
            send(test, {:first_ingress, SmolNet.ingress(stack, packet)})
            send(test, :second_ingress_started)
            send(test, {:second_ingress, SmolNet.ingress(stack, packet)})

            receive do
              :stop -> :ok
            end
        end
      end)

    feeder_monitor = Process.monitor(feeder)
    {:ok, stack} = SmolNet.start_stack(egress: {feeder, :poll_failure})
    %{stack: stack_pid} = Ref.pids(stack)
    stack_monitor = Process.monitor(stack_pid)
    send(feeder, {:feed, stack})

    assert_receive {:first_ingress, :ok}
    assert_receive {:native_stack_ingress, ^stack_pid, ^packet}
    assert_receive :second_ingress_started

    send(stack_pid, {:native_ingress_reply, empty_effects(more: true)})
    assert_receive {:native_stack_poll, ^stack_pid, _now}
    refute_receive {:second_ingress, _result}, 50

    send(stack_pid, {:native_poll_reply, {:error, :time_overflow}})

    assert_receive {:second_ingress, {:error, :closed}}

    assert_receive {:DOWN, ^stack_monitor, :process, ^stack_pid,
                    {:shutdown, {:native_poll_failed, :time_overflow}}}

    refute_receive {:native_stack_ingress, _, _}, 50

    send(feeder, :stop)
    assert_receive {:DOWN, ^feeder_monitor, :process, ^feeder, :normal}
  end

  test "orderly shutdown rejects ingress parked behind a continuation" do
    configure_native_double(:wait)
    packet = empty_ipv6_packet()
    {:ok, stack} = SmolNet.start_stack(egress: {self(), :shutdown})
    %{stack: stack_pid} = Ref.pids(stack)

    assert :ok = SmolNet.ingress(stack, packet)
    assert_receive {:native_stack_ingress, ^stack_pid, ^packet}

    pending_ingress =
      Task.async(fn ->
        GenServer.call(
          stack_pid,
          {:ingress, stack.ingress_token, packet},
          :infinity
        )
      end)

    shutdown = Task.async(fn -> SmolNet.Stack.shutdown_waiters(stack_pid) end)
    assert_eventually(fn -> message_queue_length(stack_pid) == 2 end)

    send(stack_pid, {:native_ingress_reply, empty_effects(more: true)})

    assert Task.await(pending_ingress) == {:error, :closed}
    assert Task.await(shutdown) == :ok
    assert Process.alive?(stack_pid)
    assert SmolNet.ingress(stack, packet) == {:error, :closed}
    refute_receive {:native_stack_ingress, ^stack_pid, ^packet}, 50
  end

  test "only capability-authenticated ingress calls reach native processing" do
    {:ok, stack} = SmolNet.start_stack(egress: {self(), :authenticated})
    %{stack: stack_pid} = Ref.pids(stack)
    packet = empty_ipv6_packet()

    send(stack_pid, {:smolnet_ingress, packet})
    send(stack_pid, {:smolnet_ingress, make_ref(), packet})

    assert GenServer.call(stack_pid, {:ingress, make_ref(), packet}) ==
             {:error, :invalid_ingress}

    assert {:ok, info} = SmolNet.stack_info(stack)
    assert info.processed_ingress == 0
    assert info.ingress == %{mode: :single_feeder, rejected: 0}
  end

  test "timer generations reject stale messages and poll with no new ingress" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)
    configure_native_double(:default, poll_at: 10)

    Application.put_env(
      :smolnet,
      :native_poll_result,
      empty_effects(output: [empty_ipv6_packet()])
    )

    {:ok, stack} = SmolNet.start_stack(egress: {self(), :timer})
    %{stack: stack_pid} = Ref.pids(stack)
    {:ok, info} = SmolNet.stack_info(stack)
    generation = info.timer_generation

    send(stack_pid, {:smolnet_poll, generation - 1})
    refute_receive {:native_stack_poll, _, _}, 50

    :ok = ManualClock.advance(clock, 9)
    refute_receive {:native_stack_poll, _, _}, 50

    :ok = ManualClock.advance(clock, 1)
    assert_receive {:native_stack_poll, ^stack_pid, 10}
    assert_receive {:smol_stack, :timer, :egress, [packet]}
    assert packet == empty_ipv6_packet()

    {:ok, polled} = SmolNet.stack_info(stack)
    assert polled.timer_generation > generation
    assert polled.poll_at == nil
  end

  test "non-driving socket calls preserve an existing protocol poll deadline" do
    {:ok, clock} = ManualClock.start()
    Application.put_env(:smolnet, :clock_module, ManualClock)
    Application.put_env(:smolnet, :manual_clock, clock)
    configure_native_double(:default, poll_at: 10)

    {:ok, stack} = SmolNet.start_stack()
    %{stack: stack_pid} = Ref.pids(stack)
    socket = Socket.new(stack_pid, %{id: 1, generation: 1})
    select_info = {:select_info, :recv, make_ref()}
    {:ok, before_cancel} = SmolNet.stack_info(stack)

    assert :ok = SmolNet.cancel(socket, select_info)

    {:ok, after_cancel} = SmolNet.stack_info(stack)
    assert after_cancel.poll_at == 10
    assert after_cancel.timer_generation == before_cancel.timer_generation

    :ok = ManualClock.advance(clock, 10)
    assert_receive {:native_stack_poll, ^stack_pid, 10}
  end

  test "non-driving socket calls preserve an in-progress native continuation" do
    configure_native_double(:wait)
    Application.put_env(:smolnet, :native_poll_result, :wait)

    {:ok, stack} = SmolNet.start_stack(egress: {self(), :continuation})
    %{stack: stack_pid} = Ref.pids(stack)
    packet = empty_ipv6_packet()

    assert :ok = SmolNet.ingress(stack, packet)
    assert_receive {:native_stack_ingress, ^stack_pid, ^packet}

    socket = Socket.new(stack_pid, %{id: 1, generation: 1})
    select_info = {:select_info, :recv, make_ref()}
    cancel_task = Task.async(fn -> SmolNet.cancel(socket, select_info) end)
    assert_eventually(fn -> message_queue_length(stack_pid) == 1 end)

    second_ingress = Task.async(fn -> SmolNet.ingress(stack, packet) end)
    assert_eventually(fn -> message_queue_length(stack_pid) == 2 end)

    send(stack_pid, {:native_ingress_reply, empty_effects(more: true)})
    assert Task.await(cancel_task) == :ok
    assert_receive {:native_stack_poll, ^stack_pid, _now}
    assert Task.yield(second_ingress, 0) == nil
    refute_receive {:native_stack_ingress, ^stack_pid, ^packet}, 50

    send(stack_pid, {:native_poll_reply, empty_effects()})
    assert Task.await(second_ingress) == :ok
    assert_receive {:native_stack_ingress, ^stack_pid, ^packet}
    send(stack_pid, {:native_ingress_reply, empty_effects()})
  end

  test "repeated native continuations do not starve stack messages or another stack" do
    {:ok, counter} = Agent.start(fn -> 10_000 end)

    on_exit(fn ->
      Application.put_env(:smolnet, :native_poll_result, empty_effects())
      stop_all_stacks()

      if Process.alive?(counter) do
        Agent.stop(counter)
      end
    end)

    configure_native_double(empty_effects(more: true))
    Application.put_env(:smolnet, :native_poll_result, {:countdown, counter, :silent})

    {:ok, continuing_stack} = SmolNet.start_stack(egress: {self(), :continuing})
    {:ok, independent_stack} = SmolNet.start_stack(egress: {self(), :independent})
    packet = empty_ipv6_packet()

    assert :ok = SmolNet.ingress(continuing_stack, packet)
    assert_receive {:native_stack_ingress, _stack_pid, ^packet}

    continuing_info = Task.async(fn -> SmolNet.stack_info(continuing_stack) end)
    independent_info = Task.async(fn -> SmolNet.stack_info(independent_stack) end)

    assert {:ok, %{native: %{result: %{test_double: true}}}} =
             Task.await(continuing_info, 1_000)

    assert {:ok, %{native: %{result: %{test_double: true}}}} =
             Task.await(independent_info, 1_000)

    assert Agent.get(counter, & &1) > 0

    Application.put_env(:smolnet, :native_poll_result, empty_effects())
    assert {:ok, _info} = SmolNet.stack_info(continuing_stack)
    Agent.stop(counter)
  end

  test "two stacks on independent links progress when one link is held" do
    {:ok, link_a} = IPv6Link.start_link(self())
    {:ok, link_b} = IPv6Link.start_link(self())

    {:ok, stack_a} =
      SmolNet.start_stack(
        egress: {link_a, :a},
        addresses: [{@address_a, 64}]
      )

    {:ok, stack_b} =
      SmolNet.start_stack(
        egress: {link_b, :b},
        addresses: [{@address_b, 64}]
      )

    :ok = IPv6Link.connect(link_a, :a, stack_b)
    :ok = IPv6Link.connect(link_b, :b, stack_a)
    :ok = IPv6Link.fault(link_a, :hold)

    request_a = echo_request(@address_b, @address_a, "held")
    assert :ok = SmolNet.ingress(stack_a, request_a)
    assert_receive {:test_link_egress, :a, held_response}

    {:ok, before_release} = SmolNet.stack_info(stack_b)
    assert before_release.processed_ingress == 0

    request_b = echo_request(@address_a, @address_b, "independent")
    assert :ok = SmolNet.ingress(stack_b, request_b)
    assert_receive {:test_link_egress, :b, independent_response}

    assert_eventually(fn ->
      {:ok, a_info} = SmolNet.stack_info(stack_a)
      {:ok, b_info} = SmolNet.stack_info(stack_b)
      a_info.processed_ingress == 2 and b_info.processed_ingress == 1
    end)

    assert ipv6_destination(independent_response) == ipv6_binary(@address_a)

    :ok = IPv6Link.release(link_a)

    assert_eventually(fn ->
      {:ok, b_info} = SmolNet.stack_info(stack_b)
      b_info.processed_ingress == 2
    end)

    assert ipv6_destination(held_response) == ipv6_binary(@address_b)
  end

  test "the in-memory link deterministically drops, duplicates, delays, and reorders" do
    configure_native_double(:default)
    {:ok, link} = IPv6Link.start_link(self())
    {:ok, peer} = SmolNet.start_stack(egress: {self(), :peer})
    :ok = IPv6Link.connect(link, :source, peer)

    first = empty_ipv6_packet(1)
    second = empty_ipv6_packet(2)

    :ok = IPv6Link.fault(link, :drop)
    send(link, {:smol_stack, :source, :egress, [first]})
    assert_receive {:test_link_egress, :source, ^first}
    refute_receive {:native_stack_ingress, _, _}, 50

    :ok = IPv6Link.fault(link, :duplicate)
    send(link, {:smol_stack, :source, :egress, [first]})
    assert_receive {:native_stack_ingress, peer_pid, ^first}
    assert_receive {:native_stack_ingress, ^peer_pid, ^first}

    :ok = IPv6Link.fault(link, :hold)
    send(link, {:smol_stack, :source, :egress, [first]})
    send(link, {:smol_stack, :source, :egress, [second]})
    refute_receive {:native_stack_ingress, _, _}, 50

    :ok = IPv6Link.release(link, :reverse)
    assert_receive {:native_stack_ingress, ^peer_pid, ^second}
    assert_receive {:native_stack_ingress, ^peer_pid, ^first}
  end

  test "egress death stops the stack under the stop policy" do
    link = idle_process()
    {:ok, stack} = SmolNet.start_stack(egress: {link, :stop}, link_down: :stop)
    %{bundle: bundle} = Ref.pids(stack)
    monitor = Process.monitor(bundle)

    Process.exit(link, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^bundle, :shutdown}
    assert SmolNet.ingress(stack, empty_ipv6_packet()) == {:error, :closed}
  end

  test "egress death marks down a retained stack under the mark_down policy" do
    link = idle_process()
    {:ok, stack} = SmolNet.start_stack(egress: {link, :mark}, link_down: :mark_down)

    Process.exit(link, :kill)

    assert_eventually(fn ->
      {:ok, info} = SmolNet.stack_info(stack)
      info.link_status == :down
    end)

    assert SmolNet.ingress(stack, empty_ipv6_packet()) == {:error, :link_down}
  end

  test "egress death notifies and retains the stack under the notify policy" do
    link = idle_process()
    {:ok, stack} = SmolNet.start_stack(egress: {link, :notify}, link_down: {:notify, self()})

    Process.exit(link, :kill)

    assert_receive {:smol_stack, :notify, :link_down, :killed}
    assert {:ok, %{link_status: :down}} = SmolNet.stack_info(stack)
    assert SmolNet.ingress(stack, empty_ipv6_packet()) == {:error, :link_down}
  end

  test "validates stack link and IPv6 configuration options" do
    assert SmolNet.start_stack(egress: :bad) == {:error, :invalid_egress}

    assert SmolNet.start_stack(egress: {self(), :bad, :batch}) ==
             {:error, :invalid_egress}

    assert SmolNet.start_stack(mtu: 1_279) == {:error, :invalid_mtu}

    assert SmolNet.start_stack(addresses: [{{1, 2, 3, 4}, 64}]) ==
             {:error, :invalid_addresses}

    assert SmolNet.start_stack(routes: [{@address_a, 129, @address_b}]) ==
             {:error, :invalid_routes}

    assert SmolNet.start_stack(addresses: [{{0xFF02, 0, 0, 0, 0, 0, 0, 1}, 64}]) ==
             {:error, :invalid_addresses}

    assert SmolNet.start_stack(routes: [{@address_a, 64, {0, 0, 0, 0, 0, 0, 0, 0}}]) ==
             {:error, :invalid_routes}

    assert SmolNet.start_stack(ingress_queue: [:invalid]) == {:error, :invalid_options}

    assert SmolNet.start_stack(link_down: :restart) ==
             {:error, :invalid_link_down_policy}

    assert SmolNet.start_stack(unknown: true) == {:error, :invalid_options}
  end

  defp configure_native_double(ingress_result, options \\ []) do
    Application.put_env(:smolnet, :native_module, NativeDouble)
    Application.put_env(:smolnet, :native_test_process, self())
    Application.put_env(:smolnet, :native_ingress_result, ingress_result)

    Application.put_env(
      :smolnet,
      :native_test_result,
      {:ok,
       %{
         result: make_ref(),
         output: Keyword.get(options, :output, []),
         poll_at: Keyword.get(options, :poll_at),
         more: false
       }}
    )
  end

  defp empty_effects(options \\ []) do
    {:ok,
     %{
       result: :ok,
       output: Keyword.get(options, :output, []),
       poll_at: Keyword.get(options, :poll_at),
       more: Keyword.get(options, :more, false)
     }}
  end

  defp message_queue_length(pid) do
    {:message_queue_len, length} = Process.info(pid, :message_queue_len)
    length
  end

  defp echo_request(source, destination, payload) do
    source = ipv6_binary(source)
    destination = ipv6_binary(destination)
    echo_without_checksum = <<128, 0, 0::16, 17::16, 23::16, payload::binary>>

    pseudo_header =
      <<source::binary, destination::binary, byte_size(echo_without_checksum)::32, 0::24, 58>>

    checksum = internet_checksum(pseudo_header <> echo_without_checksum)
    echo = <<128, 0, checksum::16, 17::16, 23::16, payload::binary>>

    <<6::4, 0::28, byte_size(echo)::16, 58, 64, source::binary, destination::binary,
      echo::binary>>
  end

  defp empty_ipv6_packet(flow_label \\ 0) do
    <<6::4, 0::8, flow_label::20, 0::16, 59, 64, 0::256>>
  end

  defp ipv6_binary(address) do
    address
    |> Tuple.to_list()
    |> Enum.map_join(&<<&1::16>>)
  end

  defp ipv6_source(packet), do: binary_part(packet, 8, 16)
  defp ipv6_destination(packet), do: binary_part(packet, 24, 16)

  defp internet_checksum(binary) do
    padded = if rem(byte_size(binary), 2) == 0, do: binary, else: binary <> <<0>>

    sum =
      for <<word::16 <- padded>>, reduce: 0 do
        sum -> fold_checksum(sum + word)
      end

    Bitwise.band(Bitwise.bnot(fold_checksum(sum)), 0xFFFF)
  end

  defp fold_checksum(sum) when sum > 0xFFFF do
    fold_checksum(Bitwise.band(sum, 0xFFFF) + Bitwise.bsr(sum, 16))
  end

  defp fold_checksum(sum), do: sum

  defp idle_process do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
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
