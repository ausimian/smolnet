defmodule SmolNet.Stack do
  @moduledoc false

  use GenServer, restart: :temporary, significant: true

  alias SmolNet.Native
  alias SmolNet.Stack.Clock.System, as: SystemClock
  alias SmolNet.Stack.IngressGate
  alias SmolNet.Stack.Options
  alias SmolNet.Stack.Ref

  @default_limits %{
    bytes_copied: 64 * 1024,
    output_packets: 32,
    ready_events: 128,
    maintenance_work: 256
  }

  @type limits :: %{
          bytes_copied: pos_integer(),
          output_packets: pos_integer(),
          ready_events: pos_integer(),
          maintenance_work: pos_integer()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @doc """
  Admits one complete raw IPv6 packet for asynchronous processing.

  Admission validates the packet and reserves space before sending anything to
  the stack process, so ingress cannot grow its mailbox without bound.
  """
  @spec ingress(Ref.t(), binary()) ::
          :ok
          | {:error,
             :invalid_packet
             | :unsupported_family
             | :packet_too_large
             | :queue_full
             | :link_down
             | :closed}
  def ingress(
        %Ref{stack: stack, ingress_gate: gate, ingress_token: ingress_token, mtu: mtu},
        packet
      ) do
    with :ok <- validate_packet(packet, mtu),
         :ok <- IngressGate.reserve(gate, byte_size(packet)),
         true <- Process.alive?(stack) do
      send(stack, {:smolnet_ingress, ingress_token, packet})
      :ok
    else
      false ->
        IngressGate.close(gate)
        IngressGate.release(gate, byte_size(packet))
        {:error, :closed}

      {:error, reason} = error ->
        if reason in [:invalid_packet, :unsupported_family, :packet_too_large] do
          IngressGate.reject(gate)
        end

        error
    end
  end

  @doc false
  @spec info(Ref.t()) :: {:ok, map()} | {:error, :closed}
  def info(%Ref{stack: stack}) do
    GenServer.call(stack, :stack_info)
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec native_snapshot(pid()) :: {:ok, map()} | {:error, atom()}
  def native_snapshot(stack), do: GenServer.call(stack, :native_snapshot)

  @doc false
  @spec test_contention(pid()) :: {:error, :ownership_invariant_violation}
  def test_contention(stack), do: GenServer.call(stack, :test_contention)

  @doc false
  @spec test_bounded_work(pid(), map()) :: {:ok, map()} | {:error, atom()}
  def test_bounded_work(stack, requested) do
    GenServer.call(stack, {:test_bounded_work, requested})
  end

  @impl true
  def init(options) do
    starter = Keyword.fetch!(options, :starter)
    egress = Keyword.get(options, :egress)
    ingress_queue = Keyword.get(options, :ingress_queue, Options.default_ingress_queue())

    {egress_pid, link_ref, link_monitor, link_status} = egress_state(egress)

    state = %{
      starter: starter,
      starter_monitor: Process.monitor(starter),
      ready_ref: Keyword.fetch!(options, :ready_ref),
      limits: Keyword.fetch!(options, :limits),
      native_config: Keyword.get(options, :native_config, Options.default_native_config()),
      native_module: nil,
      clock: Application.get_env(:smolnet, :clock_module, SystemClock),
      native: nil,
      egress_pid: egress_pid,
      link_ref: link_ref,
      link_monitor: link_monitor,
      link_down: Keyword.get(options, :link_down, :stop),
      link_status: link_status,
      ingress_gate:
        Keyword.get_lazy(options, :ingress_gate, fn ->
          IngressGate.new(ingress_queue.packets, ingress_queue.bytes, link_status)
        end),
      ingress_token: Keyword.get_lazy(options, :ingress_token, &make_ref/0),
      ingress_queue: :queue.new(),
      draining: false,
      timer: nil,
      timer_generation: 0,
      processed_ingress: 0,
      failed_ingress: 0,
      dropped_egress: 0
    }

    {:ok, state, {:continue, :create_native_stack}}
  end

  @impl true
  def handle_continue(:create_native_stack, state) do
    native = Application.get_env(:smolnet, :native_module, Native)
    now = state.clock.now()

    case native.stack_new(state.limits, state.native_config, now) do
      {:ok, %{result: resource} = envelope} ->
        send(state.starter, {:smolnet_stack_ready, state.ready_ref})
        state = %{state | native: resource, native_module: native}
        {:noreply, apply_effects(state, envelope)}

      {:error, reason} ->
        send(state.starter, {:smolnet_stack_error, state.ready_ref, reason})
        {:noreply, state}

      unexpected ->
        send(state.starter, {:smolnet_stack_error, state.ready_ref, :invalid_native_result})
        {:noreply, Map.put(state, :invalid_native_result, unexpected)}
    end
  end

  @impl true
  def handle_info(
        {:DOWN, monitor, :process, pid, _reason},
        %{
          starter_monitor: monitor,
          starter: pid
        } = state
      ) do
    {:stop, :normal, state}
  end

  def handle_info(
        {:smolnet_ingress, ingress_token, packet},
        %{ingress_token: ingress_token} = state
      ) do
    queue = :queue.in(packet, state.ingress_queue)
    state = %{state | ingress_queue: queue}

    if state.draining do
      {:noreply, state}
    else
      send(self(), :smolnet_drain_ingress)
      {:noreply, %{state | draining: true}}
    end
  end

  def handle_info(:smolnet_drain_ingress, state) do
    case :queue.out(state.ingress_queue) do
      {{:value, packet}, queue} ->
        state = %{state | ingress_queue: queue}
        state = process_ingress(state, packet)
        continue_drain(state)

      {:empty, _queue} ->
        {:noreply, %{state | draining: false}}
    end
  end

  def handle_info({:smolnet_poll, generation}, %{timer_generation: generation} = state) do
    now = state.clock.now()
    state = %{state | timer: nil}

    case state.native_module.stack_poll(state.native, now) do
      {:ok, envelope} -> {:noreply, apply_effects(state, envelope)}
      {:error, _reason} -> {:noreply, replace_timer(state, nil)}
    end
  end

  def handle_info({:smolnet_poll, _stale_generation}, state), do: {:noreply, state}

  def handle_info(
        {:DOWN, monitor, :process, pid, reason},
        %{link_monitor: monitor, egress_pid: pid} = state
      ) do
    handle_link_down(state, reason)
  end

  def handle_info(
        {:smolnet_stack_accepted, ready_ref},
        %{
          ready_ref: ready_ref,
          starter_monitor: monitor
        } = state
      )
      when is_reference(monitor) do
    Process.demonitor(monitor, [:flush])
    {:noreply, %{state | starter_monitor: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_call(:native_snapshot, _from, state) do
    {:reply, state.native_module.stack_snapshot(state.native), state}
  end

  def handle_call(:test_contention, _from, state) do
    {:reply, state.native_module.test_contention(state.native), state}
  end

  def handle_call({:test_bounded_work, requested}, _from, state) do
    {:reply, state.native_module.test_bounded_work(state.native, requested), state}
  end

  def handle_call(:stack_info, _from, state) do
    native =
      case state.native_module.stack_snapshot(state.native) do
        {:ok, envelope} -> envelope
        {:error, _reason} = error -> error
      end

    info = %{
      ingress: IngressGate.snapshot(state.ingress_gate),
      processed_ingress: state.processed_ingress,
      failed_ingress: state.failed_ingress,
      dropped_egress: state.dropped_egress,
      link_status: state.link_status,
      timer_generation: state.timer_generation,
      poll_at: timer_deadline(state.timer),
      native: native
    }

    {:reply, {:ok, info}, state}
  end

  @impl true
  def terminate(_reason, state) do
    IngressGate.close(state.ingress_gate)

    if state.timer do
      _cancel_result = state.clock.cancel_timer(state.timer.ref)
    end

    :ok
  end

  @spec default_limits() :: limits()
  def default_limits, do: @default_limits

  defp validate_packet(packet, mtu) when is_binary(packet) and byte_size(packet) >= 40 do
    case packet do
      <<6::4, _traffic_and_flow::28, payload_length::16, _rest::binary>> ->
        cond do
          byte_size(packet) != 40 + payload_length -> {:error, :invalid_packet}
          byte_size(packet) > mtu -> {:error, :packet_too_large}
          true -> :ok
        end

      <<4::4, _rest::bitstring>> ->
        {:error, :unsupported_family}

      _other ->
        {:error, :invalid_packet}
    end
  end

  defp validate_packet(packet, _mtu) when is_binary(packet) do
    case packet do
      <<4::4, _rest::bitstring>> -> {:error, :unsupported_family}
      _other -> {:error, :invalid_packet}
    end
  end

  defp validate_packet(_packet, _mtu), do: {:error, :invalid_packet}

  defp egress_state(nil), do: {nil, nil, nil, :down}

  defp egress_state({pid, link_ref}) do
    {pid, link_ref, Process.monitor(pid), :up}
  end

  defp process_ingress(state, packet) do
    result =
      state.native_module.stack_ingress(
        state.native,
        packet,
        state.clock.now()
      )

    IngressGate.release(state.ingress_gate, byte_size(packet))

    case result do
      {:ok, envelope} ->
        state
        |> Map.update!(:processed_ingress, &(&1 + 1))
        |> apply_effects(envelope)

      {:error, _reason} ->
        Map.update!(state, :failed_ingress, &(&1 + 1))
    end
  end

  defp continue_drain(state) do
    if :queue.is_empty(state.ingress_queue) do
      {:noreply, %{state | draining: false}}
    else
      send(self(), :smolnet_drain_ingress)
      {:noreply, state}
    end
  end

  defp apply_effects(state, envelope) do
    state = emit_packets(state, Map.get(envelope, :output, []))

    if Map.get(envelope, :more, false) do
      state = replace_timer(state, nil)
      send(self(), {:smolnet_poll, state.timer_generation})
      state
    else
      replace_timer(state, Map.get(envelope, :poll_at))
    end
  end

  defp emit_packets(%{link_status: :up} = state, packets) do
    Enum.each(packets, fn packet ->
      send(state.egress_pid, {:smol_stack, state.link_ref, :egress, packet})
    end)

    state
  end

  defp emit_packets(state, packets) do
    Map.update!(state, :dropped_egress, &(&1 + length(packets)))
  end

  defp replace_timer(state, poll_at) do
    if state.timer do
      _cancel_result = state.clock.cancel_timer(state.timer.ref)
    end

    generation = state.timer_generation + 1

    timer =
      if is_integer(poll_at) do
        now = state.clock.now()
        delay = poll_at |> Kernel.-(now) |> max(0) |> min(4_294_967_295)
        ref = state.clock.send_after(self(), {:smolnet_poll, generation}, delay)
        %{ref: ref, deadline: poll_at}
      end

    %{state | timer: timer, timer_generation: generation}
  end

  defp handle_link_down(%{link_down: :stop} = state, reason) do
    IngressGate.close(state.ingress_gate)
    {:stop, {:shutdown, {:link_down, reason}}, state}
  end

  defp handle_link_down(%{link_down: :mark_down} = state, _reason) do
    IngressGate.mark_down(state.ingress_gate)
    {:noreply, %{state | link_status: :down, link_monitor: nil}}
  end

  defp handle_link_down(%{link_down: {:notify, recipient}} = state, reason) do
    IngressGate.mark_down(state.ingress_gate)
    send(recipient, {:smol_stack, state.link_ref, :link_down, reason})
    {:noreply, %{state | link_status: :down, link_monitor: nil}}
  end

  defp timer_deadline(nil), do: nil
  defp timer_deadline(timer), do: timer.deadline
end
