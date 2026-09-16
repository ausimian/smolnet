defmodule SmolNet.Stack do
  @moduledoc false

  use GenServer, restart: :temporary, significant: true

  alias SmolNet.Native
  alias SmolNet.Socket
  alias SmolNet.Stack.Clock.System, as: SystemClock
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
  Hands one complete raw IPv6 packet from the single feeder to its stack.

  The call returns when the stack process accepts the packet. Native processing
  then runs as a continuation before the stack accepts another message.
  """
  @spec ingress(Ref.t(), binary()) ::
          :ok
          | {:error,
             :invalid_packet
             | :unsupported_family
             | :packet_too_large
             | :busy
             | :link_down
             | :closed}
  def ingress(%Ref{stack: stack, ingress_token: ingress_token}, packet) do
    GenServer.call(stack, {:ingress, ingress_token, packet}, :infinity)
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec cancel(Socket.t(), :socket.select_info()) ::
          :ok | :already_sent | :not_found | {:error, :closed | :invalid_socket}
  def cancel(
        %Socket{stack: stack, id: id, generation: generation},
        {:select_info, operation, reference}
      ) do
    GenServer.call(stack, {:socket_cancel, id, generation, operation, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_open(term()) :: {:ok, Socket.t()} | {:error, atom()}
  def socket_open(%Ref{stack: stack}) do
    GenServer.call(stack, :socket_open)
  catch
    :exit, _reason -> {:error, :closed}
  end

  def socket_open(_stack), do: {:error, :invalid_options}

  @doc false
  @spec socket_bind(Socket.t(), map()) :: :ok | {:error, atom()}
  def socket_bind(%Socket{stack: stack, id: id, generation: generation}, endpoint) do
    GenServer.call(stack, {:socket_bind, id, generation, endpoint})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_connect(Socket.t(), map()) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  def socket_connect(%Socket{stack: stack, id: id, generation: generation}, endpoint) do
    reference = make_ref()
    GenServer.call(stack, {:socket_connect, id, generation, endpoint, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_sockname(Socket.t()) :: {:ok, Socket.sockaddr_in6()} | {:error, atom()}
  def socket_sockname(%Socket{stack: stack, id: id, generation: generation}) do
    GenServer.call(stack, {:socket_sockname, id, generation})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_peername(Socket.t()) :: {:ok, Socket.sockaddr_in6()} | {:error, atom()}
  def socket_peername(%Socket{stack: stack, id: id, generation: generation}) do
    GenServer.call(stack, {:socket_peername, id, generation})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_close(Socket.t()) :: :ok | {:error, atom()}
  def socket_close(%Socket{stack: stack, id: id, generation: generation}) do
    GenServer.call(stack, {:socket_close, id, generation})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  def test_socket_open(%Ref{stack: stack}, internal_handle) do
    GenServer.call(stack, {:test_socket_open, internal_handle})
  end

  @doc false
  def test_socket_wait(
        %Socket{stack: stack, id: id, generation: generation},
        direction,
        operation,
        arm_point,
        wake_count,
        completed
      ) do
    reference = make_ref()

    GenServer.call(
      stack,
      {:test_socket_wait, id, generation, direction, operation, reference, arm_point, wake_count,
       completed}
    )
  end

  @doc false
  def test_socket_ready(keys) do
    [{%Socket{stack: stack}, _direction} | _rest] = keys

    encoded =
      Enum.map(keys, fn
        {%Socket{stack: ^stack, id: id, generation: generation}, direction} ->
          %{identity: %{id: id, generation: generation}, direction: direction}
      end)

    GenServer.call(stack, {:test_socket_ready, encoded})
  end

  @doc false
  def test_socket_close(
        %Socket{stack: stack, id: id, generation: generation},
        wake_direction \\ nil
      ) do
    GenServer.call(stack, {:test_socket_close, id, generation, wake_direction})
  end

  @doc false
  @spec shutdown_waiters(pid()) :: :ok | {:error, atom()}
  def shutdown_waiters(stack) do
    GenServer.call(stack, :shutdown_waiters)
  catch
    :exit, _reason -> {:error, :closed}
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
    Process.flag(:trap_exit, true)

    starter = Keyword.fetch!(options, :starter)
    egress = Keyword.get(options, :egress)

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
      ingress_token: Keyword.get_lazy(options, :ingress_token, &make_ref/0),
      timer: nil,
      timer_generation: 0,
      processed_ingress: 0,
      failed_ingress: 0,
      rejected_ingress: 0,
      dropped_egress: 0,
      native_continuation: false,
      pending_ingress: nil
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

  def handle_continue({:process_ingress, packet}, state) do
    state
    |> process_ingress(packet)
    |> continue_pending_ingress()
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

  def handle_info({:smolnet_poll, generation}, %{timer_generation: generation} = state) do
    now = state.clock.now()
    state = %{state | timer: nil}

    case state.native_module.stack_poll(state.native, now) do
      {:ok, envelope} ->
        state
        |> apply_effects(envelope)
        |> continue_pending_ingress()

      {:error, reason} when state.native_continuation ->
        {:stop, {:shutdown, {:native_poll_failed, reason}}, state}

      {:error, _reason} ->
        {:noreply, replace_timer(state, nil)}
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
  def handle_call(
        {:ingress, ingress_token, packet},
        from,
        %{ingress_token: ingress_token} = state
      ) do
    case validate_packet(packet, state.native_config.mtu) do
      :ok when state.link_status == :up ->
        accept_ingress(state, from, packet)

      :ok ->
        reject_ingress(state, :link_down)

      {:error, reason} ->
        reject_ingress(state, reason)
    end
  end

  def handle_call({:ingress, _invalid_token, _packet}, _from, state) do
    {:reply, {:error, :invalid_ingress}, state}
  end

  def handle_call(:native_snapshot, _from, state) do
    {:reply, state.native_module.stack_snapshot(state.native), state}
  end

  def handle_call(:test_contention, _from, state) do
    {:reply, state.native_module.test_contention(state.native), state}
  end

  def handle_call({:test_bounded_work, requested}, _from, state) do
    {:reply, state.native_module.test_bounded_work(state.native, requested), state}
  end

  def handle_call(:shutdown_waiters, _from, state) do
    state.native_module.stack_shutdown(state.native)
    |> reply_native(state)
  end

  def handle_call(
        {:socket_cancel, id, generation, operation, reference},
        _from,
        state
      ) do
    state.native_module.socket_cancel(
      state.native,
      %{id: id, generation: generation},
      operation,
      reference
    )
    |> reply_native(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(:socket_open, _from, state) do
    state.native_module.tcp_open(state.native)
    |> reply_native(
      state,
      fn identity -> {:ok, Socket.new(self(), identity)} end,
      :preserve_timer
    )
  end

  def handle_call({:socket_bind, id, generation, endpoint}, _from, state) do
    state.native_module.tcp_bind(state.native, %{id: id, generation: generation}, endpoint)
    |> reply_native(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(
        {:socket_connect, id, generation, endpoint, reference},
        {caller, _tag},
        state
      ) do
    state.native_module.tcp_connect(
      state.native,
      %{id: id, generation: generation},
      endpoint,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_native(state, &normalize_wait_result/1)
  end

  def handle_call({:socket_sockname, id, generation}, _from, state) do
    state.native_module.tcp_sockname(state.native, %{id: id, generation: generation})
    |> reply_native(state, &normalize_endpoint/1, :preserve_timer)
  end

  def handle_call({:socket_peername, id, generation}, _from, state) do
    state.native_module.tcp_peername(state.native, %{id: id, generation: generation})
    |> reply_native(state, &normalize_endpoint/1, :preserve_timer)
  end

  def handle_call({:socket_close, id, generation}, _from, state) do
    state.native_module.tcp_close(
      state.native,
      %{id: id, generation: generation},
      state.clock.now()
    )
    |> reply_native(state)
  end

  def handle_call({:test_socket_open, internal_handle}, _from, state) do
    result = state.native_module.test_socket_open(state.native, internal_handle)

    reply_native(
      result,
      state,
      fn identity -> Socket.new(self(), identity) end,
      :preserve_timer
    )
  end

  def handle_call(
        {:test_socket_wait, id, generation, direction, operation, reference, arm_point,
         wake_count, completed},
        {caller, _tag},
        state
      ) do
    state.native_module.test_socket_wait(
      state.native,
      %{id: id, generation: generation},
      %{
        direction: direction,
        operation: operation,
        pid: caller,
        reference: reference,
        arm_point: arm_point,
        wake_count: wake_count,
        completed: completed
      }
    )
    |> reply_native(state, &normalize_wait_result/1, :preserve_timer)
  end

  def handle_call({:test_socket_ready, keys}, _from, state) do
    state.native_module.test_socket_ready(state.native, keys)
    |> reply_native(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(
        {:test_socket_close, id, generation, wake_direction},
        _from,
        state
      ) do
    state.native_module.test_socket_close(
      state.native,
      %{id: id, generation: generation},
      wake_direction
    )
    |> reply_native(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(:stack_info, _from, state) do
    native =
      case state.native_module.stack_snapshot(state.native) do
        {:ok, envelope} -> envelope
        {:error, _reason} = error -> error
      end

    info = %{
      ingress: %{mode: :single_feeder, rejected: state.rejected_ingress},
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
    if state.timer do
      _cancel_result = state.clock.cancel_timer(state.timer.ref)
    end

    if state.native_module && state.native do
      _shutdown_result = state.native_module.stack_shutdown(state.native)
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

    case result do
      {:ok, envelope} ->
        state
        |> Map.update!(:processed_ingress, &(&1 + 1))
        |> apply_effects(envelope)

      {:error, _reason} ->
        state
        |> Map.update!(:failed_ingress, &(&1 + 1))
        |> Map.put(:native_continuation, false)
    end
  end

  defp apply_effects(state, envelope, timer_policy \\ :replace_timer) do
    state = emit_packets(state, Map.get(envelope, :output, []))
    more = Map.get(envelope, :more, false)
    poll_at = Map.get(envelope, :poll_at)

    cond do
      more ->
        state =
          state
          |> Map.put(:native_continuation, true)
          |> replace_timer(nil)

        send(self(), {:smolnet_poll, state.timer_generation})
        state

      timer_policy == :preserve_timer and is_nil(poll_at) ->
        state

      true ->
        state = Map.put(state, :native_continuation, false)
        replace_timer(state, poll_at)
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
    {:stop, {:shutdown, {:link_down, reason}}, state}
  end

  defp handle_link_down(%{link_down: :mark_down} = state, _reason) do
    {:noreply, %{state | link_status: :down, link_monitor: nil}}
  end

  defp handle_link_down(%{link_down: {:notify, recipient}} = state, reason) do
    send(recipient, {:smol_stack, state.link_ref, :link_down, reason})
    {:noreply, %{state | link_status: :down, link_monitor: nil}}
  end

  defp reject_ingress(state, reason) do
    state = Map.update!(state, :rejected_ingress, &(&1 + 1))
    {:reply, {:error, reason}, state}
  end

  defp accept_ingress(%{native_continuation: false} = state, _from, packet) do
    {:reply, :ok, state, {:continue, {:process_ingress, packet}}}
  end

  defp accept_ingress(%{pending_ingress: nil} = state, from, packet) do
    {:noreply, %{state | pending_ingress: {from, packet}}}
  end

  defp accept_ingress(state, _from, _packet), do: reject_ingress(state, :busy)

  defp continue_pending_ingress(%{native_continuation: true} = state) do
    {:noreply, state}
  end

  defp continue_pending_ingress(%{pending_ingress: nil} = state) do
    {:noreply, state}
  end

  defp continue_pending_ingress(%{pending_ingress: {from, packet}} = state) do
    GenServer.reply(from, :ok)
    state = %{state | pending_ingress: nil}
    {:noreply, state, {:continue, {:process_ingress, packet}}}
  end

  defp timer_deadline(nil), do: nil
  defp timer_deadline(timer), do: timer.deadline

  defp reply_native(
         result,
         state,
         transform \\ &Function.identity/1,
         timer_policy \\ :replace_timer
       )

  defp reply_native({:ok, envelope}, state, transform, timer_policy) do
    result = envelope |> Map.fetch!(:result) |> transform.()
    {:reply, result, apply_effects(state, envelope, timer_policy)}
  end

  defp reply_native({:error, reason}, state, _transform, _timer_policy) do
    {:reply, {:error, reason}, state}
  end

  defp normalize_wait_result(:ready), do: :ready
  defp normalize_wait_result(:ok), do: :ok

  defp normalize_wait_result({:select, operation, reference}) do
    {:select, {:select_info, operation, reference}}
  end

  defp normalize_endpoint(endpoint), do: {:ok, Socket.endpoint_from_native(endpoint)}
end
