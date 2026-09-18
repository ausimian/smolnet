defmodule SmolNet.Stack do
  @moduledoc false

  use GenServer, restart: :temporary, significant: true

  alias SmolNet.Native
  alias SmolNet.Socket
  alias SmolNet.Stack.Clock.System, as: SystemClock
  alias SmolNet.Stack.Options
  alias SmolNet.Stack.Ref

  import Bitwise, only: [band: 2, bnot: 1]

  @default_limits %{
    bytes_copied: 64 * 1024,
    output_packets: 32,
    ready_events: 128,
    maintenance_work: 128
  }

  # Valid bounded state converges well below this guard. The guard counts NIF
  # invocations rather than work units because a runtime deadline can expire
  # before a cleanup unit; its purpose is to bound a non-convergent fault.
  @shutdown_continuation_limit 1_024

  @type limits :: %{
          bytes_copied: pos_integer(),
          output_packets: pos_integer(),
          ready_events: pos_integer(),
          maintenance_work: pos_integer()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @doc """
  Hands one complete raw IPv4 or IPv6 packet from the single feeder to its stack.

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
  @spec socket_open(term(), :inet | :inet6, :stream | :datagram) ::
          {:ok, Socket.t()} | {:error, atom()}
  def socket_open(%Ref{stack: stack}, family, kind)
      when family in [:inet, :inet6] and kind in [:stream, :datagram] do
    GenServer.call(stack, {:socket_open, family, kind})
  catch
    :exit, _reason -> {:error, :closed}
  end

  def socket_open(_stack, _family, _kind), do: {:error, :invalid_options}

  @doc false
  @spec socket_bind(Socket.t(), map()) :: :ok | {:error, atom()}
  def socket_bind(%Socket{stack: stack, id: id, generation: generation, kind: kind}, endpoint) do
    GenServer.call(stack, {:socket_bind, id, generation, kind, endpoint})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_listen(Socket.t(), pos_integer()) :: :ok | {:error, atom()}
  def socket_listen(%Socket{stack: stack, id: id, generation: generation}, backlog) do
    GenServer.call(stack, {:socket_listen, id, generation, backlog})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_accept(Socket.t()) ::
          {:ok, Socket.t()} | {:select, :socket.select_info()} | {:error, atom()}
  def socket_accept(%Socket{stack: stack, id: id, generation: generation}) do
    reference = make_ref()
    GenServer.call(stack, {:socket_accept, id, generation, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_accept(Socket.t(), pid()) ::
          {:ok, Socket.t()} | {:select, :socket.select_info()} | {:error, atom()}
  def socket_accept(
        %Socket{stack: stack, id: id, generation: generation},
        owner
      )
      when is_pid(owner) do
    reference = make_ref()
    GenServer.call(stack, {:socket_accept_owned, id, generation, reference, owner})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_connect(Socket.t(), map()) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  def socket_connect(
        %Socket{stack: stack, id: id, generation: generation, kind: kind},
        endpoint
      ) do
    reference = make_ref()
    GenServer.call(stack, {:socket_connect, id, generation, kind, endpoint, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_send(Socket.t(), binary()) ::
          :ok | {:select, {:socket.select_info(), binary()}} | {:error, atom()}
  def socket_send(%Socket{stack: stack, id: id, generation: generation}, data) do
    reference = make_ref()
    GenServer.call(stack, {:socket_send, id, generation, data, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_recv(Socket.t(), non_neg_integer()) ::
          {:ok, binary()}
          | {:select, :socket.select_info()}
          | {:select, {:socket.select_info(), binary()}}
          | {:error, atom()}
  def socket_recv(%Socket{stack: stack, id: id, generation: generation}, length) do
    reference = make_ref()
    GenServer.call(stack, {:socket_recv, id, generation, length, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_sendto(Socket.t(), map(), binary()) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  def socket_sendto(
        %Socket{stack: stack, id: id, generation: generation},
        endpoint,
        data
      ) do
    reference = make_ref()
    GenServer.call(stack, {:socket_sendto, id, generation, endpoint, data, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_recvfrom(Socket.t(), non_neg_integer()) ::
          {:ok, Socket.datagram()} | {:select, :socket.select_info()} | {:error, atom()}
  def socket_recvfrom(%Socket{stack: stack, id: id, generation: generation}, length) do
    reference = make_ref()
    GenServer.call(stack, {:socket_recvfrom, id, generation, length, reference})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_shutdown(Socket.t(), :read | :write | :read_write) ::
          :ok | {:error, atom()}
  def socket_shutdown(%Socket{stack: stack, id: id, generation: generation}, how) do
    GenServer.call(stack, {:socket_shutdown, id, generation, how})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_sockname(Socket.t()) ::
          {:ok, Socket.sockaddr_in() | Socket.sockaddr_in6()} | {:error, atom()}
  def socket_sockname(%Socket{stack: stack, id: id, generation: generation, kind: kind}) do
    GenServer.call(stack, {:socket_sockname, id, generation, kind})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_peername(Socket.t()) ::
          {:ok, Socket.sockaddr_in() | Socket.sockaddr_in6()} | {:error, atom()}
  def socket_peername(%Socket{stack: stack, id: id, generation: generation, kind: kind}) do
    GenServer.call(stack, {:socket_peername, id, generation, kind})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_close(Socket.t()) :: :ok | {:error, atom()}
  def socket_close(%Socket{stack: stack, id: id, generation: generation, kind: kind}) do
    GenServer.call(stack, {:socket_close, id, generation, kind})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_watch_owner(Socket.t(), pid()) :: :ok | {:error, :closed}
  def socket_watch_owner(%Socket{stack: stack, id: id, generation: generation}, owner)
      when is_pid(owner) do
    GenServer.call(stack, {:socket_watch_owner, id, generation, owner})
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
    GenServer.call(stack, :shutdown_waiters, :infinity)
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
      shutdown_requested: false,
      shutdown_drain_attempted: false,
      pending_ingress: nil,
      socket_owner_monitors: %{},
      socket_owner_monitors_by_identity: %{}
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
        state |> replace_timer(nil) |> continue_pending_ingress()
    end
  end

  # A poll can go stale while a feeder's packet is held: the `more: true`
  # branch of apply_effects/3 self-sends the poll, and any socket call queued
  # ahead of it bumps the generation again. The held packet was waiting on
  # that poll, so drain here whenever no live continuation will do it, or the
  # feeder blocks in SmolNet.ingress/2 with nothing left to release it.
  def handle_info({:smolnet_poll, _stale_generation}, state), do: continue_pending_ingress(state)

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

  def handle_info({:DOWN, monitor, :process, _owner, _reason}, state) do
    case Map.pop(state.socket_owner_monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {{identity, kind}, monitors} ->
        state = %{
          state
          | socket_owner_monitors: monitors,
            socket_owner_monitors_by_identity:
              Map.delete(state.socket_owner_monitors_by_identity, identity_key(identity))
        }

        close_owned_socket(state, identity, kind)
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_call(
        {:ingress, ingress_token, packet},
        from,
        %{ingress_token: ingress_token} = state
      ) do
    case validate_packet(packet, state.native_config.mtu) do
      :ok when state.shutdown_requested ->
        reject_ingress(state, :closed)

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
    state = state |> Map.put(:shutdown_requested, true) |> reject_pending_ingress(:closed)
    reply = drain_native_shutdown(state)

    state =
      state
      |> Map.put(:native_continuation, false)
      |> Map.put(:shutdown_drain_attempted, shutdown_drain_terminal?(reply))
      |> replace_timer(nil)

    {:reply, reply, state}
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

  def handle_call({:socket_open, family, kind}, _from, state) do
    result =
      case kind do
        :stream -> state.native_module.tcp_open(state.native, family)
        :datagram -> state.native_module.udp_open(state.native, family)
      end

    result
    |> reply_native(
      state,
      fn identity -> {:ok, Socket.new(self(), identity, family, kind)} end,
      :preserve_timer
    )
  end

  def handle_call({:socket_bind, id, generation, kind, endpoint}, _from, state) do
    identity = %{id: id, generation: generation}

    result =
      case kind do
        :stream -> state.native_module.tcp_bind(state.native, identity, endpoint)
        :datagram -> state.native_module.udp_bind(state.native, identity, endpoint)
      end

    result
    |> reply_native(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call({:socket_listen, id, generation, backlog}, _from, state) do
    state.native_module.tcp_listen(
      state.native,
      %{id: id, generation: generation},
      backlog,
      state.clock.now()
    )
    |> reply_native(state)
  end

  def handle_call(
        {:socket_accept, id, generation, reference},
        {caller, _tag},
        state
      ) do
    state.native_module.tcp_accept(
      state.native,
      %{id: id, generation: generation},
      caller,
      reference,
      state.clock.now()
    )
    |> reply_native(state, &normalize_accept_result(&1, self()))
  end

  def handle_call(
        {:socket_accept_owned, id, generation, reference, owner},
        {caller, _tag},
        state
      ) do
    state.native_module.tcp_accept(
      state.native,
      %{id: id, generation: generation},
      caller,
      reference,
      state.clock.now()
    )
    |> reply_owned_accept(state, owner)
  end

  def handle_call(
        {:socket_connect, id, generation, :stream, endpoint, reference},
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

  def handle_call(
        {:socket_connect, id, generation, :datagram, endpoint, _reference},
        _from,
        state
      ) do
    state.native_module.udp_connect(
      state.native,
      %{id: id, generation: generation},
      endpoint
    )
    |> reply_native(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(
        {:socket_send, id, generation, data, reference},
        {caller, _tag},
        state
      ) do
    state.native_module.tcp_send(
      state.native,
      %{id: id, generation: generation},
      data,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_native(state, &normalize_send_result(&1, data))
  end

  def handle_call(
        {:socket_recv, id, generation, length, reference},
        {caller, _tag},
        state
      ) do
    state.native_module.tcp_recv(
      state.native,
      %{id: id, generation: generation},
      length,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_native(state, &normalize_recv_result/1)
  end

  def handle_call(
        {:socket_sendto, id, generation, endpoint, data, reference},
        {caller, _tag},
        state
      ) do
    state.native_module.udp_sendto(
      state.native,
      %{id: id, generation: generation},
      endpoint,
      data,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_native(state, &normalize_datagram_wait_result/1)
  end

  def handle_call(
        {:socket_recvfrom, id, generation, length, reference},
        {caller, _tag},
        state
      ) do
    state.native_module.udp_recvfrom(
      state.native,
      %{id: id, generation: generation},
      length,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_native(state, &normalize_recvfrom_result/1)
  end

  def handle_call({:socket_shutdown, id, generation, how}, _from, state) do
    state.native_module.tcp_shutdown(
      state.native,
      %{id: id, generation: generation},
      how,
      state.clock.now()
    )
    |> reply_native(state)
  end

  def handle_call({:socket_sockname, id, generation, kind}, _from, state) do
    identity = %{id: id, generation: generation}
    result = socket_name_call(state, kind, :sockname, identity)

    result
    |> reply_native(state, &normalize_endpoint/1, :preserve_timer)
  end

  def handle_call({:socket_peername, id, generation, kind}, _from, state) do
    identity = %{id: id, generation: generation}
    result = socket_name_call(state, kind, :peername, identity)

    result
    |> reply_native(state, &normalize_endpoint/1, :preserve_timer)
  end

  def handle_call({:socket_close, id, generation, kind}, _from, state) do
    identity = %{id: id, generation: generation}

    result = close_socket_call(state, kind, identity)

    case result do
      {:ok, envelope} ->
        state = state |> unwatch_socket_owner(identity) |> apply_effects(envelope)
        {:reply, Map.fetch!(envelope, :result), state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:socket_watch_owner, id, generation, owner}, _from, state) do
    identity = %{id: id, generation: generation}

    case state.native_module.socket_validate(state.native, identity) do
      {:ok, %{result: native_kind} = envelope} ->
        state =
          state
          |> apply_effects(envelope, :preserve_timer)
          |> watch_socket_owner(identity, native_kind, owner)

        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
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
    state = reject_pending_ingress(state, :closed)

    if state.timer do
      _cancel_result = state.clock.cancel_timer(state.timer.ref)
    end

    if state.native_module && state.native && !state.shutdown_drain_attempted do
      _shutdown_result = drain_native_shutdown(state)
    end

    :ok
  end

  @spec default_limits() :: limits()
  def default_limits, do: @default_limits

  defp validate_packet(packet, mtu) when is_binary(packet) and byte_size(packet) >= 20 do
    case packet do
      <<6::4, _rest::bitstring>> ->
        validate_ipv6_packet(packet, mtu)

      <<4::4, ihl::4, _dscp::8, total_length::16, _id::16, flags::3, fragment_offset::13,
        _rest::binary>> ->
        validate_ipv4_packet(packet, mtu, ihl, total_length, flags, fragment_offset)

      _other ->
        {:error, :invalid_packet}
    end
  end

  defp validate_packet(packet, _mtu) when is_binary(packet), do: {:error, :invalid_packet}

  defp validate_packet(_packet, _mtu), do: {:error, :invalid_packet}

  defp validate_ipv6_packet(packet, mtu) do
    case packet do
      <<6::4, _traffic_and_flow::28, payload_length::16, _rest::binary>> ->
        cond do
          byte_size(packet) < 40 -> {:error, :invalid_packet}
          byte_size(packet) != 40 + payload_length -> {:error, :invalid_packet}
          byte_size(packet) > mtu -> {:error, :packet_too_large}
          true -> :ok
        end

      _packet ->
        {:error, :invalid_packet}
    end
  end

  defp validate_ipv4_packet(packet, mtu, ihl, total_length, flags, fragment_offset) do
    header_length = ihl * 4

    cond do
      ihl < 5 -> {:error, :invalid_packet}
      total_length != byte_size(packet) -> {:error, :invalid_packet}
      header_length > byte_size(packet) -> {:error, :invalid_packet}
      band(flags, 1) != 0 or fragment_offset != 0 -> {:error, :invalid_packet}
      not valid_ipv4_checksum?(binary_part(packet, 0, header_length)) -> {:error, :invalid_packet}
      byte_size(packet) > mtu -> {:error, :packet_too_large}
      true -> :ok
    end
  end

  defp valid_ipv4_checksum?(header) do
    header
    |> :binary.bin_to_list()
    |> Enum.chunk_every(2)
    |> Enum.reduce(0, fn [high, low], sum -> sum + high * 256 + low end)
    |> fold_checksum()
    |> bnot()
    |> band(0xFFFF)
    |> Kernel.==(0)
  end

  defp fold_checksum(sum) when sum > 0xFFFF,
    do: fold_checksum(band(sum, 0xFFFF) + div(sum, 0x10000))

  defp fold_checksum(sum), do: sum

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

  defp close_owned_socket(state, identity, kind) do
    case close_socket_call(state, native_socket_kind(kind), identity) do
      {:ok, envelope} -> {:noreply, apply_effects(state, envelope)}
      {:error, _reason} -> {:noreply, state}
    end
  end

  defp close_socket_call(state, :stream, identity) do
    state.native_module.tcp_close(state.native, identity, state.clock.now())
  end

  defp close_socket_call(state, :datagram, identity) do
    state.native_module.udp_close(state.native, identity, state.clock.now())
  end

  defp socket_name_call(state, :stream, :sockname, identity),
    do: state.native_module.tcp_sockname(state.native, identity)

  defp socket_name_call(state, :stream, :peername, identity),
    do: state.native_module.tcp_peername(state.native, identity)

  defp socket_name_call(state, :datagram, :sockname, identity),
    do: state.native_module.udp_sockname(state.native, identity)

  defp socket_name_call(state, :datagram, :peername, identity),
    do: state.native_module.udp_peername(state.native, identity)

  defp native_socket_kind(:tcp), do: :stream
  defp native_socket_kind(:udp), do: :datagram

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

  defp reject_pending_ingress(%{pending_ingress: nil} = state, _reason), do: state

  defp reject_pending_ingress(%{pending_ingress: {from, _packet}} = state, reason) do
    GenServer.reply(from, {:error, reason})
    %{state | pending_ingress: nil}
  end

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

  defp drain_native_shutdown(state) do
    case state.native_module.stack_shutdown(state.native) do
      {:ok, envelope} ->
        continue_native_shutdown(state, envelope, @shutdown_continuation_limit)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp continue_native_shutdown(_state, %{more: false}, _remaining), do: :ok

  defp continue_native_shutdown(_state, %{more: true}, 0),
    do: {:error, :shutdown_incomplete}

  defp continue_native_shutdown(state, %{more: true}, remaining) do
    case state.native_module.stack_poll(state.native, state.clock.now()) do
      {:ok, envelope} -> continue_native_shutdown(state, envelope, remaining - 1)
      {:error, reason} -> {:error, reason}
    end
  end

  # A try-lock collision is transient, so terminate/2 gets one more attempt.
  # Panics and a non-convergent bounded drain must not repeat expensive work.
  defp shutdown_drain_terminal?({:error, :ownership_invariant_violation}), do: false
  defp shutdown_drain_terminal?(_result), do: true

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

  defp reply_owned_accept({:ok, envelope}, state, owner) do
    case Map.fetch!(envelope, :result) do
      {:ok, identity, family} ->
        state = state |> apply_effects(envelope) |> watch_socket_owner(identity, :tcp, owner)
        {:reply, {:ok, Socket.new(self(), identity, family)}, state}

      result ->
        {:reply, normalize_accept_result(result, self()), apply_effects(state, envelope)}
    end
  end

  defp reply_owned_accept({:error, reason}, state, _owner) do
    {:reply, {:error, reason}, state}
  end

  defp normalize_wait_result(:ready), do: :ready
  defp normalize_wait_result(:ok), do: :ok

  defp normalize_wait_result({:select, operation, reference}) do
    {:select, {:select_info, operation, reference}}
  end

  defp normalize_datagram_wait_result(:ok), do: :ok

  defp normalize_datagram_wait_result({:select, :sendto, reference}) do
    {:select, {:select_info, :sendto, reference}}
  end

  defp normalize_recvfrom_result({:ok, source, destination, data, truncated})
       when is_binary(data) and is_boolean(truncated) do
    {:ok,
     %{
       source: Socket.endpoint_from_native(source),
       destination: Socket.endpoint_from_native(destination),
       data: data,
       truncated: truncated
     }}
  end

  defp normalize_recvfrom_result({:select, :recvfrom, reference}) do
    {:select, {:select_info, :recvfrom, reference}}
  end

  defp normalize_accept_result({:ok, identity, family}, stack) do
    {:ok, Socket.new(stack, identity, family)}
  end

  defp normalize_accept_result({:select, :accept, reference}, _stack) do
    {:select, {:select_info, :accept, reference}}
  end

  defp normalize_send_result(:ok, _data), do: :ok

  defp normalize_send_result({:select, :send, reference, accepted}, data)
       when accepted >= 0 and accepted <= byte_size(data) do
    remainder = binary_part(data, accepted, byte_size(data) - accepted)
    {:select, {{:select_info, :send, reference}, remainder}}
  end

  defp normalize_recv_result({:ok, data}) when is_binary(data), do: {:ok, data}

  defp normalize_recv_result({:select, :recv, reference}) do
    {:select, {:select_info, :recv, reference}}
  end

  defp normalize_recv_result({:select, :recv, reference, data}) when is_binary(data) do
    {:select, {{:select_info, :recv, reference}, data}}
  end

  defp normalize_endpoint(endpoint), do: {:ok, Socket.endpoint_from_native(endpoint)}

  defp watch_socket_owner(state, identity, native_kind, owner) do
    state = unwatch_socket_owner(state, identity)
    key = identity_key(identity)
    monitor = Process.monitor(owner)

    %{
      state
      | socket_owner_monitors:
          Map.put(state.socket_owner_monitors, monitor, {identity, native_kind}),
        socket_owner_monitors_by_identity:
          Map.put(state.socket_owner_monitors_by_identity, key, monitor)
    }
  end

  defp unwatch_socket_owner(state, identity) do
    key = identity_key(identity)

    case Map.pop(state.socket_owner_monitors_by_identity, key) do
      {nil, _monitors_by_identity} ->
        state

      {monitor, monitors_by_identity} ->
        Process.demonitor(monitor, [:flush])

        %{
          state
          | socket_owner_monitors: Map.delete(state.socket_owner_monitors, monitor),
            socket_owner_monitors_by_identity: monitors_by_identity
        }
    end
  end

  defp identity_key(%{id: id, generation: generation}), do: {id, generation}
end
