defmodule SmolNet.Stack do
  @moduledoc false

  use GenServer, restart: :temporary, significant: true

  alias SmolNet.Socket
  alias SmolNet.Stack.Backend
  alias SmolNet.Stack.Clock.System, as: SystemClock
  alias SmolNet.Stack.Options
  alias SmolNet.Stack.Ref

  import Bitwise, only: [band: 2, bnot: 1]

  @default_limits %{
    bytes_copied: 64 * 1024,
    input_packets: 1,
    output_packets: 32,
    ready_events: 128,
    maintenance_work: 128,
    sockets: 64
  }

  # Valid bounded state converges well below this guard. The guard counts NIF
  # invocations rather than work units because a runtime deadline can expire
  # before a cleanup unit; its purpose is to bound a non-convergent fault. Each
  # socket adds a few structures to drain and up to two aborts to deliver, so
  # the guard grows with the stack's socket limit.
  @shutdown_continuation_limit 1_024
  @shutdown_continuations_per_socket 16
  @tcp_default_buffer_bytes 262_144
  @tcp_min_buffer_bytes 1_024
  @tcp_max_buffer_bytes 1_048_576
  @max_egress_credit 0xFFFF_FFFF

  @type limits :: %{
          bytes_copied: pos_integer(),
          input_packets: pos_integer(),
          output_packets: pos_integer(),
          ready_events: pos_integer(),
          maintenance_work: pos_integer(),
          sockets: pos_integer()
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
  @spec ingress(Ref.t(), [binary()]) ::
          {:ok, non_neg_integer()}
          | {:error,
             :invalid_packet
             | :packet_too_large
             | :batch_too_large
             | :busy
             | :link_down
             | :closed}
  def ingress(%Ref{stack: stack, ingress_token: ingress_token}, packet) do
    GenServer.call(stack, {:ingress, ingress_token, packet}, :infinity)
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec grant_egress(Ref.t(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, :invalid_egress_credit | :egress_credit_disabled | :closed}
  def grant_egress(%Ref{stack: stack}, packets, bytes) do
    if valid_egress_credit?(packets, bytes) do
      GenServer.call(stack, {:grant_egress, packets, bytes})
    else
      {:error, :invalid_egress_credit}
    end
  catch
    :exit, _reason -> {:error, :closed}
  end

  def grant_egress(_stack, _packets, _bytes), do: {:error, :invalid_egress_credit}

  @doc false
  @spec valid_egress_credit?(term(), term()) :: boolean()
  def valid_egress_credit?(packets, bytes) do
    is_integer(packets) and packets in 0..@max_egress_credit and is_integer(bytes) and
      bytes in 0..@max_egress_credit
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
  def socket_open(stack, family, :stream),
    do:
      socket_open(
        stack,
        family,
        :stream,
        @tcp_default_buffer_bytes,
        @tcp_default_buffer_bytes
      )

  def socket_open(%Ref{stack: stack}, family, kind)
      when family in [:inet, :inet6] and kind == :datagram do
    GenServer.call(stack, {:socket_open, family, kind})
  catch
    :exit, _reason -> {:error, :closed}
  end

  def socket_open(_stack, _family, _kind), do: {:error, :invalid_options}

  @spec socket_open(term(), :inet | :inet6, :stream, pos_integer(), pos_integer()) ::
          {:ok, Socket.t()} | {:error, atom()}
  def socket_open(%Ref{stack: stack}, family, :stream, rcvbuf, sndbuf)
      when family in [:inet, :inet6] and is_integer(rcvbuf) and is_integer(sndbuf) and
             rcvbuf in @tcp_min_buffer_bytes..@tcp_max_buffer_bytes and
             sndbuf in @tcp_min_buffer_bytes..@tcp_max_buffer_bytes do
    GenServer.call(stack, {:socket_open, family, :stream, rcvbuf, sndbuf})
  catch
    :exit, _reason -> {:error, :closed}
  end

  def socket_open(_stack, _family, _kind, _rcvbuf, _sndbuf),
    do: {:error, :invalid_options}

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
  @spec socket_set_nodelay(Socket.t(), boolean()) :: :ok | {:error, atom()}
  def socket_set_nodelay(%Socket{stack: stack, id: id, generation: generation}, nodelay) do
    GenServer.call(stack, {:socket_set_nodelay, id, generation, nodelay})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_nodelay(Socket.t()) :: {:ok, boolean()} | {:error, atom()}
  def socket_nodelay(%Socket{stack: stack, id: id, generation: generation}) do
    GenServer.call(stack, {:socket_nodelay, id, generation})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_set_keepalive(Socket.t(), boolean()) :: :ok | {:error, atom()}
  def socket_set_keepalive(%Socket{stack: stack, id: id, generation: generation}, keepalive) do
    GenServer.call(stack, {:socket_set_keepalive, id, generation, keepalive})
  catch
    :exit, _reason -> {:error, :closed}
  end

  @doc false
  @spec socket_keepalive(Socket.t()) :: {:ok, boolean()} | {:error, atom()}
  def socket_keepalive(%Socket{stack: stack, id: id, generation: generation}) do
    GenServer.call(stack, {:socket_keepalive, id, generation})
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
  # Test accessor: the stack's backend module and its current state, so a
  # test can reach the NIF's debug hooks without reading the raw GenServer
  # state.
  @spec backend_state(pid()) :: {module(), term()}
  def backend_state(stack), do: GenServer.call(stack, :backend_state)

  @doc false
  # Test accessor: how many socket owners the stack monitors, counted in both
  # of its indexes, which must agree.
  @spec owner_monitor_counts(pid()) :: %{
          by_monitor: non_neg_integer(),
          by_identity: non_neg_integer()
        }
  def owner_monitor_counts(stack), do: GenServer.call(stack, :owner_monitor_counts)

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
    backend = Keyword.get_lazy(options, :backend, &Backend.default/0)
    set_process_flags(backend)

    state = %{
      starter: starter,
      starter_monitor: Process.monitor(starter),
      ready_ref: Keyword.fetch!(options, :ready_ref),
      limits: Keyword.fetch!(options, :limits),
      native_config: Keyword.get(options, :native_config, Options.default_native_config()),
      backend: backend,
      backend_state: nil,
      backend_ready: false,
      clock: Application.get_env(:smolnet, :clock_module, SystemClock),
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

    {:ok, state, {:continue, :create_backend}}
  end

  defp set_process_flags(backend) do
    if Code.ensure_loaded?(backend) and function_exported?(backend, :process_flags, 0) do
      for {flag, value} <- backend.process_flags(), do: Process.flag(flag, value)
    end

    :ok
  end

  @impl true
  def handle_continue(:create_backend, state) do
    now = state.clock.now()

    case state.backend.stack_new(state.limits, state.native_config, now) do
      {{:ok, envelope}, _backend_state, _effects} = outcome ->
        send(state.starter, {:smolnet_stack_ready, state.ready_ref})
        {_reply, state} = run(%{state | backend_ready: true}, outcome)
        {:noreply, schedule(state, envelope)}

      {{:error, reason}, _backend_state, _effects} ->
        send(state.starter, {:smolnet_stack_error, state.ready_ref, reason})
        {:noreply, state}

      unexpected ->
        send(state.starter, {:smolnet_stack_error, state.ready_ref, :invalid_native_result})
        {:noreply, Map.put(state, :invalid_native_result, unexpected)}
    end
  end

  def handle_continue({:process_ingress, input, packet_count}, state) do
    state
    |> process_ingress(input, packet_count)
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

    case run(state, state.backend.stack_poll(state.backend_state, now)) do
      {{:ok, envelope}, state} ->
        state
        |> schedule(envelope)
        |> continue_pending_ingress()

      {{:error, reason}, state} when state.native_continuation ->
        {:stop, {:shutdown, {:native_poll_failed, reason}}, state}

      {{:error, _reason}, state} ->
        state |> replace_timer(nil) |> continue_pending_ingress()
    end
  end

  # A poll can go stale while a feeder's packet is held: the `more: true`
  # branch of schedule/3 self-sends the poll, and any socket call queued
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
        {:ingress, ingress_token, input},
        from,
        %{ingress_token: ingress_token} = state
      ) do
    case validate_ingress(input, state.native_config.mtu, state.limits) do
      {:ok, _packet_count} when state.shutdown_requested ->
        reject_ingress(state, :closed)

      {:ok, packet_count} when state.link_status == :up ->
        accept_ingress(state, from, input, packet_count)

      {:ok, _packet_count} ->
        reject_ingress(state, :link_down)

      {:error, reason} ->
        reject_ingress(state, reason)
    end
  end

  def handle_call({:ingress, _invalid_token, _packet}, _from, state) do
    {:reply, {:error, :invalid_ingress}, state}
  end

  def handle_call({:grant_egress, _packets, _bytes}, _from, %{shutdown_requested: true} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:grant_egress, packets, bytes}, _from, state) do
    state.backend.stack_grant_egress(state.backend_state, packets, bytes, state.clock.now())
    |> reply_backend(state)
  end

  def handle_call(:native_snapshot, _from, state) do
    {snapshot, state} = snapshot(state)
    {:reply, snapshot, state}
  end

  def handle_call(:backend_state, _from, state) do
    {:reply, {state.backend, state.backend_state}, state}
  end

  def handle_call(:owner_monitor_counts, _from, state) do
    counts = %{
      by_monitor: map_size(state.socket_owner_monitors),
      by_identity: map_size(state.socket_owner_monitors_by_identity)
    }

    {:reply, counts, state}
  end

  def handle_call(:test_contention, _from, state) do
    {reply, state} = run(state, state.backend.test_contention(state.backend_state))
    {:reply, reply, state}
  end

  def handle_call({:test_bounded_work, requested}, _from, state) do
    {reply, state} = run(state, state.backend.test_bounded_work(state.backend_state, requested))
    {:reply, reply, state}
  end

  def handle_call(:shutdown_waiters, _from, state) do
    state = state |> Map.put(:shutdown_requested, true) |> reject_pending_ingress(:closed)
    {reply, state} = drain_shutdown(state, :closed)

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
    state.backend.socket_cancel(
      state.backend_state,
      %{id: id, generation: generation},
      operation,
      reference
    )
    |> reply_backend(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call({:socket_open, family, kind}, _from, state) do
    result =
      case kind do
        :stream ->
          state.backend.tcp_open(
            state.backend_state,
            family,
            @tcp_default_buffer_bytes,
            @tcp_default_buffer_bytes
          )

        :datagram ->
          state.backend.udp_open(state.backend_state, family)
      end

    result
    |> reply_backend(
      state,
      fn identity -> {:ok, Socket.new(self(), identity, family, kind)} end,
      :preserve_timer
    )
  end

  def handle_call({:socket_open, family, :stream, rcvbuf, sndbuf}, _from, state) do
    state.backend.tcp_open(state.backend_state, family, rcvbuf, sndbuf)
    |> reply_backend(
      state,
      fn identity -> {:ok, Socket.new(self(), identity, family, :stream)} end,
      :preserve_timer
    )
  end

  def handle_call({:socket_bind, id, generation, kind, endpoint}, _from, state) do
    identity = %{id: id, generation: generation}

    result =
      case kind do
        :stream -> state.backend.tcp_bind(state.backend_state, identity, endpoint)
        :datagram -> state.backend.udp_bind(state.backend_state, identity, endpoint)
      end

    result
    |> reply_backend(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call({:socket_listen, id, generation, backlog}, _from, state) do
    state.backend.tcp_listen(
      state.backend_state,
      %{id: id, generation: generation},
      backlog,
      state.clock.now()
    )
    |> reply_backend(state)
  end

  def handle_call(
        {:socket_accept, id, generation, reference},
        {caller, _tag},
        state
      ) do
    state.backend.tcp_accept(
      state.backend_state,
      %{id: id, generation: generation},
      caller,
      reference,
      state.clock.now()
    )
    |> reply_backend(state, &normalize_accept_result(&1, self()))
  end

  def handle_call(
        {:socket_accept_owned, id, generation, reference, owner},
        {caller, _tag},
        state
      ) do
    state.backend.tcp_accept(
      state.backend_state,
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
    state.backend.tcp_connect(
      state.backend_state,
      %{id: id, generation: generation},
      endpoint,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_backend(state, &normalize_wait_result/1)
  end

  def handle_call(
        {:socket_connect, id, generation, :datagram, endpoint, _reference},
        _from,
        state
      ) do
    state.backend.udp_connect(
      state.backend_state,
      %{id: id, generation: generation},
      endpoint
    )
    |> reply_backend(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(
        {:socket_send, id, generation, data, reference},
        {caller, _tag},
        state
      ) do
    state.backend.tcp_send(
      state.backend_state,
      %{id: id, generation: generation},
      data,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_backend(state, &normalize_send_result(&1, data))
  end

  def handle_call(
        {:socket_recv, id, generation, length, reference},
        {caller, _tag},
        state
      ) do
    state.backend.tcp_recv(
      state.backend_state,
      %{id: id, generation: generation},
      length,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_backend(state, &normalize_recv_result/1)
  end

  def handle_call(
        {:socket_sendto, id, generation, endpoint, data, reference},
        {caller, _tag},
        state
      ) do
    state.backend.udp_sendto(
      state.backend_state,
      %{id: id, generation: generation},
      endpoint,
      data,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_backend(state, &normalize_datagram_wait_result/1)
  end

  def handle_call(
        {:socket_recvfrom, id, generation, length, reference},
        {caller, _tag},
        state
      ) do
    state.backend.udp_recvfrom(
      state.backend_state,
      %{id: id, generation: generation},
      length,
      caller,
      reference,
      state.clock.now()
    )
    |> reply_backend(state, &normalize_recvfrom_result/1)
  end

  def handle_call({:socket_shutdown, id, generation, how}, _from, state) do
    state.backend.tcp_shutdown(
      state.backend_state,
      %{id: id, generation: generation},
      how,
      state.clock.now()
    )
    |> reply_backend(state)
  end

  def handle_call({:socket_sockname, id, generation, kind}, _from, state) do
    identity = %{id: id, generation: generation}
    result = socket_name_call(state, kind, :sockname, identity)

    result
    |> reply_backend(state, &normalize_endpoint/1, :preserve_timer)
  end

  def handle_call({:socket_set_nodelay, id, generation, nodelay}, _from, state) do
    state.backend.tcp_set_nodelay(
      state.backend_state,
      %{id: id, generation: generation},
      nodelay,
      state.clock.now()
    )
    |> reply_backend(state)
  end

  def handle_call({:socket_nodelay, id, generation}, _from, state) do
    state.backend.tcp_nodelay(state.backend_state, %{id: id, generation: generation})
    |> reply_backend(state, &{:ok, &1}, :preserve_timer)
  end

  def handle_call({:socket_set_keepalive, id, generation, keepalive}, _from, state) do
    state.backend.tcp_set_keepalive(
      state.backend_state,
      %{id: id, generation: generation},
      keepalive,
      state.clock.now()
    )
    |> reply_backend(state)
  end

  def handle_call({:socket_keepalive, id, generation}, _from, state) do
    state.backend.tcp_keepalive(state.backend_state, %{id: id, generation: generation})
    |> reply_backend(state, &{:ok, &1}, :preserve_timer)
  end

  def handle_call({:socket_peername, id, generation, kind}, _from, state) do
    identity = %{id: id, generation: generation}
    result = socket_name_call(state, kind, :peername, identity)

    result
    |> reply_backend(state, &normalize_endpoint/1, :preserve_timer)
  end

  def handle_call({:socket_close, id, generation, kind}, _from, state) do
    identity = %{id: id, generation: generation}

    case run(state, close_socket_call(state, kind, identity)) do
      {{:ok, envelope}, state} ->
        state = state |> unwatch_socket_owner(identity) |> schedule(envelope)
        {:reply, Map.fetch!(envelope, :result), state}

      {{:error, reason}, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:socket_watch_owner, id, generation, owner}, _from, state) do
    identity = %{id: id, generation: generation}

    case run(state, state.backend.socket_validate(state.backend_state, identity)) do
      {{:ok, %{result: native_kind} = envelope}, state} ->
        state =
          state
          |> schedule(envelope, :preserve_timer)
          |> watch_socket_owner(identity, native_kind, owner)

        {:reply, :ok, state}

      {{:error, reason}, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:test_socket_open, internal_handle}, _from, state) do
    result = state.backend.test_socket_open(state.backend_state, internal_handle)

    reply_backend(
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
    state.backend.test_socket_wait(
      state.backend_state,
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
    |> reply_backend(state, &normalize_wait_result/1, :preserve_timer)
  end

  def handle_call({:test_socket_ready, keys}, _from, state) do
    state.backend.test_socket_ready(state.backend_state, keys)
    |> reply_backend(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(
        {:test_socket_close, id, generation, wake_direction},
        _from,
        state
      ) do
    state.backend.test_socket_close(
      state.backend_state,
      %{id: id, generation: generation},
      wake_direction
    )
    |> reply_backend(state, &Function.identity/1, :preserve_timer)
  end

  def handle_call(:stack_info, _from, state) do
    {snapshot, state} = snapshot(state)

    native =
      case snapshot do
        {:ok, envelope} -> envelope
        {:error, _reason} = error -> error
      end

    info = %{
      backend: Backend.name(state.backend),
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
  def terminate(reason, state) do
    state = reject_pending_ingress(state, :closed)

    if state.timer do
      _cancel_result = state.clock.cancel_timer(state.timer.ref)
    end

    if state.backend_ready and not state.shutdown_drain_attempted do
      _shutdown_result = drain_shutdown(state, abort_reason(reason, state))
    end

    :ok
  end

  # The reason the final drain gives the waiters it aborts. `stop_stack/1`,
  # which drains through `shutdown_waiters` first, and an orderly shutdown
  # from the supervisor close them. A stack that stops on its own, because
  # its link died under `link_down: :stop` or because it failed, gives the
  # loss, which the gen_tcp and gen_udp adapters report as `:enetdown`.
  defp abort_reason(_reason, %{shutdown_requested: true}), do: :closed
  defp abort_reason(reason, _state) when reason in [:normal, :shutdown], do: :closed
  defp abort_reason({:shutdown, {:link_down, _reason}}, _state), do: :link_down
  defp abort_reason(_reason, _state), do: :stack_down

  @spec default_limits() :: limits()
  def default_limits, do: @default_limits

  defp validate_ingress(packet, mtu, _limits) when is_binary(packet) do
    case validate_packet(packet, mtu) do
      :ok -> {:ok, 1}
      {:error, _reason} = error -> error
    end
  end

  defp validate_ingress(packets, mtu, limits) when is_list(packets) do
    validate_batch(packets, mtu, limits, 0, 0)
  end

  defp validate_ingress(_input, _mtu, _limits), do: {:error, :invalid_packet}

  defp validate_batch([], _mtu, _limits, packet_count, _byte_count),
    do: {:ok, packet_count}

  defp validate_batch([packet | packets], mtu, limits, packet_count, byte_count) do
    next_packet_count = packet_count + 1

    cond do
      next_packet_count > limits.input_packets ->
        {:error, :batch_too_large}

      not is_binary(packet) ->
        {:error, :invalid_packet}

      byte_count + byte_size(packet) > limits.bytes_copied ->
        {:error, :batch_too_large}

      true ->
        case validate_packet(packet, mtu) do
          :ok ->
            validate_batch(
              packets,
              mtu,
              limits,
              next_packet_count,
              byte_count + byte_size(packet)
            )

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp validate_batch(_improper_tail, _mtu, _limits, _packet_count, _byte_count),
    do: {:error, :invalid_packet}

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
    |> ipv4_header_sum()
    |> fold_checksum()
    |> bnot()
    |> band(0xFFFF)
    |> Kernel.==(0)
  end

  # Every ingress IPv4 packet passes through here, so the fixed header's ten
  # 16-bit words are summed in one match; options follow a word at a time.
  defp ipv4_header_sum(
         <<w0::16, w1::16, w2::16, w3::16, w4::16, w5::16, w6::16, w7::16, w8::16, w9::16,
           options::binary>>
       ) do
    sum_words(options, w0 + w1 + w2 + w3 + w4 + w5 + w6 + w7 + w8 + w9)
  end

  defp sum_words(<<word::16, rest::binary>>, sum), do: sum_words(rest, sum + word)
  defp sum_words(<<>>, sum), do: sum

  defp fold_checksum(sum) when sum > 0xFFFF,
    do: fold_checksum(band(sum, 0xFFFF) + div(sum, 0x10000))

  defp fold_checksum(sum), do: sum

  defp egress_state(nil), do: {nil, nil, nil, :down}

  defp egress_state({pid, link_ref}) do
    {pid, link_ref, Process.monitor(pid), :up}
  end

  defp process_ingress(state, input, packet_count) do
    outcome =
      case input do
        packet when is_binary(packet) ->
          state.backend.stack_ingress(state.backend_state, packet, state.clock.now())

        packets when is_list(packets) ->
          state.backend.stack_ingress_batch(state.backend_state, packets, state.clock.now())
      end

    case run(state, outcome) do
      {{:ok, envelope}, state} ->
        state
        |> Map.update!(:processed_ingress, &(&1 + packet_count))
        |> schedule(envelope)

      {{:error, _reason}, state} ->
        state
        |> Map.update!(:failed_ingress, &(&1 + packet_count))
        |> Map.put(:native_continuation, false)
    end
  end

  # Takes a backend call's outcome: keeps the backend's new state, carries out
  # its effects (see SmolNet.Stack.Backend), and returns the reply.
  defp run(state, {reply, backend_state, effects}) do
    state = %{state | backend_state: backend_state}
    {reply, carry_out(state, effects, [], :emit)}
  end

  # Wakes and aborts go as they come; the packets go to the link together,
  # in order, once the list is done. The shutdown drain passes `:drop`, as
  # it discarded the native output before backends existed.
  defp carry_out(state, [], [], _emit), do: state
  defp carry_out(state, [], _packets, :drop), do: state

  defp carry_out(state, [], packets, :emit),
    do: emit_packets(state, :lists.reverse(packets))

  defp carry_out(state, [{:emit, packet} | effects], packets, emit),
    do: carry_out(state, effects, [packet | packets], emit)

  defp carry_out(state, [{:wake, pid, identity, reference} | effects], packets, emit) do
    send(pid, {:"$smol_socket", identity, :select, reference})
    carry_out(state, effects, packets, emit)
  end

  defp carry_out(state, [{:abort, pid, identity, reference, reason} | effects], packets, emit) do
    send(pid, {:"$smol_socket", identity, :abort, reference, reason})
    carry_out(state, effects, packets, emit)
  end

  # Schedules the backend's next poll from a successful call's envelope: at
  # once when it retained work (`more`), otherwise at its `poll_at`.
  defp schedule(state, envelope, timer_policy \\ :replace_timer) do
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
    case run(state, close_socket_call(state, native_socket_kind(kind), identity)) do
      {{:ok, envelope}, state} -> {:noreply, schedule(state, envelope)}
      {{:error, _reason}, state} -> {:noreply, state}
    end
  end

  defp close_socket_call(state, :stream, identity) do
    state.backend.tcp_close(state.backend_state, identity, state.clock.now())
  end

  defp close_socket_call(state, :datagram, identity) do
    state.backend.udp_close(state.backend_state, identity, state.clock.now())
  end

  defp socket_name_call(state, :stream, :sockname, identity),
    do: state.backend.tcp_sockname(state.backend_state, identity)

  defp socket_name_call(state, :stream, :peername, identity),
    do: state.backend.tcp_peername(state.backend_state, identity)

  defp socket_name_call(state, :datagram, :sockname, identity),
    do: state.backend.udp_sockname(state.backend_state, identity)

  defp socket_name_call(state, :datagram, :peername, identity),
    do: state.backend.udp_peername(state.backend_state, identity)

  defp native_socket_kind(:tcp), do: :stream
  defp native_socket_kind(:udp), do: :datagram

  defp emit_packets(state, []), do: state

  defp emit_packets(%{link_status: :up} = state, packets) do
    send(state.egress_pid, {:smol_stack, state.link_ref, :egress, packets})
    state
  end

  defp emit_packets(state, packets) do
    Map.update!(state, :dropped_egress, &(&1 + length(packets)))
  end

  # The native stack returns its poll deadline after every call, and it is
  # often the deadline already armed. That timer then stays: it fires when a
  # new one would, and its message still carries the current generation. The
  # `more: true` branch of schedule/3 always passes nil, so a
  # continuation still cancels the timer and moves to a new generation.
  defp replace_timer(%{timer: %{deadline: poll_at}} = state, poll_at), do: state

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

  defp accept_ingress(state, _from, [], 0) do
    {:reply, {:ok, 0}, state}
  end

  defp accept_ingress(%{native_continuation: false} = state, _from, input, packet_count) do
    {:reply, ingress_reply(input, packet_count), state,
     {:continue, {:process_ingress, input, packet_count}}}
  end

  defp accept_ingress(%{pending_ingress: nil} = state, from, input, packet_count) do
    {:noreply, %{state | pending_ingress: {from, input, packet_count}}}
  end

  defp accept_ingress(state, _from, _input, _packet_count), do: reject_ingress(state, :busy)

  defp reject_pending_ingress(%{pending_ingress: nil} = state, _reason), do: state

  defp reject_pending_ingress(%{pending_ingress: {from, _input, _packet_count}} = state, reason) do
    GenServer.reply(from, {:error, reason})
    %{state | pending_ingress: nil}
  end

  defp continue_pending_ingress(%{native_continuation: true} = state) do
    {:noreply, state}
  end

  defp continue_pending_ingress(%{pending_ingress: nil} = state) do
    {:noreply, state}
  end

  defp continue_pending_ingress(%{pending_ingress: {from, input, packet_count}} = state) do
    GenServer.reply(from, ingress_reply(input, packet_count))
    state = %{state | pending_ingress: nil}
    {:noreply, state, {:continue, {:process_ingress, input, packet_count}}}
  end

  defp ingress_reply(input, packet_count) when is_list(input), do: {:ok, packet_count}
  defp ingress_reply(_packet, _packet_count), do: :ok

  defp timer_deadline(nil), do: nil
  defp timer_deadline(timer), do: timer.deadline

  defp drain_shutdown(state, reason) do
    case drain(state, state.backend.stack_shutdown(state.backend_state, reason)) do
      {{:ok, envelope}, state} ->
        continue_shutdown(state, envelope, shutdown_continuation_limit(state.limits))

      {{:error, reason}, state} ->
        {{:error, reason}, state}
    end
  end

  defp shutdown_continuation_limit(limits) do
    @shutdown_continuation_limit + @shutdown_continuations_per_socket * limits.sockets
  end

  defp continue_shutdown(state, %{more: false}, _remaining), do: {:ok, state}

  defp continue_shutdown(state, %{more: true}, 0),
    do: {{:error, :shutdown_incomplete}, state}

  defp continue_shutdown(state, %{more: true}, remaining) do
    case drain(state, state.backend.stack_poll(state.backend_state, state.clock.now())) do
      {{:ok, envelope}, state} -> continue_shutdown(state, envelope, remaining - 1)
      {{:error, reason}, state} -> {{:error, reason}, state}
    end
  end

  # run/2 for the shutdown drain, which delivers wakes and aborts but never
  # emits.
  defp drain(state, {reply, backend_state, effects}) do
    state = %{state | backend_state: backend_state}
    {reply, carry_out(state, effects, [], :drop)}
  end

  # A try-lock collision is transient, so terminate/2 gets one more attempt.
  # Panics and a non-convergent bounded drain must not repeat expensive work.
  defp shutdown_drain_terminal?({:error, :ownership_invariant_violation}), do: false
  defp shutdown_drain_terminal?(_result), do: true

  # The snapshot in the NIF's envelope shape, which `stack_info/1` documents
  # as its `:native` key. A snapshot never emits or schedules.
  defp snapshot(state) do
    case run(state, state.backend.stack_snapshot(state.backend_state)) do
      {{:ok, %{result: result}}, state} ->
        {{:ok, %{result: result, output: [], poll_at: nil, more: false}}, state}

      {{:error, _reason} = error, state} ->
        {error, state}
    end
  end

  defp reply_backend(
         outcome,
         state,
         transform \\ &Function.identity/1,
         timer_policy \\ :replace_timer
       )

  defp reply_backend(outcome, state, transform, timer_policy) do
    case run(state, outcome) do
      {{:ok, envelope}, state} ->
        result = envelope |> Map.fetch!(:result) |> transform.()
        {:reply, result, schedule(state, envelope, timer_policy)}

      {{:error, reason}, state} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp reply_owned_accept(outcome, state, owner) do
    case run(state, outcome) do
      {{:ok, %{result: {:ok, identity, family}} = envelope}, state} ->
        state = state |> schedule(envelope) |> watch_socket_owner(identity, :tcp, owner)
        {:reply, {:ok, Socket.new(self(), identity, family)}, state}

      {{:ok, %{result: result} = envelope}, state} ->
        {:reply, normalize_accept_result(result, self()), schedule(state, envelope)}

      {{:error, reason}, state} ->
        {:reply, {:error, reason}, state}
    end
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
