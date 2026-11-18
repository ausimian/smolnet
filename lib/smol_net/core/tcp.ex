defmodule SmolNet.Core.Tcp do
  @moduledoc false

  # TCP sockets in the stack core, with the NIF's semantics (the glue in
  # `native/smolnet_core` over smoltcp's `socket::tcp`), each connection a
  # `SmolNet.Core.Tcp.Connection`:
  #
  #   * A socket opens, binds once, then listens or connects. Connecting
  #     goes through phases (open, bound, connecting, connected, or failed
  #     as refused, reset or timed out), so that a RST is told apart from a
  #     refusal by the state it arrives in.
  #   * A listener keeps a real backlog: up to `backlog` handshakes under
  #     way, and up to `backlog` established children waiting for accept.
  #     A child that completes its handshake with the queue full, or with
  #     the socket limit reached, is dropped without a word, as the NIF's
  #     pool drops it, and counted as an overflow.
  #   * A connected socket closes gracefully: it leaves the socket table
  #     at once, while its connection finishes the close, for at most 30 s
  #     before it is aborted. Any other socket is aborted, with a RST if it
  #     has a peer.
  #   * Waiters are woken as smoltcp's wakers wake them: a connection's
  #     state change wakes both directions, data arriving in order the
  #     reader, acknowledged data or a sent RST the writer, but only a
  #     waker registered by the waiter armed now, and only once
  #     (`wake/3`). A listener's accept waiter registers on its handshakes
  #     under way, as on the NIF's pool.
  #   * After every call that drives the stack, each connection is asked
  #     for segments, one at a time and in turn, until none has more.
  #
  # Every function takes and returns the core (`SmolNet.Core`), which keeps
  # this module's stack-wide state in `core.tcp`. Times from the core are
  # milliseconds, and connections take microseconds.

  require SmolNet.Core.Wire.Ipv4
  require SmolNet.Core.Wire.Ipv6
  require SmolNet.Core.Wire.Tcp

  alias SmolNet.Core.Iface
  alias SmolNet.Core.Rand
  alias SmolNet.Core.Sockets
  alias SmolNet.Core.Tcp.Connection
  alias SmolNet.Core.Udp
  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Ipv6
  alias SmolNet.Core.Wire.Tcp, as: Segment

  @ms 1_000
  @min_buffer 1_024
  @max_buffer 1_048_576
  @max_socket_buffer_bytes 64 * 2 * @max_buffer
  @connect_timeout 30_000
  @close_timeout 30_000
  @backlog_max 128
  @timers %{
    user_timeout: 924_600,
    keepalive_idle: 7_200_000,
    keepalive_interval: 75_000,
    keepalive_probes: 9
  }

  defstruct conns: %{},
            listeners: %{},
            embryos: %{},
            closing: %{},
            rand: 0,
            origin: 0,
            timers: @timers,
            promotions: 0,
            refills: 0,
            overflow_drops: 0

  @type t :: %__MODULE__{}

  defmodule Record do
    @moduledoc false

    # A TCP socket in the table: the NIF's `TcpRecord`, with its connection
    # and, once it listens, its listener. `wakers` are the registrations
    # smoltcp's wakers hold on the connection, `{direction, reference}` of
    # the waiter that armed them.
    defstruct [
      :family,
      :rcvbuf,
      :sndbuf,
      :conn,
      phase: :open,
      local: nil,
      local_scope_id: 0,
      remote: nil,
      remote_scope_id: 0,
      read_shutdown: false,
      write_shutdown: false,
      accepted: false,
      listener: nil,
      wakers: %{read: nil, write: nil}
    ]
  end

  @doc "The stack-wide TCP state of a new core."
  @spec new(map(), integer()) :: t()
  def new(config, stack_id) do
    %__MODULE__{
      rand: Rand.new(Map.get(config, :random_seed) || stack_id),
      origin: Map.get(config, :timestamp_origin) || :rand.uniform(0x1_0000_0000) - 1,
      timers: Map.get(config, :tcp_timers) || @timers
    }
  end

  ## Calls

  def open(core, family, rcvbuf, sndbuf) do
    with :ok <- logical_capacity(core),
         :ok <- native_capacity(core, 1),
         :ok <-
           if(valid_buffer?(rcvbuf) and valid_buffer?(sndbuf),
             do: :ok,
             else: {:error, :invalid_options}
           ),
         :ok <- buffer_capacity(core, rcvbuf + sndbuf) do
      record = %Record{
        family: family,
        rcvbuf: rcvbuf,
        sndbuf: sndbuf,
        conn: new_conn(core, rcvbuf, sndbuf)
      }

      {:ok, identity, sockets} = Sockets.open(core.sockets, :tcp, record, table_limit(core))
      {{:ok, identity_map(identity)}, %{core | sockets: sockets}, []}
    else
      error -> {error, core, []}
    end
  end

  defp valid_buffer?(bytes), do: is_integer(bytes) and bytes in @min_buffer..@max_buffer

  defp new_conn(core, rcvbuf, sndbuf),
    do: Connection.new(rcvbuf, sndbuf, tsval_generator: {:clock, core.tcp.origin})

  def bind(core, identity, endpoint) do
    with {:ok, record} <- fetch(core, identity),
         {:ok, {address, port, scope_id}} <- Sockets.endpoint(endpoint, :bind),
         :ok <- if(record.phase == :open, do: :ok, else: {:error, :invalid_socket_state}),
         :ok <- family(record, address),
         :ok <- available(core, address),
         {:ok, port, core} <- port(core, identity, record.family, port) do
      record = %{record | phase: :bound, local: {address, port}, local_scope_id: scope_id}
      {{:ok, :ok}, put(core, identity, record), []}
    else
      {:error, _reason} = error -> {error, core, []}
    end
  end

  defp available(core, address) do
    if Sockets.unspecified?(address) or Iface.has_ip_addr?(core.iface, address),
      do: :ok,
      else: {:error, :address_not_available}
  end

  defp port(core, _identity, family, 0) do
    used = used_ports(core, family, :allocation)

    case Sockets.allocate_port(core.sockets, :tcp, family, &MapSet.member?(used, &1)) do
      {:ok, port, sockets} -> {:ok, port, %{core | sockets: sockets}}
      error -> error
    end
  end

  defp port(core, identity, family, port) do
    if port_in_use?(core, port, family, identity),
      do: {:error, :address_in_use},
      else: {:ok, port, core}
  end

  # The NIF's `port_in_use`: a port another socket of the family holds,
  # bound, connected or closing, or a listener; accepted children share
  # their listener's port and do not count.
  defp port_in_use?(core, port, family, {id, _generation}) do
    MapSet.member?(used_ports(core, family, {:check, id}), port)
  end

  # The ports held, for a check excluding a socket or for allocation, which
  # avoids accepted children's ports too.
  defp used_ports(core, family, use) do
    records =
      for {id, %{kind: :tcp, state: record}} <- core.sockets.entries, do: {id, record}

    closing = for {id, %{record: record}} <- core.tcp.closing, do: {id, record}

    for {id, record} <- records ++ closing,
        record.family == family,
        record.local != nil,
        keep_port?(use, id, record),
        into: MapSet.new(),
        do: elem(record.local, 1)
  end

  defp keep_port?(:allocation, _id, _record), do: true
  defp keep_port?({:check, id}, id, _record), do: false
  defp keep_port?({:check, _excluded}, _id, record), do: not record.accepted

  def listen(core, identity, backlog, now) do
    with {:ok, record} <- fetch_any(core, identity),
         :ok <- if(backlog in 1..@backlog_max, do: :ok, else: {:error, :invalid_backlog}),
         :ok <- if(record.listener, do: {:error, :invalid_socket_state}, else: :ok),
         :ok <- if(record.phase == :bound, do: :ok, else: {:error, :not_bound}),
         {:ok, _concrete} <- concrete_address(core, record) do
      listener = %{
        backlog: backlog,
        nodelay: not record.conn.options.nagle,
        keepalive: record.conn.options.keep_alive != nil,
        accepted: :queue.new(),
        waker: nil
      }

      record = %{record | listener: listener}
      {address, port} = record.local
      core = put(core, identity, record)

      tcp = %{
        core.tcp
        | listeners: Map.put(core.tcp.listeners, {address, port}, elem(identity, 0))
      }

      drive({{:ok, :ok}, %{core | tcp: tcp}, []}, now)
    else
      {:error, _reason} = error -> {error, core, []}
    end
  end

  # A wildcard listener needs an address of its family, as the NIF's pool
  # sockets listen on a concrete one.
  defp concrete_address(core, %Record{local: {address, _port}, family: family}) do
    if Sockets.unspecified?(address) do
      case core.iface |> Iface.addresses(family) |> Enum.reject(&Sockets.unspecified?/1) do
        [first | _rest] -> {:ok, first}
        [] -> {:error, :address_not_available}
      end
    else
      {:ok, address}
    end
  end

  def accept(core, identity, pid, reference, now) do
    with {:ok, _record, entry} <- fetch_listener(core, identity),
         :ok <- if(Sockets.waiting?(entry, :read), do: {:error, :busy}, else: :ok) do
      {core, effects} = refresh_listeners(core)
      record = listener_record(core, identity)

      case :queue.out(record.listener.accepted) do
        {{:value, child}, accepted} ->
          core =
            put(core, identity, %{record | listener: %{record.listener | accepted: accepted}})

          core = apply_listener_options(core, child, record.listener)
          {:ok, child_record} = fetch(core, child)

          finish(
            {{:ok, {:ok, identity_map(child), child_record.family}}, core, effects},
            now,
            :current
          )

        {:empty, _accepted} ->
          {core, armed} = arm_accept(core, identity, {pid, :accept, reference})
          finish({{:ok, {:select, :accept, reference}}, core, effects ++ armed}, now, :current)
      end
    else
      {:error, _reason} = error -> {error, core, []}
    end
  end

  defp fetch_listener(core, identity) do
    case Sockets.fetch(core.sockets, identity, :tcp) do
      {:ok, %{state: %Record{listener: nil}}} -> {:error, :invalid_socket_state}
      {:ok, %{state: record} = entry} -> {:ok, record, entry}
      error -> error
    end
  end

  defp listener_record(core, identity) do
    {:ok, record} = fetch_any(core, identity)
    record
  end

  # A child takes its listener's Nagle and keep-alive settings as they are
  # when it is accepted.
  defp apply_listener_options(core, child, listener) do
    update_conn(core, {:socket, elem(child, 0)}, fn conn ->
      conn
      |> Connection.set_nagle_enabled(not listener.nodelay)
      |> Connection.set_keep_alive_config(keep_alive(core, listener.keepalive))
    end)
  end

  defp keep_alive(_core, false), do: nil

  defp keep_alive(core, true) do
    timers = core.tcp.timers

    %{
      idle: timers.keepalive_idle * @ms,
      interval: timers.keepalive_interval * @ms,
      probes: timers.keepalive_probes
    }
  end

  # The accept waiter registers on every handshake under way, and on those
  # still to come; it is woken at once if a child is already established.
  defp arm_accept(core, {id, _generation} = identity, {_pid, _operation, reference} = waiter) do
    sockets = Sockets.arm(core.sockets, identity, :read, waiter)
    record = listener_record(%{core | sockets: sockets}, identity)
    record = %{record | listener: %{record.listener | waker: reference}}

    embryos =
      Map.new(core.tcp.embryos, fn
        {key, %{listener: ^id} = embryo} -> {key, %{embryo | waker: reference}}
        other -> other
      end)

    core = %{
      core
      | sockets: Sockets.put_state(sockets, identity, record),
        tcp: %{core.tcp | embryos: embryos}
    }

    ready =
      not :queue.is_empty(record.listener.accepted) or
        Enum.any?(embryos, fn {_key, embryo} ->
          embryo.listener == id and Connection.state(embryo.conn) in [:established, :close_wait]
        end)

    if ready, do: wake_now(core, identity, :read), else: {core, []}
  end

  def connect(core, identity, endpoint, pid, reference, now) do
    with {:ok, record} <- fetch(core, identity),
         {:ok, {address, port, scope_id}} <- Sockets.endpoint(endpoint, :remote),
         :ok <- family(record, address) do
      case record.phase do
        :connected ->
          {{:error, :already_connected}, core, []}

        {:failed, failure} ->
          {{:error, failure_error(failure)}, core, []}

        :connecting ->
          retry_connect(core, identity, record, {address, port, scope_id}, {pid, reference}, now)

        _open_or_bound ->
          start_connect(core, identity, record, {address, port, scope_id}, {pid, reference}, now)
      end
    else
      {:error, _reason} = error -> {error, core, []}
    end
  end

  defp retry_connect(core, identity, record, {address, port, scope_id}, waiter, now) do
    if record.remote == {address, port} and record.remote_scope_id == scope_id,
      do: finalize_connect(core, identity, record, waiter, now),
      else: {{:error, :invalid_socket_state}, core, []}
  end

  defp finalize_connect(core, identity, record, {pid, reference}, now) do
    case Connection.state(record.conn) do
      :established ->
        conn = Connection.set_timeout(record.conn, core.tcp.timers.user_timeout * @ms)
        core = put(core, identity, %{record | phase: :connected, conn: conn})
        finish({{:ok, :ok}, core, []}, now, :current)

      state when state in [:syn_sent, :syn_received] ->
        {:ok, entry} = Sockets.fetch(core.sockets, identity, :tcp)

        if Sockets.waiting?(entry, :write) do
          {{:error, :busy}, core, []}
        else
          {core, armed} = arm(core, identity, :write, {pid, :connect, reference}, false)
          drive({{:ok, {:select, :connect, reference}}, core, armed}, now)
        end

      :closed ->
        failure =
          case record.phase do
            {:failed, failure} -> failure
            _connecting -> :timed_out
          end

        core = put(core, identity, %{record | phase: {:failed, failure}})
        {{:error, failure_error(failure)}, core, []}

      _other ->
        {{:error, :invalid_socket_state}, core, []}
    end
  end

  defp start_connect(core, identity, record, {address, port, scope_id}, {pid, reference}, now) do
    with :ok <-
           if(Iface.reachable?(core.iface, address),
             do: :ok,
             else: {:error, :network_unreachable}
           ),
         {:ok, {local_address, local_port}, core} <- connect_local(core, identity, record),
         :ok <-
           if(port_in_use?(core, local_port, record.family, identity),
             do: {:error, :address_in_use},
             else: :ok
           ) do
      {isn, rand} = Rand.next_u32(core.tcp.rand)
      cx = %{cx(core, now) | isn: isn}
      conn = Connection.set_timeout(record.conn, @connect_timeout * @ms)
      local = if Sockets.unspecified?(local_address), do: nil, else: local_address

      case Connection.connect(conn, cx, {address, port}, {local, local_port}) do
        {:ok, conn} ->
          {_wakes, conn} = Connection.take_wakes(conn)
          selected = Connection.local_endpoint(conn)

          record = %{
            record
            | conn: conn,
              phase: :connecting,
              local: selected,
              remote: {address, port},
              remote_scope_id: scope_id
          }

          {id, _generation} = identity

          tcp = %{
            core.tcp
            | rand: rand,
              conns: Map.put(core.tcp.conns, {selected, {address, port}}, {:socket, id})
          }

          core = put(%{core | tcp: tcp}, identity, record)
          {core, armed} = arm(core, identity, :write, {pid, :connect, reference}, false)
          drive({{:ok, {:select, :connect, reference}}, core, armed}, now)

        {:error, :invalid_state} ->
          {{:error, :invalid_socket_state}, core, []}

        {:error, :unaddressable} ->
          {{:error, :network_unreachable}, core, []}
      end
    else
      {:error, reason} -> {{:error, reason}, core, []}
      {:error, reason, core} -> {{:error, reason}, core, []}
    end
  end

  defp connect_local(core, _identity, %Record{local: {address, port}}),
    do: {:ok, {address, port}, core}

  defp connect_local(core, identity, %Record{local: nil, family: family}) do
    case port(core, identity, family, 0) do
      {:ok, port, core} -> {:ok, {unspecified(family), port}, core}
      {:error, reason} -> {:error, reason, core}
    end
  end

  defp unspecified(:inet), do: <<0::32>>
  defp unspecified(:inet6), do: <<0::128>>

  defp failure_error(:refused), do: :connection_refused
  defp failure_error(:reset), do: :connection_reset
  defp failure_error(:timed_out), do: :connection_timeout

  def sockname(core, identity) do
    case fetch_any(core, identity) do
      {:ok, %Record{local: {address, port}} = record} ->
        {{:ok, Sockets.encode(address, port, record.local_scope_id)}, core, []}

      {:ok, %Record{local: nil}} ->
        {{:error, :not_bound}, core, []}

      error ->
        {error, core, []}
    end
  end

  def peername(core, identity) do
    case fetch_any(core, identity) do
      {:ok, %Record{listener: listener}} when listener != nil ->
        {{:error, :not_connected}, core, []}

      {:ok, %Record{phase: phase, remote: {address, port}} = record}
      when phase in [:connecting, :connected] ->
        {{:ok, Sockets.encode(address, port, record.remote_scope_id)}, core, []}

      {:ok, _record} ->
        {{:error, :not_connected}, core, []}

      error ->
        {error, core, []}
    end
  end

  def set_nodelay(core, identity, nodelay, now) do
    case fetch_any(core, identity) do
      {:ok, %Record{listener: listener} = record} when listener != nil ->
        core = put(core, identity, %{record | listener: %{listener | nodelay: nodelay}})
        drive({{:ok, :ok}, core, []}, now)

      {:ok, record} ->
        core =
          put(core, identity, %{
            record
            | conn: Connection.set_nagle_enabled(record.conn, not nodelay)
          })

        drive({{:ok, :ok}, core, []}, now)

      error ->
        {error, core, []}
    end
  end

  def nodelay(core, identity) do
    case fetch_any(core, identity) do
      {:ok, %Record{listener: listener}} when listener != nil ->
        {{:ok, listener.nodelay}, core, []}

      {:ok, record} ->
        {{:ok, not record.conn.options.nagle}, core, []}

      error ->
        {error, core, []}
    end
  end

  # Setting what is already set changes nothing, so a probe already sent
  # keeps its count.
  def set_keepalive(core, identity, keepalive, now) do
    case fetch_any(core, identity) do
      {:ok, %Record{listener: listener} = record} when listener != nil ->
        core = put(core, identity, %{record | listener: %{listener | keepalive: keepalive}})
        drive({{:ok, :ok}, core, []}, now)

      {:ok, record} ->
        conn =
          if record.conn.options.keep_alive != nil == keepalive,
            do: record.conn,
            else: Connection.set_keep_alive_config(record.conn, keep_alive(core, keepalive))

        drive({{:ok, :ok}, put(core, identity, %{record | conn: conn}), []}, now)

      error ->
        {error, core, []}
    end
  end

  def keepalive(core, identity) do
    case fetch_any(core, identity) do
      {:ok, %Record{listener: listener}} when listener != nil ->
        {{:ok, listener.keepalive}, core, []}

      {:ok, record} ->
        {{:ok, record.conn.options.keep_alive != nil}, core, []}

      error ->
        {error, core, []}
    end
  end

  def send(core, identity, data, pid, reference, now) do
    with {:ok, record, entry} <- fetch_entry(core, identity),
         :ok <- if(Sockets.waiting?(entry, :write), do: {:error, :busy}, else: :ok),
         :ok <- ensure_connected(record),
         :ok <- if(record.write_shutdown, do: {:error, :closed}, else: :ok),
         :ok <- if(Connection.may_send?(record.conn), do: :ok, else: {:error, :closed}) do
      copy = binary_part(data, 0, min(byte_size(data), core.limits.bytes_copied))
      {:ok, accepted, conn} = Connection.send(record.conn, copy)
      {_wakes, conn} = Connection.take_wakes(conn)
      core = put(core, identity, %{record | conn: conn})

      if accepted == byte_size(data) do
        drive({{:ok, :ok}, core, []}, now)
      else
        {core, armed} =
          arm(core, identity, :write, {pid, :send, reference}, Connection.can_send?(conn))

        drive({{:ok, {:select, :send, reference, accepted}}, core, armed}, now)
      end
    else
      {:error, _reason} = error -> {error, core, []}
    end
  end

  def recv(core, identity, length, pid, reference, now) do
    with {:ok, record, entry} <- fetch_entry(core, identity),
         :ok <- if(Sockets.waiting?(entry, :read), do: {:error, :busy}, else: :ok),
         :ok <- ensure_connected(record),
         :ok <- if(record.read_shutdown, do: {:error, :closed}, else: :ok) do
      requested =
        if length == 0, do: core.limits.bytes_copied, else: min(length, core.limits.bytes_copied)

      {data, conn} = read(record.conn, requested)
      core = put(core, identity, %{record | conn: conn})
      received(core, identity, {length, data, conn}, {pid, reference}, now)
    else
      {:error, _reason} = error -> {error, core, []}
    end
  end

  defp read(conn, requested) do
    {data, conn} =
      case min(requested, Connection.recv_queue(conn)) do
        0 ->
          {<<>>, conn}

        read_length ->
          {:ok, data, conn} = Connection.recv_slice(conn, read_length)
          {data, conn}
      end

    {_wakes, conn} = Connection.take_wakes(conn)
    {data, conn}
  end

  # A read with a length is complete with all of it, or with what is left
  # once the peer has closed; one without, with anything.
  defp received(core, identity, {length, data, conn}, {pid, reference}, now) do
    receive_open = Connection.may_recv?(conn)
    copied = byte_size(data)

    complete =
      if length == 0, do: copied > 0, else: copied == length or (not receive_open and copied > 0)

    cond do
      complete ->
        drive({{:ok, {:ok, data}}, core, []}, now)

      not receive_open ->
        {{:error, :end_of_stream}, core, []}

      true ->
        {core, armed} =
          arm(core, identity, :read, {pid, :recv, reference}, Connection.can_recv?(conn))

        result =
          if data == <<>>,
            do: {:select, :recv, reference},
            else: {:select, :recv, reference, data}

        drive({{:ok, result}, core, armed}, now)
    end
  end

  def shutdown(core, identity, how, now) do
    with {:ok, record} <- fetch(core, identity),
         :ok <- ensure_connected(record) do
      read = how in [:read, :read_write]
      write = how in [:write, :read_write]

      {aborts, sockets} =
        Enum.reduce([read: read, write: write], {[], core.sockets}, fn
          {direction, true}, {aborts, sockets} ->
            {waiter, sockets} = Sockets.take_waiter(sockets, identity, direction)
            {aborts ++ abort_effects(identity, waiter && [{direction, waiter}]), sockets}

          {_direction, false}, acc ->
            acc
        end)

      conn =
        if write and not record.write_shutdown,
          do: Connection.close(record.conn),
          else: record.conn

      record = %{
        record
        | read_shutdown: record.read_shutdown or read,
          write_shutdown: record.write_shutdown or write
      }

      # Closing the sending half wakes what the other direction's waiter
      # registered, as smoltcp's state change does.
      {id, _generation} = identity
      core = put(%{core | sockets: sockets}, identity, record)
      {core, woken} = take_wakes(core, {:socket, id}, conn)
      drive({{:ok, :ok}, core, aborts ++ woken}, now)
    else
      {:error, _reason} = error -> {error, core, []}
    end
  end

  def close(core, identity, now) do
    case fetch_any(core, identity) do
      {:ok, %Record{listener: listener} = record} when listener != nil ->
        close_listener(core, identity, record, now)

      {:ok, record} ->
        {waiters, sockets} = Sockets.close(core.sockets, identity)
        aborts = abort_effects(identity, waiters)
        core = %{core | sockets: sockets}
        {id, generation} = identity

        if record.phase == :connected do
          conn = record.conn |> Connection.close() |> Connection.take_wakes() |> elem(1)

          closing = %{
            record: %{record | conn: conn},
            generation: generation,
            deadline: now + @close_timeout
          }

          core = %{core | tcp: %{core.tcp | closing: Map.put(core.tcp.closing, id, closing)}}
          core = repoint(core, record, {:closing, id})
          drive({{:ok, :ok}, core, aborts}, now)
        else
          # Aborted: the RST goes out in this call, then the socket is gone.
          conn = record.conn |> Connection.abort() |> Connection.take_wakes() |> elem(1)
          closing = %{record: %{record | conn: conn}, generation: generation, deadline: nil}
          core = %{core | tcp: %{core.tcp | closing: Map.put(core.tcp.closing, id, closing)}}
          core = repoint(core, record, {:closing, id})
          {reply, core, effects} = drive({{:ok, :ok}, core, aborts}, now)
          {reply, forget_closing(core, id), effects}
        end

      error ->
        {error, core, []}
    end
  end

  # Moves a socket's demultiplexing entry to its closing connection. The
  # entry is only the socket's while it holds it: a dead connection's 4-tuple
  # may since have gone to a new handshake from the same peer.
  defp repoint(core, %Record{local: local, remote: remote}, {:closing, id} = ref)
       when local != nil and remote != nil do
    key = {local, remote}

    if Map.get(core.tcp.conns, key) == {:socket, id},
      do: %{core | tcp: %{core.tcp | conns: Map.put(core.tcp.conns, key, ref)}},
      else: core
  end

  defp repoint(core, _record, _ref), do: core

  # Deletes a demultiplexing entry, if it is still the given owner's.
  defp release(conns, %Record{local: local, remote: remote}, owner)
       when local != nil and remote != nil do
    key = {local, remote}
    if Map.get(conns, key) == owner, do: Map.delete(conns, key), else: conns
  end

  defp release(conns, _record, _owner), do: conns

  defp forget_closing(core, id) do
    case Map.pop(core.tcp.closing, id) do
      {nil, _closing} ->
        core

      {%{record: record}, closing} ->
        conns = release(core.tcp.conns, record, {:closing, id})

        %{core | tcp: %{core.tcp | closing: closing, conns: conns}}
    end
  end

  # Closing a listener ends its handshakes and drops the children no one
  # accepted, without a RST: their peers learn of it from the RSTs later
  # segments draw.
  defp close_listener(core, {id, _generation} = identity, record, now) do
    {waiters, sockets} = Sockets.close(core.sockets, identity)
    aborts = abort_effects(identity, waiters)

    {child_aborts, sockets, conns} =
      record.listener.accepted
      |> :queue.to_list()
      |> Enum.reduce({[], sockets, core.tcp.conns}, fn child, {aborts, sockets, conns} ->
        case Sockets.fetch(sockets, child, :tcp) do
          {:ok, %{state: child_record}} ->
            {waiters, sockets} = Sockets.close(sockets, child)
            {child_id, _generation} = child
            conns = release(conns, child_record, {:socket, child_id})
            {aborts ++ abort_effects(child, waiters), sockets, conns}

          _gone ->
            {aborts, sockets, conns}
        end
      end)

    {embryos, dropped} =
      Map.split_with(core.tcp.embryos, fn {_key, embryo} -> embryo.listener != id end)

    conns =
      Enum.reduce(dropped, conns, fn {key, _embryo}, conns ->
        if Map.get(conns, key) == {:embryo, key}, do: Map.delete(conns, key), else: conns
      end)

    listeners = Map.reject(core.tcp.listeners, fn {_key, listener_id} -> listener_id == id end)
    tcp = %{core.tcp | embryos: embryos, conns: conns, listeners: listeners}
    drive({{:ok, :ok}, %{core | sockets: sockets, tcp: tcp}, aborts ++ child_aborts}, now)
  end

  defp abort_effects(_identity, nil), do: []

  defp abort_effects(identity, waiters),
    do:
      for(
        {_direction, {pid, _operation, reference}} <- waiters,
        do: {:abort, pid, identity, reference, :closed}
      )

  defp ensure_connected(%Record{phase: :connected, conn: conn}) do
    if Connection.aborted_by_timeout?(conn), do: {:error, :connection_timeout}, else: :ok
  end

  defp ensure_connected(%Record{phase: {:failed, failure}}), do: {:error, failure_error(failure)}
  defp ensure_connected(_record), do: {:error, :not_connected}

  ## Waiters and wakes

  # Arms a waiter and registers it on the connection, waking it at once if
  # it is already ready, as the NIF does.
  defp arm(core, identity, direction, {_pid, _operation, reference} = waiter, ready) do
    sockets = Sockets.arm(core.sockets, identity, direction, waiter)
    {:ok, record} = fetch_in(sockets, identity)
    record = %{record | wakers: Map.put(record.wakers, direction, reference)}
    core = %{core | sockets: Sockets.put_state(sockets, identity, record)}
    if ready, do: wake_now(core, identity, direction), else: {core, []}
  end

  defp wake_now(core, identity, direction) do
    {effects, sockets} = Sockets.wake(core.sockets, identity, direction)
    {%{core | sockets: sockets}, effects}
  end

  # Fires a waker registered with `reference`: the waiter is woken only if
  # it is still the one that registered it.
  defp fire(core, _identity, _direction, nil), do: {core, []}

  defp fire(core, identity, direction, reference) do
    case Sockets.fetch(core.sockets, identity, :tcp) do
      {:ok, entry} ->
        case Sockets.waiter(entry, direction) do
          {_pid, _operation, ^reference} -> wake_now(core, identity, direction)
          _other -> {core, []}
        end

      _gone ->
        {core, []}
    end
  end

  # Takes a connection's wakes and fires the wakers they consume.
  defp take_wakes(core, {:socket, id}, conn) do
    {{rx, tx}, conn} = Connection.take_wakes(conn)
    {:ok, entry} = Map.fetch(core.sockets.entries, id)
    identity = {id, entry.generation}
    record = entry.state
    {rx_ref, read} = if rx, do: {record.wakers.read, nil}, else: {nil, record.wakers.read}
    {tx_ref, write} = if tx, do: {record.wakers.write, nil}, else: {nil, record.wakers.write}
    record = %{record | conn: conn, wakers: %{read: read, write: write}}
    core = put(core, identity, record)
    {core, read_effects} = fire(core, identity, :read, rx_ref)
    {core, write_effects} = fire(core, identity, :write, tx_ref)
    {core, read_effects ++ write_effects}
  end

  defp take_wakes(core, {:embryo, key}, conn) do
    {{rx, _tx}, conn} = Connection.take_wakes(conn)
    embryo = Map.fetch!(core.tcp.embryos, key)
    {reference, waker} = if rx, do: {embryo.waker, nil}, else: {nil, embryo.waker}
    embryo = %{embryo | conn: conn, waker: waker}
    core = %{core | tcp: %{core.tcp | embryos: Map.put(core.tcp.embryos, key, embryo)}}

    case Map.fetch(core.sockets.entries, embryo.listener) do
      {:ok, entry} -> fire(core, {embryo.listener, entry.generation}, :read, reference)
      :error -> {core, []}
    end
  end

  defp take_wakes(core, {:closing, id}, conn) do
    {_wakes, conn} = Connection.take_wakes(conn)
    closing = Map.fetch!(core.tcp.closing, id)
    closing = %{closing | record: %{closing.record | conn: conn}}
    {%{core | tcp: %{core.tcp | closing: Map.put(core.tcp.closing, id, closing)}}, []}
  end

  ## Ingress

  @doc """
  A segment from the link, in `ip`: to the connection it belongs to, else
  a listener's new handshake, else a RST. Returns the core, the replies
  as effects (reversed, added to `effects`) and the wakes.
  """
  def ingress(core, ip, segment, now, effects, wakes) do
    {src, dst} = ip_addresses(ip)
    local = {dst, Segment.tcp(segment, :dst_port)}
    remote = {src, Segment.tcp(segment, :src_port)}
    cx = cx(core, now)

    case Map.fetch(core.tcp.conns, {local, remote}) do
      {:ok, ref} ->
        conn = get_conn(core, ref)

        if Connection.accepts?(conn, {src, dst}, segment),
          do: input(core, ref, conn, cx, ip, segment, effects, wakes),
          else: to_listener(core, ip, segment, local, remote, cx, effects, wakes)

      :error ->
        to_listener(core, ip, segment, local, remote, cx, effects, wakes)
    end
  end

  defp input(core, ref, conn, cx, ip, segment, effects, wakes) do
    {src, dst} = ip_addresses(ip)
    before = Connection.state(conn)
    {reply, conn} = Connection.input(conn, cx, {src, dst}, segment)
    core = put_conn(core, ref, conn)
    core = record_reset(core, ref, before, conn)
    {core, woken} = take_wakes(core, ref, conn)
    {retire(core, ref, conn), emit(core, reply, effects), woken ++ wakes}
  end

  # A handshake a RST sent back to listening is over at once, so that the
  # next SYN on its 4-tuple starts a new one from the listener, with an ISN
  # of its own.
  defp retire(core, {:embryo, key}, conn) do
    if Connection.state(conn) == :listen, do: drop_embryo(core, key), else: core
  end

  defp retire(core, _ref, _conn), do: core

  # A RST that closes a connection being opened refuses it, and one that
  # closes an established connection resets it, as the NIF tells them
  # apart.
  defp record_reset(core, {:socket, _id} = ref, before, conn) do
    failure =
      case before do
        :established -> :reset
        state when state in [:syn_sent, :syn_received] -> :refused
        _other -> nil
      end

    if failure != nil and Connection.state(conn) == :closed,
      do: update_record(core, ref, &%{&1 | phase: {:failed, failure}}),
      else: core
  end

  defp record_reset(core, _ref, _before, _conn), do: core

  defp to_listener(core, ip, segment, {address, port} = local, remote, cx, effects, wakes) do
    listener =
      Map.get(core.tcp.listeners, local) ||
        Map.get(core.tcp.listeners, {Sockets.unspecified(address), port})

    with id when id != nil <- listener,
         {:ok, entry} <- Map.fetch(core.sockets.entries, id),
         true <- listens?(entry.state, segment),
         true <- room?(core, id, entry.state.listener) do
      handshake(core, {id, entry.state}, ip, segment, {local, remote}, cx, {effects, wakes})
    else
      _no_listener -> {core, emit(core, Iface.rst_reply(ip, segment), effects), wakes}
    end
  end

  # A listening socket takes neither ACKs nor RSTs.
  defp listens?(%Record{listener: listener}, segment) when listener != nil,
    do: Segment.tcp(segment, :ack) == nil and Segment.tcp(segment, :control) != :rst

  defp listens?(_record, _segment), do: false

  defp room?(core, id, listener),
    do:
      Enum.count(core.tcp.embryos, fn {_key, embryo} -> embryo.listener == id end) <
        listener.backlog

  # A segment for a listener: a SYN starts a handshake, taking the next
  # ISN; anything else a listening socket drops.
  defp handshake(core, {id, record}, ip, segment, {local, _remote} = key, cx, {effects, wakes}) do
    {isn, rand} = Rand.next_u32(core.tcp.rand)

    conn =
      new_conn(core, record.rcvbuf, record.sndbuf)
      |> Connection.set_timeout(@connect_timeout * @ms)
      |> Connection.listen(local)
      |> elem(1)

    {_wakes, conn} = Connection.take_wakes(conn)
    {src, dst} = ip_addresses(ip)
    {reply, conn} = Connection.input(conn, %{cx | isn: isn}, {src, dst}, segment)

    if Connection.state(conn) == :syn_received do
      embryo = %{
        conn: conn,
        listener: id,
        order: System.unique_integer([:positive, :monotonic]),
        waker: record.listener.waker
      }

      tcp = %{
        core.tcp
        | rand: rand,
          embryos: Map.put(core.tcp.embryos, key, embryo),
          conns: Map.put(core.tcp.conns, key, {:embryo, key})
      }

      core = %{core | tcp: tcp}
      {core, woken} = take_wakes(core, {:embryo, key}, conn)
      {core, emit(core, reply, effects), woken ++ wakes}
    else
      {core, emit(core, reply, effects), wakes}
    end
  end

  defp emit(_core, nil, effects), do: effects

  defp emit(core, {src, dst, _hop_limit, segment}, effects),
    do: emit(core, Iface.tcp_packet(src, dst, segment), effects)

  defp emit(core, packet, effects) when is_binary(packet) do
    if Iface.fits?(core.iface, packet), do: [{:emit, packet} | effects], else: effects
  end

  defp ip_addresses(Ipv4.ipv4(src: src, dst: dst)), do: {src, dst}
  defp ip_addresses(Ipv6.ipv6(src: src, dst: dst)), do: {src, dst}

  ## Driving

  @doc """
  Drives the stack after a call: listeners promote their established
  handshakes, every connection sends what it has, closing connections past
  their deadline are then aborted and send their RSTs, and finished ones
  are forgotten. The
  reply's deadline is the next connection timer or close deadline.
  """
  def drive({reply, core, effects}, now) do
    {core, promoted} = refresh_listeners(core)
    {core, sent, wakes} = egress(core, now)
    {core, sent, wakes} = expire_closing(core, now, sent, wakes)
    {core, promoted_after} = refresh_listeners(core)
    core = reap(core)

    finish(
      {reply, core, effects ++ promoted ++ :lists.reverse(sent) ++ wakes ++ promoted_after},
      now,
      :drive
    )
  end

  @doc "The same, for ingress, whose replies come first."
  def drive_ingress(core, now, effects, wakes) do
    {core, promoted} = refresh_listeners(core)
    {core, sent, egress_wakes} = egress(core, now)
    {core, sent, egress_wakes} = expire_closing(core, now, sent, egress_wakes)
    {core, promoted_after} = refresh_listeners(core)
    core = reap(core)
    core = %{core | poll_at: poll_at(core, now, :drive)}
    {core, effects ++ :lists.reverse(sent), wakes ++ promoted ++ egress_wakes ++ promoted_after}
  end

  defp finish({reply, core, effects}, now, kind) do
    {notices, others} = Enum.split_with(effects, &(elem(&1, 0) in [:wake, :abort]))
    {aborts, wakes} = Enum.split_with(notices, &(elem(&1, 0) == :abort))

    {reply, %{core | poll_at: poll_at(core, now, kind)},
     others ++ aborts ++ Enum.sort_by(wakes, &elem(&1, 2))}
  end

  # Closing connections past their deadline are aborted, once the others
  # have sent what they had, so that one sends what fell due at its deadline
  # before its RST, which a second pass sends: the NIF sweeps its closing
  # sockets after egress, and sends their RSTs in the continuation that
  # sweep asks for.
  defp expire_closing(core, now, sent, wakes) do
    {closing, expired} =
      Enum.map_reduce(core.tcp.closing, false, fn
        {id, %{deadline: deadline} = closing}, _expired
        when deadline != nil and now >= deadline ->
          conn = closing.record.conn

          conn =
            if Connection.state(conn) != :closed,
              do: conn |> Connection.abort() |> Connection.take_wakes() |> elem(1),
              else: conn

          {{id, %{closing | record: %{closing.record | conn: conn}, deadline: nil}}, true}

        other, expired ->
          {other, expired}
      end)

    core = %{core | tcp: %{core.tcp | closing: Map.new(closing)}}

    if expired,
      do: egress_pass(core, cx(core, now), sent, wakes),
      else: {core, sent, wakes}
  end

  # Handshakes that completed join their listener's accept queue, or are
  # dropped when it is full or the table is; those that died are dropped.
  defp refresh_listeners(core) do
    core.tcp.embryos
    |> Enum.sort_by(fn {_key, embryo} -> embryo.order end)
    |> Enum.reduce({core, []}, fn {key, embryo}, {core, effects} ->
      case Connection.state(embryo.conn) do
        state when state in [:established, :close_wait] ->
          promote(core, key, embryo, effects)

        state when state in [:listen, :syn_received] ->
          {drop_dead(core, key, embryo, state), effects}

        :closed ->
          {drop_dead(core, key, embryo, :closed), effects}

        _other ->
          {drop_embryo(core, key), effects}
      end
    end)
  end

  defp drop_dead(core, key, _embryo, :syn_received), do: keep(core, key)

  # A handshake that timed out sends its RST before it is forgotten.
  defp drop_dead(core, key, embryo, :closed) do
    if Connection.remote_endpoint(embryo.conn) != nil,
      do: keep(core, key),
      else: drop_embryo(core, key)
  end

  defp drop_dead(core, key, _embryo, :listen), do: drop_embryo(core, key)

  defp keep(core, _key), do: core

  defp drop_embryo(core, key) do
    tcp = %{
      core.tcp
      | embryos: Map.delete(core.tcp.embryos, key),
        conns:
          if(Map.get(core.tcp.conns, key) == {:embryo, key},
            do: Map.delete(core.tcp.conns, key),
            else: core.tcp.conns
          ),
        refills: core.tcp.refills + 1
    }

    %{core | tcp: tcp}
  end

  defp promote(core, {local, remote} = key, embryo, effects) do
    case Map.fetch(core.sockets.entries, embryo.listener) do
      {:ok, %{state: %Record{listener: listener} = record, generation: generation}}
      when listener != nil ->
        if :queue.len(listener.accepted) < listener.backlog and logical_capacity(core) == :ok and
             native_capacity(core, 1) == :ok and
             buffer_capacity(core, record.rcvbuf + record.sndbuf) == :ok do
          conn = Connection.set_timeout(embryo.conn, core.tcp.timers.user_timeout * @ms)

          child = %Record{
            family: record.family,
            rcvbuf: record.rcvbuf,
            sndbuf: record.sndbuf,
            conn: conn,
            phase: :connected,
            local: local,
            local_scope_id: record.local_scope_id,
            remote: remote,
            remote_scope_id: record.local_scope_id,
            accepted: true
          }

          {:ok, child_identity, sockets} =
            Sockets.open(core.sockets, :tcp, child, table_limit(core))

          {child_id, _generation} = child_identity
          listener_identity = {embryo.listener, generation}

          record = %{
            record
            | listener: %{listener | accepted: :queue.in(child_identity, listener.accepted)}
          }

          sockets = Sockets.put_state(sockets, listener_identity, record)

          tcp = %{
            core.tcp
            | embryos: Map.delete(core.tcp.embryos, key),
              conns: Map.put(core.tcp.conns, key, {:socket, child_id}),
              promotions: core.tcp.promotions + 1,
              refills: core.tcp.refills + 1
          }

          core = %{core | sockets: sockets, tcp: tcp}
          {core, ready} = wake_now(core, listener_identity, :read)
          {core, effects ++ ready}
        else
          core = drop_embryo(core, key)
          {%{core | tcp: %{core.tcp | overflow_drops: core.tcp.overflow_drops + 1}}, effects}
        end

      _gone ->
        {drop_embryo(core, key), effects}
    end
  end

  # Every connection with a peer, asked in turn for one segment, until none
  # has more. Returns the segments sent (reversed) and the wakes.
  defp egress(core, now) do
    cx = cx(core, now)
    egress_pass(core, cx, [], [])
  end

  defp egress_pass(core, cx, sent, wakes) do
    {core, sent_now, wakes} =
      core
      |> connections()
      |> Enum.reduce({core, [], wakes}, fn ref, {core, sent, wakes} ->
        conn = get_conn(core, ref)
        {result, conn} = dispatch(conn, cx)
        core = put_conn(core, ref, conn)
        {core, woken} = take_wakes(core, ref, conn)
        {core, emit(core, result, sent), woken ++ wakes}
      end)

    if sent_now == [],
      do: {core, sent, wakes},
      else: egress_pass(core, cx, sent_now ++ sent, wakes)
  end

  defp dispatch(conn, cx) do
    case Connection.dispatch(conn, cx) do
      {:segment, out, conn} -> {out, conn}
      {:nothing, conn} -> {nil, conn}
    end
  end

  # Every connection, in the order its socket opened.
  defp connections(core) do
    sockets =
      for {id, %{kind: :tcp, state: %Record{conn: conn, listener: nil}}} <- core.sockets.entries,
          Connection.remote_endpoint(conn) != nil or Connection.state(conn) != :closed,
          do: {id, {:socket, id}}

    embryos = for {key, embryo} <- core.tcp.embryos, do: {embryo.order, {:embryo, key}}
    closing = for {id, _closing} <- core.tcp.closing, do: {id, {:closing, id}}

    (sockets ++ embryos ++ closing)
    |> Enum.sort()
    |> Enum.map(&elem(&1, 1))
  end

  # Closing connections that have finished, or sent their RST, are gone.
  defp reap(core) do
    Enum.reduce(core.tcp.closing, core, fn {id, %{record: %{conn: conn}}}, core ->
      if Connection.state(conn) == :closed and Connection.remote_endpoint(conn) == nil,
        do: forget_closing(core, id),
        else: core
    end)
  end

  ## Deadlines

  # The next time the core needs a poll, in milliseconds: the earliest of
  # the connections' timers (`:current`, as the NIF reports it after a call
  # that does not drive the stack) and, after one that does, the closing
  # connections' deadlines too.
  defp poll_at(core, now, kind) do
    cx = cx(core, now)

    times =
      for ref <- connections(core),
          time = poll_time(Connection.poll_at(get_conn(core, ref), cx)),
          time != nil,
          do: time

    deadlines =
      if kind == :drive,
        do: for({_id, %{deadline: deadline}} <- core.tcp.closing, deadline != nil, do: deadline),
        else: []

    case times ++ deadlines do
      [] -> nil
      all -> Enum.min(all)
    end
  end

  defp poll_time(:now), do: 0
  defp poll_time(:ingress), do: nil
  defp poll_time({:time, at}), do: div(at, @ms)

  ## Snapshot

  @doc "The TCP part of the snapshot."
  def snapshot(core) do
    records =
      for {id, %{kind: :tcp, state: record, generation: generation}} <- core.sockets.entries,
          do: {id, generation, record}

    listeners =
      for {_id, _generation, %Record{listener: listener}} <- records,
          listener != nil,
          do: listener

    %{
      tcp_socket_count: length(records) - length(listeners) + map_size(core.tcp.closing),
      tcp_listener_count: length(listeners),
      accepted_queue_count:
        Enum.sum(for listener <- listeners, do: :queue.len(listener.accepted)),
      listener_backlog_capacity: Enum.sum(for listener <- listeners, do: listener.backlog),
      closing_tcp_socket_count: map_size(core.tcp.closing),
      buffer_bytes: buffer_bytes(core),
      # As the NIF lists its records, which a closing socket keeps until
      # its connection is done.
      sockets:
        (records ++
           for(
             {id, %{generation: generation, record: record}} <- core.tcp.closing,
             do: {id, generation, record}
           ))
        |> Enum.map(fn {id, generation, record} ->
          %{id: id, generation: generation, rcvbuf: record.rcvbuf, sndbuf: record.sndbuf}
        end)
        |> Enum.sort_by(& &1.id),
      counters: %{
        listener_promotions: core.tcp.promotions,
        listener_refills: core.tcp.refills,
        listener_overflow_drops: core.tcp.overflow_drops
      }
    }
  end

  defp buffer_bytes(core) do
    sockets =
      for {_id, %{kind: :tcp, state: record}} <- core.sockets.entries,
          do: record.rcvbuf + record.sndbuf

    closing = for {_id, %{record: record}} <- core.tcp.closing, do: record.rcvbuf + record.sndbuf
    Enum.sum(sockets) + Enum.sum(closing)
  end

  ## Helpers

  defp cx(core, now) do
    iface = core.iface

    %{
      now: now * @ms,
      ip_mtu: iface.mtu,
      addresses: Iface.addresses(iface, :inet) ++ Iface.addresses(iface, :inet6),
      isn: nil,
      source_address: &Iface.source_address(iface, &1)
    }
  end

  @doc """
  Whether the stack has room for another socket: the NIF counts closing
  TCP sockets, which have left the table, against the socket limit, for
  every protocol.
  """
  @spec logical_capacity(map()) :: :ok | {:error, :system_limit}
  def logical_capacity(core) do
    if map_size(core.sockets.entries) + map_size(core.tcp.closing) >= core.limits.sockets,
      do: {:error, :system_limit},
      else: :ok
  end

  @doc """
  Whether `added` more sockets fit beside those the NIF would hold for the
  stack: each UDP ring (a wildcard socket has one per address), each TCP
  socket and each closing connection.
  """
  @spec native_capacity(map(), non_neg_integer()) :: :ok | {:error, :system_limit}
  def native_capacity(core, added) do
    rings = core.sockets |> Sockets.states(:udp) |> Enum.map(&length(&1.rings)) |> Enum.sum()
    tcp = length(Sockets.states(core.sockets, :tcp)) + map_size(core.tcp.closing)
    if rings + tcp + added > core.limits.sockets, do: {:error, :system_limit}, else: :ok
  end

  # What the socket table may hold, the closing sockets' share taken out.
  defp table_limit(core), do: core.limits.sockets - map_size(core.tcp.closing)

  @doc """
  Whether `added` buffer bytes fit the stack's total, TCP's and UDP's
  together, as the NIF caps them.
  """
  @spec buffer_capacity(map(), non_neg_integer()) :: :ok | {:error, :system_limit}
  def buffer_capacity(core, added) do
    udp = Udp.buffer_bytes(Sockets.states(core.sockets, :udp))

    if buffer_bytes(core) + udp + added <= @max_socket_buffer_bytes,
      do: :ok,
      else: {:error, :system_limit}
  end

  defp family(record, address) do
    if Sockets.family?(record.family, address), do: :ok, else: {:error, :invalid_address}
  end

  defp fetch(core, identity) do
    case fetch_any(core, identity) do
      {:ok, %Record{listener: listener}} when listener != nil -> {:error, :invalid_socket_state}
      result -> result
    end
  end

  defp fetch_any(core, identity), do: fetch_in(core.sockets, identity)

  defp fetch_in(sockets, identity) do
    case Sockets.fetch(sockets, identity, :tcp) do
      {:ok, entry} -> {:ok, entry.state}
      error -> error
    end
  end

  defp fetch_entry(core, identity) do
    case Sockets.fetch(core.sockets, identity, :tcp) do
      {:ok, %{state: %Record{listener: listener}}} when listener != nil ->
        {:error, :invalid_socket_state}

      {:ok, entry} ->
        {:ok, entry.state, entry}

      error ->
        error
    end
  end

  defp put(core, identity, record),
    do: %{core | sockets: Sockets.put_state(core.sockets, identity, record)}

  defp get_conn(core, {:socket, id}), do: Map.fetch!(core.sockets.entries, id).state.conn
  defp get_conn(core, {:embryo, key}), do: Map.fetch!(core.tcp.embryos, key).conn
  defp get_conn(core, {:closing, id}), do: Map.fetch!(core.tcp.closing, id).record.conn

  defp put_conn(core, {:socket, _id} = ref, conn),
    do: update_record(core, ref, &%{&1 | conn: conn})

  defp put_conn(core, {:embryo, key}, conn) do
    embryos = Map.update!(core.tcp.embryos, key, &%{&1 | conn: conn})
    %{core | tcp: %{core.tcp | embryos: embryos}}
  end

  defp put_conn(core, {:closing, id}, conn) do
    closing = Map.update!(core.tcp.closing, id, &%{&1 | record: %{&1.record | conn: conn}})
    %{core | tcp: %{core.tcp | closing: closing}}
  end

  defp update_conn(core, ref, fun), do: put_conn(core, ref, fun.(get_conn(core, ref)))

  defp update_record(core, {:socket, id}, fun) do
    entries = Map.update!(core.sockets.entries, id, &%{&1 | state: fun.(&1.state)})
    %{core | sockets: %{core.sockets | entries: entries}}
  end

  defp identity_map({id, generation}), do: %{id: id, generation: generation}
end
