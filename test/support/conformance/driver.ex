defmodule SmolNet.Conformance.Driver do
  @moduledoc false

  # Drives one stack's backend directly, with no Stack process: every call
  # takes an explicit time, and the driver records it as a trace entry, so
  # that two backends given the same calls can be compared call by call.
  #
  # Calls are the `SmolNet.Stack.Backend` callbacks, named as there, with
  # their arguments in order but without the state, the caller pid or the
  # time, which the driver supplies. Sockets and references are labels:
  # `{:socket, n}` for the nth socket identity the stack returned, and
  # `{:ref, n}` for a reference from `ref/1`. Results come back the same
  # way, so a trace holds no pid, reference or backend-chosen identity.
  #
  # Each call is carried to completion as the shell would: while the reply
  # says `more: true`, the driver polls at the same time, and the entry has
  # the packets of the call and those polls together. Readiness and abort
  # notices are the call's `events`, whether the backend returned them as
  # effects or, as the NIF does, sent them to the caller; either way they
  # become `{:wake, socket, ref}` and `{:abort, socket, ref, reason}`.

  alias SmolNet.Stack.Options

  # Each callback's arguments after the state: `:arg` is the caller's next
  # argument, `:pid` the caller (this process), `:now` the call's time.
  @calls %{
    stack_ingress: [:arg, :now],
    stack_ingress_batch: [:arg, :now],
    stack_poll: [:now],
    stack_grant_egress: [:arg, :arg, :now],
    stack_shutdown: [:arg],
    stack_snapshot: [],
    socket_cancel: [:arg, :arg, :arg],
    socket_validate: [:arg],
    tcp_open: [:arg, :arg, :arg],
    tcp_bind: [:arg, :arg],
    tcp_listen: [:arg, :arg, :now],
    tcp_accept: [:arg, :pid, :arg, :now],
    tcp_connect: [:arg, :arg, :pid, :arg, :now],
    tcp_send: [:arg, :arg, :pid, :arg, :now],
    tcp_recv: [:arg, :arg, :pid, :arg, :now],
    tcp_shutdown: [:arg, :arg, :now],
    tcp_sockname: [:arg],
    tcp_peername: [:arg],
    tcp_set_nodelay: [:arg, :arg, :now],
    tcp_nodelay: [:arg],
    tcp_set_keepalive: [:arg, :arg, :now],
    tcp_keepalive: [:arg],
    tcp_close: [:arg, :now],
    udp_open: [:arg],
    udp_bind: [:arg, :arg],
    udp_connect: [:arg, :arg],
    udp_sendto: [:arg, :arg, :arg, :pid, :arg, :now],
    udp_recvfrom: [:arg, :arg, :pid, :arg, :now],
    udp_sockname: [:arg],
    udp_peername: [:arg],
    udp_close: [:arg, :now]
  }

  # Calls after which the shell keeps its poll timer when the reply names no
  # deadline (`:preserve_timer` in SmolNet.Stack), and the snapshot, which
  # schedules nothing.
  @preserve_timer [
    :socket_cancel,
    :socket_validate,
    :stack_snapshot,
    :tcp_open,
    :udp_open,
    :tcp_bind,
    :udp_bind,
    :udp_connect,
    :tcp_sockname,
    :tcp_peername,
    :udp_sockname,
    :udp_peername,
    :tcp_nodelay,
    :tcp_keepalive
  ]

  # Continuation polls one call may need before the driver gives up.
  @max_polls 10_000

  defstruct [
    :name,
    :backend,
    :state,
    :timer,
    :key,
    version: 0,
    continuations: 0,
    sockets: %{},
    socket_labels: %{},
    refs: %{},
    ref_labels: %{},
    trace: []
  ]

  @type t :: %__MODULE__{}
  @type entry :: %{
          stack: atom(),
          at: integer(),
          call: atom(),
          args: [term()],
          result: term(),
          poll_at: integer() | nil,
          emitted: [binary()],
          events: [tuple()]
        }

  def calls, do: Map.keys(@calls)

  @doc """
  Starts a stack on `backend` at `now`, with the options `SmolNet.start_stack/1`
  takes (less `:egress` and `:backend`). Returns `{result, driver}`; the
  result is `:ok` or `{:error, reason}`.
  """
  @spec start(atom(), module(), keyword(), integer()) :: {term(), t()}
  def start(name, backend, options, now) do
    driver = %__MODULE__{name: name, backend: backend, key: {__MODULE__, make_ref()}}

    {:ok, %{limits: limits, native_config: config}} = Options.parse(options)
    outcome = backend.stack_new(limits, config, now)
    # The new state is the backend's to keep, not a result to compare.
    outcome =
      case outcome do
        {{:ok, envelope}, state, effects} -> {{:ok, %{envelope | result: :stack}}, state, effects}
        error -> error
      end

    {result, driver} = finish(driver, :stack_new, [options], now, outcome)

    case result do
      {:ok, :stack} -> {:ok, driver}
      error -> {error, driver}
    end
  end

  @doc "A fresh reference label."
  @spec ref(t()) :: {{:ref, pos_integer()}, t()}
  def ref(driver) do
    label = {:ref, map_size(driver.refs) + 1}
    ref = make_ref()

    {label,
     %{
       driver
       | refs: Map.put(driver.refs, label, ref),
         ref_labels: Map.put(driver.ref_labels, ref, label)
     }}
  end

  @doc """
  Makes a call at `now`. Returns `{result, driver}`, where the result is
  `{:ok, result}` or `{:error, reason}` with labels for sockets and
  references.
  """
  @spec call(t(), atom(), [term()], integer()) :: {term(), t()}
  def call(driver, call, args, now) do
    spec = Map.fetch!(@calls, call)

    outcome =
      apply(driver.backend, call, [checked_state(driver) | backend_args(driver, spec, args, now)])

    finish(driver, call, args, now, outcome)
  end

  @doc "The entries recorded so far, oldest first."
  @spec trace(t()) :: [entry()]
  def trace(driver), do: Enum.reverse(driver.trace)

  @doc "When the shell would next poll the stack, or nil."
  def timer(driver), do: driver.timer

  @doc """
  The continuation polls the driver has made, which a backend counts
  among its calls but no entry records.
  """
  @spec continuations(t()) :: non_neg_integer()
  def continuations(driver), do: driver.continuations

  defp backend_args(driver, spec, args, now) do
    {backend_args, []} =
      Enum.map_reduce(spec, args, fn
        :arg, [arg | args] -> {resolve(driver, arg), args}
        :pid, args -> {self(), args}
        :now, args -> {now, args}
      end)

    backend_args
  end

  # Labels back to the backend's own values.
  defp resolve(driver, {:socket, _n} = label), do: Map.fetch!(driver.sockets, label)

  defp resolve(driver, {:ref, _n} = label) do
    Map.get_lazy(driver.refs, label, fn -> raise ArgumentError, "unknown #{inspect(label)}" end)
  end

  defp resolve(driver, list) when is_list(list), do: Enum.map(list, &resolve(driver, &1))
  defp resolve(_driver, value), do: value

  defp finish(driver, call, args, now, {reply, state, effects}) do
    driver = advance(driver, state)
    {reply, driver, effects} = drain(driver, reply, now, effects)
    {events, driver} = events(driver, effects)
    {result, driver} = label(driver, reply_result(reply))

    poll_at =
      case reply do
        {:ok, %{poll_at: poll_at}} -> poll_at
        _error -> nil
      end

    entry = %{
      stack: driver.name,
      at: now,
      call: call,
      args: args,
      result: result,
      poll_at: poll_at,
      emitted: for({:emit, packet} <- effects, do: packet),
      events: events
    }

    {result, %{driver | trace: [entry | driver.trace], timer: timer(driver, call, reply)}}
  end

  defp reply_result({:ok, %{result: result}}), do: {:ok, result}
  defp reply_result({:error, _reason} = error), do: error

  defp timer(driver, call, {:ok, %{poll_at: nil}}) when call in @preserve_timer, do: driver.timer
  defp timer(_driver, _call, {:ok, %{poll_at: poll_at}}), do: poll_at
  defp timer(driver, _call, {:error, _reason}), do: driver.timer

  # Polls at the same time while the backend retains work, as the shell does
  # at once, so that a call's output does not depend on where a backend's
  # own budget cut it.
  defp drain(driver, reply, now, effects, polls \\ 0)

  defp drain(driver, {:ok, %{more: true} = envelope}, now, effects, polls)
       when polls < @max_polls do
    {poll_reply, state, poll_effects} = driver.backend.stack_poll(checked_state(driver), now)
    driver = %{advance(driver, state) | continuations: driver.continuations + 1}

    case poll_reply do
      {:ok, %{more: more, poll_at: poll_at}} ->
        reply = {:ok, %{envelope | more: more, poll_at: poll_at}}
        drain(driver, reply, now, effects ++ poll_effects, polls + 1)

      {:error, reason} ->
        raise "a continuation poll failed with #{inspect(reason)}"
    end
  end

  defp drain(_driver, {:ok, %{more: true}}, _now, _effects, _polls) do
    raise "the backend retained work after #{@max_polls} polls"
  end

  defp drain(driver, reply, _now, effects, _polls), do: {reply, driver, effects}

  # The notices the call gave, from its effects and from this process's
  # mailbox, where the NIF sends them during the call.
  defp events(driver, effects) do
    sent = collect_messages([])

    returned =
      for effect <- effects, elem(effect, 0) in [:wake, :abort] do
        case effect do
          {:wake, _pid, identity, ref} -> {:wake, identity, ref}
          {:abort, _pid, identity, ref, reason} -> {:abort, identity, ref, reason}
        end
      end

    Enum.map_reduce(sent ++ returned, driver, fn event, driver ->
      {identity, ref} = {elem(event, 1), elem(event, 2)}
      {socket, driver} = label(driver, identity_map(identity))
      {ref, driver} = label(driver, ref)
      {event |> put_elem(1, socket) |> put_elem(2, ref), driver}
    end)
  end

  defp collect_messages(events) do
    receive do
      {:"$smol_socket", identity, :select, ref} ->
        collect_messages([{:wake, identity, ref} | events])

      {:"$smol_socket", identity, :abort, ref, reason} ->
        collect_messages([{:abort, identity, ref, reason} | events])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp identity_map({id, generation}), do: %{id: id, generation: generation}
  defp identity_map(%{id: _id, generation: _generation} = identity), do: identity

  # Replaces identities, references and pids in a term with labels.
  defp label(driver, %{id: id, generation: generation} = identity)
       when map_size(identity) == 2 and is_integer(id) and is_integer(generation) do
    case Map.fetch(driver.socket_labels, {id, generation}) do
      {:ok, label} ->
        {label, driver}

      :error ->
        label = {:socket, map_size(driver.sockets) + 1}

        {label,
         %{
           driver
           | sockets: Map.put(driver.sockets, label, identity),
             socket_labels: Map.put(driver.socket_labels, {id, generation}, label)
         }}
    end
  end

  defp label(driver, ref) when is_reference(ref) do
    {Map.get(driver.ref_labels, ref, {:ref, :unknown}), driver}
  end

  defp label(driver, pid) when is_pid(pid), do: {:caller, driver}

  defp label(driver, list) when is_list(list), do: Enum.map_reduce(list, driver, &label(&2, &1))

  defp label(driver, tuple) when is_tuple(tuple) do
    {list, driver} = label(driver, Tuple.to_list(tuple))
    {List.to_tuple(list), driver}
  end

  defp label(driver, map) when is_map(map) do
    {pairs, driver} =
      Enum.map_reduce(map, driver, fn {key, value}, driver ->
        {value, driver} = label(driver, value)
        {{key, value}, driver}
      end)

    {Map.new(pairs), driver}
  end

  defp label(driver, other), do: {other, driver}

  # The backend state goes through the driver only: a call made with any
  # state but the latest one is a bug in the harness, which a backend that
  # mutates its state in place, as the NIF does, would hide.
  defp checked_state(driver) do
    latest = Process.get(driver.key, 0)

    if driver.version != latest do
      raise "stale backend state for #{inspect(driver.name)}: " <>
              "version #{driver.version}, latest #{latest}"
    end

    driver.state
  end

  defp advance(driver, state) do
    version = driver.version + 1
    Process.put(driver.key, version)
    %{driver | state: state, version: version}
  end
end
