defmodule SmolNet.Conformance.LinkSim do
  @moduledoc false

  # Two stacks joined by a simulated link that loses, duplicates, delays and
  # reorders packets, as a pure fold over their backends in virtual time:
  # no processes, no timers, no wall clock. A client connects to a server,
  # sends it a stream and closes; the server reads to the end and closes.
  # The run checks integrity (the server read exactly what was sent) and
  # liveness (it did so before a virtual deadline), and returns the calls
  # both backends saw, in order, as a trace.
  #
  # Time moves to whichever comes first: the next packet's arrival or a
  # stack's poll deadline. Everything random, the link's choices and the
  # stream's bytes, comes from the seed, and the stacks get fixed
  # determinism hooks, so a run is a function of its options.

  alias SmolNet.Conformance.Driver
  alias SmolNet.Stack.Backend

  @port 80
  @addresses %{
    inet: %{client: {192, 0, 2, 1}, server: {192, 0, 2, 2}, prefix: 24},
    inet6: %{
      client: {0xFD00, 0, 0, 0, 0, 0, 0, 1},
      server: {0xFD00, 0, 0, 0, 0, 0, 0, 2},
      prefix: 64
    }
  }

  @profile %{loss: 0.0, duplicate: 0.0, reorder: 0.0, delay: 5, jitter: 0}

  # Virtual time the stacks get after the transfer, so the closes finish and
  # TIME-WAIT (10 s) ends.
  @linger 15_000

  # A bound on calls, against a stack that asks to be polled without end.
  @max_calls 200_000

  @type result :: %{
          status: :ok | {:corrupt, map()} | {:stalled, integer()} | {:timeout, integer()},
          sent: binary(),
          received: binary(),
          finished_at: integer() | nil,
          stats: map(),
          entries: [Driver.entry()]
        }

  @doc """
  Runs one transfer. Options: `:seed` (required), `:bytes` (default 32 KiB),
  `:family` (`:inet` or `:inet6`), `:profile` (`:loss`, `:duplicate` and
  `:reorder` probabilities, `:delay` and `:jitter` in milliseconds; a
  reordered packet is held back an extra `delay`), `:backend` (default the
  suite's) and `:deadline`, the virtual milliseconds the transfer may take
  (default 300 s).
  """
  @spec run(keyword()) :: result()
  def run(options) do
    seed = Keyword.fetch!(options, :seed)
    family = Keyword.get(options, :family, :inet)
    backend = Keyword.get_lazy(options, :backend, &Backend.default/0)
    profile = Map.merge(@profile, Map.new(Keyword.get(options, :profile, %{})))
    rand = :rand.seed_s(:exsss, {seed, 1, 2})
    {payload, rand} = :rand.bytes_s(Keyword.get(options, :bytes, 32_768), rand)
    client = start(:client, backend, family, seed)
    server = start(:server, backend, family, seed)

    world = %{
      now: 0,
      rand: rand,
      profile: profile,
      family: family,
      deadline: Keyword.get(options, :deadline, 300_000),
      in_flight: [],
      order: 0,
      log: Enum.reverse(Driver.trace(client) ++ Driver.trace(server)),
      calls: 0,
      finished_at: nil,
      not_before: %{client: 0, server: 0},
      stats: %{sent: 0, dropped: 0, duplicated: 0, reordered: 0, delivered: 0},
      stacks: %{client: client, server: server},
      apps: %{
        client: %{phase: :open, socket: nil, ref: nil, waiting: nil, remaining: payload},
        server: %{
          phase: :open,
          listener: nil,
          socket: nil,
          ref: nil,
          waiting: nil,
          received: []
        }
      }
    }

    world
    |> loop()
    |> result(payload)
  end

  @doc """
  Whether a run shows #177: the client stopped resending its FIN, so the
  server never learnt of the close, which ended at the client's 30 s close
  deadline in a reset, or in silence if the link lost that too. Seen from
  the trace: every byte arrived, the run did not finish cleanly, and the
  client, having sent a FIN, sent nothing but a reset in the last 25 s. A
  stack that resends its FIN at each timeout would have resent it there.
  """
  @spec lost_fin?(result()) :: boolean()
  def lost_fin?(%{status: status} = result) when status != :ok do
    sent =
      for %{stack: :client, at: at} = entry <- result.entries,
          packet <- entry.emitted,
          do: {at, flags(packet)}

    case for({at, flags} <- sent, fin?(flags), do: at) do
      [] ->
        false

      _fins ->
        quiet_from = ended_at(status) - 25_000
        late = for {at, flags} <- sent, at > quiet_from, not rst?(flags), do: flags
        result.received == result.sent and late == []
    end
  end

  def lost_fin?(_result), do: false

  defp ended_at({:failed, {_side, _reason, at}}), do: at
  defp ended_at({_status, at}), do: at

  defp flags(<<4::4, ihl::4, _::binary-size(8), 6, _rest::binary>> = packet),
    do: tcp_flags(binary_part(packet, ihl * 4, byte_size(packet) - ihl * 4))

  defp flags(<<6::4, _::28, _length::16, 6, _hop, _addresses::binary-32, tcp::binary>>),
    do: tcp_flags(tcp)

  defp flags(_packet), do: 0

  defp tcp_flags(<<_::binary-size(13), flags, _rest::binary>>), do: flags
  defp tcp_flags(_segment), do: 0

  defp fin?(flags), do: Bitwise.band(flags, 0x05) == 0x01
  defp rst?(flags), do: Bitwise.band(flags, 0x04) == 0x04

  defp start(side, backend, family, seed) do
    addresses = Map.fetch!(@addresses, family)

    options = [
      addresses: [{Map.fetch!(addresses, side), addresses.prefix}],
      test_determinism: %{
        random_seed: seed * 2 + if(side == :client, do: 0, else: 1),
        timestamp_origin: 0,
        ephemeral_port_start: 50_000
      }
    ]

    {:ok, driver} = Driver.start(side, backend, options, 0)
    driver
  end

  defp loop(world) do
    world = run_apps(world)

    cond do
      Map.has_key?(world, :failure) ->
        Map.put(world, :status, {:failed, world.failure})

      world.calls > @max_calls ->
        Map.put(world, :status, {:stalled, world.now})

      finished?(world) and quiet?(world) ->
        Map.put(world, :status, :ok)

      true ->
        advance(world, next_time(world))
    end
  end

  defp advance(world, nil), do: Map.put(world, :status, {:stalled, world.now})

  defp advance(world, at) when at > world.deadline and is_nil(world.finished_at),
    do: Map.put(world, :status, {:timeout, world.now})

  defp advance(world, at), do: %{world | now: max(at, world.now)} |> deliver() |> poll() |> loop()

  defp finished?(world), do: world.apps.server.phase == :done and world.apps.client.phase == :done

  # Done once nothing is in flight and the stacks need no poll before the
  # linger ends.
  defp quiet?(world) do
    world.in_flight == [] and
      Enum.all?([:client, :server], fn side ->
        timer = timer(world, side)
        is_nil(timer) or timer > world.finished_at + @linger
      end)
  end

  defp next_time(world) do
    arrivals = Enum.map(world.in_flight, &elem(&1, 0))
    timers = for side <- [:client, :server], timer = timer(world, side), do: timer

    case arrivals ++ timers do
      [] -> nil
      times -> Enum.min(times)
    end
  end

  defp deliver(world) do
    {due, later} =
      Enum.split_with(world.in_flight, fn {at, _order, _to, _packet} -> at <= world.now end)

    world = %{world | in_flight: later}

    due
    |> Enum.sort()
    |> Enum.reduce(world, fn {_at, _order, to, packet}, world ->
      world = update_in(world.stats.delivered, &(&1 + 1))
      {_result, world} = call(world, to, :stack_ingress, [packet])
      world
    end)
  end

  defp poll(world) do
    Enum.reduce([:client, :server], world, fn side, world ->
      timer = timer(world, side)

      if timer && timer <= world.now do
        {_result, world} = call(world, side, :stack_poll, [])
        put_in(world.not_before[side], world.now + 1)
      else
        world
      end
    end)
  end

  # When a stack is next polled. A stack polled at a time may name that same
  # time again, having had nothing to do yet (the NIF does, for timers that
  # fall due after their deadline's millisecond); the shell's timer then
  # fires again as the clock moves on, so the next poll is a millisecond on.
  defp timer(world, side) do
    case Driver.timer(world.stacks[side]) do
      nil -> nil
      timer -> max(timer, world.not_before[side])
    end
  end

  # A call on one side's stack: records it, puts what it emitted on the
  # link, and wakes the side's app if it was waiting on a notice it gave.
  defp call(world, side, call, args) do
    {result, driver} = Driver.call(world.stacks[side], call, args, world.now)
    [entry | _rest] = driver.trace
    world = %{world | stacks: Map.put(world.stacks, side, driver), log: [entry | world.log]}
    world = %{world | calls: world.calls + 1}
    world = Enum.reduce(entry.emitted, world, &transmit(&2, peer(side), &1))
    {result, wake(world, side, entry.events)}
  end

  defp wake(world, side, events) do
    app = world.apps[side]

    if Enum.any?(events, &(elem(&1, 2) == app.waiting)) do
      put_in(world.apps[side].waiting, nil)
    else
      world
    end
  end

  defp peer(:client), do: :server
  defp peer(:server), do: :client

  defp transmit(world, to, packet) do
    world = update_in(world.stats.sent, &(&1 + 1))
    {lost, rand} = :rand.uniform_s(world.rand)
    world = %{world | rand: rand}

    if lost < world.profile.loss do
      update_in(world.stats.dropped, &(&1 + 1))
    else
      {duplicate, rand} = :rand.uniform_s(world.rand)
      copies = if duplicate < world.profile.duplicate, do: 2, else: 1
      world = %{world | rand: rand}
      world = if copies == 2, do: update_in(world.stats.duplicated, &(&1 + 1)), else: world
      Enum.reduce(1..copies, world, fn _copy, world -> schedule(world, to, packet) end)
    end
  end

  defp schedule(world, to, packet) do
    profile = world.profile
    {jitter, rand} = :rand.uniform_s(profile.jitter + 1, world.rand)
    {reorder, rand} = :rand.uniform_s(rand)
    held = if reorder < profile.reorder, do: profile.delay, else: 0
    world = if held > 0, do: update_in(world.stats.reordered, &(&1 + 1)), else: world
    at = world.now + profile.delay + jitter - 1 + held

    %{
      world
      | rand: rand,
        order: world.order + 1,
        in_flight: [{at, world.order, to, packet} | world.in_flight]
    }
  end

  # Runs each app until it waits on a notice or has nothing left to do.
  defp run_apps(world) do
    world
    |> run_app(:server)
    |> run_app(:client)
  end

  defp run_app(world, side) do
    app = world.apps[side]

    if app.waiting || app.phase == :done do
      world
    else
      case step(world, side, app) do
        {:ok, world} -> run_app(world, side)
        {:wait, world} -> world
      end
    end
  end

  defp step(world, :server, %{phase: :open} = app) do
    address = endpoint(world, :server)
    {{:ok, listener}, world} = call(world, :server, :tcp_open, [world.family, 65_536, 65_536])
    {{:ok, :ok}, world} = call(world, :server, :tcp_bind, [listener, address])
    {{:ok, :ok}, world} = call(world, :server, :tcp_listen, [listener, 1])
    {:ok, put_app(world, :server, %{app | phase: :accept, listener: listener})}
  end

  defp step(world, :server, %{phase: :accept} = app) do
    {ref, world} = ref(world, :server, app)

    case call(world, :server, :tcp_accept, [app.listener, ref]) do
      {{:ok, {:ok, socket, _family}}, world} ->
        {:ok, put_app(world, :server, %{app | phase: :read, socket: socket, ref: nil})}

      {{:ok, {:select, :accept, ^ref}}, world} ->
        wait(world, :server, ref)
    end
  end

  defp step(world, :server, %{phase: :read} = app) do
    {ref, world} = ref(world, :server, %{app | ref: nil})

    case call(world, :server, :tcp_recv, [app.socket, 0, ref]) do
      {{:ok, {:ok, data}}, world} ->
        {:ok, put_app(world, :server, %{app | received: [app.received | data]})}

      {{:ok, {:select, :recv, ^ref}}, world} ->
        wait(world, :server, ref)

      {{:ok, {:select, :recv, ^ref, data}}, world} ->
        world = put_app(world, :server, %{app | received: [app.received | data]})
        wait(world, :server, ref)

      {{:error, :end_of_stream}, world} ->
        {:ok, put_app(world, :server, %{app | phase: :close})}

      {{:error, reason}, world} ->
        fail(world, :server, reason)
    end
  end

  defp step(world, :server, %{phase: :close} = app) do
    {{:ok, :ok}, world} = call(world, :server, :tcp_close, [app.socket])
    {{:ok, :ok}, world} = call(world, :server, :tcp_close, [app.listener])
    world = %{world | finished_at: world.finished_at || world.now}
    {:ok, put_app(world, :server, %{app | phase: :done})}
  end

  defp step(world, :client, %{phase: :open} = app) do
    {{:ok, socket}, world} = call(world, :client, :tcp_open, [world.family, 65_536, 65_536])
    {:ok, put_app(world, :client, %{app | phase: :connect, socket: socket})}
  end

  defp step(world, :client, %{phase: :connect} = app) do
    {ref, world} = ref(world, :client, app)

    case call(world, :client, :tcp_connect, [app.socket, endpoint(world, :server), ref]) do
      {{:ok, result}, world} when result in [:ok, :ready] ->
        {:ok, put_app(world, :client, %{app | phase: :send, ref: nil})}

      {{:ok, {:select, :connect, ^ref}}, world} ->
        wait(world, :client, ref)

      {{:error, reason}, world} ->
        fail(world, :client, reason)
    end
  end

  defp step(world, :client, %{phase: :send, remaining: <<>>} = app) do
    {{:ok, :ok}, world} = call(world, :client, :tcp_close, [app.socket])
    {:ok, put_app(world, :client, %{app | phase: :done})}
  end

  defp step(world, :client, %{phase: :send} = app) do
    {ref, world} = ref(world, :client, %{app | ref: nil})

    case call(world, :client, :tcp_send, [app.socket, app.remaining, ref]) do
      {{:ok, :ok}, world} ->
        {:ok, put_app(world, :client, %{app | remaining: <<>>})}

      {{:ok, {:select, :send, ^ref, sent}}, world} ->
        rest = binary_part(app.remaining, sent, byte_size(app.remaining) - sent)
        wait(put_app(world, :client, %{app | remaining: rest}), :client, ref)

      {{:error, reason}, world} ->
        fail(world, :client, reason)
    end
  end

  # The app's reference for a call it retries until done, or a new one.
  defp ref(world, side, %{ref: nil}) do
    {ref, driver} = Driver.ref(world.stacks[side])
    {ref, %{world | stacks: Map.put(world.stacks, side, driver)}}
  end

  defp ref(world, _side, %{ref: ref}), do: {ref, world}

  # A call failed: the app stops, and the run ends with the failure.
  defp fail(world, side, reason) do
    world = Map.update(world, :failure, {side, reason, world.now}, & &1)
    {:wait, put_app(world, side, %{world.apps[side] | phase: :done})}
  end

  defp wait(world, side, ref) do
    app = %{world.apps[side] | waiting: ref, ref: ref}
    {:wait, put_app(world, side, app)}
  end

  defp put_app(world, side, app), do: put_in(world.apps[side], app)

  defp endpoint(world, side) do
    address = @addresses |> Map.fetch!(world.family) |> Map.fetch!(side)
    %{address: address_bytes(address), port: @port, scope_id: 0}
  end

  defp address_bytes(address) when tuple_size(address) == 4, do: Tuple.to_list(address)

  defp address_bytes(address) do
    address |> Tuple.to_list() |> Enum.flat_map(&[div(&1, 256), rem(&1, 256)])
  end

  defp result(world, payload) do
    received = IO.iodata_to_binary(world.apps.server.received)

    status =
      case world.status do
        :ok when received != payload ->
          {:corrupt, %{sent: byte_size(payload), received: byte_size(received)}}

        status ->
          status
      end

    %{
      status: status,
      sent: payload,
      received: received,
      finished_at: world.finished_at,
      stats: world.stats,
      entries: Enum.reverse(world.log)
    }
  end
end
