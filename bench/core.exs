# The protocol engine alone: two stacks' backend states joined by an
# in-memory link in this one process, with no Stack process, no link,
# socket or app processes and no real clock. The client opens `STREAMS`
# connections to the server and sends `MIB` MiB over each; the run
# reports the time spent inside each end's backend calls per MiB, so the
# figures are the engine's own cost.
#
#   MIX_ENV=prod mix run bench/core.exs
#
# Environment:
#
#   BACKENDS  elixir,native (default elixir)
#   FAMILY    inet or inet6 (default inet)
#   MIB       MiB per stream (default 32)
#   STREAMS   connections (default 1)
#   REVERSE   1: the server sends as much back over each connection
#   CLOSE     1: each end closes once it has sent its data and read the
#             peer's, and the run goes on until both stacks are idle
#   DROP      n: once connected, the link loses every nth packet each way
#   REPEATS   timed transfers per backend, after one warm-up (default 3)
#   GC        tuned: run each transfer in a process with the Stack
#             process's GC flags for its backend; default: plain
#   DIGEST    1: instead of timing, print a digest of everything the
#             backends returned, in order: every reply and effect, the
#             final virtual time and the packet counts. Two runs of the
#             Elixir engine behave alike if and only if their digests do.
#             The NIF's is not reproducible: its calls yield to
#             continuations when its wall-clock budget runs out. The wire
#             digest covers only the packets each end sent, in order, so
#             it is reproducible for both and compares the two.
#             all: the same for a fixed set of runs (`tripwire` below),
#             the check that an optimisation changed no behaviour
#   PROFILE   call_count, call_time or call_memory: one more transfer per
#             backend under `:tprof`, top `TOP` (60) functions
#
# The clock is virtual: it stands still while either end has work, and
# jumps to the earliest deadline when neither has. Each sending app writes
# 64 KiB at a time and each receiving app reads whatever is there, each
# waiting for its wake when the stack has no room or no data, as the
# socket API does. The payload is one 64 KiB block of seeded random bytes.
# Only the backend calls are timed; the driver's own work is not.

defmodule Bench.Core do
  alias SmolNet.Stack.Options

  @chunk_bytes 65_536
  @buffer 262_144
  @port 5_001

  def addresses(:inet), do: {{192, 0, 2, 1}, {192, 0, 2, 2}, 24}

  def addresses(:inet6),
    do: {{0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}, {0x2001, 0xDB8, 0, 0, 0, 0, 0, 2}, 64}

  @doc "One run, returning per-end timings and counts, and the digest."
  def run(backend, options) do
    family = options.family
    {client_address, server_address, prefix} = addresses(family)

    w = %{
      backend: backend,
      now: 0,
      st: %{
        c: new_stack(backend, client_address, prefix, 1),
        s: new_stack(backend, server_address, prefix, 2)
      },
      q: %{c: :queue.new(), s: :queue.new()},
      sent: %{c: 0, s: 0},
      drop: nil,
      timer: %{c: nil, s: nil},
      idle_poll: nil,
      woken: MapSet.new(),
      native: %{c: 0, s: 0},
      calls: %{c: 0, s: 0},
      packets: %{c: 0, s: 0},
      digest: if(options.digest, do: :crypto.hash_init(:sha256)),
      wire:
        if(options.digest, do: %{c: :crypto.hash_init(:sha256), s: :crypto.hash_init(:sha256)}),
      refs: %{}
    }

    endpoint = %{address: address_bytes(server_address), port: @port, scope_id: 0}
    {w, listener} = ok(call(w, :s, &backend.tcp_open(&1, family, @buffer, @buffer), :preserve))
    {w, :ok} = ok(call(w, :s, &backend.tcp_bind(&1, listener, endpoint), :preserve))

    {w, :ok} =
      ok(call(w, :s, &backend.tcp_listen(&1, listener, min(options.streams, 128), w.now)))

    # Connections open in groups of up to four at once, as many handshakes
    # as the NIF's listener pool takes together.
    {w, pairs} =
      1..options.streams
      |> Enum.chunk_every(4)
      |> Enum.reduce({w, []}, fn group, {w, pairs} ->
        {w, connects} = connect_group(w, family, endpoint, length(group))
        {w, accepted} = accept_all(w, listener, length(group), [])
        {w, pairs ++ Enum.zip(connects, accepted)}
      end)

    connects = Enum.map(pairs, &elem(&1, 0))
    accepted = Enum.map(pairs, &elem(&1, 1))

    # Losses start once the connections are up, so every handshake completes.
    w = %{
      w
      | native: %{c: 0, s: 0},
        calls: %{c: 0, s: 0},
        packets: %{c: 0, s: 0},
        drop: options.drop
    }

    bytes = options.bytes
    reverse = if options.reverse, do: bytes, else: 0

    actors =
      Enum.flat_map(Enum.zip(connects, accepted), fn {conn, server} ->
        [
          actor(:c, conn, bytes, reverse, options.close),
          actor(:s, server, reverse, bytes, options.close)
        ]
      end)

    {w, _actors} = transfer_loop(w, actors, options.chunk)
    w = if options.close, do: settle(w), else: w

    digest =
      if w.digest do
        w.digest
        |> :crypto.hash_update(:erlang.term_to_binary({w.now, w.packets, w.calls}))
        |> :crypto.hash_final()
        |> Base.encode16(case: :lower)
        |> binary_part(0, 16)
      end

    wire =
      if w.wire do
        for side <- [:c, :s], into: "" do
          w.wire[side] |> :crypto.hash_final() |> Base.encode16(case: :lower) |> binary_part(0, 8)
        end
      end

    %{
      native: w.native,
      calls: w.calls,
      packets: w.packets,
      virtual_ms: w.now,
      digest: digest,
      wire: wire
    }
  end

  defp connect_group(w, family, endpoint, count) do
    {connects, w} =
      Enum.map_reduce(1..count, w, fn _stream, w ->
        {w, conn} =
          ok(call(w, :c, &w.backend.tcp_open(&1, family, @buffer, @buffer), :preserve))

        ref = make_ref()

        {w, {:select, :connect, ^ref}} =
          ok(call(w, :c, &w.backend.tcp_connect(&1, conn, endpoint, self(), ref, w.now)))

        {{conn, ref}, w}
      end)

    w =
      run_until(w, fn w ->
        Enum.all?(connects, fn {_conn, ref} -> MapSet.member?(w.woken, ref) end) and idle_link?(w)
      end)

    Enum.reduce(connects, {w, []}, fn {conn, ref}, {w, conns} ->
      {w, :ok} = ok(call(w, :c, &w.backend.tcp_connect(&1, conn, endpoint, self(), ref, w.now)))
      {%{w | woken: MapSet.delete(w.woken, ref)}, conns ++ [conn]}
    end)
  end

  defp actor(side, socket, send, receive, close) do
    %{
      side: side,
      socket: socket,
      send: {:ready, send},
      receive: {:ready, receive},
      close: close,
      closed: false
    }
  end

  defp new_stack(backend, address, prefix, seed) do
    {:ok, options} =
      Options.parse(
        addresses: [{address, prefix}],
        test_determinism: %{
          random_seed: seed,
          timestamp_origin: seed,
          ephemeral_port_start: 49_152
        }
      )

    {{:ok, _envelope}, state, []} = backend.stack_new(options.limits, options.native_config, 0)
    state
  end

  defp address_bytes({_, _, _, _} = address), do: Tuple.to_list(address)

  defp address_bytes(address),
    do: for(word <- Tuple.to_list(address), byte <- [div(word, 256), rem(word, 256)], do: byte)

  defp ok({w, {:ok, %{result: result}}}), do: {w, result}

  defp accept_all(w, _listener, 0, accepted), do: {w, Enum.reverse(accepted)}

  defp accept_all(w, listener, left, accepted) do
    ref = make_ref()

    case ok(call(w, :s, &w.backend.tcp_accept(&1, listener, self(), ref, w.now))) do
      {w, {:ok, socket, _family}} ->
        accept_all(w, listener, left - 1, [socket | accepted])

      {w, {:select, :accept, ^ref}} ->
        w = run_until(w, fn w -> MapSet.member?(w.woken, ref) end)
        accept_all(%{w | woken: MapSet.delete(w.woken, ref)}, listener, left, accepted)
    end
  end

  # A backend call on one end: timed, its packets queued for the other end,
  # its wakes noted, and its deadline taken as the shell takes it: a call
  # that drives the stack replaces the timer, nil cancelling it, and one
  # that does not (`:preserve`) keeps the timer unless it names a deadline.
  defp call(w, side, fun, timer \\ :replace) do
    t0 = :erlang.monotonic_time()
    {reply, state, effects} = fun.(w.st[side])
    t1 = :erlang.monotonic_time()
    w = %{w | st: %{w.st | side => state}, native: %{w.native | side => w.native[side] + t1 - t0}}
    w = %{w | calls: %{w.calls | side => w.calls[side] + 1}}
    w = digest(w, side, reply, effects)
    w = effects(w, side, effects)
    w = drain_mailbox(w)

    case reply do
      {:ok, %{more: true}} ->
        {w, _more} = call(w, side, &w.backend.stack_poll(&1, w.now))
        {w, reply}

      {:ok, %{poll_at: nil}} when timer == :preserve ->
        {w, reply}

      {:ok, %{poll_at: poll_at}} ->
        {%{w | timer: %{w.timer | side => poll_at}}, reply}

      _other ->
        {w, reply}
    end
  end

  defp digest(%{digest: nil} = w, _side, _reply, _effects), do: w

  defp digest(w, side, reply, effects) do
    {term, w} = normalize({side, reply, effects}, w)

    wire =
      for {:emit, packet} <- effects, reduce: w.wire[side] do
        hash -> :crypto.hash_update(hash, packet)
      end

    %{
      w
      | digest: :crypto.hash_update(w.digest, :erlang.term_to_binary(term)),
        wire: %{w.wire | side => wire}
    }
  end

  # References, pids and socket identities (from VM-wide counters) differ
  # from run to run; references and identities are numbered in the order
  # they first appear.
  defp normalize(ref, w) when is_reference(ref), do: number(ref, w)
  defp normalize(%{id: id, generation: generation}, w), do: number({:socket, id, generation}, w)

  defp normalize({:wake, pid, {id, generation}, ref}, w),
    do: normalize({:wake, pid, %{id: id, generation: generation}, ref}, w)

  defp normalize({:abort, pid, {id, generation}, ref, reason}, w),
    do: normalize({:abort, pid, %{id: id, generation: generation}, ref, reason}, w)

  defp normalize(pid, w) when is_pid(pid), do: {:pid, w}

  defp normalize(tuple, w) when is_tuple(tuple) do
    {list, w} = normalize(Tuple.to_list(tuple), w)
    {List.to_tuple(list), w}
  end

  defp normalize(list, w) when is_list(list), do: Enum.map_reduce(list, w, &normalize/2)

  defp normalize(%{} = map, w) do
    {pairs, w} = map |> Map.to_list() |> Enum.sort() |> normalize(w)
    {Map.new(pairs), w}
  end

  defp normalize(other, w), do: {other, w}

  defp number(key, w) do
    case w.refs do
      %{^key => n} -> {{:n, n}, w}
      refs -> {{:n, map_size(refs)}, %{w | refs: Map.put(refs, key, map_size(refs))}}
    end
  end

  defp effects(w, _side, []), do: w

  defp effects(w, side, [{:emit, packet} | rest]) do
    other = other(side)
    n = w.sent[side] + 1
    w = %{w | sent: %{w.sent | side => n}, packets: %{w.packets | side => w.packets[side] + 1}}

    w =
      if w.drop != nil and rem(n, w.drop) == 0,
        do: w,
        else: %{w | q: %{w.q | other => :queue.in(packet, w.q[other])}}

    effects(w, side, rest)
  end

  defp effects(w, side, [{:wake, _pid, _identity, ref} | rest]),
    do: effects(%{w | woken: MapSet.put(w.woken, ref)}, side, rest)

  defp effects(w, side, [{:abort, _pid, _identity, _ref, _reason} | rest]),
    do: effects(w, side, rest)

  # The NIF sends its readiness messages itself.
  defp drain_mailbox(w) do
    receive do
      {:"$smol_socket", _identity, :select, ref} ->
        drain_mailbox(%{w | woken: MapSet.put(w.woken, ref)})

      {:"$smol_socket", _identity, :abort, _ref, _reason} ->
        drain_mailbox(w)
    after
      0 -> w
    end
  end

  defp other(:c), do: :s
  defp other(:s), do: :c

  defp idle_link?(w), do: :queue.is_empty(w.q.c) and :queue.is_empty(w.q.s)

  defp run_until(w, done?) do
    if done?.(w), do: w, else: run_until(step(w), done?)
  end

  # Delivers one queued packet to each end, server first, and fires due
  # timers; with nothing queued, the clock jumps to the next deadline.
  defp step(w) do
    if idle_link?(w),
      do: advance(w),
      else: w |> deliver(:s) |> deliver(:c) |> fire_due()
  end

  defp deliver(w, side) do
    case :queue.out(w.q[side]) do
      {{:value, packet}, queue} ->
        w = %{w | q: %{w.q | side => queue}}
        {w, _reply} = call(w, side, &w.backend.stack_ingress(&1, packet, w.now))
        w

      {:empty, _queue} ->
        w
    end
  end

  defp fire_due(w) do
    Enum.reduce([:c, :s], w, fn side, w ->
      case w.timer[side] do
        at when is_integer(at) and at <= w.now -> poll(w, side)
        _later -> w
      end
    end)
  end

  defp poll(w, side) do
    w = %{w | timer: %{w.timer | side => nil}}
    {w, _reply} = call(w, side, &w.backend.stack_poll(&1, w.now))
    w
  end

  # A deadline already due when nothing else is left is polled at once,
  # as the shell's timer would fire; if that leaves it due, the clock moves
  # on a millisecond, as a real one would before the next poll.
  defp advance(w) do
    case Enum.reject([w.timer.c, w.timer.s], &is_nil/1) do
      [] ->
        raise "stalled at #{w.now} ms"

      times ->
        at = Enum.min(times)

        now =
          cond do
            at > w.now -> at
            w.idle_poll == w.now -> w.now + 1
            true -> w.now
          end

        fire_due(%{w | now: now, idle_poll: now})
    end
  end

  # After the closes: until both stacks are idle, or a minute on.
  defp settle(w), do: settle(w, w.now + 60_000)

  defp settle(w, until) do
    cond do
      not idle_link?(w) -> settle(step(w), until)
      w.timer.c == nil and w.timer.s == nil -> w
      w.now > until -> w
      true -> settle(advance(w), until)
    end
  end

  defp transfer_loop(w, actors, chunk) do
    {actors, w} =
      Enum.map_reduce(actors, w, fn actor, w ->
        {w, actor} = act(w, actor, chunk)
        {actor, w}
      end)

    cond do
      Enum.all?(actors, &done?/1) ->
        {w, actors}

      w.now > 600_000 ->
        raise "no progress by #{w.now} ms: #{inspect(actors)}"

      idle_link?(w) and not Enum.any?(actors, &ready?/1) ->
        transfer_loop(advance(w), actors, chunk)

      true ->
        transfer_loop(step(w), actors, chunk)
    end
  end

  defp done?(%{close: true} = actor), do: actor.closed
  defp done?(actor), do: actor.send == {:ready, 0} and actor.receive == {:ready, 0}

  defp ready?(actor),
    do:
      not done?(actor) and
        (match?({:ready, n} when n > 0, actor.send) or
           match?({:ready, n} when n > 0, actor.receive))

  defp act(w, actor, chunk) do
    {w, actor} = wake(w, actor)
    {w, actor} = do_send(w, actor, chunk)
    {w, actor} = do_receive(w, actor)
    maybe_close(w, actor)
  end

  defp wake(w, actor) do
    {w, send} = wake_one(w, actor.send)
    {w, receive} = wake_one(w, actor.receive)
    {w, %{actor | send: send, receive: receive}}
  end

  defp wake_one(w, {{:waiting, ref}, left} = state) do
    if MapSet.member?(w.woken, ref),
      do: {%{w | woken: MapSet.delete(w.woken, ref)}, {:ready, left}},
      else: {w, state}
  end

  defp wake_one(w, state), do: {w, state}

  defp do_send(w, %{send: {:ready, left}} = actor, chunk) when left > 0 do
    data = binary_part(chunk, 0, min(left, @chunk_bytes))
    ref = make_ref()

    case ok(call(w, actor.side, &w.backend.tcp_send(&1, actor.socket, data, self(), ref, w.now))) do
      {w, :ok} ->
        do_send(w, %{actor | send: {:ready, left - byte_size(data)}}, chunk)

      {w, {:select, :send, ^ref, accepted}} ->
        {w, %{actor | send: {{:waiting, ref}, left - accepted}}}
    end
  end

  defp do_send(w, actor, _chunk), do: {w, actor}

  # One read a turn. A receiver with nothing left to read goes on reading
  # for the peer's FIN when it closes.
  defp do_receive(w, %{receive: {:ready, left}} = actor)
       when left > 0 or (actor.close and not actor.closed) do
    ref = make_ref()

    case call(w, actor.side, &w.backend.tcp_recv(&1, actor.socket, 0, self(), ref, w.now)) do
      {w, {:ok, %{result: {:ok, data}}}} ->
        {w, %{actor | receive: {:ready, left - byte_size(data)}}}

      {w, {:ok, %{result: {:select, :recv, ^ref}}}} ->
        {w, %{actor | receive: {{:waiting, ref}, left}}}

      {w, {:error, :end_of_stream}} ->
        {w, %{actor | receive: {:eof, left}}}

      {_w, {:error, reason}} ->
        raise "receive failed: #{inspect(reason)} at #{w.now} ms"
    end
  end

  defp do_receive(w, actor), do: {w, actor}

  defp maybe_close(
         w,
         %{close: true, closed: false, send: {:ready, 0}, receive: {:eof, 0}} = actor
       ) do
    {w, _reply} = call(w, actor.side, &w.backend.tcp_close(&1, actor.socket, w.now))
    {w, %{actor | closed: true}}
  end

  defp maybe_close(w, %{close: true, closed: false, send: {:ready, 0}} = actor) do
    # The sending half closes once everything is written, so the peer sees
    # a FIN while it may still be sending.
    if Map.get(actor, :shut, false) do
      {w, actor}
    else
      {w, _reply} = call(w, actor.side, &w.backend.tcp_shutdown(&1, actor.socket, :write, w.now))
      {w, Map.put(actor, :shut, true)}
    end
  end

  defp maybe_close(w, actor), do: {w, actor}
end

Mix.ensure_application!(:runtime_tools)
Mix.ensure_application!(:tools)

env = fn name, default -> System.get_env(name, default) end

backends =
  env.("BACKENDS", "elixir")
  |> String.split(",")
  |> Enum.map(fn name ->
    {:ok, module} = SmolNet.Stack.Backend.resolve(String.to_atom(name))
    Code.ensure_loaded!(module)
    {name, module}
  end)

mib = String.to_integer(env.("MIB", "32"))
streams = String.to_integer(env.("STREAMS", "1"))
repeats = String.to_integer(env.("REPEATS", "3"))
profile = System.get_env("PROFILE")

options = %{
  family: String.to_atom(env.("FAMILY", "inet")),
  bytes: mib * 1_048_576,
  streams: streams,
  reverse: env.("REVERSE", "0") == "1",
  close: env.("CLOSE", "0") == "1",
  drop: (drop = System.get_env("DROP")) && String.to_integer(drop),
  digest: false,
  chunk: 65_536 |> :rand.bytes_s(:rand.seed_s(:exsss, {1, 2, 3})) |> elem(0)
}

# The MiB the engine carried: each stream's, both ways with REVERSE.
total_mib = mib * streams * if(options.reverse, do: 2, else: 1)

flags = fn module ->
  if env.("GC", "plain") == "tuned" and function_exported?(module, :process_flags, 0),
    do: module.process_flags(),
    else: []
end

# Each run has a fresh process, so one run's garbage is not the next one's,
# with the Stack process's GC flags when GC=tuned.
in_process = fn module, fun ->
  parent = self()

  {pid, monitor} =
    :erlang.spawn_opt(fn -> send(parent, {:result, fun.()}) end, [:monitor | flags.(module)])

  receive do
    {:result, result} ->
      receive do: ({:DOWN, ^monitor, :process, ^pid, _reason} -> :ok)
      result

    {:DOWN, ^monitor, :process, ^pid, reason} ->
      raise "run failed: #{inspect(reason)}"
  end
end

ms = fn native -> :erlang.convert_time_unit(native, :native, :nanosecond) / 1_000_000 end
per_mib = fn value -> Float.round(value / total_mib, 2) end

describe =
  "#{options.family}, #{streams} stream(s) of #{mib} MiB#{if options.reverse, do: " each way"}" <>
    "#{if options.close, do: ", closed"}#{if options.drop, do: ", losing 1 in #{options.drop}"}"

# The tripwire set for DIGEST=all: one and many streams, both families,
# both directions, closes, and losses light to heavy, which reach
# recovery, the closing connections and the listener's queue.
tripwire = [
  %{family: :inet, bytes: 4_194_304, streams: 1, reverse: false, close: false, drop: nil},
  %{family: :inet6, bytes: 4_194_304, streams: 1, reverse: false, close: true, drop: nil},
  %{family: :inet, bytes: 1_048_576, streams: 3, reverse: true, close: true, drop: nil},
  %{family: :inet, bytes: 2_097_152, streams: 4, reverse: true, close: true, drop: 50},
  %{family: :inet6, bytes: 2_097_152, streams: 8, reverse: true, close: true, drop: 13},
  %{family: :inet, bytes: 1_048_576, streams: 2, reverse: false, close: true, drop: 7},
  %{family: :inet6, bytes: 262_144, streams: 1, reverse: true, close: true, drop: 3}
]

case env.("DIGEST", "0") do
  "0" ->
    nil

  digest ->
    runs = if digest == "all", do: Enum.map(tripwire, &Map.merge(options, &1)), else: [options]

    for run <- runs, {name, module} <- backends do
      r = in_process.(module, fn -> Bench.Core.run(module, %{run | digest: true}) end)

      IO.puts(
        "#{name} #{run.family} x#{run.streams} #{div(run.bytes, 1024)} KiB" <>
          "#{if run.reverse, do: " both ways"}#{if run.close, do: " closed"}" <>
          "#{if run.drop, do: " drop 1/#{run.drop}"}: digest #{r.digest}, wire #{r.wire}, virtual #{r.virtual_ms} ms, " <>
          "packets #{r.packets.c} + #{r.packets.s}, calls #{r.calls.c} + #{r.calls.s}"
      )
    end
end

if env.("DIGEST", "0") != "0" do
  :ok
else
  IO.puts(
    "core bench: #{describe}, GC #{env.("GC", "plain")}, #{System.schedulers_online()} schedulers"
  )

  measure = fn module ->
    in_process.(module, fn ->
      {:reductions, r0} = :erlang.process_info(self(), :reductions)
      :msacc.reset()
      :msacc.start()
      t0 = :erlang.monotonic_time()
      result = Bench.Core.run(module, options)
      wall = :erlang.monotonic_time() - t0
      stats = :msacc.stats()
      :msacc.stop()
      {:reductions, r1} = :erlang.process_info(self(), :reductions)

      gc_us =
        for(
          %{type: :scheduler, counters: k} <- stats,
          {state, v} <- k,
          state in [:gc, :gc_full],
          do: v
        )
        |> Enum.sum()

      Map.merge(result, %{wall: wall, gc_ms: gc_us / 1000, reductions: r1 - r0})
    end)
  end

  for {name, module} <- backends do
    _warmup = measure.(module)

    for i <- 1..repeats do
      r = measure.(module)
      engine = ms.(r.native.c + r.native.s)

      IO.puts(
        "#{name} ##{i}: engine #{per_mib.(engine)} ms/MiB " <>
          "(client #{per_mib.(ms.(r.native.c))}, server #{per_mib.(ms.(r.native.s))}), " <>
          "wall #{per_mib.(ms.(r.wall))}, GC #{per_mib.(r.gc_ms)} ms/MiB, " <>
          "#{Float.round(total_mib * 8 * 1.048576 / engine * 1000, 1)} Mbit/s engine-bound, " <>
          "calls/MiB #{per_mib.(r.calls.c)} + #{per_mib.(r.calls.s)}, " <>
          "packets/MiB #{per_mib.(r.packets.c)} + #{per_mib.(r.packets.s)}, " <>
          "reductions/MiB #{per_mib.(r.reductions)}"
      )
    end

    if profile do
      type = String.to_atom(profile)
      IO.puts("\n#{name}: tprof #{type}")

      rows =
        in_process.(module, fn ->
          {_result, {^type, entries}} =
            :tprof.profile(fn -> Bench.Core.run(module, options) end, %{
              type: type,
              report: :return
            })

          for {m, f, a, per_pid} <- entries do
            {count, measurement} =
              Enum.reduce(per_pid, {0, 0}, fn {_pid, c, v}, {cs, vs} -> {cs + c, vs + v} end)

            {"#{inspect(m)}.#{f}/#{a}", count, measurement}
          end
        end)

      # The driver's own functions are left out of the total.
      {driver, engine} =
        Enum.split_with(rows, fn {name, _, _} -> String.starts_with?(name, "Bench.") end)

      total = engine |> Enum.map(&elem(&1, 2)) |> Enum.sum()
      driver_total = driver |> Enum.map(&elem(&1, 2)) |> Enum.sum()

      IO.puts(
        "engine #{type} #{per_mib.(total)} per MiB; the driver's #{per_mib.(driver_total)} left out"
      )

      engine
      |> Enum.sort_by(&elem(&1, 2), :desc)
      |> Enum.take(String.to_integer(env.("TOP", "60")))
      |> Enum.each(fn {name, count, value} ->
        IO.puts(
          String.pad_trailing(name, 70) <>
            String.pad_leading("#{Float.round(100 * value / max(total, 1), 2)}%", 8) <>
            String.pad_leading("#{per_mib.(count)}", 12) <>
            String.pad_leading("#{per_mib.(value)}", 14)
        )
      end)

      IO.puts("(columns: share, calls per MiB, #{type} per MiB)")
    end
  end
end
