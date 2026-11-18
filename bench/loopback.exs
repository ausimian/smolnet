# One stack on a `SmolNet.Loopback` link carries a TCP transfer to
# itself, through the public socket API, so its Stack process does both
# ends' protocol work beside the link and socket processes. Reports the
# rate; from microstate accounting, the CPU and garbage collection per
# MiB over every scheduler; and the backend's calls, polls and packets per
# MiB from `stack_info`.
#
#   MIX_ENV=prod ERL_FLAGS="+S 4 +sbwt none +sbwtdcpu none +sbwtdio none" \
#     taskset -c 8-11 mix run bench/loopback.exs
#
# Environment:
#
#   BACKENDS  native,elixir (default both, interleaved per repeat)
#   FAMILY    inet or inet6 (default inet)
#   MIB       MiB per transfer (default 64)
#   REPEATS   transfers per backend (default 3)
#   TRACE     1: also trace the Stack process for its own time on a
#             scheduler and in garbage collection per MiB, which bounds
#             the rate; tracing slows it, so the rate is then lower
#
# Use `MIX_ENV=prod` so the NIF is the release build. Turn scheduler
# busy-waiting off, or the CPU figures count spinning as work.

Mix.ensure_application!(:runtime_tools)

mib = String.to_integer(System.get_env("MIB", "64"))
repeats = String.to_integer(System.get_env("REPEATS", "3"))
family = String.to_atom(System.get_env("FAMILY", "inet"))
trace? = System.get_env("TRACE", "0") == "1"

backends =
  System.get_env("BACKENDS", "native,elixir") |> String.split(",") |> Enum.map(&String.to_atom/1)

chunk = :crypto.strong_rand_bytes(65_536)

{address, prefix} =
  case family do
    :inet -> {{192, 0, 2, 1}, 24}
    :inet6 -> {{0x2001, 0xDB8, 0, 0, 0, 0, 0, 1}, 64}
  end

scheduler_time = fn stats, states ->
  for(
    %{type: type, counters: counters} <- stats,
    type in [:scheduler, :dirty_cpu_scheduler],
    {state, value} <- counters,
    states.(state),
    do: value
  )
  |> Enum.sum()
end

# Time the Stack process spends on a scheduler, and in garbage
# collection, from trace messages: the rate is bounded by this process.
tracer = fn ->
  loop = fn loop, acc ->
    receive do
      {:trace_ts, _pid, :in, _mfa, ts} ->
        loop.(loop, %{acc | in: ts})

      {:trace_ts, _pid, :out, _mfa, ts} when acc.in != nil ->
        loop.(loop, %{acc | in: nil, on_cpu: acc.on_cpu + ts - acc.in})

      {:trace_ts, _pid, gc, _info, ts} when gc in [:gc_minor_start, :gc_major_start] ->
        loop.(loop, %{acc | gc_at: ts})

      {:trace_ts, _pid, gc, _info, ts}
      when gc in [:gc_minor_end, :gc_major_end] and acc.gc_at != nil ->
        loop.(loop, %{acc | gc_at: nil, gc: acc.gc + ts - acc.gc_at, gcs: acc.gcs + 1})

      {:report, from} ->
        send(from, {:report, acc})

      _other ->
        loop.(loop, acc)
    end
  end

  loop.(loop, %{in: nil, on_cpu: 0, gc_at: nil, gc: 0, gcs: 0})
end

run = fn backend ->
  {:ok, link, stack} =
    SmolNet.Loopback.start_link(
      backend: backend,
      addresses: [{address, prefix}],
      egress_credit: :infinity
    )

  endpoint = %{family: family, addr: address, port: 5001}
  {:ok, listener} = SmolNet.open(family, :stream, :tcp, stack: stack)
  :ok = SmolNet.bind(listener, endpoint)
  :ok = SmolNet.listen(listener, 1)
  acceptor = Task.async(fn -> SmolNet.accept(listener, 5_000) end)
  {:ok, client} = SmolNet.open(family, :stream, :tcp, stack: stack)
  :ok = SmolNet.connect(client, endpoint, 5_000)
  {:ok, server} = Task.await(acceptor)
  total = mib * 1_048_576

  counters = fn ->
    {:ok, info} = SmolNet.stack_info(stack)
    info.native.result.counters
  end

  c0 = counters.()
  stack_pid = stack.stack
  stack_tracer = if trace?, do: spawn(tracer)

  if trace? do
    :erlang.trace(stack_pid, true, [
      :running,
      :garbage_collection,
      :monotonic_timestamp,
      {:tracer, stack_tracer}
    ])
  end

  :msacc.reset()
  :msacc.start()
  t0 = System.monotonic_time(:microsecond)

  reader =
    Task.async(fn ->
      Stream.unfold(0, fn
        received when received >= total ->
          nil

        received ->
          {:ok, data} = SmolNet.recv(server, 0, 30_000)
          {received, received + byte_size(data)}
      end)
      |> Stream.run()
    end)

  for _chunk <- 1..(mib * 16), do: :ok = SmolNet.send(client, chunk, 30_000)
  Task.await(reader, 300_000)
  us = System.monotonic_time(:microsecond) - t0
  stats = :msacc.stats()
  :msacc.stop()
  c1 = counters.()

  traced =
    if trace? do
      :erlang.trace(stack_pid, false, [:running, :garbage_collection])
      send(stack_tracer, {:report, self()})
      receive do: ({:report, acc} -> acc)
    end

  traced_ms = fn native ->
    Float.round(:erlang.convert_time_unit(native, :native, :microsecond) / 1000 / mib, 3)
  end

  busy = scheduler_time.(stats, &(&1 != :sleep))
  gc = scheduler_time.(stats, &(&1 in [:gc, :gc_full]))
  per_mib = fn key -> Float.round((c1[key] - c0[key]) / mib, 1) end

  SmolNet.close(client)
  SmolNet.close(server)
  SmolNet.close(listener)
  GenServer.stop(link)

  report = %{
    mbit_s: Float.round(total * 8 / us, 1),
    cpu_ms_per_mib: Float.round(busy / 1000 / mib, 2),
    gc_ms_per_mib: Float.round(gc / 1000 / mib, 3),
    calls_per_mib: per_mib.(:native_calls),
    polls_per_mib: per_mib.(:poll_calls),
    emitted_per_mib: per_mib.(:emitted_packets),
    ingress_per_mib: per_mib.(:ingress_packets)
  }

  if traced do
    Map.merge(report, %{
      stack_cpu_ms_per_mib: traced_ms.(traced.on_cpu),
      stack_gc_ms_per_mib: traced_ms.(traced.gc),
      stack_gcs_per_mib: Float.round(traced.gcs / mib, 1)
    })
  else
    report
  end
end

IO.puts(
  "loopback bench: #{family}, #{mib} MiB per transfer, #{System.schedulers_online()} schedulers"
)

for i <- 1..repeats, backend <- backends do
  IO.puts("#{backend} ##{i}: #{inspect(run.(backend))}")
end
