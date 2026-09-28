defmodule SmolNet.Integration.Crawl.Ceiling do
  @moduledoc """
  Pushes a stack to its socket ceiling and past it, against a listener on
  the host's kernel through the device, never against an internet host.

  A stack holds at most as many sockets as its `:sockets` limit, and at
  most as many as its 128 MiB of socket buffers fit: 256 TCP sockets at the
  default 256 KiB each way. A TCP socket that closes first keeps its slot
  through TIME-WAIT, about 10 s, so a stack sustains about a tenth of that
  room in new connections per second. For each family:

    * **hold** opens connections one at a time, and keeps them, until an
      open fails, which must be with `:system_limit`. The listener then
      closes its ends first, so that no SmolNet socket waits out TIME-WAIT.
    * **churn** opens connections and closes each at once, SmolNet first,
      from 8 workers paced to half, twice and four times the documented
      rate, for a step each. Below the rate no open may fail; above it,
      opens may fail, but only with `:system_limit`.

  After each, the stack's socket count must return to its baseline, and a
  fresh connection must succeed. Every open has a deadline, so a
  `:system_limit` that does not come back promptly fails the run.
  """

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Soak
  alias SmolNet.Integration.Tls

  @connect_timeout 5_000
  @deadline 15_000
  @workers 8
  @factors [0.5, 2, 4]
  # About how long a TCP socket that closed first holds its slot.
  @time_wait_s 10
  @default_buffer 262_144
  @sample_ms 200
  @drain_timeout 30_000

  @doc """
  Runs both phases for `family` and returns what they measured, for the
  verdict. `baseline` is the stack's socket count at rest; `:step` is how
  long each churn rate runs, in milliseconds, and `:buffer` the buffers
  of SmolNet's sockets, `nil` for its default.
  """
  @spec run(Soak.Context.t(), Network.family(), non_neg_integer(), keyword()) :: map()
  def run(context, family, baseline, options) do
    {:ok, listener} = listen(context, family)

    target = %{
      family: family,
      address: Network.address(context, :peer, family),
      port: listener.port,
      buffer: Keyword.get(options, :buffer),
      acceptor: listener.acceptor
    }

    try do
      room = room(context, target.buffer)
      hold = hold(context, target, baseline, room)
      churn = churn(context, target, baseline, room, Keyword.fetch!(options, :step))
      %{room: room, documented_rate_per_s: room / @time_wait_s, hold: hold, churn: churn}
    after
      stop(listener)
    end
  end

  # How many more TCP sockets the stack has room for: slots, and buffers.
  defp room(context, buffer) do
    native = native(context)
    per_socket = 2 * (buffer || @default_buffer)
    slots = native.native_socket_capacity - native.native_socket_count
    buffers = div(native.socket_buffer_capacity - native.socket_buffer_bytes, per_socket)
    min(slots, buffers)
  end

  defp native(context) do
    {:ok, %{native: %{result: native}}} = SmolNet.stack_info(context.stack)
    native
  end

  defp snapshot(context) do
    Map.take(native(context), [
      :native_socket_count,
      :native_socket_capacity,
      :closing_tcp_socket_count,
      :socket_buffer_bytes,
      :socket_buffer_capacity
    ])
  end

  defp open(context, target, name) do
    options =
      Network.tcp_options(context, :subject, target.family) ++
        Tls.buffer_options(context, :subject, target.buffer) ++ [:binary, active: false]

    Soak.within(context, {:ceiling, target.family, "smolnet", name}, @deadline, fn ->
      :gen_tcp.connect(target.address, target.port, options, @connect_timeout)
    end)
  end

  defp now, do: System.monotonic_time(:millisecond)

  # Hold

  defp hold(context, target, baseline, room) do
    started = now()
    {sockets, ending, snapshot} = open_until_limit(context, target, room + 16, [])
    elapsed = now() - started

    close_peers(target)
    Enum.each(sockets, &:gen_tcp.close/1)

    result =
      Map.merge(snapshot, %{
        opened: length(sockets),
        ended_with: inspect(ending),
        elapsed_ms: elapsed,
        bound: bound(snapshot, target.buffer)
      })

    case ending do
      {:error, :system_limit} ->
        :ok

      {:error, reason} ->
        Soak.fail(
          context,
          :ceiling,
          "#{target.family}: open #{length(sockets) + 1} failed with #{inspect(reason)}, " <>
            "not :system_limit, with #{snapshot.native_socket_count} sockets open",
          [{"hold", result}]
        )

      :no_limit ->
        Soak.fail(
          context,
          :ceiling,
          "#{target.family}: #{length(sockets)} opens all succeeded, past the room for #{room}",
          [{"hold", result}]
        )
    end

    recover(context, target, baseline, :hold)
    result
  end

  defp open_until_limit(context, target, left, sockets) do
    if left == 0 or Soak.failed?(context) do
      {sockets, :no_limit, snapshot(context)}
    else
      case open(context, target, {:hold, length(sockets) + 1}) do
        {:ok, socket} -> open_until_limit(context, target, left - 1, [socket | sockets])
        {:error, _reason} = error -> {sockets, error, snapshot(context)}
      end
    end
  end

  defp bound(snapshot, buffer) do
    if snapshot.socket_buffer_bytes + 2 * (buffer || @default_buffer) >
         snapshot.socket_buffer_capacity,
       do: "socket buffers",
       else: "sockets"
  end

  # The socket count returns to the baseline, and a fresh connection works.
  defp recover(context, target, baseline, phase) do
    Soak.await_socket_count(context, baseline, @drain_timeout)

    case open(context, target, {phase, :recovery}) do
      {:ok, socket} ->
        close_peers(target)
        :gen_tcp.close(socket)

      {:error, reason} ->
        Soak.fail(
          context,
          :ceiling,
          "#{target.family}: after #{phase}, a fresh connection failed with #{inspect(reason)}"
        )
    end
  end

  # Churn

  defp churn(context, target, baseline, room, step) do
    steps =
      Enum.map(@factors, fn factor ->
        if Soak.failed?(context), do: nil, else: churn_step(context, target, room, factor, step)
      end)

    steps = Enum.reject(steps, &is_nil/1)
    recover(context, target, baseline, :churn)
    steps
  end

  defp churn_step(context, target, room, factor, step) do
    rate = max(room / @time_wait_s * factor, 1.0)
    interval = @workers * 1_000 / rate
    stop_at = now() + step
    sampler = Task.async(fn -> sample(context, stop_at, %{sockets: 0, closing: 0}) end)

    tallies =
      1..@workers
      |> Enum.map(fn worker ->
        # Staggered, so that the workers' opens spread over the interval.
        start = now() + round(interval * (worker - 1) / @workers)

        Task.async(fn ->
          churn_loop(context, target, {factor, worker}, start, interval, stop_at, tally())
        end)
      end)
      |> Task.await_many(:infinity)

    peak = Task.await(sampler, :infinity)
    result = summarize_step(tallies, factor, rate, step, peak)
    judge_step(context, target, result)
    result
  end

  defp tally,
    do: %{attempted: 0, connected: 0, system_limit: 0, other: 0, reasons: [], slowest_limit_ms: 0}

  defp churn_loop(context, target, name, next, interval, stop_at, tally) do
    Process.sleep(max(0, round(next) - now()))

    if now() >= stop_at or Soak.failed?(context) do
      tally
    else
      started = now()
      tally = %{tally | attempted: tally.attempted + 1}

      tally =
        case open(context, target, name) do
          {:ok, socket} ->
            :gen_tcp.close(socket)
            %{tally | connected: tally.connected + 1}

          {:error, :system_limit} ->
            slowest = max(tally.slowest_limit_ms, now() - started)
            %{tally | system_limit: tally.system_limit + 1, slowest_limit_ms: slowest}

          {:error, reason} ->
            reasons = Enum.take(Enum.uniq([inspect(reason) | tally.reasons]), 5)
            %{tally | other: tally.other + 1, reasons: reasons}
        end

      churn_loop(context, target, name, next + interval, interval, stop_at, tally)
    end
  end

  defp sample(context, stop_at, peak) do
    if now() >= stop_at do
      peak
    else
      native = native(context)

      peak = %{
        sockets: max(peak.sockets, native.native_socket_count),
        closing: max(peak.closing, native.closing_tcp_socket_count)
      }

      Process.sleep(@sample_ms)
      sample(context, stop_at, peak)
    end
  end

  defp summarize_step(tallies, factor, rate, step, peak) do
    sum = fn key -> Enum.sum_by(tallies, &Map.fetch!(&1, key)) end
    seconds = step / 1_000

    %{
      factor: factor,
      target_per_s: Float.round(rate * 1.0, 1),
      attempted: sum.(:attempted),
      connected: sum.(:connected),
      connected_per_s: Float.round(sum.(:connected) / seconds, 1),
      system_limit: sum.(:system_limit),
      other: sum.(:other),
      other_reasons: tallies |> Enum.flat_map(& &1.reasons) |> Enum.uniq() |> Enum.take(5),
      slowest_limit_ms: tallies |> Enum.map(& &1.slowest_limit_ms) |> Enum.max(),
      peak_sockets: peak.sockets,
      peak_closing: peak.closing
    }
  end

  defp judge_step(context, target, step) do
    Soak.count(context, :ceiling_system_limit, step.system_limit)

    label =
      "#{target.family} churn at #{step.factor}x the documented rate (#{step.target_per_s}/s)"

    cond do
      step.other > 0 ->
        Soak.fail(
          context,
          :ceiling,
          "#{label}: #{step.other} opens failed with other than :system_limit: " <>
            Enum.join(step.other_reasons, ", "),
          [{"step", step}]
        )

      step.factor < 1 and step.system_limit > 0 ->
        Soak.fail(
          context,
          :ceiling,
          "#{label}: #{step.system_limit} opens failed with :system_limit below the documented rate",
          [{"step", step}]
        )

      step.factor > 1 and step.system_limit == 0 ->
        Soak.note(
          context,
          "#{label}: no open reached the ceiling; it connected #{step.connected_per_s}/s"
        )

      true ->
        Soak.note(
          context,
          "#{label}: connected #{step.connected_per_s}/s, #{step.system_limit} opens refused " <>
            "with :system_limit (the slowest in #{step.slowest_limit_ms} ms), " <>
            "at most #{step.peak_sockets} sockets, #{step.peak_closing} closing"
        )
    end
  end

  # The listener, on the host's kernel. Each accepted connection gets a
  # holder, which closes it when SmolNet does, or first when told to.

  defp listen(context, family) do
    options = [
      :binary,
      family,
      active: false,
      ip: Network.address(context, :peer, family),
      backlog: 1_024,
      reuseaddr: true
    ]

    with {:ok, socket} <- :gen_tcp.listen(0, options),
         {:ok, port} <- :inet.port(socket) do
      acceptor = spawn_link(fn -> accept(socket, %{}) end)
      :ok = :gen_tcp.controlling_process(socket, acceptor)
      {:ok, %{port: port, acceptor: acceptor}}
    end
  end

  defp close_peers(target) do
    send(target.acceptor, {:close_all, self()})

    receive do
      {:closed, acceptor} when acceptor == target.acceptor -> :ok
    end
  end

  defp stop(listener) do
    monitor = Process.monitor(listener.acceptor)
    send(listener.acceptor, :stop)

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end
  end

  defp accept(listener, holders) do
    receive do
      {:DOWN, monitor, :process, _pid, _reason} ->
        accept(listener, Map.delete(holders, monitor))

      {:close_all, from} ->
        close_all(holders)
        send(from, {:closed, self()})
        accept(listener, %{})

      :stop ->
        close_all(holders)
        :gen_tcp.close(listener)
    after
      0 ->
        case :gen_tcp.accept(listener, 50) do
          {:ok, socket} ->
            {holder, monitor} = spawn_monitor(fn -> hold(socket) end)
            :ok = :gen_tcp.controlling_process(socket, holder)
            send(holder, :ready)
            accept(listener, Map.put(holders, monitor, holder))

          {:error, :timeout} ->
            accept(listener, holders)
        end
    end
  end

  defp close_all(holders) do
    Enum.each(holders, fn {_monitor, holder} -> send(holder, :close) end)

    Enum.each(holders, fn {monitor, _holder} ->
      receive do
        {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      end
    end)
  end

  defp hold(socket) do
    receive do
      :ready -> _set = :inet.setopts(socket, active: :once)
    end

    hold_open(socket)
  end

  defp hold_open(socket) do
    receive do
      {:tcp, ^socket, _data} ->
        _set = :inet.setopts(socket, active: :once)
        hold_open(socket)

      _closed_or_told ->
        :gen_tcp.close(socket)
    after
      120_000 -> :gen_tcp.close(socket)
    end
  end
end
