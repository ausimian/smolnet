defmodule SmolNet.Integration.Soak do
  @moduledoc """
  The runner every integration scenario is built on.

  A scenario script loads the harness, describes itself, and hands the
  runner a workload, a function of the run's `SmolNet.Integration.Soak.Context`:

      Code.require_file("support/load.exs", __DIR__)

      alias SmolNet.Integration.Soak

      Soak.main(System.argv(), [name: "transfers", default_duration: "10m", counters: [:transfers]],
        fn context ->
          Soak.workers(context, fn _worker ->
            Soak.within(context, :transfer, 30_000, fn -> transfer(context) end)
            Soak.count(context, :transfers)
          end)
        end)

  and is run as `sudo mix run integration/transfers.exs --duration 6h`. See
  `SmolNet.Integration.Soak.Options` for the options every script accepts,
  and `SmolNet.Integration.Network` for opening sockets in every mode.

  ## Config

    * `:name` - the script's name, as in `integration/<name>.exs`. Required.
    * `:default_duration` and `:default_concurrency` - defaults for
      `--duration` (`"30s"`) and `--concurrency` (1).
    * `:switches`, `:defaults` and `:usage` - the script's own
      `OptionParser` switches, their defaults, and usage text for them. Their
      values arrive in the context's `:extra`.
    * `:counters` - names of `count/3` counters to add to `metrics.csv`.
    * `:stack` - extra `SmolNet.start_stack/1` options, over
      `SmolNet.Integration.Network.stack_options/0`.
    * `:trend` - overrides of `SmolNet.Integration.Soak.Metrics.trend_limits/0`:
      `column: false` stops checking a column, and `column: [floor: f, ratio: r]`
      changes its limit.
    * `:quiet` - `true` to print nothing but write the same artifacts.

  ## A run

  1. The network starts: a `SmolNet.Integration.TunLink` on the device, or on
     the helper's loopback with `--self-check`, or nothing with `--baseline`.
  2. A rolling packet capture starts on the device (unless `--no-pcap`), and
     the `--netem` profile, if any, is applied.
  3. Metrics are sampled every `--metrics-interval` into `metrics.csv`.
  4. The workload runs in its own process. It should loop while `running?/1`
     is true, as `loop/3` and `workers/3` do. The run ends when the workload
     returns, when anything fails, or if the workload overruns the duration
     by its grace period.
  5. Metric trends after warm-up are judged.
  6. Everything is torn down, and the verdict printed and written.

  ## Verdict

  A run **passes** when nothing failed. It **fails** when an operation
  exceeded its `within/4` deadline, the workload called `fail/4` or
  crashed, the stack or link stopped, or a metric rose steadily after
  warm-up. It is an **error** when the environment could not be set up (the
  device is missing, say), or the workload found it unfit and called
  `abandon/2`, which is not a finding about SmolNet. `main/3` exits 0, 1 or
  2 respectively.

  A workload may also `record/3` results, measurements that are not
  judged, for the verdict.

  Artifacts go to the output directory: `verdict.json`, `metrics.csv` and
  `run.log` always, and on failure `failures/*.txt` (one per failure, with
  its diagnostics), `stack_info.txt` (taken at the first failure) and the
  `pcap/` window. A passing run deletes its capture.
  """

  alias SmolNet.Integration.Network
  alias SmolNet.Integration.Ownership
  alias SmolNet.Integration.Soak.Context
  alias SmolNet.Integration.Soak.Metrics
  alias SmolNet.Integration.Soak.Netem
  alias SmolNet.Integration.Soak.Options
  alias SmolNet.Integration.Soak.Pcap
  alias SmolNet.Integration.Soak.Server
  alias SmolNet.Integration.Soak.Trend

  @min_grace_ms 60_000
  @poll_ms 250

  @type outcome :: :pass | :fail | :error
  @type workload :: (Context.t() -> term())

  @doc """
  Runs a scenario from the command line and halts the VM with its verdict:
  0 for pass, 1 for fail, 2 for an environment error or bad arguments.
  """
  @spec main([String.t()], keyword(), workload()) :: no_return()
  def main(argv, config, workload) do
    code =
      case run(argv, config, workload) do
        {:pass, _verdict} ->
          0

        {:fail, _verdict} ->
          1

        {:error, _verdict} ->
          2

        {:help, usage} ->
          IO.puts(usage)
          0

        {:usage_error, message} ->
          IO.puts(:stderr, message)
          2
      end

    System.halt(code)
  end

  @doc """
  Runs a scenario and returns its outcome and verdict, without halting.
  """
  @spec run([String.t()], keyword(), workload()) ::
          {outcome(), map()} | {:help, String.t()} | {:usage_error, String.t()}
  def run(argv, config, workload) do
    case Enum.filter(Keyword.get(config, :counters, []), &(&1 in Metrics.columns())) do
      [] ->
        :ok

      taken ->
        raise ArgumentError, "counters #{inspect(taken)} would shadow built-in metrics columns"
    end

    case Options.parse(argv, config) do
      {:ok, options} -> execute(options, config, workload)
      {:help, usage} -> {:help, usage}
      {:error, message} -> {:usage_error, message}
    end
  end

  @doc "Returns whether the run is still going: its time is not up and nothing has failed."
  @spec running?(Context.t()) :: boolean()
  def running?(context) do
    not failed?(context) and not abandoned?(context) and
      System.monotonic_time(:millisecond) < context.ends_at
  end

  @doc """
  Ends the run as an environment error rather than a failure: `reason` says
  what about this host stops the workload, such as an internet target it
  cannot reach, which is not a finding about SmolNet. The workload should
  return after calling it; the verdict is `:error`, unless something failed.
  """
  @spec abandon(Context.t(), String.t()) :: :ok
  def abandon(context, reason) do
    log(context, "ABANDONED: #{reason}")
    :ets.insert_new(context.table, {:abandoned, reason})
    :ok
  end

  defp abandoned?(context), do: :ets.member(context.table, :abandoned)

  @doc "Returns whether anything has failed yet."
  @spec failed?(Context.t()) :: boolean()
  def failed?(context), do: :ets.member(context.table, :failed)

  @doc """
  Calls `fun` repeatedly while the run is going, and at least once.

  `:pause` waits that many milliseconds between calls, less if the run ends
  first.
  """
  @spec loop(Context.t(), (-> term()), keyword()) :: :ok
  def loop(context, fun, options \\ []) do
    fun.()

    if running?(context) do
      pause = Keyword.get(options, :pause, 0)
      remaining = context.ends_at - System.monotonic_time(:millisecond)
      Process.sleep(max(0, min(pause, remaining)))
      loop(context, fun, options)
    else
      :ok
    end
  end

  @doc """
  Runs `count` workers (by default the run's `--concurrency`), each calling
  `fun.(index)` in a `loop/3`, and returns when they have all finished.

  The workers are linked to the caller, so a worker that crashes crashes
  the workload, which fails the run.
  """
  @spec workers(Context.t(), pos_integer() | nil, (pos_integer() -> term())) :: :ok
  def workers(context, count \\ nil, fun) do
    1..(count || context.concurrency)//1
    |> Enum.map(&start_worker(context, fun, &1))
    |> Enum.each(&await_down/1)
  end

  defp start_worker(context, fun, index) do
    pid = spawn_link(fn -> loop(context, fn -> fun.(index) end) end)
    Process.monitor(pid)
  end

  defp await_down(monitor) do
    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
    end
  end

  @doc """
  Calls `fun` under a deadline of `timeout` milliseconds and returns its
  result.

  If `fun` has not returned by then, the run fails with a `:deadline`
  failure whose file holds the calling process's state, the processes it is
  waiting on, `SmolNet.stack_info/1`, and the packet capture, all taken while
  the caller is still stuck. `fun` runs in the calling process, so sockets
  it opens belong to the caller.
  """
  @spec within(Context.t(), term(), non_neg_integer(), (-> result)) :: result when result: var
  def within(context, name, timeout, fun) do
    deadline = Server.arm(context.server, self(), name, timeout)

    try do
      fun.()
    after
      Server.disarm(context.server, deadline)
    end
  end

  @doc """
  Fails the run, for an oracle the workload checks itself: a checksum that
  does not match, an unexpected error. `details` are `{title, term}` sections
  for the failure's file.
  """
  @spec fail(Context.t(), atom(), String.t(), [{String.t(), term()}]) :: :ok
  def fail(context, kind, summary, details \\ []) do
    log(context, "FAILED #{kind}: #{summary}")
    Server.fail(context.server, kind, summary, details)
  end

  @doc """
  Adds `amount` to the counter `name`. Counters are reported in the verdict
  and, if the config lists them, in `metrics.csv`.
  """
  @spec count(Context.t(), atom(), integer()) :: integer()
  def count(context, name, amount \\ 1) do
    :ets.update_counter(context.table, {:counter, name}, amount, {{:counter, name}, 0})
  end

  @doc """
  Records `value` as the run's result `name`, replacing any earlier one.

  Results are measurements rather than oracles, such as throughput next to
  the kernel baseline, and appear under `results` in `verdict.json`, so
  `value` must be JSON: maps, lists, strings, numbers, booleans, atoms.
  """
  @spec record(Context.t(), atom(), term()) :: :ok
  def record(context, name, value) do
    :ets.insert(context.table, {{:result, name}, value})
    :ok
  end

  @doc "Records an observation that is not a failure, in the log and the verdict."
  @spec note(Context.t(), String.t()) :: :ok
  def note(context, text) do
    :ets.insert(context.table, {{:note, System.unique_integer([:monotonic])}, text})
    log(context, text)
  end

  @doc "Prints `message` with the run's elapsed time, and appends it to `run.log`."
  @spec log(Context.t(), String.t()) :: :ok
  def log(context, message) do
    elapsed = (System.monotonic_time(:millisecond) - context.started_at) / 1_000
    line = "[#{:erlang.float_to_binary(elapsed, decimals: 1)}s] #{message}\n"
    File.write!(Path.join(context.out_dir, "run.log"), line, [:append])
    unless context.quiet, do: IO.write(line)
    :ok
  end

  @doc """
  Returns the number of native sockets the stack holds, or `nil` without a
  stack.
  """
  @spec socket_count(Context.t()) :: non_neg_integer() | nil
  def socket_count(%Context{stack: nil}), do: nil

  def socket_count(%Context{stack: stack}) do
    case SmolNet.stack_info(stack) do
      {:ok, %{native: %{result: native}}} -> native.native_socket_count
      _unavailable -> nil
    end
  end

  @doc """
  Waits up to `timeout` milliseconds for the stack's socket count to return
  to `expected`, and fails the run with a `:socket_leak` if it does not.

  A TCP socket that closed first holds its slot through TIME-WAIT, about
  10 s, so the default timeout is 30 s. Without a stack, returns at once.
  """
  @spec await_socket_count(Context.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def await_socket_count(context, expected \\ 0, timeout \\ 30_000)

  def await_socket_count(%Context{stack: nil}, _expected, _timeout), do: :ok

  def await_socket_count(context, expected, timeout) do
    give_up_at = System.monotonic_time(:millisecond) + timeout
    await_sockets(context, expected, timeout, give_up_at)
  end

  defp await_sockets(context, expected, timeout, give_up_at) do
    count = socket_count(context)

    cond do
      count == expected ->
        :ok

      System.monotonic_time(:millisecond) >= give_up_at ->
        fail(
          context,
          :socket_leak,
          "the stack held #{inspect(count)} sockets, not #{expected}, after #{timeout} ms",
          [{"stack_info", SmolNet.stack_info(context.stack)}]
        )

      true ->
        Process.sleep(@poll_ms)
        await_sockets(context, expected, timeout, give_up_at)
    end
  end

  # What a run writes to its output directory. A directory reused with
  # `--out` loses these first, so that no artifact outlives its run.
  @artifacts ~w(verdict.json metrics.csv run.log stack_info.txt failures pcap)

  defp execute(options, config, workload) do
    File.mkdir_p!(options.out_dir)
    Enum.each(@artifacts, &File.rm_rf!(Path.join(options.out_dir, &1)))
    table = :ets.new(:soak, [:public, :set, write_concurrency: true])
    {:ok, server} = Server.start_link(owner: self(), out_dir: options.out_dir, table: table)
    started_at = System.monotonic_time(:millisecond)

    context = %Context{
      script: options.script,
      mode: options.mode,
      families: options.families,
      concurrency: options.concurrency,
      duration_ms: options.duration_ms,
      out_dir: options.out_dir,
      device: options.device,
      netem: options.netem,
      extra: options.extra,
      server: server,
      table: table,
      started_at: started_at,
      ends_at: started_at + options.duration_ms,
      quiet: Keyword.get(config, :quiet, false)
    }

    log(context, "#{options.script}: #{describe(context)}, artifacts in #{options.out_dir}")

    {setup, context, undo} = set_up(context, options, config)

    # The run's duration and warm-up count from here, not from before setup,
    # which can take a while: building the helper, starting tcpdump.
    started_at = System.monotonic_time(:millisecond)
    context = %{context | started_at: started_at, ends_at: started_at + options.duration_ms}
    Server.start_clock(server)

    try do
      if setup == :ok, do: exercise(context, options, config, workload)
    after
      Server.detach(server)
      Enum.each(undo, fn step -> step.() end)
    end

    finish(context, setup, options)
  end

  # Each step returns an undo function; a step that fails leaves the undo
  # functions of the steps before it to run.
  defp set_up(context, options, config) do
    Enum.reduce_while(
      [&start_network/3, &start_capture/3, &impair/3],
      {:ok, context, []},
      fn step, {:ok, context, undo} ->
        case step.(context, options, config) do
          {:ok, context, nil} -> {:cont, {:ok, context, undo}}
          {:ok, context, step_undo} -> {:cont, {:ok, context, [step_undo | undo]}}
          {:error, reason} -> {:halt, {{:error, reason}, context, undo}}
        end
      end
    )
  end

  defp start_network(context, options, config) do
    stack_options = [egress_credit: options.egress_credit] ++ Keyword.get(config, :stack, [])

    case Network.start(context.mode, context.device, stack_options) do
      {:ok, %{stack: nil}} ->
        {:ok, context, nil}

      {:ok, %{stack: stack, link: link}} ->
        {:ok, %{context | stack: stack, link: link}, fn -> stop_network(stack, link) end}

      {:error, reason} ->
        {:error, "could not start the #{context.mode} network: #{inspect(reason)}"}
    end
  end

  defp stop_network(stack, link) do
    _stopped = SmolNet.stop_stack(stack)

    if Process.alive?(link) do
      GenServer.stop(link)
    end
  catch
    :exit, _reason -> :ok
  end

  defp start_capture(context, %{pcap: false}, _config), do: {:ok, context, nil}

  defp start_capture(context, options, _config) do
    directory = Path.join(options.out_dir, "pcap")

    case Pcap.start(options.device, directory) do
      {:ok, pcap} ->
        {:ok, context, fn -> Pcap.stop(pcap) end}

      # A capture is a diagnostic: a run without one is still a run.
      {:error, message} ->
        note(context, "running without a packet capture: #{message}")
        File.rm_rf(directory)
        {:ok, context, nil}
    end
  end

  defp impair(%{netem: nil} = context, _options, _config), do: {:ok, context, nil}

  defp impair(context, _options, _config) do
    case Netem.impair(context.netem, context.device) do
      :ok ->
        log(context, "impairing #{context.device} with netem profile #{context.netem}")
        {:ok, context, fn -> Netem.clear(context.device) end}

      {:error, message} ->
        {:error, "could not apply netem profile #{context.netem}: #{message}"}
    end
  end

  defp exercise(context, options, config, workload) do
    Server.attach(context.server, context.stack, context.link)
    counters = Keyword.get(config, :counters, [])
    {:ok, metrics} = Metrics.start_link(context, options.metrics_interval_ms, counters)

    run_workload(context, workload)
    samples = Metrics.stop(metrics)

    unless failed?(context) or abandoned?(context) do
      judge_trends(context, samples, options, config)
    end
  end

  defp run_workload(context, workload) do
    {pid, monitor} = spawn_monitor(fn -> workload.(context) end)
    grace = max(@min_grace_ms, div(context.duration_ms, 10))
    await_workload(context, pid, monitor, context.ends_at + grace, grace)
  end

  defp await_workload(context, pid, monitor, overrun_at, grace) do
    wait = max(0, overrun_at - System.monotonic_time(:millisecond))

    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} ->
        :ok

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        fail(context, :workload_crashed, "the workload crashed: #{crash(reason)}", [
          {"reason", reason}
        ])

      {:soak_failed, _server} ->
        stop_workload(pid, monitor)
    after
      wait ->
        info = Process.info(pid, [:current_stacktrace, :status, :message_queue_len])
        summary = "the workload was still running #{grace} ms after the run's duration"
        fail(context, :overrun, summary, [{"workload", info}])
        stop_workload(pid, monitor)
    end
  end

  defp crash({exception, _stacktrace}) when is_exception(exception) do
    Exception.format_banner(:error, exception)
  end

  defp crash(reason), do: inspect(reason, limit: 20, printable_limit: 200)

  defp stop_workload(pid, monitor) do
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    end
  end

  defp judge_trends(context, samples, options, config) do
    limits = trend_limits(Keyword.get(config, :trend, []))

    case Metrics.rising(samples, options.warmup_ms, limits) do
      :insufficient ->
        note(
          context,
          "too few samples after warm-up to judge metric trends " <>
            "(#{Trend.min_samples()} needed); run longer or sample more often"
        )

      {:ok, rising} ->
        Enum.each(rising, fn {column, detail} ->
          summary =
            "#{column} rose steadily after warm-up: window medians #{inspect(detail.medians)}"

          fail(context, :trend, summary, [{to_string(column), detail}])
        end)
    end
  end

  defp trend_limits(overrides) do
    Enum.reduce(overrides, Metrics.trend_limits(), fn
      {column, false}, limits -> Keyword.delete(limits, column)
      {column, limit}, limits -> Keyword.put(limits, column, limit)
    end)
  end

  defp finish(context, setup, options) do
    {failures, failure_count} = Server.failures(context.server)

    abandoned =
      case :ets.lookup(context.table, :abandoned) do
        [{:abandoned, reason}] -> reason
        [] -> nil
      end

    outcome =
      cond do
        setup != :ok -> :error
        failure_count > 0 -> :fail
        abandoned != nil -> :error
        true -> :pass
      end

    if outcome == :pass, do: File.rm_rf(Path.join(options.out_dir, "pcap"))

    verdict = %{
      script: options.script,
      verdict: outcome,
      mode: options.mode,
      families: options.families,
      netem: options.netem,
      duration_s: options.duration_ms / 1_000,
      elapsed_s: (System.monotonic_time(:millisecond) - context.started_at) / 1_000,
      error: setup_error(setup) || abandoned,
      failure_count: failure_count,
      failures: failures,
      counters: counters(context.table),
      notes: notes(context.table),
      results: results(context.table),
      out_dir: options.out_dir
    }

    File.write!(Path.join(options.out_dir, "verdict.json"), JSON.encode!(verdict))
    report(context, verdict)

    GenServer.stop(context.server)
    :ets.delete(context.table)
    flush_failure_notices()

    restore_ownership(options.out_dir)

    {outcome, verdict}
  end

  # Under sudo, hands back what this run created, and nothing else that
  # shares its directory. A first run also creates the default runs
  # directory as root.
  defp restore_ownership(out_dir) do
    if Path.dirname(out_dir) == Options.runs_dir() do
      Ownership.restore(Options.runs_dir(), recursive: false)
    end

    Ownership.restore(out_dir, recursive: false)
    Enum.each(@artifacts, &Ownership.restore(Path.join(out_dir, &1)))
  end

  defp setup_error(:ok), do: nil
  defp setup_error({:error, reason}), do: reason

  defp report(context, verdict) do
    headline =
      "VERDICT: #{verdict.verdict |> to_string() |> String.upcase()} #{verdict.script} " <>
        "(#{describe(context)}, #{Float.round(verdict.elapsed_s, 1)} s)"

    details =
      Enum.map(verdict.failures, &"  #{&1.kind}: #{&1.summary} (#{&1.file})") ++
        if(verdict.error, do: ["  error: #{verdict.error}"], else: [])

    log(context, Enum.join([headline | details], "\n"))
  end

  defp describe(context) do
    families = Enum.map_join(context.families, "+", &to_string/1)
    netem = if context.netem, do: ", netem #{context.netem}", else: ""
    "#{context.mode}, #{families}#{netem}"
  end

  defp counters(table) do
    table
    |> :ets.match_object({{:counter, :_}, :_})
    |> Map.new(fn {{:counter, name}, value} -> {name, value} end)
  end

  defp results(table) do
    table
    |> :ets.match_object({{:result, :_}, :_})
    |> Map.new(fn {{:result, name}, value} -> {name, value} end)
  end

  defp notes(table) do
    table
    |> :ets.match_object({{:note, :_}, :_})
    |> Enum.sort()
    |> Enum.map(fn {_key, text} -> text end)
  end

  defp flush_failure_notices do
    receive do
      {:soak_failed, _server} -> flush_failure_notices()
    after
      0 -> :ok
    end
  end
end
