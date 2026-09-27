defmodule SmolNet.Integration.Soak.Server do
  @moduledoc """
  Records a soak run's failures and watches its per-operation deadlines.

  Each failure is written to `failures/NN-<kind>.txt` in the run's output
  directory as it happens. The first also writes `stack_info.txt`, a snapshot
  of the stack and link taken at that moment, and tells the runner, which
  ends the run.

  A deadline that expires is diagnosed while its caller is still stuck: the
  failure records `Process.info/2` of the caller, the processes it is
  waiting on (those it monitors, which includes the callee of any
  `GenServer.call/3` in progress), `SmolNet.stack_info/1`, the link's
  counters, and the packet captures that hold the moments before it.
  """

  use GenServer

  alias SmolNet.Integration.TunLink

  @process_keys [
    :registered_name,
    :status,
    :initial_call,
    :current_function,
    :current_stacktrace,
    :message_queue_len,
    :links,
    :monitors,
    :monitored_by,
    :memory,
    :reductions,
    :dictionary
  ]
  @peer_keys [:registered_name, :status, :initial_call, :current_stacktrace, :message_queue_len]
  @shown_messages 10
  @shown_peers 5
  # A workload failing every operation must not fill the disk.
  @max_failure_files 100

  @type failure :: %{kind: atom(), summary: String.t(), at_ms: non_neg_integer(), file: Path.t()}

  @doc """
  Starts a server for the run whose runner is `:owner`, writing to
  `:out_dir`. `:table` is the run's public ETS table, where the server marks
  the run failed.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @doc "Watches the run's stack and link; either stopping is a failure until `detach/1`."
  @spec attach(pid(), SmolNet.Stack.Ref.t() | nil, pid() | nil) :: :ok
  def attach(server, stack, link), do: GenServer.call(server, {:attach, stack, link})

  @doc "Restarts the clock failures are timed by, when the run's workload starts."
  @spec start_clock(pid()) :: :ok
  def start_clock(server), do: GenServer.call(server, :start_clock)

  @doc "Stops watching the stack and link, before the runner stops them."
  @spec detach(pid()) :: :ok
  def detach(server), do: GenServer.call(server, :detach)

  @doc "Arms a deadline of `timeout` milliseconds for `pid`'s operation `name`."
  @spec arm(pid(), pid(), term(), non_neg_integer()) :: reference()
  def arm(server, pid, name, timeout), do: GenServer.call(server, {:arm, pid, name, timeout})

  @doc "Disarms a deadline whose operation finished."
  @spec disarm(pid(), reference()) :: :ok
  def disarm(server, deadline), do: GenServer.cast(server, {:disarm, deadline})

  @doc """
  Records a failure. `details` is a list of `{title, term}` sections for the
  failure's file.
  """
  @spec fail(pid(), atom(), String.t(), [{String.t(), term()}]) :: :ok
  def fail(server, kind, summary, details),
    do: GenServer.call(server, {:fail, kind, summary, details}, :infinity)

  @doc "Returns the failures recorded so far, oldest first, and how many there were."
  @spec failures(pid()) :: {[failure()], non_neg_integer()}
  def failures(server), do: GenServer.call(server, :failures)

  @impl true
  def init(options) do
    {:ok,
     %{
       owner: Keyword.fetch!(options, :owner),
       out_dir: Keyword.fetch!(options, :out_dir),
       table: Keyword.fetch!(options, :table),
       started_at: System.monotonic_time(:millisecond),
       deadlines: %{},
       failures: [],
       failure_count: 0,
       stack: nil,
       link: nil,
       monitors: %{}
     }}
  end

  @impl true
  def handle_call({:attach, stack, link}, _from, state) do
    monitors =
      [
        {:stack_down, stack && SmolNet.monitor(stack)},
        {:link_down, link && Process.monitor(link)}
      ]
      |> Enum.reject(fn {_kind, monitor} -> is_nil(monitor) end)
      |> Map.new(fn {kind, monitor} -> {monitor, kind} end)

    {:reply, :ok, %{state | stack: stack, link: link, monitors: monitors}}
  end

  def handle_call(:start_clock, _from, state) do
    {:reply, :ok, %{state | started_at: System.monotonic_time(:millisecond)}}
  end

  # A monitor that already fired still counts: its component stopped during
  # the run, even if the :DOWN is still in the mailbox.
  def handle_call(:detach, _from, state) do
    state =
      Enum.reduce(state.monitors, %{state | monitors: %{}}, fn {monitor, kind}, state ->
        if Process.demonitor(monitor, [:info]) do
          state
        else
          receive do
            {:DOWN, ^monitor, :process, _object, reason} -> record_down(state, kind, reason)
          after
            0 -> state
          end
        end
      end)

    {:reply, :ok, state}
  end

  def handle_call({:arm, pid, name, timeout}, _from, state) do
    deadline = make_ref()

    operation = %{
      pid: pid,
      name: name,
      timeout: timeout,
      armed_at: System.monotonic_time(:millisecond),
      timer: Process.send_after(self(), {:deadline, deadline}, timeout)
    }

    {:reply, deadline, put_in(state.deadlines[deadline], operation)}
  end

  def handle_call({:fail, kind, summary, details}, _from, state) do
    {:reply, :ok, record(state, kind, summary, details)}
  end

  def handle_call(:failures, _from, state) do
    {:reply, {Enum.reverse(state.failures), state.failure_count}, state}
  end

  @impl true
  def handle_cast({:disarm, deadline}, state) do
    {operation, deadlines} = Map.pop(state.deadlines, deadline)

    if operation do
      Process.cancel_timer(operation.timer)
    end

    {:noreply, %{state | deadlines: deadlines}}
  end

  @impl true
  def handle_info({:deadline, deadline}, state) do
    case Map.pop(state.deadlines, deadline) do
      {nil, _deadlines} -> {:noreply, state}
      {operation, deadlines} -> {:noreply, expire(%{state | deadlines: deadlines}, operation)}
    end
  end

  def handle_info({:DOWN, monitor, :process, _object, reason}, state) do
    case Map.pop(state.monitors, monitor) do
      {nil, _monitors} ->
        {:noreply, state}

      {kind, monitors} ->
        {:noreply, record_down(%{state | monitors: monitors}, kind, reason)}
    end
  end

  defp record_down(state, kind, reason) do
    summary = "#{kind}: stopped during the run with #{inspect(reason)}"
    record(state, kind, summary, [{"reason", reason}])
  end

  defp expire(state, operation) do
    elapsed = System.monotonic_time(:millisecond) - operation.armed_at
    summary = "#{inspect(operation.name)} did not finish within #{operation.timeout} ms"

    record(state, :deadline, summary, [
      {"operation",
       %{
         name: operation.name,
         timeout_ms: operation.timeout,
         elapsed_ms: elapsed,
         caller: operation.pid
       }},
      {"caller", process_info(operation.pid, @process_keys)},
      {"processes the caller is waiting on", peers(operation.pid)},
      {"stack_info", stack_info(state.stack)},
      {"link", link_stats(state.link)},
      {"pcap", pcap_files(state.out_dir)}
    ])
  end

  defp record(state, kind, summary, details) do
    index = state.failure_count + 1
    at_ms = System.monotonic_time(:millisecond) - state.started_at
    number = String.pad_leading(to_string(index), 2, "0")
    file = Path.join("failures", "#{number}-#{kind}.txt")

    if index <= @max_failure_files do
      header = ["#{kind}: #{summary}\n", "at #{at_ms} ms into the run\n\n"]
      write(state.out_dir, file, header, details)
    end

    if index == 1 do
      header = ["taken at the first failure, #{at_ms} ms into the run\n\n"]
      sections = [{"stack_info", stack_info(state.stack)}, {"link", link_stats(state.link)}]
      write(state.out_dir, "stack_info.txt", header, sections)

      :ets.insert(state.table, {:failed, true})
      send(state.owner, {:soak_failed, self()})
    end

    failure = %{kind: kind, summary: summary, at_ms: at_ms, file: file}

    failures =
      if index <= @max_failure_files, do: [failure | state.failures], else: state.failures

    %{state | failures: failures, failure_count: index}
  end

  defp write(out_dir, file, header, sections) do
    path = Path.join(out_dir, file)
    File.mkdir_p!(Path.dirname(path))

    body =
      Enum.map(sections, fn {title, term} ->
        text = inspect(term, pretty: true, limit: :infinity, printable_limit: 4_096)
        ["## ", title, "\n\n", text, "\n\n"]
      end)

    File.write!(path, [header | body])
  end

  defp process_info(pid, keys) do
    case Process.info(pid, keys) do
      nil ->
        :not_alive

      info ->
        messages =
          case Process.info(pid, :messages) do
            {:messages, messages} -> Enum.take(messages, @shown_messages)
            nil -> []
          end

        info ++ [first_messages: messages]
    end
  end

  defp peers(pid) do
    case Process.info(pid, :monitors) do
      {:monitors, monitors} ->
        for {:process, peer} when is_pid(peer) <- Enum.take(monitors, @shown_peers),
            do: {peer, process_info(peer, @peer_keys)}

      nil ->
        []
    end
  end

  defp stack_info(nil), do: :no_stack
  defp stack_info(stack), do: SmolNet.stack_info(stack)

  defp link_stats(nil), do: :no_link

  defp link_stats(link) do
    TunLink.stats(link)
  catch
    :exit, reason -> {:unavailable, reason}
  end

  defp pcap_files(out_dir) do
    directory = Path.join(out_dir, "pcap")

    case File.ls(directory) do
      {:ok, files} ->
        files
        |> Enum.sort()
        |> Enum.map(fn file -> {file, File.stat!(Path.join(directory, file)).size} end)

      {:error, _reason} ->
        :no_capture
    end
  end
end
