defmodule SmolNet.Integration.Soak.Metrics do
  @moduledoc """
  Samples a run's resource use into `metrics.csv` at a fixed interval.

  Each row holds the BEAM's total and binary memory, the OS resident set
  size (which catches native leaks the BEAM cannot see), the process count,
  the stack's socket count, TIME-WAIT sockets and socket buffer bytes from
  `SmolNet.stack_info/1`, the egress credit left, the link's counters, the
  link's mailbox, its queue of device packets waiting to be fed to the stack
  and the packets that queue dropped, and then the script's own counters. A column that does not
  apply, such as the stack's in baseline mode, is empty.

  After the run, `rising/3` judges the samples taken after warm-up with
  `SmolNet.Integration.Soak.Trend`.
  """

  use GenServer

  alias SmolNet.Integration.Soak.Context
  alias SmolNet.Integration.Soak.Trend
  alias SmolNet.Integration.TunLink

  @columns [
    :elapsed_s,
    :beam_total_bytes,
    :beam_binary_bytes,
    :os_rss_bytes,
    :process_count,
    :socket_count,
    :closing_tcp_socket_count,
    :socket_buffer_bytes,
    :egress_credit_packets,
    :egress_credit_bytes,
    :credit_waits,
    :link_rx_packets,
    :link_tx_packets,
    :link_tx_dropped,
    :link_ingress_refused,
    :link_queue_len,
    :link_ingress_queue_len,
    :link_ingress_dropped
  ]

  @mib 1_048_576
  @trend_limits [
    beam_total_bytes: [floor: 32 * @mib, ratio: 0.25],
    beam_binary_bytes: [floor: 32 * @mib, ratio: 0.5],
    os_rss_bytes: [floor: 32 * @mib, ratio: 0.25],
    process_count: [floor: 100, ratio: 0.25],
    socket_count: [floor: 8, ratio: 0.5],
    socket_buffer_bytes: [floor: @mib, ratio: 0.5]
  ]

  @type sample :: %{atom() => number() | nil}

  @doc """
  Returns the trend-checked columns and the growth each may show; see
  `SmolNet.Integration.Soak.Trend`.
  """
  @spec trend_limits() :: keyword(Trend.limit())
  def trend_limits, do: @trend_limits

  @doc "Returns the built-in columns, which a script's counters may not reuse."
  @spec columns() :: [atom()]
  def columns, do: @columns

  @doc "Starts sampling every `interval` milliseconds, with `counters` as extra columns."
  @spec start_link(Context.t(), pos_integer(), [atom()]) :: GenServer.on_start()
  def start_link(context, interval, counters) do
    GenServer.start_link(__MODULE__, {context, interval, counters})
  end

  @doc "Takes a last sample, stops sampling, and returns every sample in order."
  @spec stop(pid()) :: [sample()]
  def stop(metrics), do: GenServer.call(metrics, :stop, :infinity)

  @doc """
  Returns the trend-checked columns that rose steadily in the samples taken
  after `warmup_ms`, judged against `limits`, or `:insufficient` when there
  are too few samples to judge.
  """
  @spec rising([sample()], non_neg_integer(), keyword(Trend.limit())) ::
          {:ok, [{atom(), map()}]} | :insufficient
  def rising(samples, warmup_ms, limits) do
    samples = Enum.filter(samples, &(&1.elapsed_s * 1_000 >= warmup_ms))

    if length(samples) < Trend.min_samples() do
      :insufficient
    else
      rising =
        for {column, limit} <- limits,
            {:rising, detail} <- [Trend.check(Enum.map(samples, & &1[column]), limit)],
            do: {column, detail}

      {:ok, rising}
    end
  end

  @impl true
  def init({context, interval, counters}) do
    File.mkdir_p!(context.out_dir)
    file = File.open!(Path.join(context.out_dir, "metrics.csv"), [:write, :utf8])
    IO.write(file, [Enum.map_join(@columns ++ counters, ",", &Atom.to_string/1), "\n"])

    state = %{context: context, interval: interval, counters: counters, file: file, samples: []}
    {:ok, take(state)}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    state = take(state)
    File.close(state.file)
    {:stop, :normal, Enum.reverse(state.samples), state}
  end

  @impl true
  def handle_info(:sample, state), do: {:noreply, take(state)}

  defp take(state) do
    sample = sample(state.context, state.counters)
    row = Enum.map_join(@columns ++ state.counters, ",", &format(sample[&1]))
    IO.write(state.file, [row, "\n"])
    Process.send_after(self(), :sample, state.interval)
    %{state | samples: [sample | state.samples]}
  end

  defp sample(context, counters) do
    elapsed = System.monotonic_time(:millisecond) - context.started_at

    %{
      elapsed_s: Float.round(elapsed / 1_000, 1),
      beam_total_bytes: :erlang.memory(:total),
      beam_binary_bytes: :erlang.memory(:binary),
      os_rss_bytes: os_rss(),
      process_count: :erlang.system_info(:process_count)
    }
    |> Map.merge(stack_columns(context.stack))
    |> Map.merge(link_columns(context.link))
    |> Map.merge(Map.new(counters, &{&1, counter(context.table, &1)}))
  end

  defp stack_columns(nil), do: %{}

  defp stack_columns(stack) do
    case SmolNet.stack_info(stack) do
      {:ok, %{native: %{result: native}}} ->
        credit = native.egress_credit || %{}

        %{
          socket_count: native.native_socket_count,
          closing_tcp_socket_count: native.closing_tcp_socket_count,
          socket_buffer_bytes: native.socket_buffer_bytes,
          egress_credit_packets: credit[:packets],
          egress_credit_bytes: credit[:bytes]
        }

      _unavailable ->
        %{}
    end
  end

  defp link_columns(nil), do: %{}

  defp link_columns(link) do
    stats = TunLink.stats(link)

    queue =
      case Process.info(link, :message_queue_len) do
        {:message_queue_len, length} -> length
        nil -> nil
      end

    %{
      credit_waits: stats.credit_waits,
      link_rx_packets: stats.rx_packets,
      link_tx_packets: stats.tx_packets,
      link_tx_dropped: stats.tx_dropped,
      link_ingress_refused: stats.ingress_refused,
      link_queue_len: queue,
      link_ingress_queue_len: stats.ingress_queue_len,
      link_ingress_dropped: stats.ingress_dropped
    }
  catch
    :exit, _reason -> %{}
  end

  defp counter(table, name) do
    case :ets.lookup(table, {:counter, name}) do
      [{_key, value}] -> value
      [] -> 0
    end
  end

  # Linux reports the resident set in /proc; elsewhere, ask ps.
  defp os_rss do
    case File.read("/proc/self/status") do
      {:ok, status} ->
        case Regex.run(~r/^VmRSS:\s+(\d+) kB$/m, status) do
          [_line, kib] -> String.to_integer(kib) * 1_024
          nil -> nil
        end

      {:error, _reason} ->
        ps_rss()
    end
  end

  defp ps_rss do
    case System.cmd("ps", ["-o", "rss=", "-p", System.pid()]) do
      {output, 0} -> String.to_integer(String.trim(output)) * 1_024
      _failed -> nil
    end
  rescue
    _error -> nil
  end

  defp format(nil), do: ""
  defp format(value), do: to_string(value)
end
