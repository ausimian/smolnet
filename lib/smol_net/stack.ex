defmodule SmolNet.Stack do
  @moduledoc false

  use GenServer, restart: :temporary, significant: true

  alias SmolNet.Native

  @default_limits %{
    bytes_copied: 64 * 1024,
    output_packets: 32,
    ready_events: 128,
    maintenance_work: 256
  }

  @type limits :: %{
          bytes_copied: pos_integer(),
          output_packets: pos_integer(),
          ready_events: pos_integer(),
          maintenance_work: pos_integer()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @doc false
  @spec native_snapshot(pid()) :: {:ok, map()} | {:error, atom()}
  def native_snapshot(stack), do: GenServer.call(stack, :native_snapshot)

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
    starter = Keyword.fetch!(options, :starter)

    state = %{
      starter: starter,
      starter_monitor: Process.monitor(starter),
      ready_ref: Keyword.fetch!(options, :ready_ref),
      limits: Keyword.fetch!(options, :limits),
      native: nil
    }

    {:ok, state, {:continue, :create_native_stack}}
  end

  @impl true
  def handle_continue(:create_native_stack, state) do
    native = Application.get_env(:smolnet, :native_module, Native)
    now = System.monotonic_time(:millisecond)

    case native.stack_new(state.limits, now) do
      {:ok, %{result: resource, output: [], poll_at: nil, more: false}} ->
        send(state.starter, {:smolnet_stack_ready, state.ready_ref})
        {:noreply, %{state | native: resource}}

      {:error, reason} ->
        send(state.starter, {:smolnet_stack_error, state.ready_ref, reason})
        {:stop, {:native_initialization_failed, reason}, state}

      unexpected ->
        send(state.starter, {:smolnet_stack_error, state.ready_ref, :invalid_native_result})
        {:stop, {:invalid_native_result, unexpected}, state}
    end
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

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_call(:native_snapshot, _from, state) do
    {:reply, Native.stack_snapshot(state.native), state}
  end

  def handle_call(:test_contention, _from, state) do
    {:reply, Native.test_contention(state.native), state}
  end

  def handle_call({:test_bounded_work, requested}, _from, state) do
    {:reply, Native.test_bounded_work(state.native, requested), state}
  end

  @spec default_limits() :: limits()
  def default_limits, do: @default_limits
end
