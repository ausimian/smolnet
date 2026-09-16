defmodule SmolNet.StackSupervisor do
  @moduledoc false

  use Supervisor, restart: :temporary, shutdown: :infinity

  alias SmolNet.Stack
  alias SmolNet.Stack.IngressGate
  alias SmolNet.Stack.Options
  alias SmolNet.Stack.Ref

  @ready_timeout 5_000
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options), do: Supervisor.start_link(__MODULE__, options)

  @impl true
  def init(options) do
    children = [
      {Stack, options},
      %{
        id: :inet_backends,
        start: {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
        restart: :temporary,
        significant: true,
        shutdown: :infinity,
        type: :supervisor
      }
    ]

    Supervisor.init(children,
      strategy: :one_for_one,
      auto_shutdown: :any_significant
    )
  end

  @spec start_stack(keyword()) :: {:ok, Ref.t()} | {:error, term()}
  def start_stack(options \\ [])

  def start_stack(options) when is_list(options) do
    with {:ok, config} <- Options.parse(options) do
      ready_ref = make_ref()
      ingress_token = make_ref()
      gate_status = if config.egress, do: :up, else: :down

      gate =
        IngressGate.new(
          config.ingress_queue.packets,
          config.ingress_queue.bytes,
          gate_status
        )

      child_options =
        config
        |> Map.take([:egress, :link_down, :limits, :ingress_queue, :native_config])
        |> Map.to_list()
        |> Keyword.merge(
          starter: self(),
          ready_ref: ready_ref,
          ingress_gate: gate,
          ingress_token: ingress_token
        )

      case DynamicSupervisor.start_child(SmolNet.Supervisor, {__MODULE__, child_options}) do
        {:ok, bundle} ->
          await_ready(bundle, ready_ref, gate, ingress_token, config.native_config.mtu)

        {:error, reason} ->
          {:error, normalize_start_error(reason)}
      end
    end
  end

  def start_stack(_options), do: {:error, :invalid_options}

  @spec stop_stack(Ref.t()) :: :ok | {:error, :closed}
  def stop_stack(%Ref{bundle: bundle}) do
    case DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle) do
      :ok -> :ok
      {:error, :not_found} -> {:error, :closed}
    end
  end

  @doc false
  @spec start_inet_backend(Ref.t(), Supervisor.child_spec()) :: DynamicSupervisor.on_start_child()
  def start_inet_backend(%Ref{inet_backends: supervisor}, child_spec) do
    child_spec
    |> Supervisor.child_spec([])
    |> Map.put(:restart, :temporary)
    |> then(&DynamicSupervisor.start_child(supervisor, &1))
  end

  defp await_ready(bundle, ready_ref, ingress_gate, ingress_token, mtu) do
    bundle_monitor = Process.monitor(bundle)

    case resolve_children(bundle) do
      {:ok, children} ->
        await_stack_ready(
          bundle,
          bundle_monitor,
          ready_ref,
          children,
          ingress_gate,
          ingress_token,
          mtu
        )

      {:error, reason} ->
        stop_and_wait(bundle, bundle_monitor)
        {:error, reason}
    end
  end

  defp await_stack_ready(
         bundle,
         bundle_monitor,
         ready_ref,
         children,
         ingress_gate,
         ingress_token,
         mtu
       ) do
    %{stack: stack, inet_backends: inet_backends} = children
    stack_monitor = Process.monitor(stack)

    receive do
      {:smolnet_stack_ready, ^ready_ref} ->
        send(stack, {:smolnet_stack_accepted, ready_ref})
        demonitor_all([bundle_monitor, stack_monitor])

        {:ok,
         %Ref{
           bundle: bundle,
           stack: stack,
           inet_backends: inet_backends,
           ingress_gate: ingress_gate,
           ingress_token: ingress_token,
           mtu: mtu
         }}

      {:smolnet_stack_error, ^ready_ref, reason} ->
        stop_and_wait(bundle, bundle_monitor)
        Process.demonitor(stack_monitor, [:flush])
        {:error, reason}

      {:DOWN, ^stack_monitor, :process, ^stack, reason} ->
        stop_and_wait(bundle, bundle_monitor)
        {:error, normalize_start_error(reason)}

      {:DOWN, ^bundle_monitor, :process, ^bundle, reason} ->
        Process.demonitor(stack_monitor, [:flush])
        {:error, normalize_start_error(reason)}
    after
      @ready_timeout ->
        stop_and_wait(bundle, bundle_monitor)
        Process.demonitor(stack_monitor, [:flush])
        {:error, :initialization_timeout}
    end
  end

  defp resolve_children(bundle) do
    children = Supervisor.which_children(bundle)

    with {_, stack, :worker, _} when is_pid(stack) <- List.keyfind(children, Stack, 0),
         {:inet_backends, inet_backends, :supervisor, _} when is_pid(inet_backends) <-
           List.keyfind(children, :inet_backends, 0) do
      {:ok, %{stack: stack, inet_backends: inet_backends}}
    else
      _ -> {:error, :invalid_stack_bundle}
    end
  catch
    :exit, _ -> {:error, :stack_initialization_failed}
  end

  defp stop_and_wait(bundle, monitor) do
    if Process.alive?(bundle) do
      _ = DynamicSupervisor.terminate_child(SmolNet.Supervisor, bundle)
    end

    receive do
      {:DOWN, ^monitor, :process, ^bundle, _reason} -> :ok
    after
      @ready_timeout -> Process.demonitor(monitor, [:flush])
    end
  end

  defp demonitor_all(monitors) do
    Enum.each(monitors, &Process.demonitor(&1, [:flush]))
  end

  defp normalize_start_error({:native_initialization_failed, reason}), do: reason
  defp normalize_start_error({:shutdown, reason}), do: normalize_start_error(reason)
  defp normalize_start_error(:normal), do: :stack_initialization_failed
  defp normalize_start_error(:shutdown), do: :stack_initialization_failed
  defp normalize_start_error(_reason), do: :stack_initialization_failed
end
