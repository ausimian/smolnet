defmodule SmolNet.Loopback do
  @moduledoc """
  A link process that feeds a stack's outbound packets back into itself.

  SmolNet stacks are transport-neutral: a stack emits complete IPv4 or IPv6
  packets to a link process, which carries them to wherever the other end of
  the wire is. A loopback link is the degenerate case of that contract, where
  the other end of the wire is the same stack. It lets one stack reach its own
  addresses with no peer, no external transport, and no privileges, which makes
  it the simplest way to exercise the library in an example, a doctest, or a
  test case.

      {:ok, link, stack} =
        SmolNet.Loopback.start_link(
          addresses: [{{0, 0, 0, 0, 0, 0, 0, 1}, 128}, {{127, 0, 0, 1}, 8}]
        )

  The link owns the stack it loops. `start_link/1` accepts the same options as
  `SmolNet.start_stack/1` apart from `:egress`, which the link supplies, and
  returns once the stack is running. Stopping the link stops the stack through
  the stack's own `:link_down` policy. The link watches its stack with
  `SmolNet.monitor/1`, so stopping the stack with `SmolNet.stop_stack/1`, or a
  stack crash, stops the link.

  Because the loop is an ordinary link process, packets re-enter through the
  public `SmolNet.ingress/2` and are validated exactly like packets arriving
  from a real transport. A packet the stack refuses, which in practice means a
  packet offered while the stack is already busy with another feeder, is
  dropped as a real link would drop it.

  ## Reaching a loopback address

  A loopback link carries packets; it does not invent addresses. A stack
  answers on the addresses it was configured with, so give it whichever
  addresses the example needs. Conventional localhost addresses work, and so
  does any other address the stack holds:

      {:ok, _link, stack} =
        SmolNet.Loopback.start_link(addresses: [{{192, 0, 2, 1}, 24}])

      {:ok, listener} = SmolNet.open(:inet, :stream, :tcp, stack: stack)
      :ok = SmolNet.bind(listener, %{family: :inet, addr: {192, 0, 2, 1}, port: 8080})
      :ok = SmolNet.listen(listener, 1)

  A connection to `192.0.2.1:8080` on that stack now completes against its own
  listener.
  """

  # Restarting would build a second stack that no existing reference names.
  use GenServer, restart: :temporary

  alias SmolNet.Stack.Ref

  @link_ref :loopback

  @doc """
  Starts a loopback link and the stack it loops.

  Options are the `SmolNet.start_stack/1` options, plus an optional `:name` for
  the link process itself. Supplying `:egress` is an error, because the link is
  the stack's egress. Returns the link process and the stack reference as
  `{:ok, link, stack}`.
  """
  @spec start_link(keyword()) :: {:ok, pid(), Ref.t()} | :ignore | {:error, term()}
  def start_link(options) when is_list(options) do
    {name, stack_options} = Keyword.pop(options, :name)

    result =
      case name do
        nil -> GenServer.start_link(__MODULE__, stack_options)
        name -> GenServer.start_link(__MODULE__, stack_options, name: name)
      end

    case result do
      {:ok, link} -> {:ok, link, stack(link)}
      other -> other
    end
  end

  @doc "Returns the stack for an already-running loopback link."
  @spec stack(GenServer.server()) :: Ref.t()
  def stack(link), do: GenServer.call(link, :stack)

  @impl true
  def init(options) do
    if Keyword.has_key?(options, :egress) do
      {:stop, :invalid_options}
    else
      start_looped_stack(options)
    end
  end

  @impl true
  def handle_call(:stack, _from, state), do: {:reply, state.stack, state}

  @impl true
  def handle_info({:smol_stack, @link_ref, :egress, packets}, state) do
    Enum.reduce_while(packets, {:noreply, state}, fn packet, _result ->
      case SmolNet.ingress(state.stack, packet) do
        {:error, :closed} -> {:halt, {:stop, :normal, state}}
        _accepted_or_dropped -> {:cont, {:noreply, state}}
      end
    end)
  end

  # The stack was stopped from elsewhere, so the link it fed has no purpose.
  def handle_info({:DOWN, monitor, :process, _object, _reason}, %{monitor: monitor} = state) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp start_looped_stack(options) do
    case SmolNet.start_stack(Keyword.put(options, :egress, {self(), @link_ref})) do
      {:ok, stack} ->
        {:ok, %{stack: stack, monitor: SmolNet.monitor(stack)}}

      {:error, reason} ->
        {:stop, reason}
    end
  end
end
