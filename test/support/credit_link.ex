defmodule SmolNet.Test.CreditLink do
  @moduledoc false

  # A link between two stacks that queues each direction's egress in a buffer
  # bounded by the credit it grants, and forwards it later, a few packets at a
  # time, as a transport that encrypts or paces its traffic would. Forwarding
  # a packet grants its credit back to the stack that sent it.
  #
  # A batch that does not fit in the credit outstanding is an overrun: a
  # bounded link without egress credit would have had to drop it.

  use GenServer

  @forward_batch 4

  @doc "Options: `:credit` is the `{packets, bytes}` each stack started with."
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  @doc "Carries `link_ref`'s egress from `stack` into `peer`."
  def connect(link, link_ref, stack, peer),
    do: GenServer.call(link, {:connect, link_ref, stack, peer})

  @doc """
  Returns overruns, refused ingress, and how often a direction's buffer filled
  its credit.
  """
  def stats(link), do: GenServer.call(link, :stats)

  @impl true
  def init(options) do
    {packets, bytes} = Keyword.fetch!(options, :credit)

    {:ok,
     %{
       credit: %{packets: packets, bytes: bytes},
       mtu: Keyword.get(options, :mtu, 1_500),
       directions: %{},
       overruns: 0,
       refused: 0,
       saturated: 0
     }}
  end

  @impl true
  def handle_call({:connect, link_ref, stack, peer}, _from, state) do
    direction = %{stack: stack, peer: peer, queue: :queue.new(), packets: 0, bytes: 0}
    {:reply, :ok, put_in(state.directions[link_ref], direction)}
  end

  def handle_call(:stats, _from, state) do
    {:reply, Map.take(state, [:overruns, :refused, :saturated]), state}
  end

  @impl true
  def handle_info({:smol_stack, link_ref, :egress, packets}, state) do
    direction = Map.fetch!(state.directions, link_ref)
    queued_packets = direction.packets + length(packets)
    queued_bytes = direction.bytes + IO.iodata_length(packets)

    overrun? = queued_packets > state.credit.packets or queued_bytes > state.credit.bytes

    saturated? =
      queued_packets == state.credit.packets or
        state.credit.bytes - queued_bytes < state.mtu

    direction = %{
      direction
      | queue: Enum.reduce(packets, direction.queue, &:queue.in/2),
        packets: queued_packets,
        bytes: queued_bytes
    }

    send(self(), {:forward, link_ref})

    {:noreply,
     %{
       state
       | directions: Map.put(state.directions, link_ref, direction),
         overruns: state.overruns + if(overrun?, do: 1, else: 0),
         saturated: state.saturated + if(saturated?, do: 1, else: 0)
     }}
  end

  def handle_info({:forward, link_ref}, state) do
    direction = Map.fetch!(state.directions, link_ref)
    {packets, queue} = take(direction.queue, @forward_batch, [])

    refused =
      Enum.count(packets, fn packet -> SmolNet.ingress(direction.peer, packet) != :ok end)

    bytes = IO.iodata_length(packets)

    if packets != [] do
      :ok = SmolNet.grant_egress(direction.stack, length(packets), bytes)
    end

    unless :queue.is_empty(queue), do: send(self(), {:forward, link_ref})

    direction = %{
      direction
      | queue: queue,
        packets: direction.packets - length(packets),
        bytes: direction.bytes - bytes
    }

    {:noreply,
     %{
       state
       | directions: Map.put(state.directions, link_ref, direction),
         refused: state.refused + refused
     }}
  end

  defp take(queue, 0, taken), do: {Enum.reverse(taken), queue}

  defp take(queue, remaining, taken) do
    case :queue.out(queue) do
      {{:value, packet}, queue} -> take(queue, remaining - 1, [packet | taken])
      {:empty, queue} -> {Enum.reverse(taken), queue}
    end
  end
end
