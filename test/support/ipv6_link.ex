defmodule SmolNet.Test.IPv6Link do
  @moduledoc false

  use GenServer

  def start_link(test), do: GenServer.start_link(__MODULE__, test)

  def connect(link, link_ref, peer), do: GenServer.call(link, {:connect, link_ref, peer})
  def fault(link, mode), do: GenServer.call(link, {:fault, mode})
  def release(link, order \\ :fifo), do: GenServer.call(link, {:release, order})

  @impl true
  def init(test) do
    {:ok, %{test: test, peers: %{}, fault: :pass, held: []}}
  end

  @impl true
  def handle_call({:connect, link_ref, peer}, _from, state) do
    {:reply, :ok, %{state | peers: Map.put(state.peers, link_ref, peer)}}
  end

  def handle_call({:fault, mode}, _from, state) when mode in [:pass, :drop, :duplicate, :hold] do
    {:reply, :ok, %{state | fault: mode}}
  end

  def handle_call({:release, order}, _from, state) when order in [:fifo, :reverse] do
    packets = if order == :fifo, do: Enum.reverse(state.held), else: state.held

    Enum.each(packets, fn {peer, packet} ->
      SmolNet.ingress(peer, packet)
    end)

    {:reply, :ok, %{state | held: []}}
  end

  @impl true
  def handle_info({:smol_stack, link_ref, :egress, packets}, state) do
    peer = Map.fetch!(state.peers, link_ref)

    state =
      Enum.reduce(packets, state, fn packet, state ->
        send(state.test, {:test_link_egress, link_ref, packet})

        case state.fault do
          :pass ->
            SmolNet.ingress(peer, packet)
            state

          :drop ->
            state

          :duplicate ->
            SmolNet.ingress(peer, packet)
            SmolNet.ingress(peer, packet)
            state

          :hold ->
            %{state | held: [{peer, packet} | state.held]}
        end
      end)

    {:noreply, state}
  end
end
