defmodule SmolNet.Test.LossyLink do
  @moduledoc false

  # A link between two stacks that drops bursts of TCP data segments leaving one
  # of them, the pattern a bounded queue produces when it overflows. Only first
  # transmissions are dropped, so every loss is repaired by its first resend and
  # the resend count shows how the sender recovered: once per lost segment for
  # fast recovery, or once for every segment after the hole for a timeout.
  #
  # IPv4 only, and it assumes one TCP connection over the lossy direction.

  use GenServer

  import Bitwise, only: [band: 2]

  @doc """
  Options: `:lossy` is the link ref whose egress drops segments, and every
  `:every`th new data segment starts a burst of `:burst` dropped ones.
  """
  def start_link(options), do: GenServer.start_link(__MODULE__, options)

  def connect(link, link_ref, peer), do: GenServer.call(link, {:connect, link_ref, peer})

  @doc "Returns the number of data segments dropped and resent on the lossy direction."
  def stats(link), do: GenServer.call(link, :stats)

  @impl true
  def init(options) do
    {:ok,
     %{
       peers: %{},
       lossy: Keyword.fetch!(options, :lossy),
       every: Keyword.fetch!(options, :every),
       burst: Keyword.fetch!(options, :burst),
       new_segments: 0,
       next_seq: nil,
       dropped: 0,
       retransmitted: 0
     }}
  end

  @impl true
  def handle_call({:connect, link_ref, peer}, _from, state) do
    {:reply, :ok, %{state | peers: Map.put(state.peers, link_ref, peer)}}
  end

  def handle_call(:stats, _from, state) do
    {:reply, Map.take(state, [:dropped, :retransmitted]), state}
  end

  @impl true
  def handle_info({:smol_stack, link_ref, :egress, packets}, state) do
    peer = Map.fetch!(state.peers, link_ref)

    state =
      Enum.reduce(packets, state, fn packet, state ->
        segment = if link_ref == state.lossy, do: data_segment(packet)
        carry(state, peer, packet, segment)
      end)

    {:noreply, state}
  end

  defp carry(state, peer, packet, nil) do
    SmolNet.ingress(peer, packet)
    state
  end

  defp carry(%{next_seq: next_seq} = state, peer, packet, {seq, _length})
       when is_integer(next_seq) and next_seq != seq do
    if before?(seq, next_seq) do
      forward(%{state | retransmitted: state.retransmitted + 1}, peer, packet)
    else
      raise "unexpected gap in the sender's sequence space"
    end
  end

  defp carry(state, peer, packet, {seq, length}) do
    count = state.new_segments + 1
    state = %{state | new_segments: count, next_seq: band(seq + length, 0xFFFF_FFFF)}

    if rem(count, state.every) < state.burst do
      %{state | dropped: state.dropped + 1}
    else
      forward(state, peer, packet)
    end
  end

  # A refused data segment is lost like a dropped one, and must count as one
  # for the resend total to balance.
  defp forward(state, peer, packet) do
    case SmolNet.ingress(peer, packet) do
      :ok -> state
      {:error, _reason} -> %{state | dropped: state.dropped + 1}
    end
  end

  defp before?(seq, next_seq), do: band(next_seq - seq, 0xFFFF_FFFF) in 1..0x7FFF_FFFF

  defp data_segment(<<4::4, ihl::4, _tos, total::16, _::binary-size(5), 6, _::binary>> = packet) do
    header = ihl * 4
    <<_::binary-size(header), _ports::32, seq::32, _ack::32, offset::4, _::bitstring>> = packet
    length = total - header - offset * 4

    if length > 0, do: {seq, length}
  end

  defp data_segment(_packet), do: nil
end
