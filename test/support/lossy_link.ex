defmodule SmolNet.Test.LossyLink do
  @moduledoc false

  # A link between two stacks that drops bursts of TCP data segments leaving one
  # of them, the pattern a bounded queue produces when it overflows. Only first
  # transmissions are dropped, so every loss is repaired by its first resend and
  # the resend count shows how the sender recovered: once per lost segment for
  # fast recovery, or once for every segment after the hole for a timeout.
  #
  # A sender that runs short of window, as one does when a slow host's reader
  # falls behind, does not cut the stream into full segments alone:
  #
  #   * It sends a shorter segment that fills the window exactly. A resend is
  #     cut at the full segment size from the hole it repairs, so the resend
  #     of a shorter segment would also carry the bytes after it: another
  #     loss, repaired along with it, or bytes never sent before. Only
  #     full-size segments are dropped, so each resend repairs one loss.
  #   * Once the window is closed it probes it with the next byte, and repeats
  #     the probe until the window opens. The segment it sends then starts at
  #     that byte again. A probe repairs nothing, so neither a repeated probe
  #     nor the byte the next segment repeats counts as a resend.
  #
  # IPv4 only, and it assumes one TCP connection over the lossy direction.

  use GenServer

  import Bitwise, only: [band: 2]

  # Duplicate ACKs that start a fast retransmission. A loss with fewer
  # segments after it can be repaired only by the retransmission timeout.
  @duplicate_acks 3

  # The payload of a zero-window probe.
  @probe_length 1

  @doc """
  Options: `:lossy` is the link ref whose egress drops segments, and every
  `:every`th new data segment starts a burst of `:burst` dropped ones. With
  `:stream_bytes`, the length of the stream sent, a segment too close to its
  end for duplicate ACKs to follow is never dropped.
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
       stream_bytes: Keyword.get(options, :stream_bytes),
       new_segments: 0,
       first_seq: nil,
       next_seq: nil,
       full_length: 0,
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

  defp carry(%{next_seq: next_seq} = state, peer, packet, {seq, length})
       when is_integer(next_seq) and next_seq != seq do
    seq_end = band(seq + length, 0xFFFF_FFFF)

    cond do
      not before?(seq, next_seq) ->
        raise "unexpected gap in the sender's sequence space"

      # Bytes never sent before, after one already carried: the segment that
      # follows a zero-window probe. It is a first transmission, but one
      # that starts behind the others, so it is never dropped.
      before?(next_seq, seq_end) ->
        forward(%{state | new_segments: state.new_segments + 1, next_seq: seq_end}, peer, packet)

      length == @probe_length ->
        forward(state, peer, packet)

      true ->
        forward(%{state | retransmitted: state.retransmitted + 1}, peer, packet)
    end
  end

  defp carry(state, peer, packet, {seq, length}) do
    count = state.new_segments + 1

    state = %{
      state
      | new_segments: count,
        first_seq: state.first_seq || seq,
        next_seq: band(seq + length, 0xFFFF_FFFF),
        full_length: max(state.full_length, length)
    }

    if rem(count, state.every) < state.burst and droppable?(state, seq, length) do
      %{state | dropped: state.dropped + 1}
    else
      forward(state, peer, packet)
    end
  end

  defp droppable?(state, seq, length) do
    length == state.full_length and not in_tail?(state, seq, length)
  end

  defp in_tail?(%{stream_bytes: nil}, _seq, _length), do: false

  defp in_tail?(state, seq, length) do
    stream_end = band(seq - state.first_seq, 0xFFFF_FFFF) + length
    state.stream_bytes - stream_end < @duplicate_acks * state.full_length
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
