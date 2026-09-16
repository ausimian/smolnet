defmodule SmolNet.Test.IPv6TcpPeer do
  @moduledoc false

  use GenServer

  import Bitwise

  @peer_sequence 1_000_000

  def start_link(test, mode), do: GenServer.start_link(__MODULE__, {test, mode})
  def attach(peer, stack), do: GenServer.call(peer, {:attach, stack})
  def mode(peer, mode), do: GenServer.call(peer, {:mode, mode})
  def release(peer), do: GenServer.call(peer, :release)
  def stats(peer), do: GenServer.call(peer, :stats)

  @impl true
  def init({test, mode}) do
    {:ok, %{test: test, mode: mode, stack: nil, held: [], connections: %{}, packets: []}}
  end

  @impl true
  def handle_call({:attach, stack}, _from, state), do: {:reply, :ok, %{state | stack: stack}}
  def handle_call({:mode, mode}, _from, state), do: {:reply, :ok, %{state | mode: mode}}

  def handle_call(:release, _from, state) do
    Enum.each(Enum.reverse(state.held), &SmolNet.ingress(state.stack, &1))
    {:reply, :ok, %{state | held: []}}
  end

  def handle_call(:stats, _from, state) do
    {:reply, %{packets: Enum.reverse(state.packets), held: length(state.held)}, state}
  end

  @impl true
  def handle_info({:smol_stack, :tcp_client, :egress, packet}, state) do
    tcp = decode_tcp(packet)
    send(state.test, {:tcp_peer_egress, tcp.flags, packet})
    state = %{state | packets: [tcp | state.packets]}

    cond do
      flag?(tcp.flags, 0x02) and not flag?(tcp.flags, 0x10) ->
        handle_syn(state, tcp)

      flag?(tcp.flags, 0x10) and Map.has_key?(state.connections, tcp.source_port) ->
        handle_ack(state, tcp)

      true ->
        {:noreply, state}
    end
  end

  defp handle_syn(%{mode: :ignore} = state, _tcp), do: {:noreply, state}

  defp handle_syn(%{mode: mode} = state, tcp)
       when mode in [:refuse, {:hop_by_hop, :refuse}] do
    response = response(tcp, 0, tcp.sequence + 1, 0x14, hop_by_hop: hop_by_hop?(mode))
    :ok = SmolNet.ingress(state.stack, response)
    {:noreply, state}
  end

  defp handle_syn(%{mode: mode} = state, tcp) when mode in [:accept, :reset] do
    accept_connection(state, tcp, mode, false)
  end

  defp handle_syn(%{mode: {:hop_by_hop, mode}} = state, tcp)
       when mode in [:accept, :reset] do
    accept_connection(state, tcp, mode, true)
  end

  defp handle_syn(%{mode: {:delay, outcome}} = state, tcp)
       when outcome in [:accept, :refuse] do
    flags = if outcome == :accept, do: 0x12, else: 0x14
    sequence = if outcome == :accept, do: @peer_sequence, else: 0
    response = response(tcp, sequence, tcp.sequence + 1, flags)

    connections =
      if outcome == :accept do
        Map.put(state.connections, tcp.source_port, %{
          client_sequence: tcp.sequence + 1,
          reset?: false,
          hop_by_hop?: false
        })
      else
        state.connections
      end

    {:noreply, %{state | held: [response | state.held], connections: connections}}
  end

  defp accept_connection(state, tcp, mode, hop_by_hop?) do
    response =
      response(tcp, @peer_sequence, tcp.sequence + 1, 0x12, hop_by_hop: hop_by_hop?)

    :ok = SmolNet.ingress(state.stack, response)

    connections =
      Map.put(state.connections, tcp.source_port, %{
        client_sequence: tcp.sequence + 1,
        reset?: mode == :reset,
        hop_by_hop?: hop_by_hop?
      })

    {:noreply, %{state | connections: connections}}
  end

  defp handle_ack(state, tcp) do
    connection = Map.fetch!(state.connections, tcp.source_port)

    if connection.reset? do
      response =
        response(tcp, @peer_sequence + 1, connection.client_sequence, 0x14,
          hop_by_hop: connection.hop_by_hop?
        )

      :ok = SmolNet.ingress(state.stack, response)
    end

    {:noreply, state}
  end

  defp decode_tcp(
         <<6::4, _traffic::28, payload_length::16, 6, _hop_limit, source::binary-size(16),
           destination::binary-size(16), tcp::binary-size(payload_length)>>
       ) do
    <<source_port::16, destination_port::16, sequence::32, acknowledgement::32, data_offset::4,
      _reserved::4, flags, _rest::binary>> = tcp

    %{
      source: source,
      destination: destination,
      source_port: source_port,
      destination_port: destination_port,
      sequence: sequence,
      acknowledgement: acknowledgement,
      data_offset: data_offset,
      flags: flags
    }
  end

  defp response(tcp, sequence, acknowledgement, flags, options \\ []) do
    header =
      <<tcp.destination_port::16, tcp.source_port::16, sequence::32, acknowledgement::32, 5::4,
        0::4, flags, 4_096::16, 0::16, 0::16>>

    checksum = tcp_checksum(tcp.destination, tcp.source, header)
    <<prefix::binary-size(16), _old_checksum::16, suffix::binary>> = header
    segment = <<prefix::binary, checksum::16, suffix::binary>>

    {next_header, payload} =
      if Keyword.get(options, :hop_by_hop, false) do
        {0, <<6, 0, 1, 4, 0, 0, 0, 0, segment::binary>>}
      else
        {6, segment}
      end

    <<6::4, 0::28, byte_size(payload)::16, next_header, 64, tcp.destination::binary,
      tcp.source::binary, payload::binary>>
  end

  defp hop_by_hop?({:hop_by_hop, _mode}), do: true
  defp hop_by_hop?(_mode), do: false

  defp tcp_checksum(source, destination, segment) do
    pseudo_header =
      <<source::binary, destination::binary, byte_size(segment)::32, 0::24, 6, segment::binary>>

    pseudo_header
    |> words()
    |> Enum.reduce(0, &add_word/2)
    |> then(&(bnot(&1) &&& 0xFFFF))
  end

  defp words(<<word::16, rest::binary>>), do: [word | words(rest)]
  defp words(<<byte>>), do: [byte <<< 8]
  defp words(<<>>), do: []

  defp add_word(word, sum) do
    sum = sum + word
    (sum &&& 0xFFFF) + (sum >>> 16)
  end

  defp flag?(flags, flag), do: (flags &&& flag) != 0
end
