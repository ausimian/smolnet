defmodule SmolNet.Test.IPv6TcpPeer do
  @moduledoc false

  use GenServer

  import Bitwise

  @peer_sequence 1_000_000

  def start_link(test, mode), do: GenServer.start_link(__MODULE__, {test, mode})
  def attach(peer, stack), do: GenServer.call(peer, {:attach, stack})
  def mode(peer, mode), do: GenServer.call(peer, {:mode, mode})
  def release(peer), do: GenServer.call(peer, :release)
  def send_data(peer, data), do: GenServer.call(peer, {:send_data, data})
  def finish(peer), do: GenServer.call(peer, :finish)
  def reset(peer), do: GenServer.call(peer, :reset)
  def hold_acks(peer, hold?), do: GenServer.call(peer, {:hold_acks, hold?})
  def release_acks(peer), do: GenServer.call(peer, :release_acks)
  def stats(peer), do: GenServer.call(peer, :stats)

  @impl true
  def init({test, mode}) do
    {:ok,
     %{
       test: test,
       mode: mode,
       stack: nil,
       held: [],
       held_acks: [],
       hold_acks?: false,
       connections: %{},
       packets: []
     }}
  end

  @impl true
  def handle_call({:attach, stack}, _from, state), do: {:reply, :ok, %{state | stack: stack}}
  def handle_call({:mode, mode}, _from, state), do: {:reply, :ok, %{state | mode: mode}}

  def handle_call(:release, _from, state) do
    Enum.each(Enum.reverse(state.held), &SmolNet.ingress(state.stack, &1))
    {:reply, :ok, %{state | held: []}}
  end

  def handle_call({:send_data, data}, _from, state) when is_binary(data) do
    {port, connection} = only_connection(state)

    {connection, packets} =
      data
      |> chunk_binary(1_000)
      |> Enum.map_reduce(connection, fn chunk, connection ->
        packet =
          response(
            connection.tcp,
            connection.peer_sequence,
            connection.client_sequence,
            0x18,
            payload: chunk,
            hop_by_hop: connection.hop_by_hop?
          )

        connection = %{connection | peer_sequence: connection.peer_sequence + byte_size(chunk)}
        {packet, connection}
      end)
      |> then(fn {packets, connection} -> {connection, packets} end)

    Enum.each(packets, &SmolNet.ingress(state.stack, &1))
    connections = Map.put(state.connections, port, connection)
    {:reply, :ok, %{state | connections: connections}}
  end

  def handle_call(:finish, _from, state) do
    {port, connection} = only_connection(state)

    packet =
      response(
        connection.tcp,
        connection.peer_sequence,
        connection.client_sequence,
        0x11,
        hop_by_hop: connection.hop_by_hop?
      )

    :ok = SmolNet.ingress(state.stack, packet)
    connection = %{connection | peer_sequence: connection.peer_sequence + 1}
    {:reply, :ok, put_in(state.connections[port], connection)}
  end

  def handle_call(:reset, _from, state) do
    {_port, connection} = only_connection(state)

    packet =
      response(
        connection.tcp,
        connection.peer_sequence,
        connection.client_sequence,
        0x14,
        hop_by_hop: connection.hop_by_hop?
      )

    :ok = SmolNet.ingress(state.stack, packet)
    {:reply, :ok, state}
  end

  def handle_call({:hold_acks, hold?}, _from, state) when is_boolean(hold?) do
    {:reply, :ok, %{state | hold_acks?: hold?}}
  end

  def handle_call(:release_acks, _from, state) do
    Enum.each(Enum.reverse(state.held_acks), &SmolNet.ingress(state.stack, &1))
    {:reply, :ok, %{state | held_acks: [], hold_acks?: false}}
  end

  def handle_call(:stats, _from, state) do
    received =
      state.connections
      |> Map.values()
      |> Enum.map_join(& &1.received)

    {:reply,
     %{
       packets: Enum.reverse(state.packets),
       held: length(state.held),
       held_acks: length(state.held_acks),
       received: received
     }, state}
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
          peer_sequence: @peer_sequence + 1,
          received: <<>>,
          tcp: tcp,
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
        peer_sequence: @peer_sequence + 1,
        received: <<>>,
        tcp: tcp,
        reset?: mode == :reset,
        hop_by_hop?: hop_by_hop?
      })

    {:noreply, %{state | connections: connections}}
  end

  defp handle_ack(state, tcp) do
    connection = Map.fetch!(state.connections, tcp.source_port)

    cond do
      connection.reset? ->
        response =
          response(tcp, connection.peer_sequence, connection.client_sequence, 0x14,
            hop_by_hop: connection.hop_by_hop?
          )

        :ok = SmolNet.ingress(state.stack, response)
        {:noreply, state}

      byte_size(tcp.payload) > 0 or flag?(tcp.flags, 0x01) ->
        client_sequence =
          tcp.sequence + byte_size(tcp.payload) + if(flag?(tcp.flags, 0x01), do: 1, else: 0)

        flags = 0x10

        response =
          response(tcp, connection.peer_sequence, client_sequence, flags,
            hop_by_hop: connection.hop_by_hop?
          )

        {held_acks, peer_sequence} =
          if state.hold_acks? do
            {[response | state.held_acks], connection.peer_sequence}
          else
            :ok = SmolNet.ingress(state.stack, response)
            {state.held_acks, connection.peer_sequence}
          end

        connection = %{
          connection
          | client_sequence: client_sequence,
            peer_sequence: peer_sequence,
            received: connection.received <> tcp.payload,
            tcp: tcp
        }

        {:noreply,
         %{
           state
           | connections: Map.put(state.connections, tcp.source_port, connection),
             held_acks: held_acks
         }}

      true ->
        {:noreply, state}
    end
  end

  defp decode_tcp(
         <<6::4, _traffic::28, payload_length::16, 6, _hop_limit, source::binary-size(16),
           destination::binary-size(16), tcp::binary-size(payload_length)>>
       ) do
    <<source_port::16, destination_port::16, sequence::32, acknowledgement::32, data_offset::4,
      _reserved::4, flags, _rest::binary>> = tcp

    header_length = data_offset * 4

    %{
      source: source,
      destination: destination,
      source_port: source_port,
      destination_port: destination_port,
      sequence: sequence,
      acknowledgement: acknowledgement,
      data_offset: data_offset,
      flags: flags,
      payload: binary_part(tcp, header_length, byte_size(tcp) - header_length)
    }
  end

  defp response(tcp, sequence, acknowledgement, flags, options \\ []) do
    payload = Keyword.get(options, :payload, <<>>)

    header =
      <<tcp.destination_port::16, tcp.source_port::16, sequence::32, acknowledgement::32, 5::4,
        0::4, flags, 4_096::16, 0::16, 0::16>>

    segment = header <> payload

    checksum = tcp_checksum(tcp.destination, tcp.source, segment)
    <<prefix::binary-size(16), _old_checksum::16, suffix::binary>> = segment
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

  defp only_connection(%{connections: connections}) do
    [{port, connection}] = Map.to_list(connections)
    {port, connection}
  end

  defp chunk_binary(<<>>, _size), do: []

  defp chunk_binary(binary, size) do
    chunk_size = min(byte_size(binary), size)
    <<chunk::binary-size(^chunk_size), rest::binary>> = binary
    [chunk | chunk_binary(rest, size)]
  end
end
