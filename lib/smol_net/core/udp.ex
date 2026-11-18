defmodule SmolNet.Core.Udp do
  @moduledoc false

  # UDP sockets in the stack core, with the NIF's semantics (the glue in
  # `native/smolnet_core` over smoltcp's `socket::udp`):
  #
  #   * A socket binds once, to one of the interface's addresses or to the
  #     unspecified address, which covers all of its family's addresses.
  #     A port is in use if another UDP socket of the family holds it,
  #     whatever the address; port 0 takes the next ephemeral port.
  #   * Received datagrams wait in a ring of 16 packets and 16 KiB
  #     (`SmolNet.Core.PacketBuffer`). A wildcard socket keeps one ring per
  #     address, as the NIF's one smoltcp socket per address did, and reads
  #     them round-robin, so that what it drops and the order it returns
  #     datagrams in stay the same. It is still one socket in the table and
  #     one entry in the demultiplexer.
  #   * A datagram reaches the socket bound to its destination's port and
  #     address, or a wildcard socket on its port that has the address. A
  #     broadcast or multicast datagram reaches any socket on its port, of
  #     its own family first. One no socket takes draws a Port Unreachable.
  #   * Connecting sets the peer. A receive discards datagrams from other
  #     peers, or of another family, as it reads them; a send to another
  #     endpoint is refused.
  #   * A receive returns one datagram, cut to the length asked for (0 for
  #     all of it) and flagged as truncated if it was. With nothing to
  #     return it arms a read waiter and replies `{:select, :recvfrom, ref}`.
  #   * A send is emitted at once, as one IP packet, with no transmit ring:
  #     without egress credit the NIF's ring never holds a datagram past the
  #     call. Egress credit, which can hold one, comes in a later phase.
  #
  # Every function takes and returns the core; `SmolNet.Core` checks the
  # lifecycle first and counts the work.

  alias SmolNet.Core.Iface
  alias SmolNet.Core.PacketBuffer
  alias SmolNet.Core.Sockets

  @ring_packets 16
  @ring_bytes 16_384

  # What one socket's receive and transmit rings take, as the NIF counts
  # its buffers.
  @buffer_bytes 2 * @ring_bytes

  defstruct [:family, local: nil, peer: nil, rings: [], cursor: 0]

  @doc "The packets a receive ring holds."
  def ring_packets, do: @ring_packets

  @doc "The payload bytes a receive ring holds."
  def ring_bytes, do: @ring_bytes

  @doc """
  The largest datagram a socket of `family` may send: the MTU less the IP
  and UDP headers, at most a ring's payload.
  """
  @spec max_datagram_bytes(pos_integer(), :inet | :inet6) :: non_neg_integer()
  def max_datagram_bytes(mtu, :inet), do: max(mtu - 28, 0) |> min(@ring_bytes)
  def max_datagram_bytes(mtu, :inet6), do: max(mtu - 48, 0) |> min(@ring_bytes)

  @doc "The buffer bytes UDP sockets hold, as the NIF counts them."
  @spec buffer_bytes([%__MODULE__{}]) :: non_neg_integer()
  def buffer_bytes(sockets),
    do: Enum.sum(for(socket <- sockets, do: length(socket.rings))) * @buffer_bytes

  ## Calls

  def open(core, family) do
    state = %__MODULE__{family: family, rings: [{nil, new_ring()}]}

    with :ok <- rings_available(core, 1),
         {:ok, identity, sockets} <- Sockets.open(core.sockets, :udp, state, core.limits.sockets) do
      {{:ok, identity_map(identity)}, %{core | sockets: sockets}, []}
    else
      error -> {error, core, []}
    end
  end

  # The NIF counts each ring, one smoltcp socket for it, against the socket
  # limit, as well as each socket.
  defp rings_available(core, added) do
    rings = core.sockets |> Sockets.states(:udp) |> Enum.map(&length(&1.rings)) |> Enum.sum()
    if rings + added > core.limits.sockets, do: {:error, :system_limit}, else: :ok
  end

  # A wildcard bind's extra rings; a refusal returns the core, whose
  # ephemeral cursor may have moved.
  defp rings_available_for(core, addresses) do
    case rings_available(core, length(addresses) - 1) do
      :ok -> :ok
      {:error, reason} -> {:error, reason, core}
    end
  end

  def bind(core, identity, endpoint) do
    with {:ok, udp} <- fetch(core, identity),
         {:ok, {address, port, scope_id}} <- Sockets.endpoint(endpoint, :bind),
         :ok <- if(udp.local, do: {:error, :invalid_socket_state}, else: :ok),
         :ok <- family(udp, address),
         :ok <- available(core, address),
         {:ok, port, core} <- port(core, identity, udp.family, port),
         {:ok, addresses} <- addresses(core, udp.family, address),
         :ok <- rings_available_for(core, addresses) do
      [primary | others] = addresses
      [{nil, ring}] = udp.rings

      udp = %{
        udp
        | local: {address, port, scope_id},
          rings: [{primary, ring} | for(other <- others, do: {other, new_ring()})],
          cursor: 0
      }

      sockets =
        core.sockets
        |> Sockets.put_state(identity, udp)
        |> Sockets.bind(identity, :udp, {address, port}, nil)

      {{:ok, :ok}, %{core | sockets: sockets}, []}
    else
      {:error, _reason} = error -> {error, core, []}
      {:error, reason, core} -> {{:error, reason}, core, []}
    end
  end

  # Before a specific address is checked to be the interface's.
  defp available(core, address) do
    if Sockets.unspecified?(address) or Iface.has_ip_addr?(core.iface, address),
      do: :ok,
      else: {:error, :address_not_available}
  end

  # The cursor moves even when the bind then fails for want of an address,
  # as the NIF's does.
  defp port(core, _identity, family, 0) do
    case Sockets.allocate_port(core.sockets, :udp, family) do
      {:ok, port, sockets} -> {:ok, port, %{core | sockets: sockets}}
      error -> error
    end
  end

  defp port(core, identity, family, port) do
    if Sockets.port_in_use?(core.sockets, :udp, family, port, identity),
      do: {:error, :address_in_use},
      else: {:ok, port, core}
  end

  # The bind's addresses: the one given, or the family's for a wildcard.
  # A failure returns the core, whose ephemeral cursor may have moved.
  defp addresses(core, family, address) do
    if Sockets.unspecified?(address) do
      case core.iface |> Iface.addresses(family) |> Enum.reject(&Sockets.unspecified?/1) do
        [] -> {:error, :address_not_available, core}
        addresses -> {:ok, addresses}
      end
    else
      {:ok, [address]}
    end
  end

  def connect(core, identity, endpoint) do
    with {:ok, udp} <- fetch(core, identity),
         {:ok, {address, _port, _scope_id} = remote} <- Sockets.endpoint(endpoint, :remote),
         :ok <- bound(udp),
         :ok <- family(udp, address),
         :ok <- reachable(core, address) do
      {{:ok, :ok}, put(core, identity, %{udp | peer: remote}), []}
    else
      error -> {error, core, []}
    end
  end

  @doc """
  Sends a datagram, replying `{:ok, :ok}` with the packet as an effect.
  """
  def sendto(core, identity, endpoint, data, _pid, _reference) do
    with {:ok, udp, entry} <- fetch_entry(core, identity),
         :ok <- if(Sockets.waiting?(entry, :write), do: {:error, :busy}, else: :ok),
         {:ok, {address, port, _scope_id} = remote} <- Sockets.endpoint(endpoint, :remote),
         :ok <- bound(udp),
         :ok <- family(udp, address),
         :ok <- if(udp.peer in [nil, remote], do: :ok, else: {:error, :invalid_socket_state}),
         :ok <- reachable(core, address),
         :ok <- fits(core, udp, data),
         {:ok, source} <- source(core, udp, address) do
      {_local, local_port, _scope_id} = udp.local
      packet = Iface.udp_packet(source, address, local_port, port, data)
      {{:ok, :ok}, core, [{:emit, packet}]}
    else
      error -> {error, core, []}
    end
  end

  defp fits(core, udp, data) do
    if byte_size(data) > max_datagram_bytes(core.iface.mtu, udp.family),
      do: {:error, :message_too_large},
      else: :ok
  end

  defp source(core, %__MODULE__{local: {local, _port, _scope_id}, family: family}, address) do
    cond do
      not Sockets.unspecified?(local) ->
        {:ok, local}

      (source = Iface.source_address(core.iface, address)) && Sockets.family?(family, source) ->
        {:ok, source}

      true ->
        {:error, :network_unreachable}
    end
  end

  @doc """
  Receives a datagram: `{:ok, {:ok, source, destination, data, truncated}}`
  or, with none to return, `{:ok, {:select, :recvfrom, reference}}` with
  the read waiter armed.
  """
  def recvfrom(core, identity, length, pid, reference) do
    with {:ok, udp, entry} <- fetch_entry(core, identity),
         :ok <- if(Sockets.waiting?(entry, :read), do: {:error, :busy}, else: :ok),
         :ok <- bound(udp) do
      {received, udp} = take(udp)
      core = put(core, identity, udp)

      case received do
        nil -> wait(core, identity, udp, {pid, :recvfrom, reference})
        datagram -> {{:ok, datagram(udp, datagram, length)}, core, []}
      end
    else
      error -> {error, core, []}
    end
  end

  defp datagram(udp, {data, {source, source_port}, destination}, length) do
    {_local, local_port, scope_id} = udp.local
    copied = if length == 0, do: byte_size(data), else: min(length, byte_size(data))

    {:ok, Sockets.encode(source, source_port, scope_id),
     Sockets.encode(destination, local_port, scope_id), binary_part(data, 0, copied),
     copied < byte_size(data)}
  end

  # A ring left with entries the read did not reach, or with only padding,
  # wakes the waiter at once, as smoltcp's `can_recv` does.
  defp wait(core, identity, udp, {_pid, operation, reference} = waiter) do
    sockets = Sockets.arm(core.sockets, identity, :read, waiter)

    {effects, sockets} =
      if Enum.all?(udp.rings, fn {_address, ring} -> PacketBuffer.empty?(ring) end),
        do: {[], sockets},
        else: Sockets.wake(sockets, identity, :read)

    {{:ok, {:select, operation, reference}}, %{core | sockets: sockets}, effects}
  end

  # The NIF's read: from the cursor's ring on, each ring until it is empty,
  # discarding datagrams of another family or from another peer, at most
  # 16 datagrams in all. The cursor moves past the ring a datagram came
  # from, or the ring the 16th was read from.
  defp take(%__MODULE__{rings: rings, cursor: cursor} = udp) do
    count = length(rings)
    start = rem(cursor, count)
    take(udp, for(offset <- 0..(count - 1), do: rem(start + offset, count)), 0)
  end

  defp take(udp, [], _read), do: {nil, udp}

  defp take(udp, [index | indexes], read) do
    {address, ring} = Enum.at(udp.rings, index)
    {received, ring, read} = read_ring(udp, ring, read)
    udp = %{udp | rings: List.replace_at(udp.rings, index, {address, ring})}

    if received != nil or read == @ring_packets,
      do: {received, %{udp | cursor: rem(index + 1, length(udp.rings))}},
      else: take(udp, indexes, read)
  end

  defp read_ring(_udp, ring, @ring_packets), do: {nil, ring, @ring_packets}

  defp read_ring(udp, ring, read) do
    case PacketBuffer.dequeue(ring) do
      {:ok, data, {{source, _port} = remote, destination}, ring} ->
        if Sockets.family?(udp.family, source) and Sockets.family?(udp.family, destination) and
             peer?(udp.peer, remote),
           do: {{data, remote, destination}, ring, read + 1},
           else: read_ring(udp, ring, read + 1)

      {:empty, ring} ->
        {nil, ring, read}
    end
  end

  defp peer?(nil, _remote), do: true
  defp peer?({address, port, _scope_id}, remote), do: {address, port} == remote

  def sockname(core, identity) do
    case fetch(core, identity) do
      {:ok, %__MODULE__{local: {address, port, scope_id}}} ->
        {{:ok, Sockets.encode(address, port, scope_id)}, core, []}

      {:ok, %__MODULE__{local: nil}} ->
        {{:error, :not_bound}, core, []}

      error ->
        {error, core, []}
    end
  end

  def peername(core, identity) do
    case fetch(core, identity) do
      {:ok, %__MODULE__{peer: {address, port, scope_id}}} ->
        {{:ok, Sockets.encode(address, port, scope_id)}, core, []}

      {:ok, %__MODULE__{peer: nil}} ->
        {{:error, :not_connected}, core, []}

      error ->
        {error, core, []}
    end
  end

  @doc "Closes a socket, aborting its waiters with `:closed`."
  def close(core, identity) do
    case fetch(core, identity) do
      {:ok, _udp} ->
        {waiters, sockets} = Sockets.close(core.sockets, identity)

        aborts =
          for {_direction, {pid, _operation, reference}} <- waiters,
              do: {:abort, pid, identity, reference, :closed}

        {{:ok, :ok}, %{core | sockets: sockets}, aborts}

      error ->
        {error, core, []}
    end
  end

  ## Ingress

  @doc """
  Hands a datagram from `source` to `destination` to the socket it is for:
  `{:ok, core, wakes}`, whether or not its ring had room, or `:unmatched`
  when no socket takes it.
  """
  @spec deliver(term(), Sockets.endpoint(), Sockets.endpoint(), binary()) ::
          {:ok, term(), [tuple()]} | :unmatched
  def deliver(core, {destination, _port} = local, remote, data) do
    case recipient(core, local, remote) do
      nil ->
        :unmatched

      {id, entry} ->
        identity = {id, entry.generation}
        udp = enqueue(entry.state, destination, remote, data)
        sockets = Sockets.put_state(core.sockets, identity, udp)
        {wakes, sockets} = Sockets.wake(sockets, identity, :read)
        {:ok, %{core | sockets: sockets}, wakes}
    end
  end

  defp recipient(core, {destination, port} = local, remote) do
    found =
      case Sockets.lookup(core.sockets, :udp, local, remote) do
        {_id, entry} = found -> if has_address?(entry.state, destination), do: found
        nil -> nil
      end

    cond do
      found -> found
      unicast?(core, destination) -> nil
      true -> port_owner(core, Sockets.family(destination), port)
    end
  end

  # A wildcard socket takes only the addresses it was bound on.
  defp has_address?(%__MODULE__{rings: rings}, destination),
    do: List.keymember?(rings, destination, 0)

  defp unicast?(core, <<_::32>> = address),
    do: not (Iface.ipv4_broadcast?(core.iface, address) or Sockets.multicast?(address))

  defp unicast?(_core, address), do: not Sockets.multicast?(address)

  # smoltcp gives a broadcast or multicast datagram to the first socket on
  # its port of either family, and a socket of the other family discards
  # it as it reads. Here a socket of the datagram's family comes first.
  defp port_owner(core, family, port) do
    other = if family == :inet, do: :inet6, else: :inet

    case Sockets.port_owners(core.sockets, :udp, family, port) ++
           Sockets.port_owners(core.sockets, :udp, other, port) do
      [id | _rest] ->
        {:ok, entry} = Map.fetch(core.sockets.entries, id)
        {id, entry}

      [] ->
        nil
    end
  end

  # Into the ring of the destination address, or the first ring for a
  # broadcast or multicast one.
  defp enqueue(udp, destination, remote, data) do
    index = Enum.find_index(udp.rings, fn {address, _ring} -> address == destination end) || 0
    {address, ring} = Enum.at(udp.rings, index)
    {_ok_or_full, ring} = PacketBuffer.enqueue(ring, data, {remote, destination})
    %{udp | rings: List.replace_at(udp.rings, index, {address, ring})}
  end

  ## Helpers

  defp new_ring, do: PacketBuffer.new(@ring_packets, @ring_bytes)

  defp fetch(core, identity) do
    case Sockets.fetch(core.sockets, identity, :udp) do
      {:ok, entry} -> {:ok, entry.state}
      error -> error
    end
  end

  defp fetch_entry(core, identity) do
    case Sockets.fetch(core.sockets, identity, :udp) do
      {:ok, entry} -> {:ok, entry.state, entry}
      error -> error
    end
  end

  defp put(core, identity, udp),
    do: %{core | sockets: Sockets.put_state(core.sockets, identity, udp)}

  defp bound(%__MODULE__{local: nil}), do: {:error, :not_bound}
  defp bound(_udp), do: :ok

  defp family(udp, address) do
    if Sockets.family?(udp.family, address), do: :ok, else: {:error, :invalid_address}
  end

  defp reachable(core, address) do
    if Iface.reachable?(core.iface, address), do: :ok, else: {:error, :network_unreachable}
  end

  defp identity_map({id, generation}), do: %{id: id, generation: generation}
end
