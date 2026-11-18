defmodule SmolNet.Core.Sockets do
  @moduledoc false

  # The socket table of a stack core: identities, waiters, ports and
  # demultiplexing, whatever the protocol. The protocol modules
  # (`SmolNet.Core.Udp`, and TCP in a later phase of #173) keep their own
  # per-socket state in an entry's `state` and call in here for the rest.
  #
  # Identities are `{id, generation}`, both drawn from the VM's unique
  # integers as the NIF draws them from global counters, so an identity is
  # never reused, in this stack or another: a stale handle, or a
  # `$smol_socket` message, can never be taken for a newer socket.
  #
  # Each socket has at most one waiter per direction, read and write, as
  # `{pid, operation, reference}`. Waking one sends `{:wake, ...}` and moves
  # it to the direction's "sent" slot, so `cancel/4` can tell a waiter it
  # removed (`:ok`) from one already notified (`:already_sent`) and from
  # one it never had (`:not_found`). Arming a new waiter forgets the sent
  # one.
  #
  # Demultiplexing is a map from `{protocol, local, remote}`, endpoints as
  # `{address, port}`, to a socket id, with `remote` nil for a socket bound
  # without a peer and the local address unspecified for a wildcard bind.
  # `lookup/4` tries the exact key, then the bound address, then the
  # wildcard: three map reads, never a scan. Port ownership is kept apart,
  # per protocol, family and port, for address-in-use checks and ephemeral
  # allocation; protocols with port-wide matching (UDP broadcast) read it
  # too.

  import Bitwise

  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Ipv6

  @ephemeral_first 49_152
  @ephemeral_last 50_175

  defstruct entries: %{}, demux: %{}, ports: %{}, next_port: %{}

  @type t :: %__MODULE__{}
  @type identity :: {pos_integer(), pos_integer()}
  @type kind :: :udp | :tcp
  @type direction :: :read | :write
  @type waiter :: {pid(), atom(), reference()}
  @type address :: <<_::32>> | <<_::128>>
  @type endpoint :: {address(), 0..0xFFFF}
  @type family :: :inet | :inet6

  defmodule Entry do
    @moduledoc false
    @type t :: %__MODULE__{}

    defstruct [
      :generation,
      :kind,
      :state,
      read: nil,
      write: nil,
      read_sent: nil,
      write_sent: nil,
      demux_keys: [],
      port_keys: []
    ]
  end

  @doc "An empty table whose ephemeral ports start at `ephemeral_start`."
  @spec new(0..0xFFFF) :: t()
  def new(ephemeral_start),
    do: %__MODULE__{next_port: %{udp: ephemeral_start, tcp: ephemeral_start}}

  ## Identities and entries

  @doc """
  Opens a socket of `kind` with protocol state `state`, unless the table
  already holds `limit` sockets.
  """
  @spec open(t(), kind(), term(), pos_integer()) ::
          {:ok, identity(), t()} | {:error, :system_limit}
  def open(table, kind, state, limit) do
    if map_size(table.entries) >= limit do
      {:error, :system_limit}
    else
      id = :erlang.unique_integer([:positive, :monotonic])
      generation = :erlang.unique_integer([:positive, :monotonic])
      entry = %Entry{generation: generation, kind: kind, state: state}
      {:ok, {id, generation}, %{table | entries: Map.put(table.entries, id, entry)}}
    end
  end

  @doc """
  The entry of a live socket of `kind`: `:invalid_socket` for an identity
  the table does not hold, `:wrong_socket_kind` for another protocol's.
  """
  @spec fetch(t(), identity(), kind() | :any) :: {:ok, Entry.t()} | {:error, atom()}
  def fetch(table, {id, generation}, kind) do
    case Map.fetch(table.entries, id) do
      {:ok, %Entry{generation: ^generation, kind: entry_kind} = entry}
      when kind in [:any, entry_kind] ->
        {:ok, entry}

      {:ok, %Entry{generation: ^generation}} ->
        {:error, :wrong_socket_kind}

      _stale_or_unknown ->
        {:error, :invalid_socket}
    end
  end

  @doc "Replaces a live socket's protocol state."
  @spec put_state(t(), identity(), term()) :: t()
  def put_state(table, {id, _generation}, state),
    do: %{table | entries: Map.update!(table.entries, id, &%{&1 | state: state})}

  @doc """
  Removes a socket with its demultiplexing and port entries, returning its
  waiters, read first, for the caller to abort.
  """
  @spec close(t(), identity()) :: {[{direction(), waiter()}], t()}
  def close(table, {id, _generation}) do
    {entry, entries} = Map.pop!(table.entries, id)

    ports =
      Enum.reduce(entry.port_keys, table.ports, fn key, ports ->
        ids = ports |> Map.fetch!(key) |> MapSet.delete(id)
        if MapSet.size(ids) == 0, do: Map.delete(ports, key), else: Map.put(ports, key, ids)
      end)

    table = %{
      table
      | entries: entries,
        demux: Map.drop(table.demux, entry.demux_keys),
        ports: ports
    }

    waiters =
      for {direction, waiter} <- [read: entry.read, write: entry.write],
          waiter,
          do: {direction, waiter}

    {waiters, table}
  end

  @doc """
  Removes every socket, in identity order, returning each one's waiters as
  `close/2` does.
  """
  @spec close_all(t()) :: {[{identity(), direction(), waiter()}], t()}
  def close_all(table) do
    waiters =
      for {id, entry} <- Enum.sort(table.entries),
          {direction, waiter} <- [read: entry.read, write: entry.write],
          waiter,
          do: {{id, entry.generation}, direction, waiter}

    {waiters, %{table | entries: %{}, demux: %{}, ports: %{}}}
  end

  ## Waiters

  @doc "Whether a live socket has a waiter in `direction`."
  @spec waiting?(Entry.t(), direction()) :: boolean()
  def waiting?(%Entry{read: read}, :read), do: read != nil
  def waiting?(%Entry{write: write}, :write), do: write != nil

  @doc "Arms a waiter, forgetting the direction's notified one."
  @spec arm(t(), identity(), direction(), waiter()) :: t()
  def arm(table, {id, _generation}, :read, waiter),
    do: update_entry(table, id, &%{&1 | read: waiter, read_sent: nil})

  def arm(table, {id, _generation}, :write, waiter),
    do: update_entry(table, id, &%{&1 | write: waiter, write_sent: nil})

  @doc """
  Wakes a socket's waiter in `direction`, if it has one: the wake effect
  and the table with the waiter moved to its sent slot.
  """
  @spec wake(t(), identity(), direction()) :: {[tuple()], t()}
  def wake(table, {id, generation} = identity, direction) do
    case Map.fetch(table.entries, id) do
      {:ok, %Entry{generation: ^generation} = entry} ->
        case take(entry, direction) do
          {nil, _entry} ->
            {[], table}

          {{pid, _operation, reference} = waiter, entry} ->
            entry = put_sent(entry, direction, waiter)

            {[{:wake, pid, identity, reference}],
             %{table | entries: Map.put(table.entries, id, entry)}}
        end

      _gone ->
        {[], table}
    end
  end

  @doc "A live socket's waiter in `direction`, or nil."
  @spec waiter(Entry.t(), direction()) :: waiter() | nil
  def waiter(%Entry{read: read}, :read), do: read
  def waiter(%Entry{write: write}, :write), do: write

  @doc """
  Takes a socket's waiter in `direction` away, without notifying it,
  returning it for the caller to abort.
  """
  @spec take_waiter(t(), identity(), direction()) :: {waiter() | nil, t()}
  def take_waiter(table, {id, _generation}, direction) do
    {waiter, entry} = table.entries |> Map.fetch!(id) |> take(direction)
    {waiter, %{table | entries: Map.put(table.entries, id, entry)}}
  end

  @doc """
  Cancels a waiter by operation and reference: `:ok` if it was armed,
  `:already_sent` if it was woken, `:not_found` otherwise.
  """
  @spec cancel(t(), identity(), atom(), reference()) ::
          {:ok, :ok | :already_sent | :not_found, t()} | {:error, atom()}
  def cancel(table, {id, generation}, operation, reference) do
    with {:ok, direction} <- direction(operation),
         {:ok, %Entry{generation: ^generation} = entry} <- Map.fetch(table.entries, id) do
      cond do
        match?({_pid, ^operation, ^reference}, waiter(entry, direction)) ->
          {_waiter, entry} = take(entry, direction)
          {:ok, :ok, %{table | entries: Map.put(table.entries, id, entry)}}

        match?({_pid, ^operation, ^reference}, sent(entry, direction)) ->
          {:ok, :already_sent, table}

        true ->
          {:ok, :not_found, table}
      end
    else
      {:error, _reason} = error -> error
      _stale_or_unknown -> {:error, :invalid_socket}
    end
  end

  @doc "The direction an operation waits in."
  @spec direction(atom()) :: {:ok, direction()} | {:error, :invalid_operation}
  def direction(operation) when operation in [:recv, :recvfrom, :accept], do: {:ok, :read}
  def direction(operation) when operation in [:send, :sendto, :connect], do: {:ok, :write}
  def direction(_operation), do: {:error, :invalid_operation}

  defp sent(%Entry{read_sent: sent}, :read), do: sent
  defp sent(%Entry{write_sent: sent}, :write), do: sent
  defp take(%Entry{read: read} = entry, :read), do: {read, %{entry | read: nil}}
  defp take(%Entry{write: write} = entry, :write), do: {write, %{entry | write: nil}}
  defp put_sent(entry, :read, waiter), do: %{entry | read_sent: waiter}
  defp put_sent(entry, :write, waiter), do: %{entry | write_sent: waiter}

  defp update_entry(table, id, fun), do: %{table | entries: Map.update!(table.entries, id, fun)}

  ## Ports

  @doc "Whether another socket of `protocol` and `family` holds `port`."
  @spec port_in_use?(t(), kind(), family(), 0..0xFFFF, identity()) :: boolean()
  def port_in_use?(table, protocol, family, port, {id, _generation}) do
    case Map.fetch(table.ports, {protocol, family, port}) do
      {:ok, ids} -> Enum.any?(ids, &(&1 != id))
      :error -> false
    end
  end

  @doc "The sockets of `protocol` and `family` that hold `port`."
  @spec port_owners(t(), kind(), family(), 0..0xFFFF) :: [pos_integer()]
  def port_owners(table, protocol, family, port),
    do: table.ports |> Map.get({protocol, family, port}, MapSet.new()) |> Enum.sort()

  @doc """
  The next free ephemeral port for `protocol` and `family`, from the
  protocol's round-robin cursor over 49152..50175, which moves past it
  whether or not the caller then uses it, as the NIF's does.
  """
  @spec allocate_port(t(), kind(), family()) ::
          {:ok, 0..0xFFFF, t()} | {:error, :ephemeral_ports_exhausted}
  def allocate_port(table, protocol, family),
    do: allocate_port(table, protocol, family, &Map.has_key?(table.ports, {protocol, family, &1}))

  @doc """
  The same, with the protocol's own test of a port in use: TCP counts
  ports that sockets which have left the table still hold.
  """
  @spec allocate_port(t(), kind(), family(), (0..0xFFFF -> boolean())) ::
          {:ok, 0..0xFFFF, t()} | {:error, :ephemeral_ports_exhausted}
  def allocate_port(table, protocol, _family, in_use?) do
    start = Map.fetch!(table.next_port, protocol)
    count = @ephemeral_last - @ephemeral_first + 1

    0..(count - 1)
    |> Enum.map(&(@ephemeral_first + rem(start - @ephemeral_first + &1, count)))
    |> Enum.find(&(not in_use?.(&1)))
    |> case do
      nil ->
        {:error, :ephemeral_ports_exhausted}

      port ->
        next = if port == @ephemeral_last, do: @ephemeral_first, else: port + 1
        {:ok, port, %{table | next_port: Map.put(table.next_port, protocol, next)}}
    end
  end

  @doc """
  Binds a socket: it holds `local`'s port, and `{protocol, local, remote}`
  leads to it.
  """
  @spec bind(t(), identity(), kind(), endpoint(), endpoint() | nil) :: t()
  def bind(table, {id, _generation}, protocol, {address, port} = local, remote) do
    port_key = {protocol, family(address), port}
    demux_key = {protocol, local, remote}

    entries =
      Map.update!(table.entries, id, fn entry ->
        %{
          entry
          | demux_keys: [demux_key | entry.demux_keys],
            port_keys: Enum.uniq([port_key | entry.port_keys])
        }
      end)

    %{
      table
      | entries: entries,
        demux: Map.put(table.demux, demux_key, id),
        ports: Map.update(table.ports, port_key, MapSet.new([id]), &MapSet.put(&1, id))
    }
  end

  ## Demultiplexing

  @doc """
  The socket a packet from `remote` to `local` reaches: the connection
  with both endpoints, else the socket bound to `local`, else the one
  bound to its port on every address of the family; nil for none.
  """
  @spec lookup(t(), kind(), endpoint(), endpoint()) :: {pos_integer(), Entry.t()} | nil
  def lookup(table, protocol, {address, port} = local, remote) do
    demux = table.demux

    id =
      Map.get(demux, {protocol, local, remote}) ||
        Map.get(demux, {protocol, local, nil}) ||
        Map.get(demux, {protocol, {unspecified(address), port}, nil})

    if id, do: {id, Map.fetch!(table.entries, id)}
  end

  ## Endpoints

  @doc """
  Validates an endpoint from the shell, `%{address: bytes, port:,
  scope_id:}`, as the NIF does, for a bind (unspecified address and port 0
  allowed) or a peer. Returns `{address, port, scope_id}` with the address
  as a binary.
  """
  @spec endpoint(map(), :bind | :remote) ::
          {:ok, {address(), 0..0xFFFF, non_neg_integer()}} | {:error, atom()}
  def endpoint(%{address: bytes, port: port, scope_id: scope_id}, use) do
    with {:ok, address} <- address(bytes),
         :ok <- usable_address(address, use),
         :ok <- usable_port(port, use),
         :ok <- usable_scope(address, scope_id) do
      {:ok, {address, port, scope_id}}
    end
  end

  def endpoint(_endpoint, _use), do: {:error, :invalid_address}

  defp address(bytes) when is_list(bytes) and length(bytes) in [4, 16] do
    if Enum.all?(bytes, &(is_integer(&1) and &1 in 0..255)) do
      case :erlang.list_to_binary(bytes) do
        <<0::80, 0xFFFF::16, _ipv4::32>> -> {:error, :invalid_address}
        address -> {:ok, address}
      end
    else
      {:error, :invalid_address}
    end
  end

  defp address(_bytes), do: {:error, :invalid_address}

  defp usable_address(address, use) do
    if multicast?(address) or address == <<255, 255, 255, 255>> or
         (use == :remote and unspecified?(address)),
       do: {:error, :invalid_address},
       else: :ok
  end

  defp usable_port(port, use) when is_integer(port) and port in 0..0xFFFF,
    do: if(use == :remote and port == 0, do: {:error, :invalid_port}, else: :ok)

  defp usable_port(_port, _use), do: {:error, :invalid_port}

  defp usable_scope(<<0xFE, second, _rest::binary-14>>, scope_id)
       when (second &&& 0xC0) == 0x80 do
    cond do
      scope_id === 0 -> {:error, :scope_required}
      is_integer(scope_id) and scope_id in 1..0xFFFF_FFFF -> :ok
      true -> {:error, :invalid_scope}
    end
  end

  defp usable_scope(_address, 0), do: :ok
  defp usable_scope(_address, _scope_id), do: {:error, :invalid_scope}

  @doc "An endpoint as the shell takes it back."
  @spec encode(address(), 0..0xFFFF, non_neg_integer()) :: map()
  def encode(address, port, scope_id),
    do: %{address: :erlang.binary_to_list(address), port: port, scope_id: scope_id}

  @doc "The family of an address."
  @spec family(address()) :: family()
  def family(<<_::32>>), do: :inet
  def family(<<_::128>>), do: :inet6

  @doc "Whether an address belongs to a family."
  @spec family?(family(), address()) :: boolean()
  def family?(family, address), do: family(address) == family

  @doc "The unspecified address of an address's family."
  @spec unspecified(address()) :: address()
  def unspecified(<<_::32>>), do: <<0::32>>
  def unspecified(<<_::128>>), do: <<0::128>>

  @doc "Whether an address is its family's unspecified address."
  @spec unspecified?(address()) :: boolean()
  def unspecified?(address), do: address == unspecified(address)

  @doc "Whether an address is multicast."
  @spec multicast?(address()) :: boolean()
  def multicast?(<<_::32>> = address), do: Ipv4.multicast?(address)
  def multicast?(address), do: Ipv6.multicast?(address)

  ## Counts

  @doc """
  The table's counts for the snapshot: sockets of each kind, and waiters
  armed and notified in each direction.
  """
  @spec counts(t()) :: map()
  def counts(table) do
    Enum.reduce(
      table.entries,
      %{udp: 0, tcp: 0, read: 0, write: 0, read_sent: 0, write_sent: 0},
      fn {_id, entry}, counts ->
        counts
        |> Map.update!(entry.kind, &(&1 + 1))
        |> count(:read, entry.read)
        |> count(:write, entry.write)
        |> count(:read_sent, entry.read_sent)
        |> count(:write_sent, entry.write_sent)
      end
    )
  end

  defp count(counts, _key, nil), do: counts
  defp count(counts, key, _waiter), do: Map.update!(counts, key, &(&1 + 1))

  @doc "The protocol states of every socket of `kind`."
  @spec states(t(), kind()) :: [term()]
  def states(table, kind),
    do: for({_id, %Entry{kind: ^kind, state: state}} <- table.entries, do: state)
end
