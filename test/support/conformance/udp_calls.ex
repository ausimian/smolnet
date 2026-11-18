defmodule SmolNet.Conformance.UdpCalls do
  @moduledoc false

  # Sequences of UDP socket calls and datagrams for the differential test
  # (test/conformance/udp_sockets_test.exs) and the `udp_sockets` trace.
  # A call names its socket by the order it was opened in (modulo how many
  # there are), and a receive or send takes a fresh reference label, so the
  # same sequence can be made on any backend and compared call for call.
  #
  # Two of the NIF's choices depend on where smoltcp happened to put its
  # sockets, which the Elixir backend does not model (see
  # `SmolNet.Core.Udp`), so the sequences stay clear of them: IPv4 and IPv6
  # sockets bind different ports, so no broadcast or multicast datagram has
  # a socket of each family to choose from; and a stack with two IPv4
  # addresses, whose wildcard socket has two rings, is sent no broadcast or
  # multicast datagram.

  use ExUnitProperties

  alias SmolNet.Conformance.Driver
  alias SmolNet.Conformance.Packets

  @local4 [192, 0, 2, 1]
  @alternate4 [192, 0, 2, 3]
  @local6 [0xFD, 0 | List.duplicate(0, 13)] ++ [1]
  @peer4 <<192, 0, 2, 9>>
  @routed4 <<198, 51, 100, 7>>
  @peer6 <<0xFD00::16, 0::96, 9::16>>
  @link_local6 [0xFE, 0x80 | List.duplicate(0, 13)] ++ [1]
  @link_local6_binary <<0xFE80::16, 0::96, 1::16>>
  @link_local_peer6 <<0xFE80::16, 0::96, 9::16>>
  @link_local_peer6_bytes [0xFE, 0x80 | List.duplicate(0, 13)] ++ [9]

  @doc """
  The stack the calls are made on, by name: `:single`, one IPv4 and one
  IPv6 address, and a route; `:two_ipv4`, a second IPv4 address;
  `:ipv4_only`, no IPv6 address; `:link_local`, a link-local IPv6 address
  as well; `:few_sockets`, two IPv4 addresses and a limit of three
  sockets. `true` and `false` are `:two_ipv4` and `:single`, as the random
  sequences choose.
  """
  def options(true), do: options(:two_ipv4)
  def options(false), do: options(:single)

  def options(stack) do
    ipv4 =
      [{List.to_tuple(@local4), 24}] ++
        if(stack in [:two_ipv4, :few_sockets], do: [{List.to_tuple(@alternate4), 24}], else: [])

    ipv6 =
      case stack do
        :ipv4_only -> []
        :link_local -> [{{0xFD00, 0, 0, 0, 0, 0, 0, 1}, 64}, {{0xFE80, 0, 0, 0, 0, 0, 0, 1}, 64}]
        _other -> [{{0xFD00, 0, 0, 0, 0, 0, 0, 1}, 64}]
      end

    [
      addresses: ipv4 ++ ipv6,
      routes: [{{198, 51, 100, 0}, 24, {192, 0, 2, 254}}],
      test_determinism: %{random_seed: 1, timestamp_origin: 0, ephemeral_port_start: 50_170}
    ] ++ if(stack == :few_sockets, do: [limits: %{sockets: 3}], else: [])
  end

  @doc """
  Named sequences for corners a random one rarely reaches, as
  `{description, stack, calls}` with `stack` as `options/1` takes it.
  """
  def scripted do
    [
      {"a wildcard socket on two addresses reads their rings in turn", :two_ipv4,
       [{:open, :inet}, {:bind, 0, {[0, 0, 0, 0], 0}, 7}] ++
         for(
           n <- 1..18,
           do: {:ingress, :inet, @peer4, 40_000 + n, {:local, rem(n, 2)}, 7, n * 50}
         ) ++
         for(n <- 1..18, do: {:ingress, :inet, @peer4, 41_000 + n, {:local, 1}, 7, n * 20}) ++
         for(_n <- 1..40, do: {:recvfrom, 0, 0})},
      {"a woken, cancelled or closed waiter is reported as the NIF reports it", :single,
       [
         {:open, :inet},
         {:bind, 0, {@local4, 0}, 7},
         {:recvfrom, 0, 0},
         {:ingress, :inet, @peer4, 40_000, {:local, 0}, 7, 10},
         {:cancel, 0, :recvfrom, 0},
         {:cancel, 0, :sendto, 0},
         {:recvfrom, 0, 0},
         {:recvfrom, 0, 0},
         {:recvfrom, 0, 0},
         {:cancel, 0, :recvfrom, 0},
         {:cancel, 0, :recvfrom, 0},
         {:recvfrom, 0, 0},
         {:close, 0},
         {:cancel, 0, :recvfrom, 0}
       ]},
      {"a read that gives up after 16 datagrams of another peer wakes its waiter", :two_ipv4,
       [
         {:open, :inet},
         {:bind, 0, {[0, 0, 0, 0], 0}, 7},
         {:connect, 0, {[192, 0, 2, 9], 0}, 40_000}
       ] ++
         for(n <- 1..17, do: {:ingress, :inet, @peer4, 40_000 + n, {:local, 0}, 7, n}) ++
         [
           {:ingress, :inet, @peer4, 40_000, {:local, 1}, 7, 4},
           {:recvfrom, 0, 0},
           {:recvfrom, 0, 0},
           {:recvfrom, 0, 0}
         ]},
      {"shutting down aborts every waiter with its reason", :single,
       for(_n <- 1..3, do: {:open, :inet}) ++
         for(n <- 0..2, do: {:bind, n, {@local4, 0}, 0}) ++
         for(n <- [2, 0, 1], do: {:recvfrom, n, 0}) ++
         [{:shutdown, :link_down}, {:recvfrom, 0, 0}, {:shutdown, :closed}]},
      {"a wildcard bind without an address of its family still takes a port", :ipv4_only,
       [
         {:open, :inet6},
         {:bind, 0, {List.duplicate(0, 16), 0}, 0},
         {:open, :inet},
         {:bind, 1, {[0, 0, 0, 0], 0}, 0},
         {:sockname, 1}
       ]},
      {"a link-local socket reports its scope", :link_local,
       [
         {:open, :inet6},
         {:bind, 0, {@link_local6, 0}, 8},
         {:bind, 0, {@link_local6, 3}, 8},
         {:sockname, 0},
         {:ingress, :inet6, @link_local_peer6, 40_000, @link_local6_binary, 8, 6},
         {:recvfrom, 0, 0},
         {:connect, 0, {@link_local_peer6_bytes, 2}, 40_000},
         {:peername, 0},
         {:sendto, 0, {@link_local_peer6_bytes, 3}, 40_000, 4},
         {:sendto, 0, {@link_local_peer6_bytes, 2}, 40_000, 4},
         {:ingress, :inet6, @link_local_peer6, 40_000, @link_local6_binary, 8, 6},
         {:recvfrom, 0, 0}
       ]},
      {"each address of a wildcard bind counts against the socket limit", :few_sockets,
       [
         {:open, :inet},
         {:bind, 0, {[0, 0, 0, 0], 0}, 0},
         {:open, :inet},
         {:bind, 1, {[0, 0, 0, 0], 0}, 0},
         {:bind, 1, {@local4, 0}, 0},
         {:open, :inet},
         {:close, 0},
         {:open, :inet},
         {:open, :inet}
       ]}
    ]
  end

  @doc """
  Random sequences for a stack with one IPv4 address or two, sometimes
  ending in a shutdown.
  """
  def sequence(two_addresses) do
    gen all(
          calls <- list_of(call(two_addresses), min_length: 1, max_length: 60),
          ending <- member_of([nil, :closed, :link_down, :stack_down])
        ) do
      if ending, do: calls ++ [{:shutdown, ending}, {:open, :inet}], else: calls
    end
  end

  @doc "Makes `calls` on one stack, for a trace."
  def run(driver, calls) do
    {driver, _model} =
      calls
      |> Enum.with_index(1)
      |> Enum.reduce({driver, new()}, fn {call, at}, {driver, model} ->
        case prepare(model, call) do
          nil ->
            {driver, model}

          {name, args} ->
            {args, driver} = Enum.map_reduce(args, driver, &fresh_ref/2)
            {result, driver} = Driver.call(driver, name, args, at)
            {driver, observe(model, name, result)}
        end
      end)

    driver
  end

  defp fresh_ref(:fresh_ref, driver), do: Driver.ref(driver)
  defp fresh_ref(arg, driver), do: {arg, driver}

  @doc "What a sequence knows before its first call."
  def new, do: %{sockets: [], refs: []}

  @doc """
  The backend call for a step, `{name, args}` with `:fresh_ref` where a
  new reference label goes, or nil for a socket call before any socket.
  """
  def prepare(_model, {:open, family}), do: {:udp_open, [family]}
  def prepare(_model, {:shutdown, reason}), do: {:stack_shutdown, [reason]}

  def prepare(
        _model,
        {:ingress, _family, _source, _port, _destination, _dst_port, _size} = ingress
      ),
      do: {:stack_ingress, [packet(ingress)]}

  # Several datagrams in one call, for one socket's ring to fill and for
  # several sockets' waiters to wake together.
  def prepare(_model, {:burst, ingresses}),
    do: {:stack_ingress_batch, [Enum.map(ingresses, &packet/1)]}

  def prepare(%{sockets: []}, _call), do: nil

  def prepare(model, call) do
    socket = Enum.at(model.sockets, rem(elem(call, 1), length(model.sockets)))
    socket_call(model, socket, call)
  end

  defp socket_call(model, socket, call) do
    case call do
      {:bind, _n, {address, scope_id}, port} ->
        {:udp_bind, [socket, endpoint(address, port, scope_id)]}

      {:connect, _n, {address, scope_id}, port} ->
        {:udp_connect, [socket, endpoint(address, port, scope_id)]}

      {:sendto, _n, {address, scope_id}, port, size} ->
        {:udp_sendto,
         [socket, endpoint(address, port, scope_id), :binary.copy("s", size), :fresh_ref]}

      {:recvfrom, _n, length} ->
        {:udp_recvfrom, [socket, length, :fresh_ref]}

      {:cancel, _n, operation, pick} ->
        {:socket_cancel, [socket, operation, pick_ref(model.refs, pick)]}

      {query, _n} ->
        {query_call(query), [socket]}
    end
  end

  defp pick_ref([], _pick), do: :fresh_ref
  defp pick_ref(refs, pick), do: Enum.at(refs, rem(pick, length(refs)))

  defp query_call(:sockname), do: :udp_sockname
  defp query_call(:peername), do: :udp_peername
  defp query_call(:validate), do: :socket_validate
  defp query_call(:close), do: :udp_close

  @doc "Learns the sockets and waiting references a call's result names."
  def observe(model, :udp_open, {:ok, socket}), do: %{model | sockets: model.sockets ++ [socket]}

  def observe(model, :udp_recvfrom, {:ok, {:select, :recvfrom, ref}}),
    do: %{model | refs: [ref | model.refs]}

  def observe(model, _name, _result), do: model

  defp packet({:ingress, family, source, source_port, destination, port, size}) do
    destination = destination(destination)
    payload = :binary.copy(<<size::16>>, div(size, 2))
    datagram = Packets.udp(source, destination, source_port, port, payload)

    case family do
      :inet -> Packets.ipv4(source, destination, 17, datagram)
      :inet6 -> Packets.ipv6(source, destination, 17, datagram)
    end
  end

  defp endpoint(address, port, scope_id), do: %{address: address, port: port, scope_id: scope_id}

  defp destination({:local, 0}), do: :erlang.list_to_binary(@local4)
  defp destination({:local, 1}), do: :erlang.list_to_binary(@alternate4)
  defp destination(address), do: address

  ## Generators

  defp call(two_addresses) do
    frequency([
      {3, map(member_of([:inet, :inet6]), &{:open, &1})},
      {3, bind()},
      {1,
       map(tuple({index(), remote(), remote_port()}), fn {n, address, port} ->
         {:connect, n, address, port}
       end)},
      {3, sendto()},
      {4,
       map(tuple({index(), member_of([0, 0, 1, 3, 100, 2_000])}), fn {n, length} ->
         {:recvfrom, n, length}
       end)},
      {1,
       map(
         tuple({index(), member_of([:recvfrom, :sendto, :recv]), integer(0..5)}),
         &Tuple.insert_at(&1, 0, :cancel)
       )},
      {1, map(index(), &{:sockname, &1})},
      {1, map(index(), &{:peername, &1})},
      {1, map(index(), &{:validate, &1})},
      {1, map(index(), &{:close, &1})},
      {8, ingress(two_addresses)},
      {2, burst(two_addresses)}
    ])
  end

  defp index, do: integer(0..7)

  # IPv4 sockets bind odd ports and IPv6 even ones, and ephemeral ports
  # come from one cursor, so no port has a socket of each family.
  defp bind do
    frequency([
      {4,
       map(
         tuple(
           {index(), member_of([[0, 0, 0, 0], @local4, @local4, @alternate4, [192, 0, 2, 9]]),
            member_of([0, 7, 7, 9])}
         ),
         fn {n, address, port} -> {:bind, n, {address, 0}, port} end
       )},
      {4,
       map(
         tuple(
           {index(),
            member_of([
              List.duplicate(0, 16),
              @local6,
              @local6,
              [0xFE, 0x80 | List.duplicate(0, 13)] ++ [1]
            ]), member_of([0, 8, 8, 10])}
         ),
         fn {n, address, port} -> {:bind, n, {address, 0}, port} end
       )},
      {1,
       map(
         tuple(
           {index(), member_of([[224, 0, 0, 1], [192, 0, 2], @local4]), member_of([-1, 70_000])}
         ),
         fn {n, address, port} -> {:bind, n, {address, 0}, port} end
       )}
    ])
  end

  defp sendto do
    map(
      tuple(
        {index(), remote(), remote_port(), member_of([0, 5, 1_000, 1_452, 1_453, 1_472, 1_473])}
      ),
      fn {n, address, port, size} -> {:sendto, n, address, port, size} end
    )
  end

  defp remote do
    member_of([
      {[192, 0, 2, 9], 0},
      {[192, 0, 2, 255], 0},
      {[255, 255, 255, 255], 0},
      {[224, 0, 0, 1], 0},
      {[198, 51, 100, 9], 0},
      {[203, 0, 113, 5], 0},
      {[0, 0, 0, 0], 0},
      {@local4, 0},
      {[0xFD, 0 | List.duplicate(0, 13)] ++ [9], 0},
      {[0xFD, 0 | List.duplicate(0, 13)] ++ [9], 3},
      {[0xFE, 0x80 | List.duplicate(0, 13)] ++ [9], 0},
      {[0xFE, 0x80 | List.duplicate(0, 13)] ++ [9], 2},
      {[0xFF, 0x02 | List.duplicate(0, 13)] ++ [1], 0},
      {List.duplicate(0, 10) ++ [0xFF, 0xFF, 192, 0, 2, 9], 0},
      {[0x20, 0x01, 0x0D, 0xB8 | List.duplicate(0, 11)] ++ [1], 0}
    ])
  end

  defp remote_port, do: member_of([7, 9, 8, 10, 40_000, 0])

  defp ingress(two_addresses) do
    ipv4_destinations =
      [{:local, 0}, {:local, 1}, <<192, 0, 2, 7>>] ++
        if two_addresses,
          do: [],
          else: [<<192, 0, 2, 255>>, <<255, 255, 255, 255>>, <<224, 0, 0, 1>>]

    ipv6_destinations = [
      :erlang.list_to_binary(@local6),
      <<0xFF02::16, 0::96, 1::16>>,
      <<0xFD00::16, 0::96, 7::16>>
    ]

    one_of([
      map(
        tuple(
          {member_of([@peer4, @routed4]), member_of([40_000, 40_001, 9]),
           member_of(ipv4_destinations), member_of([7, 9, 50_170, 50_172, 50_174]), size()}
        ),
        fn {source, source_port, destination, port, size} ->
          {:ingress, :inet, source, source_port, destination, port, size}
        end
      ),
      map(
        tuple(
          {member_of([40_000, 40_001, 10]), member_of(ipv6_destinations),
           member_of([8, 10, 50_171, 50_173, 50_175]), size()}
        ),
        fn {source_port, destination, port, size} ->
          {:ingress, :inet6, @peer6, source_port, destination, port, size}
        end
      )
    ])
  end

  # Up to 20 datagrams, mostly to one port, so that a ring overflows.
  defp burst(two_addresses) do
    bind(tuple({ingress(two_addresses), integer(4..20)}), fn {ingress, count} ->
      list_of(one_of([constant(ingress), ingress(two_addresses)]), length: count)
      |> map(&{:burst, &1})
    end)
  end

  defp size, do: frequency([{3, integer(0..64)}, {2, integer(900..1_400)}])
end
