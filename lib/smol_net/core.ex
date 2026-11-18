defmodule SmolNet.Core do
  @moduledoc false

  # The stack core: a pure value holding everything a stack knows, with no
  # process and no clock of its own. Every operation takes the core and the
  # current time and returns `{reply, core, effects}`, where `reply` is
  # `{:ok, result}` or `{:error, reason}` and `effects` the side effects in
  # order, `{:emit, packet}` so far (see `SmolNet.Stack.Backend`). The next
  # time the core needs a call is `deadline/1`, a value rather than an
  # effect.
  #
  # The core is the IP layer, the socket table and UDP: it validates
  # ingress as the NIF does, answers what an interface answers by itself
  # (echo, Port and Protocol Unreachable, Parameter Problem, RST for a
  # segment no socket takes), hands datagrams to UDP sockets
  # (`SmolNet.Core.Udp`, over `SmolNet.Core.Sockets`), counts ICMP errors
  # that report a path MTU, keeps the stack's lifecycle and its counters,
  # and describes itself in the NIF's snapshot shape. Wakes and aborts for
  # socket waiters are `{:wake, ...}` and `{:abort, ...}` effects. TCP comes
  # in a later phase of #173; until then every segment is answered with a
  # RST and TCP socket operations reply `{:error, :not_implemented}`.

  alias SmolNet.Core.Iface
  alias SmolNet.Core.Sockets
  alias SmolNet.Core.Udp
  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Ipv6

  require SmolNet.Core.Wire.Ipv4
  require SmolNet.Core.Wire.Ipv6

  @max_limits %{
    bytes_copied: 65_575,
    input_packets: 32,
    output_packets: 32,
    ready_events: 128,
    maintenance_work: 128,
    sockets: 512
  }
  @ephemeral_ports 49_152..50_175
  @tcp_default_buffer_bytes 262_144
  @tcp_max_buffer_bytes 1_048_576

  defstruct [
    :id,
    :limits,
    :iface,
    :random_seed,
    :timestamp_origin,
    :ephemeral_port_start,
    :sockets,
    lifecycle: :running,
    notifications_delivered: 0,
    ingress_packets: 0,
    rejected_packets: 0,
    emitted_packets: 0,
    poll_calls: 0,
    native_calls: 0,
    max_bytes_copied: 0,
    max_input_packets: 0,
    max_output_packets: 0,
    icmp_too_big_received: 0,
    icmp_too_big_rejected: 0,
    path_mtu_reductions: 0
  ]

  @type t :: %__MODULE__{}
  @type reply :: {:ok, term()} | {:error, atom()}
  @type effect ::
          {:emit, binary()}
          | {:wake, pid(), Sockets.identity(), reference()}
          | {:abort, pid(), Sockets.identity(), reference(), atom()}
  @type outcome :: {reply(), t(), [effect()]}

  @doc """
  A stack from the `limits` and `config` the shell passes its backend
  (`SmolNet.Stack.Options`), validated as the NIF validates them.
  """
  @spec new(map(), map(), integer()) :: {:ok, t()} | {:error, atom()}
  def new(limits, config, _now) do
    cond do
      not valid_limits?(limits) ->
        {:error, :invalid_limits}

      not valid_config?(config, limits) ->
        {:error, :invalid_stack_config}

      # Egress credit is the shell's flow control with the link, which the
      # core takes on in a later phase of #173.
      Map.get(config, :egress_credit) != nil ->
        {:error, :not_implemented}

      true ->
        ephemeral_port_start = Map.get(config, :ephemeral_port_start, @ephemeral_ports.first)

        {:ok,
         %__MODULE__{
           id: System.unique_integer([:positive, :monotonic]),
           limits: limits,
           iface: Iface.new(config.mtu, addresses(config.addresses), routes(config.routes)),
           random_seed: Map.get(config, :random_seed),
           timestamp_origin: Map.get(config, :timestamp_origin),
           ephemeral_port_start: ephemeral_port_start,
           sockets: Sockets.new(ephemeral_port_start)
         }}
    end
  end

  defp valid_limits?(limits) when is_map(limits) do
    Enum.all?(@max_limits, fn {name, max} ->
      value = Map.get(limits, name)
      is_integer(value) and value in 1..max
    end)
  end

  defp valid_limits?(_limits), do: false

  defp valid_config?(%{mtu: mtu, addresses: addresses, routes: routes} = config, limits) do
    is_integer(mtu) and mtu in 1_280..65_575 and mtu <= limits.bytes_copied and
      length(addresses) <= 8 and length(routes) <= 4 and
      Enum.all?(addresses, &valid_address?/1) and Enum.all?(routes, &valid_route?/1) and
      Map.get(config, :ephemeral_port_start, @ephemeral_ports.first) in @ephemeral_ports
  end

  defp valid_config?(_config, _limits), do: false

  # As the NIF's `AddressConfig::valid`: a unicast or unspecified address
  # with a prefix length its family allows.
  defp valid_address?(%{address: bytes, prefix_length: prefix_length}) do
    case address(bytes) do
      {:ok, address} ->
        prefix_length in 0..prefix_limit(address) and unicast_or_unspecified?(address)

      :error ->
        false
    end
  end

  defp valid_address?(_address), do: false

  # As the NIF's `RouteConfig::valid`.
  defp valid_route?(%{destination: destination, prefix_length: prefix_length, gateway: gateway}) do
    with {:ok, destination} <- address(destination),
         {:ok, gateway} <- address(gateway) do
      byte_size(destination) == byte_size(gateway) and
        prefix_length in 0..prefix_limit(destination) and
        not multicast?(destination) and unicast?(gateway)
    else
      :error -> false
    end
  end

  defp valid_route?(_route), do: false

  # As the NIF's `ip_address`, which refuses an IPv4-mapped IPv6 address.
  defp address(bytes) when is_list(bytes) and length(bytes) in [4, 16] do
    case Enum.all?(bytes, &(is_integer(&1) and &1 in 0..255)) and :erlang.list_to_binary(bytes) do
      false -> :error
      <<0::80, 0xFFFF::16, _ipv4::32>> -> :error
      address -> {:ok, address}
    end
  end

  defp address(_bytes), do: :error

  defp prefix_limit(address), do: byte_size(address) * 8

  defp unicast_or_unspecified?(address), do: unicast?(address) or unspecified?(address)

  defp unicast?(<<_::32>> = address), do: Ipv4.unicast?(address)
  defp unicast?(address), do: Ipv6.unicast?(address)

  defp multicast?(<<_::32>> = address), do: Ipv4.multicast?(address)
  defp multicast?(address), do: Ipv6.multicast?(address)

  defp unspecified?(address), do: address == <<0::size(byte_size(address))-unit(8)>>

  defp addresses(addresses) do
    for %{address: bytes, prefix_length: prefix_length} <- addresses,
        do: {:erlang.list_to_binary(bytes), prefix_length}
  end

  defp routes(routes) do
    for %{destination: destination, prefix_length: prefix_length, gateway: gateway} <- routes,
        do: {:erlang.list_to_binary(destination), prefix_length, :erlang.list_to_binary(gateway)}
  end

  @doc "When the core next needs `poll/2`, or nil. Nothing is timed yet."
  @spec deadline(t()) :: integer() | nil
  def deadline(%__MODULE__{}), do: nil

  ## Ingress

  @doc "One raw IP packet from the link, replying `{:ok, :ok}`."
  @spec ingress(t(), binary(), integer()) :: outcome()
  def ingress(core, packet, now) do
    case ingress_batch(core, [packet], now) do
      {{:ok, _count}, core, effects} -> {{:ok, :ok}, core, effects}
      error -> error
    end
  end

  @doc """
  Raw IP packets from the link, replying `{:ok, count}`. The batch is
  checked as the NIF checks it before any packet is processed: at most
  the `input_packets` limit, each a whole valid IPv4 packet that is not a
  fragment or a whole IPv6 packet, within the MTU and the `bytes_copied`
  limit, together within `bytes_copied`.
  """
  @spec ingress_batch(t(), [binary()], integer()) :: outcome()
  def ingress_batch(core, packets, now) do
    core = observe(core)

    with :ok <- running(core),
         :ok <- batch_size(core, packets),
         {:ok, bytes, core} <- validate(core, packets, 0) do
      count = length(packets)
      core = %{core | ingress_packets: core.ingress_packets + count}
      {core, effects, wakes} = process(core, packets, now, [], [])
      core = core |> account(bytes, effects) |> account_notifications(wakes)
      core = %{core | max_input_packets: max(core.max_input_packets, count)}

      # The NIF delivers a call's readiness after its packets, in identity
      # order.
      {{:ok, count}, core, effects ++ Enum.sort_by(wakes, &elem(&1, 2))}
    else
      {:error, reason, core} -> {{:error, reason}, core, []}
      {:error, reason} -> {{:error, reason}, core, []}
    end
  end

  defp batch_size(core, packets) do
    if length(packets) > core.limits.input_packets, do: {:error, :batch_too_large}, else: :ok
  end

  defp validate(core, [], bytes), do: {:ok, bytes, core}

  defp validate(core, [packet | packets], bytes) do
    case valid_packet(core, packet) do
      :ok ->
        bytes = bytes + byte_size(packet)

        if bytes > core.limits.bytes_copied,
          do: {:error, :batch_too_large, core},
          else: validate(core, packets, bytes)

      {:error, reason} ->
        {:error, reason, %{core | rejected_packets: core.rejected_packets + 1}}
    end
  end

  # The NIF's `validate_raw_packet`.
  defp valid_packet(core, packet) do
    cond do
      not whole_packet?(packet) -> {:error, :invalid_packet}
      byte_size(packet) > max(core.iface.mtu, 0) -> {:error, :packet_too_large}
      byte_size(packet) > core.limits.bytes_copied -> {:error, :packet_too_large}
      true -> :ok
    end
  end

  defp whole_packet?(
         <<4::4, ihl::4, _tos, total_len::16, _ident::16, _reserved::1, _df::1, mf::1, offset::13,
           _rest::binary>> = packet
       ) do
    header_len = ihl * 4

    header_len >= 20 and byte_size(packet) >= header_len and header_len <= total_len and
      total_len == byte_size(packet) and mf == 0 and offset == 0 and
      Checksum.valid?(binary_part(packet, 0, header_len))
  end

  defp whole_packet?(<<6::4, _class_and_flow::28, payload_len::16, _rest::binary>> = packet),
    do: byte_size(packet) >= 40 and byte_size(packet) == 40 + payload_len

  defp whole_packet?(_packet), do: false

  defp process(core, [], _now, effects, wakes), do: {core, :lists.reverse(effects), wakes}

  defp process(core, [packet | packets], now, effects, wakes) do
    {core, effects, wakes} = receive_packet(core, packet, effects, wakes)
    process(core, packets, now, effects, wakes)
  end

  defp receive_packet(core, packet, effects, wakes) do
    case Iface.receive(core.iface, packet) do
      :drop ->
        {core, effects, wakes}

      {:reply, reply} ->
        {core, emit(core, reply, effects), wakes}

      {:udp, ip, {src_port, dst_port}, data, ip_payload} ->
        {src, dst} = ip_addresses(ip)

        case Udp.deliver(core, {dst, dst_port}, {src, src_port}, data) do
          {:ok, core, woken} -> {core, effects, woken ++ wakes}
          :unmatched -> {core, udp_unmatched(core, ip, ip_payload, effects), wakes}
        end

      {:tcp, ip, segment} ->
        {core, tcp_unmatched(core, ip, segment, effects), wakes}

      {:path_mtu, src, _dst, quoted, _mtu} ->
        {path_mtu(core, src, quoted), effects, wakes}

      :path_mtu_rejected ->
        {path_mtu_rejected(core), effects, wakes}
    end
  end

  defp ip_addresses(Ipv4.ipv4(src: src, dst: dst)), do: {src, dst}
  defp ip_addresses(Ipv6.ipv6(src: src, dst: dst)), do: {src, dst}

  defp udp_unmatched(core, ip, ip_payload, effects),
    do: emit(core, Iface.port_unreachable(core.iface, ip, ip_payload), effects)

  defp tcp_unmatched(core, ip, segment, effects),
    do: emit(core, Iface.rst_reply(ip, segment), effects)

  # An ICMP error reporting a path MTU counts as received; it is rejected
  # unless it quotes a TCP segment this interface sent that a connection
  # recognises, and there are no connections yet.
  defp path_mtu(core, _src, _quoted) do
    %{
      core
      | icmp_too_big_received: core.icmp_too_big_received + 1,
        icmp_too_big_rejected: core.icmp_too_big_rejected + 1
    }
  end

  defp path_mtu_rejected(core) do
    %{
      core
      | icmp_too_big_received: core.icmp_too_big_received + 1,
        icmp_too_big_rejected: core.icmp_too_big_rejected + 1
    }
  end

  defp emit(_core, nil, effects), do: effects

  defp emit(core, packet, effects) do
    if Iface.fits?(core.iface, packet), do: [{:emit, packet} | effects], else: effects
  end

  ## The stack

  @doc "A timer call; nothing is timed yet."
  @spec poll(t(), integer()) :: outcome()
  def poll(core, _now) do
    core = observe(core)
    {{:ok, :ok}, %{core | poll_calls: core.poll_calls + 1}, []}
  end

  @doc "Egress credit from the link; the core has no credit to add to yet."
  @spec grant_egress(t(), non_neg_integer(), non_neg_integer(), integer()) :: outcome()
  def grant_egress(core, _packets, _bytes, _now) do
    core = observe(core)

    case running(core) do
      :ok -> {{:error, :egress_credit_disabled}, core, []}
      error -> {error, core, []}
    end
  end

  @doc """
  Shuts the stack down: every socket closes and every waiter is aborted
  with `reason`, in identity order, read before write, as the NIF drains
  them. Afterwards the stack refuses ingress, egress grants and socket
  calls with `:closed`.
  """
  @spec shutdown(t(), :closed | :link_down | :stack_down) :: outcome()
  def shutdown(%__MODULE__{lifecycle: :shutdown} = core, _reason),
    do: {{:ok, :ok}, observe(core), []}

  def shutdown(core, reason) do
    core = observe(core)
    {waiters, sockets} = Sockets.close_all(core.sockets)

    aborts =
      for {identity, _direction, {pid, _operation, reference}} <- waiters,
          do: {:abort, pid, identity, reference, reason}

    core = %{core | lifecycle: :shutdown, sockets: sockets} |> account_notifications(aborts)
    {{:ok, :ok}, core, aborts}
  end

  @doc "A refused socket operation: TCP comes in a later phase of #173."
  @spec not_implemented(t()) :: outcome()
  def not_implemented(core), do: {{:error, :not_implemented}, observe(core), []}

  defp running(%__MODULE__{lifecycle: :running}), do: :ok
  defp running(_core), do: {:error, :closed}

  ## Sockets
  #
  # Identities arrive from the shell as `%{id:, generation:}`.

  @doc "Cancels a waiter: `:ok`, `:already_sent` or `:not_found`."
  @spec socket_cancel(t(), map(), atom(), reference()) :: outcome()
  def socket_cancel(core, identity, operation, reference) do
    call(core, fn core ->
      case Sockets.cancel(core.sockets, identity(identity), operation, reference) do
        {:ok, result, sockets} -> {{:ok, result}, %{core | sockets: sockets}, []}
        error -> {error, core, []}
      end
    end)
  end

  @doc "A live socket's kind, `:udp` or `:tcp`."
  @spec socket_validate(t(), map()) :: outcome()
  def socket_validate(core, identity) do
    call(core, fn core ->
      case Sockets.fetch(core.sockets, identity(identity), :any) do
        {:ok, entry} -> {{:ok, entry.kind}, core, []}
        error -> {error, core, []}
      end
    end)
  end

  @doc "Opens a UDP socket of `family`, replying with its identity."
  @spec udp_open(t(), :inet | :inet6) :: outcome()
  def udp_open(core, family), do: call(core, &Udp.open(&1, family))

  @spec udp_bind(t(), map(), map()) :: outcome()
  def udp_bind(core, identity, endpoint),
    do: call(core, &Udp.bind(&1, identity(identity), endpoint))

  @spec udp_connect(t(), map(), map()) :: outcome()
  def udp_connect(core, identity, endpoint),
    do: call(core, &Udp.connect(&1, identity(identity), endpoint))

  @spec udp_sendto(t(), map(), map(), binary(), pid(), reference(), integer()) :: outcome()
  def udp_sendto(core, identity, endpoint, data, pid, reference, _now) do
    call(
      core,
      &Udp.sendto(&1, identity(identity), endpoint, data, pid, reference),
      fn _result -> byte_size(data) end
    )
  end

  @spec udp_recvfrom(t(), map(), non_neg_integer(), pid(), reference(), integer()) :: outcome()
  def udp_recvfrom(core, identity, length, pid, reference, _now) do
    call(core, &Udp.recvfrom(&1, identity(identity), length, pid, reference), fn
      {:ok, _source, _destination, data, _truncated} -> byte_size(data)
      _select -> 0
    end)
  end

  @spec udp_sockname(t(), map()) :: outcome()
  def udp_sockname(core, identity), do: call(core, &Udp.sockname(&1, identity(identity)))

  @spec udp_peername(t(), map()) :: outcome()
  def udp_peername(core, identity), do: call(core, &Udp.peername(&1, identity(identity)))

  @spec udp_close(t(), map(), integer()) :: outcome()
  def udp_close(core, identity, _now),
    do: call(core, &Udp.close(&1, identity(identity)), fn _result -> 0 end)

  # A socket call: counted, refused with `:closed` unless the stack runs,
  # and, for a call the NIF drives its interface for (`input` given), its
  # packets and the bytes it copied in counted as the NIF counts them.
  defp call(core, fun, input \\ nil) do
    core = observe(core)

    with :ok <- running(core),
         {{:ok, result}, core, effects} <- fun.(core) do
      notices = for effect <- effects, elem(effect, 0) in [:wake, :abort], do: effect
      core = account_notifications(core, notices)
      core = if input, do: account(core, input.(result), effects), else: core
      {{:ok, result}, core, effects}
    else
      {:error, _reason} = error -> {error, core, []}
      {{:error, _reason}, _core, _effects} = refused -> refused
    end
  end

  defp identity(%{id: id, generation: generation}), do: {id, generation}

  defp account(core, input_bytes, effects) do
    {packets, bytes} =
      for {:emit, packet} <- effects, reduce: {0, 0} do
        {packets, bytes} -> {packets + 1, bytes + byte_size(packet)}
      end

    %{
      core
      | emitted_packets: core.emitted_packets + packets,
        max_bytes_copied: max(core.max_bytes_copied, max(input_bytes, bytes)),
        max_output_packets: max(core.max_output_packets, packets)
    }
  end

  defp account_notifications(core, notices),
    do: %{core | notifications_delivered: core.notifications_delivered + length(notices)}

  # Every backend call but the snapshot counts, as the NIF counts its own.
  defp observe(core), do: %{core | native_calls: core.native_calls + 1}

  ## Snapshot

  @doc """
  The stack's description in the shape of the NIF's snapshot, which
  `stack_info/1` reports as `native.result` (decision 4 of #173): the
  socket and waiter counts, UDP's fixed sizes, limits, lifecycle and
  counters. The NIF's own machinery and budgets are zero.
  """
  @spec snapshot(t()) :: {{:ok, map()}, t(), []}
  def snapshot(core) do
    counts = Sockets.counts(core.sockets)
    mtu = core.iface.mtu

    {{:ok,
      %{
        id: core.id,
        limits: core.limits,
        socket_count: counts.udp + counts.tcp,
        native_socket_count: 0,
        native_socket_capacity: core.limits.sockets,
        socket_buffer_bytes: Udp.buffer_bytes(Sockets.states(core.sockets, :udp)),
        socket_buffer_capacity: 64 * 2 * @tcp_max_buffer_bytes,
        tcp_socket_count: counts.tcp,
        tcp_listener_count: 0,
        udp_socket_count: counts.udp,
        listener_pool_socket_count: 0,
        listener_pool_target_count: 0,
        accepted_queue_count: 0,
        listener_backlog_capacity: 0,
        closing_tcp_socket_count: 0,
        tcp_buffer_bytes: %{
          default_rcvbuf: @tcp_default_buffer_bytes,
          default_sndbuf: @tcp_default_buffer_bytes,
          sockets: []
        },
        udp_packet_capacity: Udp.ring_packets(),
        udp_payload_bytes: Udp.ring_bytes(),
        udp_max_datagram_bytes: Udp.max_datagram_bytes(mtu, :inet6),
        udp_ipv4_max_datagram_bytes: Udp.max_datagram_bytes(mtu, :inet),
        waiter_count: counts.read + counts.write,
        read_waiter_count: counts.read,
        write_waiter_count: counts.write,
        sent_waiter_count: counts.read_sent + counts.write_sent,
        read_sent_waiter_count: counts.read_sent,
        write_sent_waiter_count: counts.write_sent,
        ready_count: 0,
        ready_overflow_pending: false,
        readiness: %{queued: 0, coalesced: 0, overflows: 0},
        receive_packets: 0,
        transmit_packets: 0,
        egress_credit: nil,
        mtu: mtu,
        ip_address_count: Iface.address_count(core.iface),
        lifecycle: core.lifecycle,
        call_target_nanoseconds: 0,
        work_budget_nanoseconds: 0,
        encoding_headroom_nanoseconds: 0,
        counters: counters(core)
      }}, core, []}
  end

  defp counters(core) do
    %{
      max_bytes_copied: core.max_bytes_copied,
      max_input_packets: core.max_input_packets,
      max_output_packets: core.max_output_packets,
      max_ready_events: 0,
      max_maintenance_work: 0,
      ingress_packets: core.ingress_packets,
      rejected_packets: core.rejected_packets,
      emitted_packets: core.emitted_packets,
      poll_calls: core.poll_calls,
      notifications_delivered: core.notifications_delivered,
      notifications_dropped: 0,
      readiness_dropped: 0,
      listener_promotions: 0,
      listener_refills: 0,
      listener_overflow_drops: 0,
      native_calls: core.native_calls,
      deadline_yields: 0,
      timeslice_exhaustions: 0,
      max_native_work_nanoseconds: 0,
      icmp_too_big_received: core.icmp_too_big_received,
      icmp_too_big_rejected: core.icmp_too_big_rejected,
      path_mtu_reductions: core.path_mtu_reductions
    }
  end
end
