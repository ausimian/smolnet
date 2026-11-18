defmodule SmolNet.Core.Iface do
  @moduledoc false

  # The IP layer of a stack, as smoltcp's `iface::interface` behaves on an
  # IP medium with SmolNet's features: raw IPv4 and IPv6 packets, no link
  # layer, no fragmentation, no forwarding, no multicast groups joined.
  #
  # `receive/2` takes one ingress packet and says what it is:
  #
  #   * `:drop`, for a packet that is malformed, not for this interface, or
  #     needs no answer;
  #   * `{:reply, packet}`, for one the interface answers by itself: an echo
  #     reply, a Protocol Unreachable (IPv4) or Parameter Problem (IPv6),
  #     the latter also for a Hop-by-Hop option it must not skip;
  #   * `{:udp, ip, {src_port, dst_port}, payload, ip_payload}` and
  #     `{:tcp, ip, segment}`, for a datagram or segment for the sockets,
  #     `ip` being the packet's header record and `ip_payload` the bytes
  #     after it (and any Hop-by-Hop header);
  #   * `{:path_mtu, src, dst, quoted, mtu}`, for an ICMP error reporting the
  #     path MTU of a TCP segment from `src` to `dst`, `quoted` being what
  #     follows the quoted IP header; and `:path_mtu_rejected` for one that
  #     is malformed.
  #
  # The caller dispatches transport packets to its sockets and, when none
  # takes one, answers with `port_unreachable/3` or `rst_reply/2`, as
  # smoltcp does. Replies go out only if they fit the MTU (`fits?/2`).
  #
  # Addresses are 4- or 16-byte binaries, kept in configuration order:
  # smoltcp picks the first IPv4 address, or the first IPv6 one, in
  # several places.

  import Bitwise
  require SmolNet.Core.Wire.Ipv4
  require SmolNet.Core.Wire.Ipv6
  require SmolNet.Core.Wire.Tcp

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Hbh
  alias SmolNet.Core.Wire.Icmpv4
  alias SmolNet.Core.Wire.Icmpv6
  alias SmolNet.Core.Wire.Ipv4
  alias SmolNet.Core.Wire.Ipv6
  alias SmolNet.Core.Wire.Tcp
  alias SmolNet.Core.Wire.Udp

  @enforce_keys [:mtu]
  defstruct mtu: nil, ipv4: [], ipv6: [], routes: [], all_addresses: []

  @type address :: <<_::32>> | <<_::128>>
  @type t :: %__MODULE__{
          mtu: pos_integer(),
          ipv4: [{<<_::32>>, 0..32}],
          ipv6: [{<<_::128>>, 0..128}],
          routes: [{address(), non_neg_integer(), address()}],
          all_addresses: [address()]
        }
  @type ip :: Ipv4.repr() | Ipv6.repr()
  @type received ::
          :drop
          | {:reply, binary()}
          | {:udp, ip(), {0..0xFFFF, 1..0xFFFF}, binary(), binary()}
          | {:tcp, ip(), Tcp.repr()}
          | {:path_mtu, address(), address(), binary(), non_neg_integer()}
          | :path_mtu_rejected

  @hop_limit 64
  @icmp 1
  @tcp 6
  @udp 17
  @hop_by_hop 0
  @icmpv6 58

  @doc """
  An interface with an MTU, addresses as `{address, prefix_length}` and
  static routes as `{destination, prefix_length, gateway}`, addresses
  4-byte or 16-byte binaries.
  """
  @spec new(pos_integer(), [{address(), non_neg_integer()}], [
          {address(), non_neg_integer(), address()}
        ]) :: t()
  def new(mtu, addresses, routes) do
    iface = %__MODULE__{
      mtu: mtu,
      ipv4: for({<<_::32>>, _prefix} = address <- addresses, do: address),
      ipv6: for({<<_::128>>, _prefix} = address <- addresses, do: address),
      routes: routes
    }

    %{iface | all_addresses: addresses(iface, :inet) ++ addresses(iface, :inet6)}
  end

  @doc "How many addresses the interface has."
  @spec address_count(t()) :: non_neg_integer()
  def address_count(%__MODULE__{ipv4: ipv4, ipv6: ipv6}), do: length(ipv4) + length(ipv6)

  @doc "Whether a packet fits the MTU, as smoltcp's `dispatch_ip` requires."
  @spec fits?(t(), binary()) :: boolean()
  def fits?(%__MODULE__{mtu: mtu}, packet), do: byte_size(packet) <= mtu

  ## Ingress

  @spec receive(t(), binary()) :: received()
  def receive(iface, <<4::4, _rest::bitstring>> = packet), do: receive_ipv4(iface, packet)
  def receive(iface, <<6::4, _rest::bitstring>> = packet), do: receive_ipv6(iface, packet)
  def receive(_iface, _packet), do: :drop

  defp receive_ipv4(iface, packet) do
    with {:ok, ip, payload} <- Ipv4.parse(packet),
         Ipv4.ipv4(src: src, dst: dst) = ip,
         true <- ipv4_unicast?(iface, src) or Ipv4.unspecified?(src),
         true <-
           has_ip_addr?(iface, dst) or dst == Ipv4.multicast_all_systems() or
             ipv4_broadcast?(iface, dst) do
      ipv4_protocol(iface, ip, payload)
    else
      _drop -> :drop
    end
  end

  defp ipv4_protocol(iface, Ipv4.ipv4(protocol: @icmp) = ip, payload),
    do: receive_icmpv4(iface, ip, payload)

  defp ipv4_protocol(_iface, Ipv4.ipv4(protocol: @udp) = ip, payload),
    do: receive_udp(ip, payload)

  defp ipv4_protocol(_iface, Ipv4.ipv4(protocol: @tcp) = ip, payload),
    do: receive_tcp(ip, payload)

  defp ipv4_protocol(iface, ip, payload) do
    data = quoted(payload, Ipv4.min_mtu(), Ipv4.header_len())
    reply(icmpv4_reply(iface, ip, {:dst_unreachable, Icmpv4.protocol_unreachable(), ip, data}))
  end

  # smoltcp reads a Fragmentation Needed error for the path MTU before it
  # parses the message, because the message parser refuses the truncated
  # packet such an error quotes.
  defp receive_icmpv4(_iface, _ip, <<3, 4, _rest::binary>> = message)
       when byte_size(message) >= 8,
       do: frag_required(message)

  defp receive_icmpv4(iface, ip, message) do
    case Icmpv4.parse(message) do
      {:ok, {:echo_request, ident, seq_no, data}} ->
        reply(icmpv4_reply(iface, ip, {:echo_reply, ident, seq_no, data}))

      _other ->
        :drop
    end
  end

  # RFC 1191: a next-hop MTU of zero, from a router that predates it, is
  # taken as the largest plateau below the refused packet's length.
  defp frag_required(
         <<_type, _code, _checksum::16, _unused::16, next_hop_mtu::16, quoted::binary>> = message
       ) do
    case quoted do
      <<4::4, ihl::4, _tos, total_len::16, _ident::16, _flags::3, offset::13, _ttl, @tcp,
        _sum::16, src::binary-4, dst::binary-4, _rest::binary>> ->
        header_len = ihl * 4
        mtu = if next_hop_mtu == 0, do: plateau(total_len), else: next_hop_mtu

        # The router refused the packet as larger than the MTU it gives, so
        # an MTU at least the packet's length is not about this packet.
        if Checksum.valid?(message) and header_len >= 20 and byte_size(quoted) >= header_len and
             offset == 0 and mtu < total_len do
          tcp = binary_part(quoted, header_len, byte_size(quoted) - header_len)
          {:path_mtu, src, dst, tcp, mtu}
        else
          :path_mtu_rejected
        end

      _not_tcp ->
        :drop
    end
  end

  @plateaus [65_535, 32_000, 17_914, 8_166, 4_352, 2_002, 1_492, 1_006, 508, 296, 68]

  defp plateau(total_len), do: Enum.find(@plateaus, 68, &(&1 < total_len))

  defp receive_ipv6(iface, packet) do
    with {:ok, ip, payload} <- Ipv6.parse(packet),
         Ipv6.ipv6(src: src) = ip,
         true <- Ipv6.unicast?(src) do
      case Ipv6.ipv6(ip, :next_header) do
        @hop_by_hop -> hop_by_hop(iface, ip, payload)
        next_header -> ipv6_for_us(iface, ip, next_header, payload)
      end
    else
      _drop -> :drop
    end
  end

  # smoltcp reads the Hop-by-Hop header before it checks the destination,
  # so a packet for another address can still draw a Parameter Problem.
  defp hop_by_hop(iface, ip, payload) do
    with {:ok, {next_header, _length, data}, rest} <- Hbh.parse_ext_header(payload),
         {:ok, options} <- Hbh.parse(data) do
      case option_verdict(options, Ipv6.ipv6(ip, :dst)) do
        :continue ->
          ipv6_for_us(iface, ip, next_header, rest)

        :param_problem ->
          data = quoted(payload, Ipv6.min_mtu(), Ipv6.header_len())

          message =
            {:param_problem, Icmpv6.unrecognized_option(), Ipv6.header_len(), ip, data}

          reply(icmpv6_reply(iface, ip, message))

        :discard ->
          :drop
      end
    else
      :error -> :drop
    end
  end

  defp option_verdict([], _dst), do: :continue

  defp option_verdict([{:unknown, type, _length, _data} | options], dst) do
    case Hbh.failure_type(type) do
      :skip -> option_verdict(options, dst)
      :discard -> :discard
      :discard_send_all -> :param_problem
      :discard_send_unicast -> if Ipv6.multicast?(dst), do: :discard, else: :param_problem
    end
  end

  defp option_verdict([_known | options], dst), do: option_verdict(options, dst)

  defp ipv6_for_us(iface, Ipv6.ipv6(dst: dst) = ip, next_header, payload) do
    if has_ip_addr?(iface, dst) or ipv6_multicast_group?(iface, dst) or Ipv6.loopback?(dst) do
      ipv6_protocol(iface, ip, next_header, payload)
    else
      :drop
    end
  end

  defp ipv6_protocol(iface, ip, @icmpv6, payload), do: receive_icmpv6(iface, ip, payload)
  defp ipv6_protocol(_iface, ip, @udp, payload), do: receive_udp(ip, payload)
  defp ipv6_protocol(_iface, ip, @tcp, payload), do: receive_tcp(ip, payload)

  defp ipv6_protocol(iface, ip, _next_header, payload) do
    data = quoted(payload, Ipv6.min_mtu(), Ipv6.header_len())
    message = {:param_problem, Icmpv6.unrecognized_next_header(), Ipv6.header_len(), ip, data}
    reply(icmpv6_reply(iface, ip, message))
  end

  defp receive_icmpv6(iface, Ipv6.ipv6(src: src, dst: dst) = ip, message) do
    case Icmpv6.parse(src, dst, message) do
      {:ok, {:echo_request, ident, seq_no, data}} ->
        reply(icmpv6_reply(iface, ip, {:echo_reply, ident, seq_no, data}))

      {:ok, {:pkt_too_big, mtu, header, quoted}} ->
        packet_too_big(mtu, header, quoted)

      _other ->
        :drop
    end
  end

  # RFC 8201. smoltcp sends TCP with no extension headers, so only an
  # error quoting TCP as the next header can be about one of its segments.
  defp packet_too_big(mtu, Ipv6.ipv6(next_header: @tcp) = header, quoted) do
    Ipv6.ipv6(src: src, dst: dst, payload_len: payload_len) = header

    if mtu >= Ipv6.header_len() + payload_len,
      do: :path_mtu_rejected,
      else: {:path_mtu, src, dst, quoted, mtu}
  end

  defp packet_too_big(_mtu, _header, _quoted), do: :drop

  defp receive_udp(ip, payload) do
    {src, dst} = addresses(ip)

    case Udp.parse(payload, src, dst) do
      {:ok, ports, data} -> {:udp, ip, ports, data, payload}
      :error -> :drop
    end
  end

  # RFC 1122 3.2.1.3: TCP never uses the unspecified address.
  defp receive_tcp(ip, payload) do
    {src, dst} = addresses(ip)

    with false <- unspecified?(src) or unspecified?(dst),
         {:ok, segment} <- Tcp.parse(payload, src, dst) do
      {:tcp, ip, segment}
    else
      _drop -> :drop
    end
  end

  defp reply(nil), do: :drop
  defp reply(packet), do: {:reply, packet}

  # As much of `payload` as fits in an error within the minimum MTU, after
  # two IP headers and the ICMP header (RFC 1812 4.3.2.3).
  defp quoted(payload, min_mtu, header_len) do
    room = min_mtu - header_len * 2 - 8
    if byte_size(payload) > room, do: binary_part(payload, 0, room), else: payload
  end

  ## Replies

  @doc """
  The ICMPv4 reply to a packet with header `ip`: from its destination if
  that is unicast, or, for an echo reply to a broadcast, from the first
  IPv4 address. Nil for a non-unicast source and for any other message to
  a destination that is not unicast.
  """
  @spec icmpv4_reply(t(), Ipv4.repr(), Icmpv4.message()) :: binary() | nil
  def icmpv4_reply(iface, Ipv4.ipv4(src: src, dst: dst), message) do
    cond do
      not ipv4_unicast?(iface, src) ->
        nil

      ipv4_unicast?(iface, dst) ->
        icmpv4_packet(dst, src, message)

      ipv4_broadcast?(iface, dst) and elem(message, 0) == :echo_reply ->
        case iface.ipv4 do
          [{first, _prefix} | _rest] -> icmpv4_packet(first, src, message)
          [] -> nil
        end

      true ->
        nil
    end
  end

  defp icmpv4_packet(src, dst, message) do
    icmp = Icmpv4.emit(message)

    Ipv4.emit(
      Ipv4.ipv4(
        src: src,
        dst: dst,
        protocol: @icmp,
        payload_len: byte_size(icmp),
        hop_limit: @hop_limit
      ),
      icmp
    )
  end

  @doc """
  The ICMPv6 reply to a packet with header `ip`: from its destination if
  that is unicast, otherwise from the source address chosen for its
  source.
  """
  @spec icmpv6_reply(t(), Ipv6.repr(), Icmpv6.message()) :: binary()
  def icmpv6_reply(iface, Ipv6.ipv6(src: src, dst: dst), message) do
    reply_src = if Ipv6.unicast?(dst), do: dst, else: source_address_ipv6(iface, src)
    icmp = Icmpv6.emit(reply_src, src, message)

    Ipv6.emit(
      Ipv6.ipv6(
        src: reply_src,
        dst: src,
        next_header: @icmpv6,
        payload_len: byte_size(icmp),
        hop_limit: @hop_limit
      ),
      icmp
    )
  end

  @doc """
  The Port Unreachable for a datagram no socket took, quoting as much of
  its IP payload as fits the minimum MTU, or nil when smoltcp sends none.
  """
  @spec port_unreachable(t(), ip(), binary()) :: binary() | nil
  def port_unreachable(iface, Ipv4.ipv4() = ip, ip_payload) do
    data = quoted(ip_payload, Ipv4.min_mtu(), Ipv4.header_len())
    icmpv4_reply(iface, ip, {:dst_unreachable, Icmpv4.port_unreachable(), ip, data})
  end

  def port_unreachable(iface, Ipv6.ipv6() = ip, ip_payload) do
    data = quoted(ip_payload, Ipv6.min_mtu(), Ipv6.header_len())
    icmpv6_reply(iface, ip, {:dst_unreachable, Icmpv6.port_unreachable(), ip, data})
  end

  @doc """
  The RST for a segment no socket took (smoltcp's `rst_reply`), or nil for
  a RST. Its sequence number is the segment's acknowledgement, or zero;
  only a SYN without an ACK is acknowledged.
  """
  @spec rst_reply(ip(), Tcp.repr()) :: binary() | nil
  def rst_reply(_ip, Tcp.tcp(control: :rst)), do: nil

  def rst_reply(ip, Tcp.tcp(src_port: src_port, dst_port: dst_port, ack: ack) = segment) do
    {src, dst} = addresses(ip)

    acknowledgement =
      case segment do
        Tcp.tcp(control: :syn, ack: nil, seq: seq) ->
          band(seq + Tcp.segment_len(segment), 0xFFFF_FFFF)

        _other ->
          nil
      end

    reply =
      Tcp.tcp(
        src_port: dst_port,
        dst_port: src_port,
        control: :rst,
        seq: ack || 0,
        ack: acknowledgement
      )

    tcp_packet(dst, src, reply)
  end

  @doc "A TCP segment in an IP packet with the hop limit smoltcp sends with."
  @spec tcp_packet(address(), address(), Tcp.repr()) :: binary()
  def tcp_packet(src, dst, segment) do
    {header, payload} = Tcp.emit_header(segment, src, dst)
    length = byte_size(header) + byte_size(payload)
    <<ip_header(src, dst, @tcp, length)::binary, header::binary, payload::binary>>
  end

  @doc "A UDP datagram in an IP packet with the hop limit smoltcp sends with."
  @spec udp_packet(address(), address(), 0..0xFFFF, 0..0xFFFF, binary()) :: binary()
  def udp_packet(src, dst, src_port, dst_port, payload) do
    bytes = Udp.emit(src_port, dst_port, payload, src, dst)
    ip_packet(src, dst, @udp, bytes)
  end

  defp ip_packet(src, dst, protocol, payload),
    do: <<ip_header(src, dst, protocol, byte_size(payload))::binary, payload::binary>>

  defp ip_header(<<_::32>> = src, dst, protocol, length) do
    Ipv4.header(
      Ipv4.ipv4(
        src: src,
        dst: dst,
        protocol: protocol,
        payload_len: length,
        hop_limit: @hop_limit
      )
    )
  end

  defp ip_header(<<_::128>> = src, dst, protocol, length) do
    Ipv6.header(
      Ipv6.ipv6(
        src: src,
        dst: dst,
        next_header: protocol,
        payload_len: length,
        hop_limit: @hop_limit
      )
    )
  end

  ## Addresses, routes and source selection

  @doc "Whether the interface has `address`."
  @spec has_ip_addr?(t(), address()) :: boolean()
  def has_ip_addr?(%__MODULE__{ipv4: ipv4}, <<_::32>> = address),
    do: List.keymember?(ipv4, address, 0)

  def has_ip_addr?(%__MODULE__{ipv6: ipv6}, <<_::128>> = address),
    do: List.keymember?(ipv6, address, 0)

  @doc """
  The limited broadcast address, or the broadcast address of one of the
  interface's IPv4 subnets.
  """
  @spec ipv4_broadcast?(t(), <<_::32>>) :: boolean()
  def ipv4_broadcast?(%__MODULE__{ipv4: ipv4}, address) do
    Ipv4.broadcast?(address) or
      Enum.any?(ipv4, fn {own, prefix_len} -> Ipv4.broadcast(own, prefix_len) == address end)
  end

  defp ipv4_unicast?(iface, address),
    do: Ipv4.unicast?(address) and not ipv4_broadcast?(iface, address)

  # smoltcp counts as joined all-nodes and, for each address but ::1, its
  # "solicited-node" group, which it matches on the last two bytes alone,
  # so a unicast address sharing them with one of the interface's is taken
  # as its own too.
  defp ipv6_multicast_group?(%__MODULE__{ipv6: ipv6}, address) do
    address == Ipv6.link_local_all_nodes() or
      Enum.any?(ipv6, fn {own, _prefix_len} ->
        own != Ipv6.loopback() and binary_part(own, 14, 2) == binary_part(address, 14, 2)
      end)
  end

  @doc """
  The next hop for `address`: itself on one of the interface's subnets or
  for the limited broadcast address, else the gateway of the longest
  matching static route (the last of equals), else nil.
  """
  @spec route(t(), address()) :: address() | nil
  def route(iface, address) do
    if in_same_network?(iface, address) or broadcast_address?(address),
      do: address,
      else: lookup_route(iface.routes, address)
  end

  @doc """
  Whether a socket may send to `address`, as the NIF checks before it
  hands smoltcp a datagram or a connection: the address is on one of the
  interface's subnets or under a static route's destination prefix.
  """
  @spec reachable?(t(), address()) :: boolean()
  def reachable?(iface, address) do
    in_same_network?(iface, address) or
      Enum.any?(iface.routes, fn {destination, prefix_len, _gateway} ->
        same_family?(destination, address) and contains?(destination, prefix_len, address)
      end)
  end

  @doc "The interface's addresses of one family, in configuration order."
  @spec addresses(t(), :inet | :inet6) :: [address()]
  def addresses(%__MODULE__{ipv4: ipv4}, :inet), do: for({address, _prefix} <- ipv4, do: address)
  def addresses(%__MODULE__{ipv6: ipv6}, :inet6), do: for({address, _prefix} <- ipv6, do: address)

  @doc "Every address, IPv4 then IPv6, as `addresses/2` lists them."
  @spec all_addresses(t()) :: [address()]
  def all_addresses(%__MODULE__{all_addresses: addresses}), do: addresses

  @doc """
  Whether smoltcp would send to `address`, its `has_neighbor`: on an IP
  medium, whether it has a route.
  """
  @spec has_neighbor?(t(), address()) :: boolean()
  def has_neighbor?(iface, address), do: route(iface, address) != nil

  defp in_same_network?(%__MODULE__{ipv4: ipv4}, <<_::32>> = address),
    do: Enum.any?(ipv4, fn {own, prefix_len} -> Ipv4.contains?(own, prefix_len, address) end)

  defp in_same_network?(%__MODULE__{ipv6: ipv6}, <<_::128>> = address),
    do: Enum.any?(ipv6, fn {own, prefix_len} -> Ipv6.contains?(own, prefix_len, address) end)

  defp broadcast_address?(<<_::32>> = address), do: Ipv4.broadcast?(address)
  defp broadcast_address?(_address), do: false

  defp lookup_route(routes, address) do
    Enum.reduce(routes, nil, fn {destination, prefix_len, gateway}, best ->
      if same_family?(destination, address) and contains?(destination, prefix_len, address) and
           (best == nil or prefix_len >= elem(best, 0)),
         do: {prefix_len, gateway},
         else: best
    end)
    |> case do
      nil -> nil
      {_prefix_len, gateway} -> gateway
    end
  end

  defp same_family?(a, b), do: byte_size(a) == byte_size(b)

  defp contains?(<<_::32>> = network, prefix_len, address),
    do: Ipv4.contains?(network, prefix_len, address)

  defp contains?(network, prefix_len, address), do: Ipv6.contains?(network, prefix_len, address)

  @doc """
  The source address smoltcp picks for `dst`: for IPv4, the first address
  on `dst`'s subnet, else the first IPv4 address, else nil; for IPv6, by
  `source_address_ipv6/2`.
  """
  @spec source_address(t(), address()) :: address() | nil
  def source_address(%__MODULE__{ipv4: ipv4}, <<_::32>> = dst) do
    case Enum.find(ipv4, fn {own, prefix_len} -> Ipv4.contains?(own, prefix_len, dst) end) do
      {own, _prefix_len} ->
        own

      nil ->
        case ipv4 do
          [{first, _prefix_len} | _rest] -> first
          [] -> nil
        end
    end
  end

  def source_address(iface, <<_::128>> = dst), do: source_address_ipv6(iface, dst)

  @doc """
  The IPv6 source address for `dst` (not unspecified), as smoltcp's
  `get_source_address_ipv6` chooses it from RFC 6724's candidates: ::1
  for a loopback destination or an interface without IPv6 addresses;
  otherwise, starting from the first IPv6 address, each candidate replaces
  the choice if it equals `dst` (rule 1), has a better scope (rule 2) or
  a longer common prefix with `dst` (rule 8). Rules 3 to 7 are not
  implemented, as in smoltcp.
  """
  @spec source_address_ipv6(t(), <<_::128>>) :: <<_::128>>
  def source_address_ipv6(%__MODULE__{ipv6: ipv6}, dst) do
    if Ipv6.loopback?(dst) or ipv6 == [] do
      Ipv6.loopback()
    else
      [first | _rest] = ipv6

      ipv6
      |> Enum.filter(fn {address, _prefix_len} -> candidate?(dst, address) end)
      |> Enum.reduce(first, &prefer(&1, &2, dst))
      |> elem(0)
    end
  end

  # RFC 6724 4: a candidate for a link-local destination must be
  # link-local, and none is multicast or unspecified.
  defp candidate?(dst, address) do
    not (Ipv6.link_local?(dst) and not Ipv6.link_local?(address)) and
      not Ipv6.unspecified?(address) and not Ipv6.multicast?(address)
  end

  defp prefer({address, _prefix_len} = candidate, {chosen, _chosen_prefix} = choice, dst) do
    choice =
      if chosen != dst and address == dst, do: candidate, else: choice

    {chosen, _} = choice
    chosen_scope = Ipv6.scope(chosen)
    candidate_scope = Ipv6.scope(address)
    dst_scope = Ipv6.scope(dst)

    choice =
      cond do
        chosen_scope < candidate_scope -> if chosen_scope < dst_scope, do: candidate, else: choice
        candidate_scope > dst_scope -> candidate
        true -> choice
      end

    if common_prefix_len(choice, dst) < common_prefix_len(candidate, dst),
      do: candidate,
      else: choice
  end

  # RFC 6724 2.2, capped at the address's prefix length as smoltcp caps it.
  defp common_prefix_len({<<address::128>>, prefix_len}, <<dst::128>>) do
    bits =
      case bxor(address, dst) do
        0 -> 128
        difference -> 128 - bit_length(difference)
      end

    min(bits, prefix_len)
  end

  defp bit_length(0), do: 0
  defp bit_length(value), do: 1 + bit_length(value >>> 1)

  defp addresses(Ipv4.ipv4(src: src, dst: dst)), do: {src, dst}
  defp addresses(Ipv6.ipv6(src: src, dst: dst)), do: {src, dst}

  defp unspecified?(<<_::32>> = address), do: Ipv4.unspecified?(address)
  defp unspecified?(address), do: Ipv6.unspecified?(address)
end
