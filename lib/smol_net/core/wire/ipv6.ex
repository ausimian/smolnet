defmodule SmolNet.Core.Wire.Ipv6 do
  @moduledoc false

  # IPv6 headers and addresses, as smoltcp's `wire::ipv6` parses and emits
  # them:
  #
  #   * `parse/1` takes a packet and returns its header as an `ipv6` record
  #     and its payload, the `payload_len` bytes after the header. It
  #     refuses a buffer shorter than the header or than the payload length,
  #     and another version. Extension headers stay in the payload.
  #   * `emit/2` and `header/1` write a header with traffic class and flow
  #     label 0, as smoltcp does for everything it sends.
  #   * `fields/1` and `build/1` read and write every header field as it is.
  #
  # Addresses are 16-byte binaries. The predicates follow `core::net` and
  # smoltcp's `AddressExt`.

  import Bitwise
  require Record

  Record.defrecord(:ipv6, src: nil, dst: nil, next_header: 0, payload_len: 0, hop_limit: 64)

  @type address :: <<_::128>>
  @type repr ::
          record(:ipv6,
            src: address(),
            dst: address(),
            next_header: 0..255,
            payload_len: non_neg_integer(),
            hop_limit: 0..255
          )

  @header_len 40

  @doc "The length of an IPv6 header."
  def header_len, do: @header_len

  @doc "The minimum MTU of an IPv6 link (RFC 8200), which bounds ICMPv6 errors."
  def min_mtu, do: 1_280

  @doc "ff02::1, all nodes on the link."
  def link_local_all_nodes, do: <<0xFF02::16, 0::96, 1::16>>

  @doc "::1."
  def loopback, do: <<0::120, 1>>

  @spec parse(binary()) :: {:ok, repr(), binary()} | :error
  def parse(
        <<6::4, _traffic_class::8, _flow::20, payload_len::16, next_header, hop_limit,
          src::binary-16, dst::binary-16, rest::binary>>
      )
      when byte_size(rest) >= payload_len do
    {:ok,
     ipv6(
       src: src,
       dst: dst,
       next_header: next_header,
       payload_len: payload_len,
       hop_limit: hop_limit
     ), binary_part(rest, 0, payload_len)}
  end

  def parse(_packet), do: :error

  @doc "A packet of `header/1` and `payload`."
  @spec emit(repr(), binary()) :: binary()
  def emit(repr, payload), do: <<header(repr)::binary, payload::binary>>

  @doc "The 40-byte header for `repr`, its payload length as the record's."
  @spec header(repr()) :: binary()
  def header(
        ipv6(src: src, dst: dst, next_header: next_header, payload_len: len, hop_limit: hop_limit)
      ) do
    <<6::4, 0::28, band(len, 0xFFFF)::16, next_header, hop_limit, src::binary-16, dst::binary-16>>
  end

  @doc """
  Every field of the header at the start of `packet`, which must be at
  least 40 bytes. `:payload` is the payload length's bytes after the
  header, or nil if the buffer is too short.
  """
  @spec fields(binary()) :: {:ok, map()} | :error
  def fields(
        <<version::4, traffic_class::8, flow_label::20, payload_len::16, next_header, hop_limit,
          src::binary-16, dst::binary-16, rest::binary>>
      ) do
    {:ok,
     %{
       version: version,
       traffic_class: traffic_class,
       flow_label: flow_label,
       payload_len: payload_len,
       next_header: next_header,
       hop_limit: hop_limit,
       src: src,
       dst: dst,
       payload: if(byte_size(rest) >= payload_len, do: binary_part(rest, 0, payload_len))
     }}
  end

  def fields(_packet), do: :error

  @doc """
  A packet from header fields, the inverse of `fields/1`. Fields left out
  are zero (`:version` 6, `:payload_len` the payload's).
  """
  @spec build(map()) :: binary()
  def build(fields) do
    payload = Map.get(fields, :payload) || <<>>

    <<Map.get(fields, :version, 6)::4, Map.get(fields, :traffic_class, 0)::8,
      Map.get(fields, :flow_label, 0)::20, Map.get(fields, :payload_len, byte_size(payload))::16,
      Map.get(fields, :next_header, 0), Map.get(fields, :hop_limit, 0),
      Map.get(fields, :src, <<0::128>>)::binary-16, Map.get(fields, :dst, <<0::128>>)::binary-16,
      payload::binary>>
  end

  ## Addresses

  @spec unspecified?(address()) :: boolean()
  def unspecified?(address), do: address == <<0::128>>

  @spec loopback?(address()) :: boolean()
  def loopback?(address), do: address == <<0::120, 1>>

  @spec multicast?(address()) :: boolean()
  def multicast?(<<first, _rest::binary-15>>), do: first == 0xFF

  @doc "Neither multicast nor unspecified (smoltcp's `x_is_unicast`)."
  @spec unicast?(address()) :: boolean()
  def unicast?(address), do: not (multicast?(address) or unspecified?(address))

  @doc "In fe80::/64, as smoltcp's `is_link_local` has it."
  @spec link_local?(address()) :: boolean()
  def link_local?(<<0xFE80::16, 0::48, _interface::64>>), do: true
  def link_local?(_address), do: false

  @doc "In fc00::/7."
  @spec unique_local?(address()) :: boolean()
  def unique_local?(<<first, _rest::binary-15>>), do: band(first, 0xFE) == 0xFC

  @doc "In 2000::/3."
  @spec global_unicast?(address()) :: boolean()
  def global_unicast?(<<first, _rest::binary-15>>), do: first >>> 5 == 0b001

  @doc """
  The scope of an address, as smoltcp's `x_multicast_scope` gives it: a
  multicast address's own scope field, link-local (2) for fe80::/64,
  global (0xE) for unique-local and global unicast, and otherwise unknown
  (0xFF). A multicast scope smoltcp does not name is unknown too.
  """
  @spec scope(address()) :: 1 | 2 | 4 | 5 | 8 | 0xE | 0xFF
  def scope(<<0xFF, _flags::4, scope::4, _rest::binary-14>>) when scope in [1, 2, 4, 5, 8, 0xE],
    do: scope

  def scope(<<0xFF, _rest::binary-15>>), do: 0xFF

  def scope(address) do
    cond do
      link_local?(address) -> 2
      unique_local?(address) or global_unicast?(address) -> 0xE
      true -> 0xFF
    end
  end

  @doc "The solicited-node multicast address of a unicast address."
  @spec solicited_node(address()) :: address()
  def solicited_node(<<_prefix::binary-13, low::binary-3>>),
    do: <<0xFF02::16, 0::64, 1::16, 0xFF, low::binary>>

  @spec solicited_node_multicast?(address()) :: boolean()
  def solicited_node_multicast?(<<0xFF02::16, 0::64, 1::16, 0xFF, _low::binary-3>>), do: true
  def solicited_node_multicast?(_address), do: false

  @doc "`address` with every bit past `prefix_len` cleared."
  @spec mask(address(), 0..128) :: address()
  def mask(<<address::128>>, prefix_len), do: <<band(address, netmask_bits(prefix_len))::128>>

  @doc "The prefix length of a netmask, or nil if it is not one."
  @spec mask_prefix_len(address()) :: 0..128 | nil
  def mask_prefix_len(<<mask::128>>) do
    Enum.find(0..128, fn prefix_len -> netmask_bits(prefix_len) == mask end)
  end

  @spec contains?(address(), 0..128, address()) :: boolean()
  def contains?(_network, 0, _address), do: true

  def contains?(<<network::128>>, prefix_len, <<address::128>>) do
    mask = netmask_bits(prefix_len)
    band(network, mask) == band(address, mask)
  end

  defp netmask_bits(0), do: 0

  defp netmask_bits(prefix_len),
    do: band(((1 <<< 128) - 1) <<< (128 - prefix_len), (1 <<< 128) - 1)
end
