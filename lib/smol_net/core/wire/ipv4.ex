defmodule SmolNet.Core.Wire.Ipv4 do
  @moduledoc false

  # IPv4 headers and addresses, as smoltcp's `wire::ipv4` parses and emits
  # them with `proto-ipv4-fragmentation` compiled out:
  #
  #   * `parse/1` takes a packet and returns its header as an `ipv4` record
  #     and its payload. It refuses what smoltcp's `Repr::parse` refuses: a
  #     short buffer, a header length under 20 or over the total length, a
  #     buffer shorter than the total length, another version, a bad header
  #     checksum, and any fragment. Bytes past the total length are ignored.
  #   * `emit/2` and `header/1` write a header from a record, as smoltcp
  #     does for everything it sends: no options, TOS 0, identification 0,
  #     Don't Fragment set.
  #   * `fields/1` and `build/1` read and write every header field as it is,
  #     smoltcp's `Packet` accessors, for tests and diagnostics.
  #
  # Addresses are 4-byte binaries.

  import Bitwise
  require Record

  alias SmolNet.Core.Wire.Checksum

  Record.defrecord(:ipv4, src: nil, dst: nil, protocol: 0, payload_len: 0, hop_limit: 64)

  @type address :: <<_::32>>
  @type repr ::
          record(:ipv4,
            src: address(),
            dst: address(),
            protocol: 0..255,
            payload_len: non_neg_integer(),
            hop_limit: 0..255
          )

  @header_len 20

  @doc "The length of the header smoltcp emits."
  def header_len, do: @header_len

  @doc "The minimum MTU of an IPv4 link (RFC 791), which bounds ICMP errors."
  def min_mtu, do: 576

  @doc "224.0.0.1, all systems on this subnet."
  def multicast_all_systems, do: <<224, 0, 0, 1>>

  @spec parse(binary()) :: {:ok, repr(), binary()} | :error
  def parse(
        <<4::4, ihl::4, _tos, total_len::16, _ident::16, _reserved::1, _df::1, mf::1, offset::13,
          ttl, protocol, _checksum::16, src::binary-4, dst::binary-4, _rest::binary>> = packet
      ) do
    header_len = ihl * 4
    size = byte_size(packet)

    cond do
      size < header_len or header_len > total_len or size < total_len or
          header_len < @header_len ->
        :error

      not Checksum.valid?(binary_part(packet, 0, header_len)) ->
        :error

      mf == 1 or offset != 0 ->
        :error

      true ->
        payload_len = total_len - header_len

        {:ok,
         ipv4(
           src: src,
           dst: dst,
           protocol: protocol,
           payload_len: payload_len,
           hop_limit: ttl
         ), binary_part(packet, header_len, payload_len)}
    end
  end

  def parse(_packet), do: :error

  @doc "A packet of `header/1` and `payload`."
  @spec emit(repr(), binary()) :: binary()
  def emit(repr, payload), do: <<header(repr)::binary, payload::binary>>

  @doc """
  The 20-byte header smoltcp emits for `repr`. The total length is the
  record's `payload_len` plus 20, so a header quoted in an ICMP error keeps
  the length of the packet it describes.
  """
  @spec header(repr()) :: binary()
  def header(ipv4(src: src, dst: dst, protocol: protocol, payload_len: len, hop_limit: ttl)) do
    total_len = band(@header_len + len, 0xFFFF)

    <<w0::16, w1::16, w2::16, w3::16, w4::16>> =
      <<0x45, 0, total_len::16, 0::16, 0x40, 0, ttl, protocol>>

    <<s1::16, s2::16>> = src
    <<d1::16, d2::16>> = dst
    checksum = bxor(Checksum.fold(w0 + w1 + w2 + w3 + w4 + s1 + s2 + d1 + d2), 0xFFFF)

    <<0x45, 0, total_len::16, 0::16, 0x40, 0, ttl, protocol, checksum::16, src::binary,
      dst::binary>>
  end

  @doc """
  Every field of the header at the start of `packet`, which must be at
  least 20 bytes. `:frag_offset` is in octets; `:options` are the bytes
  between the fixed header and the header length, and `:payload` those
  from there to the total length, each nil if the buffer is too short.
  """
  @spec fields(binary()) :: {:ok, map()} | :error
  def fields(
        <<version::4, ihl::4, dscp::6, ecn::2, total_len::16, ident::16, _reserved::1, df::1,
          mf::1, offset::13, ttl, protocol, checksum::16, src::binary-4, dst::binary-4,
          _rest::binary>> = packet
      ) do
    header_len = ihl * 4

    {:ok,
     %{
       version: version,
       header_len: header_len,
       dscp: dscp,
       ecn: ecn,
       total_len: total_len,
       ident: ident,
       dont_frag: df == 1,
       more_frags: mf == 1,
       frag_offset: offset <<< 3,
       hop_limit: ttl,
       protocol: protocol,
       checksum: checksum,
       src: src,
       dst: dst,
       options: slice(packet, @header_len, header_len - @header_len),
       payload: slice(packet, header_len, total_len - header_len)
     }}
  end

  def fields(_packet), do: :error

  @doc """
  A packet from header fields, the inverse of `fields/1`. Fields left out
  are zero (`:version` 4, `:header_len` 20, `:total_len` the header and
  payload). A `:checksum` of nil is filled in.
  """
  @spec build(map()) :: binary()
  def build(fields) do
    options = Map.get(fields, :options) || <<>>
    payload = Map.get(fields, :payload) || <<>>
    header_len = Map.get(fields, :header_len, @header_len + byte_size(options))
    total_len = Map.get(fields, :total_len, header_len + byte_size(payload))

    header = fn checksum ->
      <<Map.get(fields, :version, 4)::4, div(header_len, 4)::4, Map.get(fields, :dscp, 0)::6,
        Map.get(fields, :ecn, 0)::2, total_len::16, Map.get(fields, :ident, 0)::16, 0::1,
        flag(fields, :dont_frag)::1, flag(fields, :more_frags)::1,
        Map.get(fields, :frag_offset, 0) >>> 3::13, Map.get(fields, :hop_limit, 0),
        Map.get(fields, :protocol, 0), checksum::16, Map.get(fields, :src, <<0::32>>)::binary-4,
        Map.get(fields, :dst, <<0::32>>)::binary-4, options::binary>>
    end

    checksum =
      case Map.get(fields, :checksum) do
        nil -> Checksum.checksum(header.(0))
        checksum -> checksum
      end

    <<header.(checksum)::binary, payload::binary>>
  end

  defp flag(fields, key), do: if(Map.get(fields, key, false), do: 1, else: 0)

  defp slice(packet, start, length)
       when length >= 0 and start + length <= byte_size(packet),
       do: binary_part(packet, start, length)

  defp slice(_packet, _start, _length), do: nil

  ## Addresses

  @spec unspecified?(address()) :: boolean()
  def unspecified?(address), do: address == <<0, 0, 0, 0>>

  @doc "The limited broadcast address, 255.255.255.255."
  @spec broadcast?(address()) :: boolean()
  def broadcast?(address), do: address == <<255, 255, 255, 255>>

  @spec multicast?(address()) :: boolean()
  def multicast?(<<first, _rest::binary-3>>), do: first in 224..239

  @doc "Neither broadcast, multicast nor unspecified (smoltcp's `x_is_unicast`)."
  @spec unicast?(address()) :: boolean()
  def unicast?(address),
    do: not (broadcast?(address) or multicast?(address) or unspecified?(address))

  @doc "The prefix length of a netmask, or nil if it is not one."
  @spec mask_prefix_len(address()) :: 0..32 | nil
  def mask_prefix_len(<<mask::32>>) do
    ones = 32 - trailing_zeros(mask, 32)
    if mask == netmask_bits(ones), do: ones
  end

  defp trailing_zeros(0, bits), do: bits
  defp trailing_zeros(value, _bits), do: count_trailing(value, 0)

  defp count_trailing(value, count) when band(value, 1) == 1, do: count
  defp count_trailing(value, count), do: count_trailing(value >>> 1, count + 1)

  ## CIDR blocks, as an address and a prefix length

  @doc "The netmask of a prefix length."
  @spec netmask(0..32) :: address()
  def netmask(prefix_len), do: <<netmask_bits(prefix_len)::32>>

  @doc "The prefix length of `netmask`, as `Cidr::from_netmask` checks it."
  @spec prefix_from_netmask(address()) :: {:ok, 0..32} | :error
  def prefix_from_netmask(<<mask::32>> = netmask) do
    if band(mask, 0x8000_0000) != 0 and mask_prefix_len(netmask) != nil,
      do: {:ok, mask_prefix_len(netmask)},
      else: :error
  end

  @spec network(address(), 0..32) :: address()
  def network(<<address::32>>, prefix_len), do: <<band(address, netmask_bits(prefix_len))::32>>

  @doc "The subnet's broadcast address, or nil for a /31 or /32."
  @spec broadcast(address(), 0..32) :: address() | nil
  def broadcast(_address, prefix_len) when prefix_len >= 31, do: nil

  def broadcast(<<address::32>>, prefix_len) do
    <<bor(band(address, netmask_bits(prefix_len)), 0xFFFF_FFFF >>> prefix_len)::32>>
  end

  @spec contains?(address(), 0..32, address()) :: boolean()
  def contains?(<<network::32>>, prefix_len, <<address::32>>) do
    mask = netmask_bits(prefix_len)
    band(network, mask) == band(address, mask)
  end

  defp netmask_bits(0), do: 0
  defp netmask_bits(prefix_len), do: band(0xFFFF_FFFF <<< (32 - prefix_len), 0xFFFF_FFFF)
end
