defmodule SmolNet.Conformance.Packets do
  @moduledoc false

  # Raw IP packets built by hand, for feeding a stack what its peers would
  # send: IPv4 and IPv6 headers, an IPv6 extension header, and TCP, UDP and
  # ICMP messages, each with correct checksums unless told otherwise.
  # Addresses are 4- or 16-byte binaries.

  import Bitwise

  @tcp 6
  @udp 17
  @icmpv4 1
  @icmpv6 58

  @doc "The Internet checksum of `data` (RFC 1071)."
  @spec checksum(binary()) :: 0..0xFFFF
  def checksum(data), do: data |> sum(0) |> fold() |> bnot() |> band(0xFFFF)

  defp sum(<<word::16, rest::binary>>, acc), do: sum(rest, acc + word)
  defp sum(<<byte>>, acc), do: acc + (byte <<< 8)
  defp sum(<<>>, acc), do: acc

  defp fold(sum) when sum > 0xFFFF, do: fold(band(sum, 0xFFFF) + (sum >>> 16))
  defp fold(sum), do: sum

  @doc """
  An IPv4 packet. Options: `:ttl` (64), `:flags` (DF), `:fragment_offset`
  (0), `:identification` (0).
  """
  def ipv4(source, destination, protocol, payload, options \\ []) do
    ttl = Keyword.get(options, :ttl, 64)
    flags = Keyword.get(options, :flags, 0b010)
    offset = Keyword.get(options, :fragment_offset, 0)
    identification = Keyword.get(options, :identification, 0)
    length = 20 + byte_size(payload)

    header = fn checksum ->
      <<4::4, 5::4, 0, length::16, identification::16, flags::3, offset::13, ttl, protocol,
        checksum::16, source::binary-4, destination::binary-4>>
    end

    header.(checksum(header.(0))) <> payload
  end

  @doc "An IPv6 packet. Options: `:hop_limit` (64)."
  def ipv6(source, destination, next_header, payload, options \\ []) do
    hop_limit = Keyword.get(options, :hop_limit, 64)

    <<6::4, 0::8, 0::20, byte_size(payload)::16, next_header, hop_limit, source::binary-16,
      destination::binary-16, payload::binary>>
  end

  @doc "An IPv6 extension header (Hop-by-Hop or Destination Options) padded to 8 bytes."
  def ipv6_options(next_header, payload) do
    <<next_header, 0, 1, 4, 0::32, payload::binary>>
  end

  @doc """
  A TCP segment between two addresses, as the IP payload. Fields: `:source`
  and `:destination` ports, `:seq`, `:ack`, `:flags` (a list of `:fin`,
  `:syn`, `:rst`, `:psh`, `:ack`), `:window`, `:options` (raw bytes, padded
  here to a word) and `:payload`.
  """
  def tcp(source_address, destination_address, fields) do
    options = pad(Map.get(fields, :options, <<>>))
    payload = Map.get(fields, :payload, <<>>)
    offset = 5 + div(byte_size(options), 4)
    flags = fields |> Map.get(:flags, []) |> Enum.reduce(0, &bor(&2, flag(&1)))

    segment = fn checksum ->
      <<Map.fetch!(fields, :source)::16, Map.fetch!(fields, :destination)::16,
        Map.get(fields, :seq, 0)::32, Map.get(fields, :ack, 0)::32, offset::4, 0::4, flags,
        Map.get(fields, :window, 65_535)::16, checksum::16, 0::16, options::binary,
        payload::binary>>
    end

    with_checksum(segment, source_address, destination_address, @tcp)
  end

  @doc "A UDP datagram between two addresses, as the IP payload."
  def udp(source_address, destination_address, source_port, destination_port, payload) do
    datagram = fn checksum ->
      <<source_port::16, destination_port::16, byte_size(payload) + 8::16, checksum::16,
        payload::binary>>
    end

    with_checksum(datagram, source_address, destination_address, @udp)
  end

  @doc "An ICMPv4 echo request, as the IP payload."
  def icmpv4_echo(identifier, sequence, payload) do
    message = fn checksum ->
      <<8, 0, checksum::16, identifier::16, sequence::16, payload::binary>>
    end

    message.(checksum(message.(0)))
  end

  @doc "An ICMPv6 echo request between two addresses, as the IP payload."
  def icmpv6_echo(source_address, destination_address, identifier, sequence, payload) do
    message = fn checksum ->
      <<128, 0, checksum::16, identifier::16, sequence::16, payload::binary>>
    end

    with_checksum(message, source_address, destination_address, @icmpv6)
  end

  def protocol(:tcp), do: @tcp
  def protocol(:udp), do: @udp
  def protocol(:icmpv4), do: @icmpv4
  def protocol(:icmpv6), do: @icmpv6

  # A checksum over the IPv4 or IPv6 pseudo-header and the message.
  defp with_checksum(message, source, destination, protocol) do
    length = byte_size(message.(0))

    pseudo =
      case byte_size(source) do
        4 -> <<source::binary, destination::binary, 0, protocol, length::16>>
        16 -> <<source::binary, destination::binary, length::32, 0::24, protocol>>
      end

    case checksum(pseudo <> message.(0)) do
      # UDP sends a zero sum as all ones; TCP and ICMP may send either.
      0 -> message.(0xFFFF)
      sum -> message.(sum)
    end
  end

  defp pad(options) do
    case rem(byte_size(options), 4) do
      0 -> options
      r -> options <> :binary.copy(<<0>>, 4 - r)
    end
  end

  defp flag(:fin), do: 0x01
  defp flag(:syn), do: 0x02
  defp flag(:rst), do: 0x04
  defp flag(:psh), do: 0x08
  defp flag(:ack), do: 0x10
end
