defmodule SmolNet.Core.Wire.Udp do
  @moduledoc false

  # UDP datagrams, as smoltcp's `wire::udp` parses and emits them.
  #
  # `parse/3` takes a datagram, the IP payload, and its addresses, and
  # returns `{src_port, dst_port}` and the payload, the bytes up to the
  # length field. It refuses a buffer shorter than the header or than the
  # length field, a length field under 8, destination port 0, and a bad
  # checksum. A zero checksum means none was sent, which smoltcp accepts
  # over IPv6 as well as IPv4.
  #
  # `emit/5` writes a datagram with its checksum, sending a zero sum as
  # 0xFFFF.

  alias SmolNet.Core.Wire.Checksum

  @protocol 17
  @header_len 8

  @type address :: <<_::32>> | <<_::128>>

  def protocol, do: @protocol
  def header_len, do: @header_len

  @spec parse(binary(), address(), address()) ::
          {:ok, {0..0xFFFF, 1..0xFFFF}, binary()} | :error
  def parse(
        <<src_port::16, dst_port::16, length::16, checksum::16, _rest::binary>> = datagram,
        src,
        dst
      )
      when length >= @header_len and length <= byte_size(datagram) and dst_port != 0 do
    if checksum == 0 or
         Checksum.valid?(
           binary_part(datagram, 0, length),
           Checksum.pseudo_header(src, dst, @protocol, length)
         ) do
      {:ok, {src_port, dst_port}, binary_part(datagram, @header_len, length - @header_len)}
    else
      :error
    end
  end

  def parse(_datagram, _src, _dst), do: :error

  @spec emit(0..0xFFFF, 0..0xFFFF, binary(), address(), address()) :: binary()
  def emit(src_port, dst_port, payload, src, dst) do
    length = @header_len + byte_size(payload)
    pseudo = Checksum.pseudo_header(src, dst, @protocol, length)
    sum = Checksum.sum(payload, pseudo + src_port + dst_port + length)

    checksum =
      case Bitwise.bxor(Checksum.fold(sum), 0xFFFF) do
        0 -> 0xFFFF
        checksum -> checksum
      end

    <<src_port::16, dst_port::16, length::16, checksum::16, payload::binary>>
  end

  @doc """
  The header fields of a datagram of at least 8 bytes as they are, and
  `:payload`, the bytes after the header up to the length field, or nil if
  the field is out of range.
  """
  @spec fields(binary()) :: {:ok, map()} | :error
  def fields(<<src_port::16, dst_port::16, length::16, checksum::16, rest::binary>>) do
    payload_len = length - @header_len

    {:ok,
     %{
       src_port: src_port,
       dst_port: dst_port,
       length: length,
       checksum: checksum,
       payload:
         if(payload_len >= 0 and payload_len <= byte_size(rest),
           do: binary_part(rest, 0, payload_len)
         )
     }}
  end

  def fields(_datagram), do: :error

  @doc """
  A datagram from fields, the inverse of `fields/1`. `:length` defaults to
  the header and payload; a nil `:checksum` is computed between `src` and
  `dst` (0 is sent as 0xFFFF).
  """
  @spec build(map(), address(), address()) :: binary()
  def build(fields, src, dst) do
    payload = Map.get(fields, :payload) || <<>>
    length = Map.get(fields, :length, @header_len + byte_size(payload))

    datagram = fn checksum ->
      <<Map.get(fields, :src_port, 0)::16, Map.get(fields, :dst_port, 0)::16, length::16,
        checksum::16, payload::binary>>
    end

    case Map.get(fields, :checksum) do
      nil ->
        unsummed = datagram.(0)
        pseudo = Checksum.pseudo_header(src, dst, @protocol, length)

        case Checksum.checksum(binary_part(unsummed, 0, min(length, byte_size(unsummed))), pseudo) do
          0 -> datagram.(0xFFFF)
          checksum -> datagram.(checksum)
        end

      checksum ->
        datagram.(checksum)
    end
  end
end
