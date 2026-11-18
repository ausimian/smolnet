defmodule SmolNet.Core.Wire.Icmpv6 do
  @moduledoc false

  # ICMPv6 messages, as smoltcp's `wire::icmpv6` parses and emits them on
  # an IP medium. A message is one of
  #
  #   * `{:echo_request, ident, seq_no, data}`
  #   * `{:echo_reply, ident, seq_no, data}`
  #   * `{:dst_unreachable, code, header, data}`
  #   * `{:pkt_too_big, mtu, header, data}`
  #   * `{:time_exceeded, code, header, data}`
  #   * `{:param_problem, code, pointer, header, data}`
  #
  # where `header` is an `Ipv6.ipv6` record of the packet an error is
  # about, read from its first 40 bytes however much of it was quoted, and
  # `data` the bytes after them. The checksum covers the IPv6
  # pseudo-header, so parsing and emitting take the message's addresses.
  #
  # `parse/3` refuses a message shorter than its type's header, an
  # unknown type, a bad checksum, an echo with a code other than 0, a
  # Packet Too Big with one, and an error quoting less than 40 bytes.
  # smoltcp also parses Neighbor Discovery and MLD messages; on an IP
  # medium it does nothing with them, and here they do not parse.
  #
  # `emit/3` caps an error at 1240 bytes, so that with its IPv6 header it
  # fits the minimum MTU, truncating the quoted data.

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Ipv6

  require Ipv6

  @dst_unreachable 1
  @pkt_too_big 2
  @time_exceeded 3
  @param_problem 4
  @echo_request 128
  @echo_reply 129

  @protocol 58
  @max_error_len 1_240

  @type message ::
          {:echo_request | :echo_reply, 0..0xFFFF, 0..0xFFFF, binary()}
          | {:dst_unreachable | :time_exceeded, 0..255, Ipv6.repr(), binary()}
          | {:pkt_too_big, 0..0xFFFF_FFFF, Ipv6.repr(), binary()}
          | {:param_problem, 0..255, 0..0xFFFF_FFFF, Ipv6.repr(), binary()}

  @doc "Codes SmolNet sends."
  def port_unreachable, do: 4
  def unrecognized_next_header, do: 1
  def unrecognized_option, do: 2

  @doc "The protocol number of ICMPv6."
  def protocol, do: @protocol

  @doc """
  The header length smoltcp's `Packet::header_len` gives a type, or nil
  for a type its `check_len` refuses.
  """
  @spec header_len(0..255) :: non_neg_integer() | nil
  def header_len(type) when type in [1, 2, 3, 4, 128, 129, 133], do: 8
  def header_len(134), do: 16
  def header_len(type) when type in [135, 136], do: 24
  def header_len(137), do: 40
  def header_len(130), do: 28
  def header_len(143), do: 8
  def header_len(_type), do: nil

  @spec parse(Ipv6.address(), Ipv6.address(), binary()) :: {:ok, message()} | :error
  def parse(src, dst, <<type, code, _checksum::16, _rest::binary>> = message) do
    case header_len(type) do
      nil ->
        :error

      header_len when byte_size(message) < header_len ->
        :error

      _header_len ->
        pseudo = Checksum.pseudo_header(src, dst, @protocol, byte_size(message))

        if Checksum.valid?(message, pseudo),
          do: parse_message(type, code, message),
          else: :error
    end
  end

  def parse(_src, _dst, _message), do: :error

  defp parse_message(@echo_request, 0, <<_::32, ident::16, seq_no::16, data::binary>>),
    do: {:ok, {:echo_request, ident, seq_no, data}}

  defp parse_message(@echo_reply, 0, <<_::32, ident::16, seq_no::16, data::binary>>),
    do: {:ok, {:echo_reply, ident, seq_no, data}}

  defp parse_message(@dst_unreachable, code, <<_::64, quoted::binary>>),
    do: quoted(quoted, &{:dst_unreachable, code, &1, &2})

  defp parse_message(@pkt_too_big, 0, <<_::32, mtu::32, quoted::binary>>),
    do: quoted(quoted, &{:pkt_too_big, mtu, &1, &2})

  defp parse_message(@time_exceeded, code, <<_::64, quoted::binary>>),
    do: quoted(quoted, &{:time_exceeded, code, &1, &2})

  defp parse_message(@param_problem, code, <<_::32, pointer::32, quoted::binary>>),
    do: quoted(quoted, &{:param_problem, code, pointer, &1, &2})

  defp parse_message(_type, _code, _message), do: :error

  defp quoted(
         <<_version::4, _class_and_flow::28, payload_len::16, next_header, hop_limit,
           src::binary-16, dst::binary-16, data::binary>>,
         message
       ) do
    header =
      Ipv6.ipv6(
        src: src,
        dst: dst,
        next_header: next_header,
        payload_len: payload_len,
        hop_limit: hop_limit
      )

    {:ok, message.(header, data)}
  end

  defp quoted(_quoted, _message), do: :error

  @doc "A message between two addresses, its checksum filled in."
  @spec emit(Ipv6.address(), Ipv6.address(), message()) :: binary()
  def emit(src, dst, message) do
    {type, code, rest, data} = layout(message)
    length = 4 + byte_size(rest) + byte_size(data)
    pseudo = Checksum.pseudo_header(src, dst, @protocol, length)
    sum = Checksum.sum(data, Checksum.sum(rest, pseudo + type * 256 + code))
    checksum = Bitwise.bxor(Checksum.fold(sum), 0xFFFF)
    <<type, code, checksum::16, rest::binary, data::binary>>
  end

  defp layout({:echo_request, ident, seq_no, data}),
    do: {@echo_request, 0, <<ident::16, seq_no::16>>, data}

  defp layout({:echo_reply, ident, seq_no, data}),
    do: {@echo_reply, 0, <<ident::16, seq_no::16>>, data}

  defp layout({:dst_unreachable, code, header, data}),
    do: {@dst_unreachable, code, <<0::32>>, quote_packet(header, data)}

  defp layout({:pkt_too_big, mtu, header, data}),
    do: {@pkt_too_big, 0, <<mtu::32>>, quote_packet(header, data)}

  defp layout({:time_exceeded, code, header, data}),
    do: {@time_exceeded, code, <<0::32>>, quote_packet(header, data)}

  defp layout({:param_problem, code, pointer, header, data}),
    do: {@param_problem, code, <<pointer::32>>, quote_packet(header, data)}

  defp quote_packet(header, data) do
    room = @max_error_len - 8 - Ipv6.header_len()
    data = if byte_size(data) > room, do: binary_part(data, 0, room), else: data
    <<Ipv6.header(header)::binary, data::binary>>
  end

  @doc "The length `emit/3` gives a message."
  @spec buffer_len(message()) :: non_neg_integer()
  def buffer_len({kind, _ident, _seq_no, data}) when kind in [:echo_request, :echo_reply],
    do: 8 + byte_size(data)

  def buffer_len(error),
    do: min(8 + Ipv6.header_len() + byte_size(elem(error, tuple_size(error) - 1)), @max_error_len)

  @doc """
  The fields of a message of at least 8 bytes as they are: the type, code
  and checksum, the four bytes after them as `:echo_ident` and
  `:echo_seq_no` and as one `:word` (a Packet Too Big's MTU, a Parameter
  Problem's pointer), and `:payload`, the bytes after those.
  """
  @spec fields(binary()) :: {:ok, map()} | :error
  def fields(<<type, code, checksum::16, word::binary-4, payload::binary>>) do
    <<ident::16, seq_no::16>> = word
    <<value::32>> = word

    {:ok,
     %{
       msg_type: type,
       msg_code: code,
       checksum: checksum,
       echo_ident: ident,
       echo_seq_no: seq_no,
       word: value,
       payload: payload
     }}
  end

  def fields(_message), do: :error

  @doc """
  A message from fields between two addresses, the inverse of `fields/1`.
  `:word` wins over `:echo_ident` and `:echo_seq_no`; a nil `:checksum` is
  filled in.
  """
  @spec build(Ipv6.address(), Ipv6.address(), map()) :: binary()
  def build(src, dst, fields) do
    word =
      case Map.get(fields, :word) do
        nil -> <<Map.get(fields, :echo_ident, 0)::16, Map.get(fields, :echo_seq_no, 0)::16>>
        value -> <<value::32>>
      end

    message = fn checksum ->
      <<Map.get(fields, :msg_type, 0), Map.get(fields, :msg_code, 0), checksum::16, word::binary,
        Map.get(fields, :payload, <<>>)::binary>>
    end

    case Map.get(fields, :checksum) do
      nil ->
        unsummed = message.(0)
        pseudo = Checksum.pseudo_header(src, dst, @protocol, byte_size(unsummed))
        message.(Checksum.checksum(unsummed, pseudo))

      checksum ->
        message.(checksum)
    end
  end
end
