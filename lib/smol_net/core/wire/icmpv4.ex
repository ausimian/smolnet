defmodule SmolNet.Core.Wire.Icmpv4 do
  @moduledoc false

  # ICMPv4 messages, as smoltcp's `wire::icmpv4` parses and emits them.
  # A message is one of
  #
  #   * `{:echo_request, ident, seq_no, data}`
  #   * `{:echo_reply, ident, seq_no, data}`
  #   * `{:dst_unreachable, code, header, data}`
  #   * `{:time_exceeded, code, header, data}`
  #
  # where `header` is an `Ipv4.ipv4` record of the packet an error is
  # about and `data` the bytes of it that follow its header. `parse/1`
  # refuses every other type, an echo with a code other than 0, a bad
  # checksum, and an error whose quoted packet is not complete: smoltcp
  # checks the quoted header's lengths against what was quoted, so an
  # error quoting a truncated packet does not parse. An error needs at
  # least 8 bytes after the quoted header.
  #
  # `emit/1` writes an error's quoted header with `Ipv4.header/1`, as
  # smoltcp does, so it is the header smoltcp would send for the record:
  # options, TOS and identification are not kept. The four unused bytes
  # are zero.

  alias SmolNet.Core.Wire.Checksum
  alias SmolNet.Core.Wire.Ipv4

  require Ipv4

  @echo_reply 0
  @dst_unreachable 3
  @echo_request 8
  @time_exceeded 11

  @type message ::
          {:echo_request | :echo_reply, 0..0xFFFF, 0..0xFFFF, binary()}
          | {:dst_unreachable | :time_exceeded, 0..255, Ipv4.repr(), binary()}

  @doc "Destination Unreachable codes SmolNet sends or reads."
  def protocol_unreachable, do: 2
  def port_unreachable, do: 3
  def frag_required, do: 4

  @spec parse(binary()) :: {:ok, message()} | :error
  def parse(<<type, code, _checksum::16, rest::binary-4, data::binary>> = message) do
    if Checksum.valid?(message), do: parse(type, code, rest, data), else: :error
  end

  def parse(_message), do: :error

  defp parse(@echo_request, 0, <<ident::16, seq_no::16>>, data),
    do: {:ok, {:echo_request, ident, seq_no, data}}

  defp parse(@echo_reply, 0, <<ident::16, seq_no::16>>, data),
    do: {:ok, {:echo_reply, ident, seq_no, data}}

  defp parse(@dst_unreachable, code, _unused, data), do: parse_error(:dst_unreachable, code, data)
  defp parse(@time_exceeded, code, _unused, data), do: parse_error(:time_exceeded, code, data)
  defp parse(_type, _code, _rest, _data), do: :error

  # smoltcp reads the quoted header with `Ipv4Packet::new_checked`, which
  # checks its lengths but not its version or checksum.
  defp parse_error(kind, code, data) do
    with {:ok, header_len, src, dst, protocol, ttl} <- quoted_header(data),
         payload_len = byte_size(data) - header_len,
         true <- payload_len >= 8 do
      header =
        Ipv4.ipv4(
          src: src,
          dst: dst,
          protocol: protocol,
          payload_len: payload_len,
          hop_limit: ttl
        )

      {:ok, {kind, code, header, binary_part(data, header_len, payload_len)}}
    else
      _invalid -> :error
    end
  end

  defp quoted_header(
         <<_version::4, ihl::4, _tos, total_len::16, _ident::16, _frag::16, ttl, protocol,
           _checksum::16, src::binary-4, dst::binary-4, _rest::binary>> = data
       ) do
    header_len = ihl * 4
    size = byte_size(data)

    if size < header_len or header_len > total_len or size < total_len or header_len < 20,
      do: :error,
      else: {:ok, header_len, src, dst, protocol, ttl}
  end

  defp quoted_header(_data), do: :error

  @spec emit(message()) :: binary()
  def emit(message) do
    {type, code, rest, data} = layout(message)
    sum = Checksum.sum(data, Checksum.sum(rest, type * 256 + code))
    checksum = Bitwise.bxor(Checksum.fold(sum), 0xFFFF)
    <<type, code, checksum::16, rest::binary, data::binary>>
  end

  defp layout({:echo_request, ident, seq_no, data}),
    do: {@echo_request, 0, <<ident::16, seq_no::16>>, data}

  defp layout({:echo_reply, ident, seq_no, data}),
    do: {@echo_reply, 0, <<ident::16, seq_no::16>>, data}

  defp layout({:dst_unreachable, code, header, data}),
    do: {@dst_unreachable, code, <<0::32>>, <<Ipv4.header(header)::binary, data::binary>>}

  defp layout({:time_exceeded, code, header, data}),
    do: {@time_exceeded, code, <<0::32>>, <<Ipv4.header(header)::binary, data::binary>>}

  @doc "The length `emit/1` gives a message."
  @spec buffer_len(message()) :: non_neg_integer()
  def buffer_len({kind, _ident, _seq_no, data}) when kind in [:echo_request, :echo_reply],
    do: 8 + byte_size(data)

  def buffer_len({_kind, _code, _header, data}), do: 8 + Ipv4.header_len() + byte_size(data)

  @doc """
  The fields of a message of at least 8 bytes, as smoltcp's `Packet`
  reads them: `:echo_ident` and `:echo_seq_no` are bytes 4 to 8 whatever
  the type, and `:data` the bytes after them.
  """
  @spec fields(binary()) :: {:ok, map()} | :error
  def fields(<<type, code, checksum::16, ident::16, seq_no::16, data::binary>>) do
    {:ok,
     %{
       msg_type: type,
       msg_code: code,
       checksum: checksum,
       echo_ident: ident,
       echo_seq_no: seq_no,
       data: data
     }}
  end

  def fields(_message), do: :error

  @doc "A message from fields, the inverse of `fields/1`; a nil `:checksum` is filled in."
  @spec build(map()) :: binary()
  def build(fields) do
    message = fn checksum ->
      <<Map.get(fields, :msg_type, 0), Map.get(fields, :msg_code, 0), checksum::16,
        Map.get(fields, :echo_ident, 0)::16, Map.get(fields, :echo_seq_no, 0)::16,
        Map.get(fields, :data, <<>>)::binary>>
    end

    case Map.get(fields, :checksum) do
      nil -> message.(Checksum.checksum(message.(0)))
      checksum -> message.(checksum)
    end
  end
end
