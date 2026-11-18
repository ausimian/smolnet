defmodule SmolNet.Core.Wire.Hbh do
  @moduledoc false

  # The IPv6 Hop-by-Hop Options header (RFC 8200 4.3), as smoltcp's
  # `wire::ipv6ext_header`, `wire::ipv6option` and `wire::ipv6hbh` handle
  # it, in three layers:
  #
  #   * The extension header's framing: a next header, a length in 8-byte
  #     units past the first 8, and the data after those two bytes.
  #     `parse_ext_header/1` returns `{next_header, length, data}` and the
  #     bytes after the header.
  #   * Options, each `:pad1`, `{:padn, length}`, `{:router_alert, value}`
  #     or `{:unknown, type, length, data}`. smoltcp parses RPL options
  #     only with `proto-rpl`, compiled out here, so type 0x63 is unknown.
  #   * The header's options: `parse/1` keeps at most four, smoltcp's
  #     `IPV6_HBH_MAX_OPTIONS`, and ignores the rest unread past the fifth.

  @max_options 4

  @type option ::
          :pad1
          | {:padn, 0..255}
          | {:router_alert, 0..0xFFFF}
          | {:unknown, 0..255, 0..255, binary()}

  @type failure_type :: :skip | :discard | :discard_send_all | :discard_send_unicast

  ## The extension header's framing

  @doc """
  The extension header at the start of `data`: `{:ok, {next_header,
  length, header_data}, rest}`, where `header_data` is the `length * 8 + 6`
  bytes after the first two and `rest` what follows the header.
  """
  @spec parse_ext_header(binary()) ::
          {:ok, {0..255, 0..255, binary()}, binary()} | :error
  def parse_ext_header(<<next_header, length, _rest::binary>> = data)
      when byte_size(data) >= 8 and byte_size(data) >= length * 8 + 8 do
    size = length * 8 + 6
    header_data = binary_part(data, 2, size)
    rest = binary_part(data, size + 2, byte_size(data) - size - 2)
    {:ok, {next_header, length, header_data}, rest}
  end

  def parse_ext_header(_data), do: :error

  @doc "The two bytes smoltcp's extension header `Repr::emit` writes."
  @spec emit_ext_header(0..255, 0..255) :: binary()
  def emit_ext_header(next_header, length), do: <<next_header, length>>

  ## Options

  @doc "The option at the start of `data`, and what follows it."
  @spec parse_option(binary()) :: {:ok, option(), binary()} | :error
  def parse_option(<<0, rest::binary>>), do: {:ok, :pad1, rest}

  def parse_option(<<type, length, data::binary-size(length), rest::binary>>) do
    case {type, length, data} do
      {1, length, _data} -> {:ok, {:padn, length}, rest}
      {5, 2, <<value::16>>} -> {:ok, {:router_alert, value}, rest}
      {5, _length, _data} -> :error
      {type, length, data} -> {:ok, {:unknown, type, length, data}, rest}
    end
  end

  def parse_option(_data), do: :error

  @doc """
  Every option in `data` in order, as `{:ok, option}`, until the data
  ends or one fails to parse, which ends the list as `:error`.
  """
  @spec parse_options(binary()) :: [{:ok, option()} | :error]
  def parse_options(<<>>), do: []

  def parse_options(data) do
    case parse_option(data) do
      {:ok, option, rest} -> [{:ok, option} | parse_options(rest)]
      :error -> [:error]
    end
  end

  @spec option_buffer_len(option()) :: pos_integer()
  def option_buffer_len(:pad1), do: 1
  def option_buffer_len({:padn, length}), do: length + 2
  def option_buffer_len({:router_alert, _value}), do: 4
  def option_buffer_len({:unknown, _type, length, _data}), do: length + 2

  @spec emit_option(option()) :: binary()
  def emit_option(:pad1), do: <<0>>
  def emit_option({:padn, length}), do: <<1, length, 0::size(length)-unit(8)>>
  def emit_option({:router_alert, value}), do: <<5, 2, value::16>>

  def emit_option({:unknown, type, length, data}),
    do: <<type, length, binary_part(data, 0, length)::binary>>

  @doc "What an unrecognised option asks for, from its type's top two bits."
  @spec failure_type(0..255) :: failure_type()
  def failure_type(type) do
    case Bitwise.band(type, 0xC0) do
      0x00 -> :skip
      0x40 -> :discard
      0x80 -> :discard_send_all
      0xC0 -> :discard_send_unicast
    end
  end

  ## The Hop-by-Hop header's options

  @doc """
  The options of a Hop-by-Hop header's data, as smoltcp's
  `Ipv6HopByHopRepr::parse` reads them: at most four, any error among the
  first five fails the whole, and the rest are ignored.
  """
  @spec parse(binary()) :: {:ok, [option()]} | :error
  def parse(<<>>), do: :error
  def parse(data), do: parse(data, [], 0)

  defp parse(<<>>, options, _count), do: {:ok, Enum.reverse(options)}

  defp parse(data, options, count) do
    case parse_option(data) do
      {:ok, _option, _rest} when count == @max_options -> {:ok, Enum.reverse(options)}
      {:ok, option, rest} -> parse(rest, [option | options], count + 1)
      :error -> :error
    end
  end

  @spec emit([option()]) :: binary()
  def emit(options), do: options |> Enum.map(&emit_option/1) |> IO.iodata_to_binary()

  @spec buffer_len([option()]) :: non_neg_integer()
  def buffer_len(options), do: options |> Enum.map(&option_buffer_len/1) |> Enum.sum()
end
