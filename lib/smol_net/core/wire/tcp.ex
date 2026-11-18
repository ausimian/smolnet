defmodule SmolNet.Core.Wire.Tcp do
  @moduledoc false

  # TCP segments, as smoltcp's `wire::tcp` parses and emits them.
  #
  # A segment is a `tcp` record:
  #
  #   * `control`: `:none`, `:psh`, `:syn`, `:fin` or `:rst`. PSH is kept
  #     only on a segment with no other flag, as smoltcp keeps it.
  #   * `seq`, and `ack`, nil unless the ACK flag is set; 32-bit values.
  #   * `window`, the raw window field, and the options smoltcp reads:
  #     `window_scale` (capped at 14), `mss`, `sack_permitted`,
  #     `sack_ranges` (at most three `{left, right}`) and `timestamp`
  #     (`{tsval, tsecr}`), each nil, false or empty when absent.
  #   * `payload`.
  #
  # `parse/3` refuses a buffer shorter than 20 bytes or than its data
  # offset, a data offset under 20 bytes, port 0 at either end, a bad
  # checksum, a forbidden flag combination (two of SYN, FIN and RST), and
  # a malformed MSS, window scale, SACK-permitted or SACK option. URG,
  # ECE, CWR and NS are ignored.
  #
  # `emit/3` writes MSS, window scale, then SACK-permitted or (on an ACK)
  # the SACK blocks, then the timestamp, padding with zeros to a word.
  # Like smoltcp it reserves room for SACK blocks it does not write.

  import Bitwise
  require Record

  alias SmolNet.Core.Wire.Checksum

  Record.defrecord(:tcp,
    src_port: 0,
    dst_port: 0,
    control: :none,
    seq: 0,
    ack: nil,
    window: 0,
    window_scale: nil,
    mss: nil,
    sack_permitted: false,
    sack_ranges: [],
    timestamp: nil,
    payload: <<>>
  )

  @type address :: <<_::32>> | <<_::128>>
  @type control :: :none | :psh | :syn | :fin | :rst
  @type repr ::
          record(:tcp,
            src_port: 0..0xFFFF,
            dst_port: 0..0xFFFF,
            control: control(),
            seq: 0..0xFFFF_FFFF,
            ack: 0..0xFFFF_FFFF | nil,
            window: 0..0xFFFF,
            window_scale: 0..14 | nil,
            mss: 0..0xFFFF | nil,
            sack_permitted: boolean(),
            sack_ranges: [{0..0xFFFF_FFFF, 0..0xFFFF_FFFF}],
            timestamp: {0..0xFFFF_FFFF, 0..0xFFFF_FFFF} | nil,
            payload: binary()
          )
  @type option ::
          :end_of_list
          | :nop
          | {:mss, 0..0xFFFF}
          | {:window_scale, 0..255}
          | :sack_permitted
          | {:sack_range, [{0..0xFFFF_FFFF, 0..0xFFFF_FFFF}]}
          | {:timestamp, 0..0xFFFF_FFFF, 0..0xFFFF_FFFF}
          | {:unknown, 0..255, binary()}

  @protocol 6
  @header_len 20

  @fin 0x01
  @syn 0x02
  @rst 0x04
  @psh 0x08
  @ack 0x10

  def protocol, do: @protocol

  @spec parse(binary(), address(), address()) :: {:ok, repr()} | :error
  def parse(
        <<src_port::16, dst_port::16, seq::32, ack::32, offset::4, _reserved::4, flags,
          window::16, _checksum::16, _urgent::16, _rest::binary>> = segment,
        src,
        dst
      ) do
    header_len = offset * 4
    size = byte_size(segment)

    with true <- header_len >= @header_len and size >= header_len,
         true <- src_port != 0 and dst_port != 0,
         true <- Checksum.valid?(segment, Checksum.pseudo_header(src, dst, @protocol, size)),
         {:ok, control} <- control(flags),
         options = binary_part(segment, @header_len, header_len - @header_len),
         payload = binary_part(segment, header_len, size - header_len),
         {:ok, repr} <-
           parse_options(
             options,
             tcp(
               src_port: src_port,
               dst_port: dst_port,
               control: control,
               seq: seq,
               ack: if(band(flags, @ack) != 0, do: ack),
               window: window,
               payload: payload
             )
           ) do
      {:ok, repr}
    else
      _invalid -> :error
    end
  end

  def parse(_segment, _src, _dst), do: :error

  defp control(flags) do
    case {band(flags, @syn) != 0, band(flags, @fin) != 0, band(flags, @rst) != 0,
          band(flags, @psh) != 0} do
      {false, false, false, false} -> {:ok, :none}
      {false, false, false, true} -> {:ok, :psh}
      {true, false, false, _psh} -> {:ok, :syn}
      {false, true, false, _psh} -> {:ok, :fin}
      {false, false, true, _psh} -> {:ok, :rst}
      _forbidden -> :error
    end
  end

  defp parse_options(<<>>, repr), do: {:ok, repr}

  defp parse_options(options, repr) do
    case parse_option(options) do
      {:ok, :end_of_list, _rest} -> {:ok, repr}
      {:ok, option, rest} -> parse_options(rest, apply_option(option, repr))
      :error -> :error
    end
  end

  defp apply_option({:mss, value}, repr), do: tcp(repr, mss: value)
  defp apply_option({:window_scale, value}, repr), do: tcp(repr, window_scale: min(value, 14))
  defp apply_option(:sack_permitted, repr), do: tcp(repr, sack_permitted: true)
  defp apply_option({:sack_range, ranges}, repr), do: tcp(repr, sack_ranges: ranges)

  defp apply_option({:timestamp, tsval, tsecr}, repr),
    do: tcp(repr, timestamp: {tsval, tsecr})

  defp apply_option(_option, repr), do: repr

  @doc "The option at the start of `options`, and what follows it."
  @spec parse_option(binary()) :: {:ok, option(), binary()} | :error
  def parse_option(<<0, rest::binary>>), do: {:ok, :end_of_list, rest}
  def parse_option(<<1, rest::binary>>), do: {:ok, :nop, rest}

  def parse_option(<<kind, length, rest::binary>>)
      when length >= 2 and length - 2 <= byte_size(rest) do
    size = length - 2
    data = binary_part(rest, 0, size)
    rest = binary_part(rest, size, byte_size(rest) - size)

    case option(kind, data) do
      {:ok, option} -> {:ok, option, rest}
      :error -> :error
    end
  end

  def parse_option(_options), do: :error

  # An option from its kind and data; the data's length is the option's
  # less two, so a size match is a length check.
  defp option(2, <<value::16>>), do: {:ok, {:mss, value}}
  defp option(3, <<value>>), do: {:ok, {:window_scale, value}}
  defp option(4, <<>>), do: {:ok, :sack_permitted}
  defp option(kind, _data) when kind in [2, 3, 4], do: :error
  defp option(5, data), do: sack_range(data)
  defp option(8, <<tsval::32, tsecr::32>>), do: {:ok, {:timestamp, tsval, tsecr}}
  defp option(kind, data), do: {:ok, {:unknown, kind, data}}

  # Up to three blocks; smoltcp drops a fourth.
  defp sack_range(data) when byte_size(data) >= 8 and rem(byte_size(data), 8) == 0 do
    ranges = for <<left::32, right::32 <- data>>, do: {left, right}
    {:ok, {:sack_range, Enum.take(ranges, 3)}}
  end

  defp sack_range(_data), do: :error

  @doc "The bytes of one option, as smoltcp's `TcpOption::emit` writes it."
  @spec emit_option(option()) :: binary()
  def emit_option(:end_of_list), do: <<0>>
  def emit_option(:nop), do: <<1>>
  def emit_option({:mss, value}), do: <<2, 4, value::16>>
  def emit_option({:window_scale, value}), do: <<3, 3, value>>
  def emit_option(:sack_permitted), do: <<4, 2>>

  def emit_option({:sack_range, ranges}) do
    blocks = for {left, right} <- ranges, into: <<>>, do: <<left::32, right::32>>
    <<5, byte_size(blocks) + 2, blocks::binary>>
  end

  def emit_option({:timestamp, tsval, tsecr}), do: <<8, 10, tsval::32, tsecr::32>>
  def emit_option({:unknown, kind, data}), do: <<kind, byte_size(data) + 2, data::binary>>

  @doc "The length of the header `emit/3` writes, a multiple of 4."
  @spec header_len(repr()) :: non_neg_integer()
  def header_len(
        tcp(
          mss: mss,
          window_scale: window_scale,
          sack_permitted: sack_permitted,
          sack_ranges: sack_ranges,
          timestamp: timestamp
        )
      ) do
    length =
      @header_len + if(mss, do: 4, else: 0) + if(window_scale, do: 3, else: 0) +
        if(sack_permitted, do: 2, else: 0) + if(timestamp, do: 10, else: 0) +
        if(sack_ranges == [], do: 0, else: 8 * length(sack_ranges) + 2)

    div(length + 3, 4) * 4
  end

  @doc "The length of a segment, header and payload."
  @spec buffer_len(repr()) :: non_neg_integer()
  def buffer_len(tcp(payload: payload) = repr), do: header_len(repr) + byte_size(payload)

  @doc "The sequence space a segment takes: its payload, and one for SYN or FIN."
  @spec segment_len(repr()) :: non_neg_integer()
  def segment_len(tcp(control: control, payload: payload)) when control in [:syn, :fin],
    do: byte_size(payload) + 1

  def segment_len(tcp(payload: payload)), do: byte_size(payload)

  @spec emit(repr(), address(), address()) :: binary()
  def emit(repr, src, dst) do
    {header, payload} = emit_header(repr, src, dst)
    <<header::binary, payload::binary>>
  end

  @doc """
  `emit/3` in two parts, the header with its checksum and the payload, so
  that a packet can be built around them copying the payload once.
  """
  @spec emit_header(repr(), address(), address()) :: {binary(), binary()}
  def emit_header(
        tcp(
          src_port: src_port,
          dst_port: dst_port,
          control: control,
          seq: seq,
          ack: ack,
          window: window,
          payload: payload
        ) = repr,
        src,
        dst
      ) do
    header_len = header_len(repr)
    options = options(repr)
    padding = header_len - @header_len - byte_size(options)
    flags = control_flag(control) ||| if(ack, do: @ack, else: 0)

    header =
      <<src_port::16, dst_port::16, seq::32, ack || 0::32, div(header_len, 4)::4, 0::4, flags,
        window::16>>

    rest = <<0::16, options::binary, 0::size(padding)-unit(8)>>
    length = header_len + byte_size(payload)
    pseudo = Checksum.pseudo_header(src, dst, @protocol, length)
    checksum = Checksum.checksum(payload, Checksum.sum(rest, Checksum.sum(header, pseudo)))
    {<<header::binary, checksum::16, rest::binary>>, payload}
  end

  defp options(
         tcp(
           mss: mss,
           window_scale: window_scale,
           sack_permitted: sack_permitted,
           sack_ranges: sack_ranges,
           ack: ack,
           timestamp: timestamp
         )
       ) do
    sack =
      cond do
        sack_permitted -> emit_option(:sack_permitted)
        ack != nil and sack_ranges != [] -> emit_option({:sack_range, sack_ranges})
        true -> <<>>
      end

    <<if(mss, do: emit_option({:mss, mss}), else: <<>>)::binary,
      if(window_scale, do: emit_option({:window_scale, window_scale}), else: <<>>)::binary,
      sack::binary, timestamp_option(timestamp)::binary>>
  end

  defp timestamp_option(nil), do: <<>>
  defp timestamp_option({tsval, tsecr}), do: emit_option({:timestamp, tsval, tsecr})

  defp control_flag(:none), do: 0
  defp control_flag(:psh), do: @psh
  defp control_flag(:syn), do: @syn
  defp control_flag(:fin), do: @fin
  defp control_flag(:rst), do: @rst

  @doc """
  The fields of a segment of at least 20 bytes as they are: ports,
  numbers, `:header_len` in bytes, each flag, the window, checksum and
  urgent pointer, `:options` (the bytes up to the data offset) and
  `:payload`, the two nil when the data offset is out of range.
  """
  @spec fields(binary()) :: {:ok, map()} | :error
  def fields(
        <<src_port::16, dst_port::16, seq::32, ack::32, offset::4, _reserved::3, ns::1, cwr::1,
          ece::1, urg::1, ack_flag::1, psh::1, rst::1, syn::1, fin::1, window::16, checksum::16,
          urgent::16, rest::binary>>
      ) do
    header_len = offset * 4
    in_range = header_len >= @header_len and header_len - @header_len <= byte_size(rest)

    {:ok,
     %{
       src_port: src_port,
       dst_port: dst_port,
       seq: seq,
       ack: ack,
       header_len: header_len,
       fin: fin == 1,
       syn: syn == 1,
       rst: rst == 1,
       psh: psh == 1,
       ack_flag: ack_flag == 1,
       urg: urg == 1,
       ece: ece == 1,
       cwr: cwr == 1,
       ns: ns == 1,
       window: window,
       checksum: checksum,
       urgent: urgent,
       options: if(in_range, do: binary_part(rest, 0, header_len - @header_len)),
       payload:
         if(in_range,
           do: binary_part(rest, header_len - @header_len, byte_size(rest) - header_len + 20)
         )
     }}
  end

  def fields(_segment), do: :error

  @doc """
  A segment from fields, the inverse of `fields/1`. Flags default to
  clear, `:header_len` to the header and options; a nil `:checksum` is
  computed between `src` and `dst`.
  """
  @spec build(map(), address(), address()) :: binary()
  def build(fields, src, dst) do
    options = Map.get(fields, :options) || <<>>
    payload = Map.get(fields, :payload) || <<>>
    header_len = Map.get(fields, :header_len, @header_len + byte_size(options))
    bit = &if(Map.get(fields, &1, false), do: 1, else: 0)

    segment = fn checksum ->
      <<Map.get(fields, :src_port, 0)::16, Map.get(fields, :dst_port, 0)::16,
        Map.get(fields, :seq, 0)::32, Map.get(fields, :ack, 0)::32, div(header_len, 4)::4, 0::3,
        bit.(:ns)::1, bit.(:cwr)::1, bit.(:ece)::1, bit.(:urg)::1, bit.(:ack_flag)::1,
        bit.(:psh)::1, bit.(:rst)::1, bit.(:syn)::1, bit.(:fin)::1,
        Map.get(fields, :window, 0)::16, checksum::16, Map.get(fields, :urgent, 0)::16,
        options::binary, payload::binary>>
    end

    case Map.get(fields, :checksum) do
      nil ->
        unsummed = segment.(0)
        pseudo = Checksum.pseudo_header(src, dst, @protocol, byte_size(unsummed))
        segment.(Checksum.checksum(unsummed, pseudo))

      checksum ->
        segment.(checksum)
    end
  end
end
