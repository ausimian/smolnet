defmodule SmolNet.Core.Tcp.Connection do
  @moduledoc false

  # One TCP connection, a pure value: smoltcp 0.14's `socket::tcp::Socket`
  # with SmolNet's patches (ADR 0013), ported function for function so that
  # it sends what the NIF sends. That includes the deliberate deviations:
  # a 200 ms least RTO, 1 s initial and 60 s most; a 10 ms delayed ACK;
  # Nagle's algorithm in Minshall's variant; an initial window of ten
  # segments; a 10 s TIME-WAIT; NewReno, SACK recovery, RACK-TLP and CUBIC.
  #
  # Nothing here reads a clock or draws a random number. Every call that
  # needs them takes a context, `cx`:
  #
  #   * `now`: the time, in microseconds, as smoltcp's `Instant`. Every
  #     instant and duration in the state is in microseconds too, except
  #     the estimator's, which smoltcp keeps in milliseconds (`Rtte`).
  #   * `ip_mtu`: the interface's MTU.
  #   * `addresses`: the interface's addresses, as binaries; a connection
  #     whose local address goes is forgotten.
  #   * `isn`: the initial sequence number to take, should the call open a
  #     connection (`connect/4`, or a SYN reaching a listening one). The
  #     caller draws it, and only keeps the draw when the call took it.
  #   * `source_address`: for `connect/4` without a local address, a
  #     function from the remote address to the local one, or nil.
  #
  # Segments are `SmolNet.Core.Wire.Tcp` records. What a connection sends,
  # an immediate reply from `input/4` or a segment from `dispatch/3`, is
  # `{src_addr, dst_addr, hop_limit, segment}`. Addresses are 4- or 16-byte
  # binaries and endpoints `{address, port}`.
  #
  # A field is named as smoltcp names it, so that a test vector's state
  # maps onto it (`SmolNet.Conformance.TcpReplay.Connection`).

  import Bitwise

  require SmolNet.Core.Wire.Tcp

  alias SmolNet.Core.Tcp.Assembler
  alias SmolNet.Core.Tcp.Cubic
  alias SmolNet.Core.Tcp.Rack
  alias SmolNet.Core.Tcp.RecvBuffer
  alias SmolNet.Core.Tcp.Rtte
  alias SmolNet.Core.Tcp.Scoreboard
  alias SmolNet.Core.Tcp.SendBuffer
  alias SmolNet.Core.Tcp.Seq
  alias SmolNet.Core.Tcp.Timer
  alias SmolNet.Core.Wire.Tcp

  @tcp_header_len 20
  @default_mss 536
  @min_remote_mss 48
  @ipv4_min_path_mtu 576
  @ipv6_min_path_mtu 1280

  @ms 1_000
  @ack_delay_default 10 * @ms
  @tlp_min_timeout 2 * @ms
  @tlp_min_pto 10 * @ms
  @tlp_delayed_ack 200 * @ms
  @challenge_ack_interval 1_000 * @ms

  @usize_max Cubic.usize_max()

  defstruct state: :closed,
            timer: {:idle, nil},
            rtte: %Rtte{},
            assembler: %Assembler{},
            rx_buffer: %RecvBuffer{},
            rx_fin_received: false,
            tx_buffer: %SendBuffer{},
            liveness: %{remote_last_ts: nil, keep_alive_probes: 0, timeout_abort: false},
            wakes: 0,
            listen_endpoint: {nil, 0},
            tuple: nil,
            local_seq_no: 0,
            remote_seq_no: 0,
            remote_last_seq: 0,
            remote_last_ack: nil,
            remote_last_win: 0,
            remote_win_shift: 0,
            remote_win_len: 0,
            remote_win_scale: nil,
            remote_has_sack: false,
            remote_mss: @default_mss,
            path_mtu: nil,
            local_rx_last_seq: nil,
            recovery: %{
              pending_fast_retransmit: false,
              recover: nil,
              recovery_rxt_end: nil,
              scoreboard: [],
              local_rx_dup_acks: 0,
              local_rx_last_ack: nil
            },
            rack: %Rack{},
            options: %{
              timeout: nil,
              keep_alive: nil,
              hop_limit: nil,
              ack_delay: @ack_delay_default,
              nagle: true,
              tsval_generator: nil
            },
            ack_delay_timer: :idle,
            challenge_ack_timer: 0,
            nagle_small_segment_end: nil,
            congestion: nil,
            last_remote_tsval: 0

  @type endpoint :: {binary(), 0..0xFFFF}
  @type t :: %__MODULE__{}
  @type out :: {binary(), binary(), 1..255, Tcp.repr()}
  @type cx :: %{
          required(:now) => integer(),
          optional(:ip_mtu) => pos_integer(),
          optional(:addresses) => [binary()],
          optional(:isn) => Seq.t() | nil,
          optional(:source_address) => (binary() -> binary() | nil) | nil
        }

  @doc """
  A closed connection with receive and send buffers of the given sizes.
  Options: `:assembler_capacity` (32), `:congestion` (`:cubic` or
  `:none`, `:cubic`) and `:tsval_generator` (nil, `{:constant, value}` or
  `{:clock, origin}`, whose TSval is `origin` plus the time in
  milliseconds).
  """
  @spec new(non_neg_integer(), non_neg_integer(), keyword()) :: t()
  def new(rx_capacity, tx_capacity, options \\ []) do
    if rx_capacity > 1 <<< 30,
      do: raise(ArgumentError, "receiving buffer too large, cannot exceed 1 GiB")

    %__MODULE__{
      rtte: Rtte.new(),
      assembler: Assembler.new(Keyword.get(options, :assembler_capacity, 32)),
      rx_buffer: RecvBuffer.new(rx_capacity),
      tx_buffer: SendBuffer.new(tx_capacity),
      remote_win_shift: win_shift(rx_capacity),
      rack: Rack.new(),
      congestion: congestion(Keyword.get(options, :congestion, :cubic)),
      options: %{
        timeout: nil,
        keep_alive: nil,
        hop_limit: nil,
        ack_delay: @ack_delay_default,
        nagle: true,
        tsval_generator: Keyword.get(options, :tsval_generator)
      }
    }
  end

  defp congestion(:cubic), do: Cubic.new()
  defp congestion(:none), do: nil

  defp win_shift(capacity), do: max(bit_length(capacity) - 16, 0)

  defp bit_length(0), do: 0
  defp bit_length(n), do: bit_length(n >>> 1) + 1

  ## Options

  def set_tsval_generator(c, generator), do: option(c, :tsval_generator, generator)
  def timestamp_enabled?(c), do: c.options.tsval_generator != nil

  def set_congestion_control(c, control), do: %{c | congestion: congestion(control)}
  def congestion_control(%__MODULE__{congestion: nil}), do: :none
  def congestion_control(_c), do: :cubic

  def set_timeout(c, duration), do: option(c, :timeout, duration)
  def set_ack_delay(c, duration), do: option(c, :ack_delay, duration)
  def set_nagle_enabled(c, enabled), do: option(c, :nagle, enabled)

  @doc "Keep-alive every `interval`, probing without end, as upstream smoltcp's."
  def set_keep_alive(c, interval) do
    keep_alive = interval && %{idle: interval, interval: interval, probes: nil}
    c = c |> option(:keep_alive, keep_alive) |> liveness(:keep_alive_probes, 0)

    # On an idle connection the option would not take effect until the
    # next packet, so the timer is wound up at once.
    if keep_alive, do: %{c | timer: Timer.set_keep_alive(c.timer)}, else: c
  end

  @doc """
  Keep-alive as Linux sets it, `%{idle, interval, probes}`: the first probe
  is due `idle` after the last packet received.
  """
  def set_keep_alive_config(c, keep_alive) do
    last = c.liveness.remote_last_ts || 0
    c = c |> option(:keep_alive, keep_alive) |> liveness(:keep_alive_probes, 0)
    arm_keep_alive(c, keep_alive && last + keep_alive.idle)
  end

  def aborted_by_timeout?(c), do: c.liveness.timeout_abort

  def set_hop_limit(_c, 0),
    do: raise(ArgumentError, "the time-to-live value of a packet must not be zero")

  def set_hop_limit(c, hop_limit), do: option(c, :hop_limit, hop_limit)

  def local_endpoint(%__MODULE__{tuple: nil}), do: nil
  def local_endpoint(%__MODULE__{tuple: tuple}), do: tuple.local
  def remote_endpoint(%__MODULE__{tuple: nil}), do: nil
  def remote_endpoint(%__MODULE__{tuple: tuple}), do: tuple.remote

  ## Windows

  defp scaled_window(c), do: min(RecvBuffer.window(c.rx_buffer) >>> c.remote_win_shift, 0xFFFF)

  defp last_scaled_window(%__MODULE__{remote_last_ack: nil}), do: nil

  defp last_scaled_window(c) do
    next_ack = Seq.add(c.remote_seq_no, RecvBuffer.len(c.rx_buffer))
    last_win = c.remote_last_win <<< c.remote_win_shift
    adjusted = Seq.diff(Seq.add(c.remote_last_ack, last_win), next_ack)
    min(adjusted >>> c.remote_win_shift, 0xFFFF)
  end

  ## Timer

  defp set_for_idle(c, now),
    do: %{c | timer: Timer.set_for_idle(c.timer, now, c.options.keep_alive)}

  defp rewind_keep_alive(c, now),
    do: %{c | timer: Timer.rewind_keep_alive(c.timer, now, c.options.keep_alive)}

  defp arm_keep_alive(c, at), do: %{c | timer: Timer.arm_keep_alive(c.timer, at)}

  defp set_for_retransmit(c, now, delay),
    do: %{c | timer: Timer.set_for_retransmit(c.timer, now, delay)}

  defp set_for_close(c, now), do: %{c | timer: Timer.set_for_close(c.timer, now)}

  defp set_for_zero_window_probe(c, now, delay),
    do: %{c | timer: Timer.set_for_zero_window_probe(c.timer, now, delay)}

  defp rewind_zero_window_probe(c, now),
    do: %{c | timer: Timer.rewind_zero_window_probe(c.timer, now)}

  ## Congestion control

  defp cc_window(%__MODULE__{congestion: nil}), do: @usize_max
  defp cc_window(c), do: Cubic.window(c.congestion)

  defp cc(%__MODULE__{congestion: nil} = c, _fun), do: c
  defp cc(c, fun), do: %{c | congestion: fun.(c.congestion)}

  defp option(c, key, value), do: %{c | options: Map.replace!(c.options, key, value)}
  defp recovery(c, key, value), do: %{c | recovery: Map.replace!(c.recovery, key, value)}
  defp liveness(c, key, value), do: %{c | liveness: Map.replace!(c.liveness, key, value)}

  # smoltcp wakes both of a socket's wakers on every state change, made or
  # not, the receiver when data arrives in order and the sender when data
  # is acknowledged or a RST is sent. The stack reads these to wake
  # waiters (`take_wakes/1`).
  @wake_rx 1
  @wake_tx 2
  @wake_both 3

  defp wake(c, mask), do: %{c | wakes: bor(c.wakes, mask)}
  defp set_state(c, state), do: wake(%{c | state: state}, @wake_both)

  @doc """
  The wakes since the last call, `{receive?, send?}`, and the connection
  without them.
  """
  @spec take_wakes(t()) :: {{boolean(), boolean()}, t()}
  def take_wakes(c),
    do: {{band(c.wakes, @wake_rx) != 0, band(c.wakes, @wake_tx) != 0}, %{c | wakes: 0}}

  ## Lifecycle

  defp reset(c) do
    c = wake(c, @wake_both)

    %{
      c
      | state: :closed,
        timer: Timer.new(),
        rtte: Rtte.new(),
        assembler: Assembler.clear(c.assembler),
        tx_buffer: SendBuffer.clear(c.tx_buffer),
        rx_buffer: RecvBuffer.clear(c.rx_buffer),
        rx_fin_received: false,
        listen_endpoint: {nil, 0},
        tuple: nil,
        local_seq_no: 0,
        remote_seq_no: 0,
        remote_last_seq: 0,
        remote_last_ack: nil,
        remote_last_win: 0,
        remote_win_len: 0,
        remote_win_scale: nil,
        remote_win_shift: win_shift(RecvBuffer.capacity(c.rx_buffer)),
        remote_mss: @default_mss,
        path_mtu: nil,
        liveness: %{remote_last_ts: nil, keep_alive_probes: 0, timeout_abort: false},
        recovery: %{c.recovery | recover: nil, scoreboard: [], recovery_rxt_end: nil},
        rack: Rack.new(),
        nagle_small_segment_end: nil,
        ack_delay_timer: :idle,
        challenge_ack_timer: 0
    }
  end

  @doc "Listens on `{address | nil, port}`."
  @spec listen(t(), {binary() | nil, 0..0xFFFF}) ::
          {:ok, t()} | {:error, :invalid_state | :unaddressable}
  def listen(_c, {_address, 0}), do: {:error, :unaddressable}

  def listen(c, endpoint) do
    cond do
      open?(c) and c.state == :listen and c.listen_endpoint == endpoint -> {:ok, c}
      open?(c) -> {:error, :invalid_state}
      true -> {:ok, set_state(%{reset(c) | listen_endpoint: endpoint, tuple: nil}, :listen)}
    end
  end

  @doc """
  Connects to `remote` from `{address | nil, port}`, taking `cx.isn`. A
  nil address is chosen by `cx.source_address`.
  """
  @spec connect(t(), cx(), endpoint(), {binary() | nil, 0..0xFFFF}) ::
          {:ok, t()} | {:error, :invalid_state | :unaddressable}
  def connect(c, cx, {remote_addr, remote_port} = remote, {local_addr, local_port}) do
    with true <- not open?(c) || {:error, :invalid_state},
         true <- (remote_port != 0 and not unspecified?(remote_addr)) || {:error, :unaddressable},
         true <- local_port != 0 || {:error, :unaddressable},
         {:ok, local_addr} <- local_address(cx, local_addr, remote_addr),
         true <- byte_size(local_addr) == byte_size(remote_addr) || {:error, :unaddressable} do
      c =
        set_state(
          %{reset(c) | tuple: %{local: {local_addr, local_port}, remote: remote}},
          :syn_sent
        )

      {:ok, %{c | local_seq_no: cx.isn, remote_last_seq: cx.isn}}
    end
  end

  defp local_address(cx, nil = _local, remote) do
    case Map.get(cx, :source_address) do
      nil ->
        {:error, :unaddressable}

      source_address ->
        if address = source_address.(remote), do: {:ok, address}, else: {:error, :unaddressable}
    end
  end

  defp local_address(_cx, local, _remote) do
    if unspecified?(local), do: {:error, :unaddressable}, else: {:ok, local}
  end

  defp unspecified?(address), do: address == <<0::size(byte_size(address))-unit(8)>>

  @doc "Closes the sending half; a connection that never opened closes at once."
  @spec close(t()) :: t()
  def close(%__MODULE__{state: state} = c) when state in [:listen, :syn_sent],
    do: set_state(c, :closed)

  def close(%__MODULE__{state: state} = c) when state in [:syn_received, :established],
    do: c |> restart_timeout_if_idle() |> set_state(:fin_wait_1)

  def close(%__MODULE__{state: :close_wait} = c),
    do: c |> restart_timeout_if_idle() |> set_state(:last_ack)

  def close(c), do: c

  # A FIN queued on an idle connection counts its timeout from when it is
  # sent, not from the last packet received.
  defp restart_timeout_if_idle(c) do
    if SendBuffer.empty?(c.tx_buffer),
      do: c |> liveness(:remote_last_ts, nil) |> liveness(:keep_alive_probes, 0),
      else: c
  end

  @doc "Aborts: the connection is closed and sends one RST, if it has a peer."
  @spec abort(t()) :: t()
  def abort(c), do: set_state(c, :closed)

  @doc "Forgets the connection at once, sending nothing."
  @spec forget(t()) :: t()
  def forget(c), do: reset(c)

  def state(c), do: c.state
  def listening?(c), do: c.state == :listen
  def open?(c), do: c.state not in [:closed, :time_wait]
  def active?(c), do: c.state not in [:closed, :time_wait, :listen]
  def may_send?(c), do: c.state in [:established, :close_wait]

  def may_recv?(c) do
    c.state in [:established, :fin_wait_1, :fin_wait_2] or can_recv?(c)
  end

  def can_send?(c), do: may_send?(c) and not SendBuffer.full?(c.tx_buffer)
  def can_recv?(c), do: not RecvBuffer.empty?(c.rx_buffer)
  def recv_capacity(c), do: RecvBuffer.capacity(c.rx_buffer)
  def send_capacity(c), do: SendBuffer.capacity(c.tx_buffer)
  def send_queue(c), do: SendBuffer.len(c.tx_buffer)
  def recv_queue(c), do: RecvBuffer.len(c.rx_buffer)

  ## Sending and receiving

  @doc "Queues as much of `data` as fits; returns how much did."
  @spec send(t(), binary()) :: {:ok, non_neg_integer(), t()} | {:error, :invalid_state}
  def send(c, data) do
    if may_send?(c) do
      old_length = SendBuffer.len(c.tx_buffer)
      {size, tx_buffer} = SendBuffer.enqueue(c.tx_buffer, data)
      c = %{c | tx_buffer: tx_buffer}
      {:ok, size, if(size > 0, do: queued(c, old_length), else: c)}
    else
      {:error, :invalid_state}
    end
  end

  defp queued(c, old_length) do
    # A connection idle for long must not count its timeout from the last
    # packet received.
    c =
      if old_length == 0,
        do: c |> liveness(:remote_last_ts, nil) |> liveness(:keep_alive_probes, 0),
        else: c

    # With no time at hand smoltcp arms the probe timer at zero, so the
    # first probe goes out at once.
    if c.remote_win_len == 0 and Timer.idle?(c.timer),
      do: set_for_zero_window_probe(c, 0, Rtte.retransmission_timeout(c.rtte)),
      else: c
  end

  defp recv_error_check(c) do
    cond do
      may_recv?(c) -> :ok
      c.rx_fin_received -> {:error, :finished}
      true -> {:error, :invalid_state}
    end
  end

  @doc """
  Takes up to `size` octets, no more than a ring would hand over in one
  piece (smoltcp's `recv`). Returns the data and how much was offered.
  """
  @spec recv(t(), non_neg_integer()) ::
          {:ok, binary(), non_neg_integer(), t()} | {:error, :finished | :invalid_state}
  def recv(c, size) do
    with :ok <- recv_error_check(c) do
      offered = RecvBuffer.contiguous(c.rx_buffer)
      {data, c} = dequeue(c, min(size, offered))
      {:ok, data, offered, c}
    end
  end

  @doc "Takes up to `size` octets, wherever the ring would wrap (`recv_slice`)."
  @spec recv_slice(t(), non_neg_integer()) ::
          {:ok, binary(), t()} | {:error, :finished | :invalid_state}
  def recv_slice(c, size) do
    with :ok <- recv_error_check(c) do
      {data, c} = dequeue(c, size)
      {:ok, data, c}
    end
  end

  defp dequeue(c, size) do
    {data, rx_buffer} = RecvBuffer.dequeue(c.rx_buffer, size)
    {data, %{c | rx_buffer: rx_buffer, remote_seq_no: Seq.add(c.remote_seq_no, byte_size(data))}}
  end

  @doc "Up to `size` octets without taking them; it never fails (`peek_slice`)."
  @spec peek(t(), non_neg_integer()) :: binary()
  def peek(c, size), do: RecvBuffer.peek(c.rx_buffer, size)

  defp flight_size(c), do: Seq.diff(c.remote_last_seq, c.local_seq_no)

  defp send_ip_mtu(c, cx) do
    case c.path_mtu do
      nil -> cx.ip_mtu
      path_mtu -> min(path_mtu, cx.ip_mtu)
    end
  end

  defp ip_header_len(address) when byte_size(address) == 4, do: 20
  defp ip_header_len(_address), do: 40

  ## Path MTU

  @doc """
  Applies an MTU that an ICMP error reports for a segment it quotes, sent
  from `local` to `remote` with sequence number `seq`. Returns
  `:not_matched`, `:rejected`, `:unchanged` or `:reduced`.
  """
  @spec process_path_mtu(t(), cx(), endpoint(), endpoint(), Seq.t(), non_neg_integer()) ::
          {:not_matched | :rejected | :unchanged | :reduced, t()}
  def process_path_mtu(c, cx, local, remote, seq, mtu) do
    if c.tuple == %{local: local, remote: remote},
      do: path_mtu(c, cx, local, seq, mtu),
      else: {:not_matched, c}
  end

  # An MTU below the family's floor counts as the floor. A lower one must
  # be about data in flight (RFC 5927 4.1), so that an error forged
  # without seeing the connection cannot shrink it.
  defp path_mtu(c, cx, {address, _port}, seq, mtu) do
    header_len = ip_header_len(address)
    min_path_mtu = if byte_size(address) == 4, do: @ipv4_min_path_mtu, else: @ipv6_min_path_mtu
    current = send_ip_mtu(c, cx)
    mtu = max(mtu, min_path_mtu)
    sending = c.state in [:established, :fin_wait_1, :close_wait, :closing, :last_ack]
    mss_at = fn mtu -> min(mtu - header_len - @tcp_header_len, c.remote_mss) end

    cond do
      mtu >= current ->
        {:unchanged, c}

      not sending or Seq.lt?(seq, c.local_seq_no) or Seq.ge?(seq, c.remote_last_seq) ->
        {:rejected, c}

      true ->
        reduce_mss(%{c | path_mtu: mtu}, mss_at.(current), mss_at.(mtu))
    end
  end

  # The peer's MSS keeps the segments within the path already.
  defp reduce_mss(c, old_mss, mss) when mss >= old_mss, do: {:unchanged, c}

  # Resends from the last ACK, as a retransmission timeout does: every
  # segment in flight larger than the path was dropped. The drop was not
  # congestion, so the window stays.
  defp reduce_mss(c, _old_mss, mss) do
    c = cc(c, &Cubic.set_mss(&1, mss))

    {:reduced,
     %{
       c
       | remote_last_seq: c.local_seq_no,
         recovery: %{
           c.recovery
           | pending_fast_retransmit: false,
             recover: nil,
             recovery_rxt_end: nil,
             scoreboard: []
         },
         rack: Rack.on_rewind(c.rack),
         nagle_small_segment_end: nil,
         rtte: Rtte.on_retransmit(c.rtte)
     }}
  end

  ## SACK and RACK

  defp sack_blocks(%__MODULE__{remote_has_sack: false}, _repr, _ack), do: []

  defp sack_blocks(c, repr, ack) do
    for {left, right} <- Tcp.tcp(repr, :sack_ranges),
        not (Seq.le?(right, ack) or Seq.gt?(right, c.remote_last_seq)),
        do: {Seq.max(left, ack), right}
  end

  defp sacks_new_data(c, blocks, ack) do
    board = Scoreboard.advance(c.recovery.scoreboard, ack)

    {new_data, board} =
      Enum.reduce(blocks, {false, board}, fn {left, right}, {new_data, board} ->
        {added, board} = Scoreboard.add(board, left, right)
        {new_data or added, board}
      end)

    {new_data, recovery(c, :scoreboard, board)}
  end

  defp has_dsack?(c, repr, ack) do
    case {c.remote_has_sack, Tcp.tcp(repr, :sack_ranges)} do
      {true, [{left, right} | rest]} ->
        Seq.le?(right, ack) or
          case rest do
            [{l, r} | _] -> Seq.le?(l, left) and Seq.le?(right, r)
            [] -> false
          end

      _ ->
        false
    end
  end

  defp on_ack_delivery(c, now, repr, ack) do
    blocks = sack_blocks(c, repr, ack)

    {sample, rack} =
      Rack.on_ack(c.rack, now, c.recovery.scoreboard, {c.local_seq_no, ack}, blocks)

    c = %{c | rack: rack}

    # Karn's algorithm allows a sample from a first transmission. The
    # timer's own sample waits for a cumulative ACK, which recovery holds
    # back, and a retransmission cancels it.
    c =
      if sample != nil and (c.recovery.recover != nil or not Rtte.sampling?(c.rtte)),
        do: %{c | rtte: Rtte.sample(c.rtte, div(sample, @ms))},
        else: c

    {sacks_new_data, c} = sacks_new_data(c, blocks, ack)
    c = %{c | rack: %{c.rack | log: Rack.advance_log(c.rack.log, ack)}}

    c =
      if has_dsack?(c, repr, ack) do
        rack = Rack.on_dsack(c.rack, ack, c.remote_last_seq)
        probe = rack.probe && %{rack.probe | dsacked: true}
        %{c | rack: %{rack | probe: probe}}
      else
        c
      end

    {lost, c} = detect_losses(c, now)
    {sacks_new_data, lost, c}
  end

  defp detect_losses(%__MODULE__{remote_has_sack: false} = c, _now), do: {false, c}

  defp detect_losses(c, now) do
    rack =
      case Scoreboard.lost_below(c.recovery.scoreboard, c.remote_mss) do
        nil -> c.rack
        boundary -> Rack.mark_lost_below(c.rack, boundary)
      end

    {lost, rack} = Rack.detect(rack, c.recovery.scoreboard, now, Rtte.srtt(c.rtte))
    {lost, %{c | rack: rack}}
  end

  defp start_recovery(c) do
    rack = c.rack |> Rack.cancel_probe() |> Map.put(:probe, nil)
    c = recovery(c, :recover, c.remote_last_seq)
    %{c | timer: Timer.set_for_fast_retransmit(c.timer), rack: rack}
  end

  defp in_sack_recovery?(c), do: c.recovery.recover != nil and c.remote_has_sack

  defp pipe(c), do: Rack.pipe(c.rack, c.recovery.scoreboard, c.local_seq_no, c.remote_last_seq)

  defp cwnd_remaining(c) do
    in_flight = if in_sack_recovery?(c), do: pipe(c), else: flight_size(c)

    limited_transmit =
      if c.recovery.recover == nil,
        do: min(c.recovery.local_rx_dup_acks, 2) * c.remote_mss,
        else: 0

    max(min(cc_window(c) + limited_transmit, @usize_max) - in_flight, 0)
  end

  # RFC 6675 `NextSeg` rules (1) and (3): during SACK recovery, the next
  # hole to resend, `{seq, length}`, if the window has room. Rule (1) takes
  # the lowest octets deemed lost; a hole not yet deemed lost is resent
  # only when rule (2) has no new data to send.
  defp sack_retransmission(c, mss) do
    if in_sack_recovery?(c) and cwnd_remaining(c) > 0,
      do: next_resend(c, Rack.first_lost(c.rack, c.recovery.scoreboard, c.remote_last_seq), mss)
  end

  defp next_resend(c, nil = _lost, mss) do
    if no_new_data?(c),
      do: hole_segment(c, Scoreboard.next_hole(c.recovery.scoreboard, rxt_from(c)), mss)
  end

  defp next_resend(c, hole, mss), do: hole_segment(c, hole, mss)

  defp rxt_from(%__MODULE__{recovery: %{recovery_rxt_end: nil}} = c), do: c.local_seq_no
  defp rxt_from(c), do: Seq.max(c.recovery.recovery_rxt_end, c.local_seq_no)

  defp hole_segment(_c, nil, _mss), do: nil

  defp hole_segment(c, {start, end_}, mss) do
    end_ = Seq.min(end_, Seq.add(c.local_seq_no, SendBuffer.len(c.tx_buffer)))
    if Seq.lt?(start, end_), do: {start, min(Seq.diff(end_, start), mss)}
  end

  defp loss_probe_allowed?(c) do
    c.remote_has_sack and c.state in [:established, :close_wait, :fin_wait_1, :closing, :last_ack] and
      Seq.gt?(c.remote_last_seq, c.local_seq_no) and c.recovery.recover == nil and
      c.recovery.scoreboard == [] and
      c.rack.probe == nil and not Rack.recovering?(c.rack) and c.remote_win_len > 0
  end

  defp arm_loss_probe(c, now) do
    c = %{c | rack: Rack.cancel_probe(c.rack)}

    with srtt when srtt != nil <- Rtte.srtt(c.rtte),
         true <- loss_probe_allowed?(c) do
      pto = max(srtt * 2 + @tlp_min_timeout, @tlp_min_pto)
      pto = if flight_size(c) <= c.remote_mss, do: pto + @tlp_delayed_ack, else: pto

      case c.timer do
        {:retransmit, expires_at} when now + pto < expires_at ->
          %{c | rack: %{c.rack | probe_at: now + pto}}

        _ ->
          c
      end
    else
      _ -> c
    end
  end

  defp poll_rack_timers(c, now) do
    c =
      if c.rack.reo_timeout != nil and now >= c.rack.reo_timeout do
        c = %{c | rack: %{c.rack | reo_timeout: nil}}
        {lost, c} = detect_losses(c, now)
        if lost and c.recovery.recover == nil, do: start_recovery(c), else: c
      else
        c
      end

    if c.rack.probe_at != nil and now >= c.rack.probe_at do
      c = %{c | rack: %{c.rack | probe_at: nil}}
      %{c | rack: %{c.rack | probe_pending: loss_probe_allowed?(c)}}
    else
      c
    end
  end

  defp no_new_data?(c) do
    limit = min(SendBuffer.len(c.tx_buffer), c.remote_win_len)
    Seq.ge?(c.remote_last_seq, Seq.add(c.local_seq_no, limit))
  end

  ## Replies

  defp reply({src, dst}, repr) do
    segment =
      Tcp.tcp(
        src_port: Tcp.tcp(repr, :dst_port),
        dst_port: Tcp.tcp(repr, :src_port),
        control: :none,
        seq: 0,
        ack: nil,
        window: 0
      )

    {dst, src, 64, segment}
  end

  @doc """
  The RST answering `repr`, which arrived from `src` to `dst`, as smoltcp's
  `rst_reply`: from its ACK number, acknowledging a bare SYN.
  """
  @spec rst_reply({binary(), binary()}, Tcp.repr()) :: out()
  def rst_reply(ip, repr) do
    {src, dst, hop_limit, segment} = reply(ip, repr)
    ack = Tcp.tcp(repr, :ack)

    ack_number =
      if Tcp.tcp(repr, :control) == :syn and ack == nil,
        do: Seq.add(Tcp.tcp(repr, :seq), Tcp.segment_len(repr))

    {src, dst, hop_limit, Tcp.tcp(segment, control: :rst, seq: ack || 0, ack: ack_number)}
  end

  # The ACK smoltcp's `ack_reply` makes: our next sequence number, the next
  # one we expect, our window, a timestamp echoing the segment's, and SACK
  # blocks, a D-SACK block first if there is one (RFC 2883).
  defp ack_reply(c, cx, seg, dsack \\ nil) do
    {src, dst, hop_limit, segment} = reply(seg.ip, seg.repr)

    timestamp =
      case Tcp.tcp(seg.repr, :timestamp) do
        {tsval, _tsecr} -> timestamp_reply(c, cx, tsval)
        nil -> nil
      end

    ack = Seq.add(c.remote_seq_no, RecvBuffer.len(c.rx_buffer))
    window = scaled_window(c)
    c = %{c | remote_last_ack: ack, remote_last_win: window}

    segment =
      Tcp.tcp(segment,
        timestamp: timestamp,
        seq: c.remote_last_seq,
        ack: ack,
        window: window,
        sack_ranges: sack_ranges(c, ack, dsack)
      )

    {{src, dst, hop_limit, segment}, c}
  end

  defp sack_ranges(%__MODULE__{remote_has_sack: false}, _ack, _dsack), do: []

  defp sack_ranges(c, ack, dsack) do
    ranges = for {l, r} <- Assembler.ranges(c.assembler), do: {Seq.add(ack, l), Seq.add(ack, r)}

    # RFC 2018: the first block is the one holding the segment that drew
    # the ACK; without one, the lowest.
    first = Enum.find(ranges, &holds_last_segment?(c, &1)) || List.first(ranges)
    Enum.reject([dsack, first], &is_nil/1)
  end

  defp holds_last_segment?(%__MODULE__{local_rx_last_seq: nil}, _range), do: false

  defp holds_last_segment?(c, {l, r}),
    do: Seq.le?(l, c.local_rx_last_seq) and Seq.ge?(r, c.local_rx_last_seq)

  # At most one challenge ACK a second (RFC 5961 7).
  defp challenge_ack_reply(c, cx, seg) do
    if cx.now < c.challenge_ack_timer do
      {nil, c}
    else
      ack_reply(%{c | challenge_ack_timer: cx.now + @challenge_ack_interval}, cx, seg)
    end
  end

  defp tsval(%__MODULE__{options: %{tsval_generator: nil}}, _cx), do: nil
  defp tsval(%__MODULE__{options: %{tsval_generator: {:constant, value}}}, _cx), do: value

  defp tsval(%__MODULE__{options: %{tsval_generator: {:clock, origin}}}, cx),
    do: band(origin + div(cx.now, @ms), 0xFFFF_FFFF)

  defp timestamp_reply(c, cx, tsecr) do
    case tsval(c, cx) do
      nil -> nil
      tsval -> {tsval, tsecr}
    end
  end

  @doc "The TSval the connection would send now, or nil without timestamps."
  def current_tsval(c, cx), do: tsval(c, cx)

  ## Ingress

  @doc "Whether a segment from `src` to `dst` is this connection's."
  @spec accepts?(t(), {binary(), binary()}, Tcp.repr()) :: boolean()
  def accepts?(%__MODULE__{state: :closed}, _ip, _repr), do: false

  # A listener takes no ACK or RST; another socket may own it.
  def accepts?(%__MODULE__{state: :listen}, _ip, Tcp.tcp(ack: ack)) when ack != nil, do: false
  def accepts?(%__MODULE__{state: :listen}, _ip, Tcp.tcp(control: :rst)), do: false

  def accepts?(%__MODULE__{tuple: nil, listen_endpoint: {address, port}}, {_src, dst}, repr) do
    dst_port = Tcp.tcp(repr, :dst_port)
    (address == nil or dst == address) and dst_port != 0 and dst_port == port
  end

  def accepts?(%__MODULE__{tuple: tuple}, {src, dst}, repr) do
    {dst, Tcp.tcp(repr, :dst_port)} == tuple.local and
      {src, Tcp.tcp(repr, :src_port)} == tuple.remote
  end

  @doc """
  A segment arrives from `src` to `dst`; it must be accepted
  (`accepts?/3`). Returns the immediate reply, if any.
  """
  @spec input(t(), cx(), {binary(), binary()}, Tcp.repr()) :: {out() | nil, t()}
  def input(c, cx, ip, repr) do
    # What of the sequence space we sent is a control flag, not data.
    {sent_syn, sent_fin} =
      case c.state do
        state when state in [:syn_sent, :syn_received] -> {true, false}
        state when state in [:fin_wait_1, :last_ack, :closing] -> {false, true}
        _ -> {false, false}
      end

    seg = %{ip: ip, repr: repr, sent_syn: sent_syn, sent_fin: sent_fin}

    case check_ack(c.state, Tcp.tcp(repr, :control), Tcp.tcp(repr, :ack), c, cx, seg) do
      {:reject, reply, c} -> {reply, c}
      :ok -> check_window(c, cx, seg)
    end
  end

  # Unacceptable acknowledgements, in smoltcp's order.
  defp check_ack(:syn_sent, :rst, nil, c, _cx, _seg), do: {:reject, nil, c}

  defp check_ack(:syn_sent, :rst, ack, c, _cx, _seg),
    do: if(ack == Seq.add(c.local_seq_no, 1), do: :ok, else: {:reject, nil, c})

  defp check_ack(_state, :rst, _ack, _c, _cx, _seg), do: :ok
  defp check_ack(:listen, _control, nil, _c, _cx, _seg), do: :ok

  defp check_ack(:syn_sent, :syn, ack, c, _cx, seg) when ack != nil,
    do:
      if(ack == Seq.add(c.local_seq_no, 1),
        do: :ok,
        else: {:reject, rst_reply(seg.ip, seg.repr), c}
      )

  defp check_ack(:syn_sent, :syn, nil, _c, _cx, _seg), do: :ok

  # The right ACK number is ignored rather than reset, as Linux does.
  defp check_ack(:syn_sent, :none, ack, c, _cx, seg) when ack != nil do
    if ack == Seq.add(c.local_seq_no, 1),
      do: {:reject, nil, c},
      else: {:reject, rst_reply(seg.ip, seg.repr), c}
  end

  defp check_ack(:syn_sent, _control, _ack, c, _cx, _seg), do: {:reject, nil, c}
  defp check_ack(_state, _control, nil, c, _cx, _seg), do: {:reject, nil, c}

  defp check_ack(:syn_received, _control, ack, c, _cx, seg),
    do:
      if(ack == Seq.add(c.local_seq_no, 1),
        do: :ok,
        else: {:reject, rst_reply(seg.ip, seg.repr), c}
      )

  # Every other ACK must be of something sent and not yet acknowledged.
  defp check_ack(_state, _control, ack, c, cx, seg) do
    control_len = if(seg.sent_syn, do: 1, else: 0) + if(seg.sent_fin, do: 1, else: 0)
    ack_min = if seg.sent_syn, do: Seq.add(c.local_seq_no, 1), else: c.local_seq_no
    ack_max = Seq.add(c.local_seq_no, SendBuffer.len(c.tx_buffer) + control_len)

    cond do
      Seq.lt?(ack, ack_min) ->
        {:reject, nil, c}

      Seq.gt?(ack, ack_max) ->
        {reply, c} = challenge_ack_reply(c, cx, seg)
        {:reject, reply, c}

      true ->
        :ok
    end
  end

  defp check_window(c, cx, seg) do
    window_start = Seq.add(c.remote_seq_no, RecvBuffer.len(c.rx_buffer))

    window_end =
      case c.remote_last_ack do
        nil -> window_start
        last_ack -> Seq.add(last_ack, c.remote_last_win <<< c.remote_win_shift)
      end

    segment_start = Tcp.tcp(seg.repr, :seq)
    segment_end = Seq.add(segment_start, byte_size(Tcp.tcp(seg.repr, :payload)))
    window = {window_start, window_end, segment_start, segment_end}

    cond do
      # Not yet synchronized: no data is taken.
      c.state in [:listen, :syn_sent] ->
        after_window(c, cx, seg, %{
          payload: <<>>,
          offset: 0,
          dsack: nil,
          window_start: window_start
        })

      in_window?(window) ->
        in_window(c, cx, seg, window)

      true ->
        out_of_window(c, cx, seg, window)
    end
  end

  # RFC 9293 3.10.7.4's acceptability tests.
  defp in_window?({window_start, window_end, segment_start, segment_end})
       when segment_start == segment_end do
    cond do
      # A keep-alive or window probe, which draws an ACK.
      segment_end == Seq.sub(window_start, 1) -> false
      window_start == window_end -> window_start == segment_start
      true -> Seq.le?(window_start, segment_start) and Seq.lt?(segment_start, window_end)
    end
  end

  # Data in a zero window is not taken; the segment draws an ACK.
  defp in_window?({window_start, window_end, _segment_start, _segment_end})
       when window_start == window_end,
       do: false

  defp in_window?({window_start, window_end, segment_start, segment_end}) do
    (Seq.le?(window_start, segment_start) and Seq.lt?(segment_start, window_end)) or
      (Seq.lt?(window_start, segment_end) and Seq.le?(segment_end, window_end))
  end

  defp in_window(c, cx, seg, {window_start, window_end, segment_start, segment_end}) do
    overlap_start = Seq.max(window_start, segment_start)
    overlap_end = Seq.min(window_end, segment_end)
    from = Seq.diff(overlap_start, segment_start)

    payload =
      binary_part(Tcp.tcp(seg.repr, :payload), from, Seq.diff(overlap_end, segment_start) - from)

    data = %{
      payload: payload,
      offset: Seq.diff(overlap_start, window_start),
      # Data received before, which the ACK reports as a D-SACK.
      dsack: if(Seq.lt?(segment_start, window_start), do: {segment_start, window_start}),
      window_start: window_start
    }

    after_window(%{c | local_rx_last_seq: segment_start}, cx, seg, data)
  end

  # Out-of-window RSTs are dropped silently (RFC 9293 3.10.7.4, RFC 5961
  # 3.2), and do not refresh TIME-WAIT.
  defp out_of_window(c, _cx, %{repr: Tcp.tcp(control: :rst)}, _window), do: {nil, c}

  defp out_of_window(c, cx, seg, {window_start, _window_end, segment_start, segment_end}) do
    c = if c.state == :time_wait, do: set_for_close(c, cx.now), else: c

    # Segments carrying data are exempt from the challenge ACK rate limit:
    # they are a resend whose ACK was lost, or a window probe. A SYN is not.
    if Tcp.tcp(seg.repr, :payload) != <<>> and Tcp.tcp(seg.repr, :control) in [:none, :psh, :fin] do
      dsack =
        if Seq.lt?(segment_start, window_start),
          do: {segment_start, Seq.min(segment_end, window_start)}

      ack_reply(c, cx, seg, dsack)
    else
      challenge_ack_reply(c, cx, seg)
    end
  end

  defp after_window(c, cx, seg, data) do
    acked = acked(c, seg)

    case transition(c.state, control(seg, data.window_start), c, cx, seg, acked) do
      {:reject, reply, c} ->
        {reply, c}

      {:ok, c} ->
        c
        |> update_remote(cx, seg.repr, acked)
        |> receive_payload(cx, seg, data)
    end
  end

  # How much of what we sent the segment acknowledges, less the SYN and
  # FIN: `{length, of our FIN, of everything sent}`.
  defp acked(c, seg) do
    case {Tcp.tcp(seg.repr, :control), Tcp.tcp(seg.repr, :ack)} do
      {control, ack} when control != :rst and ack != nil ->
        tx_start = Seq.add(c.local_seq_no, if(seg.sent_syn, do: 1, else: 0))

        if Seq.ge?(ack, tx_start),
          do: acked(c, seg, Seq.diff(ack, tx_start), ack),
          else: {0, false, false}

      _ ->
        {0, false, false}
    end
  end

  defp acked(c, seg, ack_len, ack) do
    # Data may have gone before the FIN, which is acknowledged only with it.
    of_fin = seg.sent_fin and SendBuffer.len(c.tx_buffer) + 1 == ack_len
    {if(of_fin, do: ack_len - 1, else: ack_len), of_fin, Seq.le?(c.remote_last_seq, ack)}
  end

  # The control flag to act on: PSH means nothing here, and a FIN after a
  # hole waits for the hole to fill.
  defp control(seg, window_start) do
    case Tcp.tcp(seg.repr, :control) do
      :psh -> :none
      :fin -> if Seq.lt?(window_start, Tcp.tcp(seg.repr, :seq)), do: :none, else: :fin
      control -> control
    end
  end

  # The state machine, `(state, control)` as smoltcp matches them.
  defp transition(:listen, :rst, c, _cx, _seg, _acked), do: {:reject, nil, c}

  # SYN-RECEIVED from LISTEN goes back to listening; from a simultaneous
  # open it closes.
  defp transition(
         :syn_received,
         :rst,
         %__MODULE__{listen_endpoint: {_address, port}} = c,
         _cx,
         _seg,
         _acked
       )
       when port != 0,
       do: {:reject, nil, set_state(%{c | tuple: nil}, :listen)}

  defp transition(_state, :rst, c, _cx, _seg, _acked),
    do: {:reject, nil, set_state(%{c | tuple: nil}, :closed)}

  defp transition(:listen, :syn, c, cx, seg, _acked) do
    repr = seg.repr
    {src, dst} = seg.ip
    c = take_mss(c, repr)

    c = %{
      c
      | tuple: %{local: {dst, Tcp.tcp(repr, :dst_port)}, remote: {src, Tcp.tcp(repr, :src_port)}},
        local_seq_no: cx.isn,
        remote_seq_no: Seq.add(Tcp.tcp(repr, :seq), 1),
        remote_last_seq: cx.isn,
        remote_has_sack: Tcp.tcp(repr, :sack_permitted),
        remote_win_scale: Tcp.tcp(repr, :window_scale)
    }

    c = take_peer_options(c, repr)
    {:ok, set_for_idle(set_state(c, :syn_received), cx.now)}
  end

  defp transition(:syn_received, :none, c, _cx, _seg, _acked),
    do: {:ok, set_state(c, :established)}

  defp transition(:syn_received, :fin, c, _cx, _seg, _acked),
    do: {:ok, fin_received(c, :close_wait)}

  defp transition(:syn_sent, :syn, c, _cx, seg, _acked) do
    repr = seg.repr
    seq = Tcp.tcp(repr, :seq)
    c = take_mss(c, repr)

    c = %{
      c
      | remote_seq_no: Seq.add(seq, 1),
        remote_last_seq: Seq.add(c.local_seq_no, 1),
        remote_last_ack: seq,
        remote_has_sack: Tcp.tcp(repr, :sack_permitted),
        remote_win_scale: Tcp.tcp(repr, :window_scale)
    }

    c = take_peer_options(c, repr)

    # A SYN-ACK establishes; a bare SYN is a simultaneous open.
    state = if Tcp.tcp(repr, :ack) != nil, do: :established, else: :syn_received
    {:ok, set_state(c, state)}
  end

  defp transition(:established, :none, c, _cx, _seg, _acked), do: {:ok, c}

  defp transition(:established, :fin, c, _cx, _seg, _acked),
    do: {:ok, fin_received(c, :close_wait)}

  defp transition(:fin_wait_1, :none, c, _cx, _seg, {_len, of_fin, _all}),
    do: {:ok, if(of_fin, do: set_state(c, :fin_wait_2), else: c)}

  defp transition(:fin_wait_1, :fin, c, cx, _seg, {_len, true, _all}),
    do: {:ok, set_for_close(fin_received(c, :time_wait), cx.now)}

  defp transition(:fin_wait_1, :fin, c, _cx, _seg, _acked), do: {:ok, fin_received(c, :closing)}
  defp transition(:fin_wait_2, :none, c, _cx, _seg, _acked), do: {:ok, c}

  defp transition(:fin_wait_2, :fin, c, cx, _seg, _acked),
    do: {:ok, set_for_close(fin_received(c, :time_wait), cx.now)}

  defp transition(:closing, :none, c, cx, _seg, {_len, true, _all}),
    do: {:ok, set_for_close(set_state(c, :time_wait), cx.now)}

  defp transition(:closing, :none, c, _cx, _seg, _acked), do: {:ok, c}
  defp transition(:close_wait, :none, c, _cx, _seg, _acked), do: {:ok, c}

  # Forgets the peer once the FIN is acknowledged, or a RST would go to it.
  defp transition(:last_ack, :none, c, _cx, _seg, {_len, true, _all}),
    do: {:ok, set_state(%{c | tuple: nil}, :closed)}

  # A duplicate ACK: our FIN is not acknowledged (RFC 9293 3.10.7.4).
  defp transition(:last_ack, :none, c, cx, seg, {0, false, _all}) do
    {reply, c} = challenge_ack_reply(c, cx, seg)
    {:reject, reply, c}
  end

  defp transition(:last_ack, :none, c, _cx, _seg, _acked), do: {:ok, c}
  defp transition(_state, _control, c, _cx, _seg, _acked), do: {:reject, nil, c}

  defp fin_received(c, state),
    do: set_state(%{c | remote_seq_no: Seq.add(c.remote_seq_no, 1), rx_fin_received: true}, state)

  # A zero MSS is as no MSS, as Linux takes it; a tiny one is raised.
  defp take_mss(c, repr) do
    case Tcp.tcp(repr, :mss) do
      mss when mss in [nil, 0] ->
        c

      mss ->
        remote_mss = max(mss, @min_remote_mss)
        cc(%{c | remote_mss: remote_mss}, &Cubic.set_mss(&1, remote_mss))
    end
  end

  # No window scaling or timestamps unless the peer offers them too.
  defp take_peer_options(c, repr) do
    c = if c.remote_win_scale == nil, do: %{c | remote_win_shift: 0}, else: c
    if Tcp.tcp(repr, :timestamp) == nil, do: option(c, :tsval_generator, nil), else: c
  end

  defp update_remote(c, cx, repr, {ack_len, _of_fin, ack_all}) do
    now = cx.now

    # Any packet answers the keep-alive probes.
    c = c |> liveness(:remote_last_ts, now) |> liveness(:keep_alive_probes, 0)

    # RFC 7323: the window of every segment but a SYN is scaled.
    scale = if Tcp.tcp(repr, :control) == :syn, do: 0, else: c.remote_win_scale || 0
    new_remote_win_len = Tcp.tcp(repr, :window) <<< scale
    is_window_update = new_remote_win_len != c.remote_win_len
    c = %{c | remote_win_len: new_remote_win_len}
    c = cc(c, &Cubic.set_remote_window(&1, new_remote_win_len))

    c =
      if ack_len > 0,
        do: wake(%{c | tx_buffer: SendBuffer.dequeue(c.tx_buffer, ack_len)}, @wake_tx),
        else: c

    {c, rack_lost} =
      case Tcp.tcp(repr, :ack) do
        nil -> {c, false}
        ack -> process_ack(c, now, repr, ack, ack_len, is_window_update)
      end

    c =
      case Tcp.tcp(repr, :timestamp) do
        {tsval, _tsecr} -> %{c | last_remote_tsval: tsval}
        nil -> c
      end

    c
    |> retransmit_timer_on_ack(now, ack_len, ack_all)
    |> zero_window_timer_on_ack(now, ack_len)
    |> loss_on_ack(now, ack_len, rack_lost)
  end

  # RFC 6298 (5.2), (5.3): an ACK of everything stops the retransmission
  # timer, an ACK of new data restarts it. Any packet refreshes keep-alive.
  defp retransmit_timer_on_ack(c, now, ack_len, ack_all) do
    cond do
      Timer.retransmit?(c.timer) and ack_all ->
        set_for_idle(c, now)

      Timer.retransmit?(c.timer) and ack_len > 0 ->
        set_for_retransmit(c, now, Rtte.retransmission_timeout(c.rtte))

      Timer.idle?(c.timer) ->
        set_for_idle(c, now)

      true ->
        c
    end
  end

  defp zero_window_timer_on_ack(c, now, ack_len) do
    cond do
      c.remote_win_len == 0 and not SendBuffer.empty?(c.tx_buffer) and
          (Timer.idle?(c.timer) or ack_len > 0) ->
        set_for_zero_window_probe(c, now, Rtte.retransmission_timeout(c.rtte))

      c.remote_win_len != 0 and Timer.zero_window_probe?(c.timer) ->
        set_for_idle(c, now)

      true ->
        c
    end
  end

  # RFC 8985 6.2: data RACK deems lost starts recovery, after the timer has
  # restarted for a partial ACK. 7.2: an ACK of new data restarts the loss
  # probe timer.
  defp loss_on_ack(c, now, ack_len, rack_lost) do
    c = if rack_lost and c.recovery.recover == nil, do: start_recovery(c), else: c

    cond do
      ack_len > 0 -> arm_loss_probe(c, now)
      not loss_probe_allowed?(c) -> %{c | rack: Rack.cancel_probe(c.rack)}
      true -> c
    end
  end

  defp process_ack(c, now, repr, ack, ack_len, is_window_update) do
    {sacks_new_data, rack_lost, c} = on_ack_delivery(c, now, repr, ack)
    c = end_probe_episode(c, now, ack)

    # RFC 6675 (2): an ACK SACKing new data is a duplicate whatever its
    # window, as Linux's autotuned windows grow on nearly every ACK.
    duplicate =
      c.recovery.local_rx_last_ack != nil and Tcp.tcp(repr, :payload) == <<>> and
        c.recovery.local_rx_last_ack == ack and Seq.lt?(ack, c.remote_last_seq) and
        (not is_window_update or sacks_new_data)

    c = if duplicate, do: duplicate_ack(c, ack), else: fresh_ack(c, now, ack, ack_len)
    {advance_una(c, ack), rack_lost}
  end

  defp advance_una(c, ack) do
    c = %{c | local_seq_no: ack}

    c =
      if c.nagle_small_segment_end != nil and Seq.le?(c.nagle_small_segment_end, ack),
        do: %{c | nagle_small_segment_end: nil},
        else: c

    # An earlier loss may let the ACK pass what was resent since.
    if Seq.lt?(c.remote_last_seq, ack), do: %{c | remote_last_seq: ack}, else: c
  end

  # RFC 8985 7.4: a probe episode ends once what was in flight is
  # acknowledged. A resent probe with no D-SACK repaired a loss, and the
  # window answers for it.
  defp end_probe_episode(%__MODULE__{rack: %Rack{probe: %{end: probe_end} = probe}} = c, now, ack) do
    if Seq.ge?(ack, probe_end) do
      c = %{c | rack: %{c.rack | probe: nil}}

      if probe.retransmitted and not probe.dsacked do
        in_flight = flight_size(c)
        cc(c, &Cubic.on_loss(&1, now, in_flight))
      else
        c
      end
    else
      c
    end
  end

  defp end_probe_episode(c, _now, _ack), do: c

  defp duplicate_ack(c, ack) do
    c = recovery(c, :local_rx_dup_acks, min(c.recovery.local_rx_dup_acks + 1, 255))

    # RFC 6582: in recovery, partial ACKs drive the resends. RFC 6675 also
    # starts it once SACKs show the first unacknowledged segment lost.
    head_lost =
      case Scoreboard.lost_below(c.recovery.scoreboard, c.remote_mss) do
        nil -> false
        lost -> Seq.lt?(ack, lost)
      end

    c =
      if (c.recovery.local_rx_dup_acks == 3 or head_lost) and c.recovery.recover == nil,
        do: start_recovery(c),
        else: c

    # SACK recovery measures the data in flight with `pipe`, which already
    # leaves out what each duplicate SACKs.
    if in_sack_recovery?(c), do: c, else: cc(c, &Cubic.on_dup_ack(&1, c.remote_mss))
  end

  defp fresh_ack(c, now, ack, ack_len) do
    recovery = %{c.recovery | local_rx_dup_acks: 0, local_rx_last_ack: ack}
    c = %{c | recovery: recovery, rtte: Rtte.on_ack(c.rtte, now, ack)}

    new_flight_size = max(flight_size(c) - ack_len, 0)
    smoothed_rtt = Rtte.smoothed_rtt(c.rtte)
    c = cc(c, &Cubic.on_ack(&1, now, ack_len, new_flight_size, smoothed_rtt))
    if c.recovery.recover != nil and ack_len > 0, do: recovery_ack(c, ack), else: c
  end

  # RFC 6582: an ACK reaching `recover` ends fast recovery, and one short of
  # it is partial: the segment it points at was lost too, and is resent at
  # once, unless SACK recovery has resent it already.
  defp recovery_ack(c, ack) do
    cond do
      Seq.ge?(ack, c.recovery.recover) ->
        recovery = %{
          c.recovery
          | recover: nil,
            recovery_rxt_end: nil,
            pending_fast_retransmit: false
        }

        %{c | recovery: recovery, rack: Rack.on_recovery_end(c.rack)}

      not c.remote_has_sack or c.recovery.recovery_rxt_end == nil or
          Seq.le?(c.recovery.recovery_rxt_end, ack) ->
        %{recovery(c, :pending_fast_retransmit, true) | rtte: Rtte.on_retransmit(c.rtte)}

      true ->
        c
    end
  end

  defp receive_payload(c, _cx, _seg, %{payload: <<>>}), do: {nil, c}

  defp receive_payload(c, cx, seg, data) do
    %{payload: payload, offset: offset, window_start: window_start} = data
    assembler_was_empty = Assembler.empty?(c.assembler)
    dsack = data.dsack || repeated(c, offset, byte_size(payload), window_start)

    case Assembler.add_then_remove_front(c.assembler, offset, byte_size(payload)) do
      :error ->
        # Too many holes: dropped, without an ACK (#117).
        {nil, c}

      {:ok, contig_len, assembler} ->
        {_written, rx_buffer} = RecvBuffer.write(c.rx_buffer, offset, payload)
        c = %{c | assembler: assembler, rx_buffer: RecvBuffer.enqueue(rx_buffer, contig_len)}
        c = if contig_len != 0, do: wake(c, @wake_rx), else: c
        c = delay_ack(c, cx)

        # RFC 5681: out-of-order data, or data filling a hole, is ACKed at
        # once; so is a duplicate from a peer with SACK, for its D-SACK.
        if (dsack != nil and c.remote_has_sack) or not Assembler.empty?(c.assembler) or
             not assembler_was_empty,
           do: ack_reply(c, cx, seg, dsack),
           else: {nil, c}
    end
  end

  # Out-of-order data the segment repeats is a duplicate too; the first
  # range it repeats stands for them all.
  defp repeated(c, offset, size, window_start) do
    Enum.find_value(Assembler.ranges(c.assembler), fn {left, right} ->
      {left, right} = {max(left, offset), min(right, offset + size)}
      if left < right, do: {Seq.add(window_start, left), Seq.add(window_start, right)}
    end)
  end

  defp delay_ack(%__MODULE__{options: %{ack_delay: nil}} = c, _cx), do: c

  defp delay_ack(c, cx) do
    if ack_to_transmit?(c),
      do: %{c | ack_delay_timer: ack_delay_timer(c, cx)},
      else: c
  end

  # The delayed ACK waits, unless a full segment's worth is unacknowledged.
  defp ack_delay_timer(%__MODULE__{ack_delay_timer: :idle} = c, cx),
    do: {:waiting, cx.now + c.options.ack_delay}

  defp ack_delay_timer(%__MODULE__{ack_delay_timer: {:waiting, _at} = timer} = c, _cx),
    do: if(immediate_ack_to_transmit?(c), do: :immediate, else: timer)

  defp ack_delay_timer(_c, _cx), do: :immediate

  ## Timers and egress

  defp timed_out?(c, now) do
    case {c.liveness.remote_last_ts, c.options.timeout} do
      {last, timeout} when last != nil and timeout != nil ->
        awaits_remote?(c) and now >= last + timeout

      _ ->
        false
    end
  end

  # The timeout applies while the connection waits on its peer: in the
  # handshake, with data or a FIN outstanding or unsent, and while it sends
  # keep-alive probes with no limit of their own. An idle connection does
  # not wait.
  defp awaits_remote?(c) do
    case c.state do
      state when state in [:closed, :listen, :time_wait] ->
        false

      state when state in [:syn_sent, :syn_received, :fin_wait_1, :closing, :last_ack] ->
        true

      _established ->
        not SendBuffer.empty?(c.tx_buffer) or match?(%{probes: nil}, c.options.keep_alive)
    end
  end

  defp keep_alive_exhausted?(c, now) do
    case c.options.keep_alive do
      %{probes: probes} when probes != nil and c.liveness.keep_alive_probes >= probes ->
        Timer.should_keep_alive?(c.timer, now)

      _ ->
        false
    end
  end

  defp effective_mss(c, cx, options_len) do
    {address, _port} = c.tuple.local
    local_mss = send_ip_mtu(c, cx) - ip_header_len(address) - @tcp_header_len
    max(min(local_mss, c.remote_mss) - options_len, 0)
  end

  # Whether there is data or a control flag to send.
  defp seq_to_transmit?(c, cx) do
    cond do
      # A fast retransmission or a loss probe goes whatever the windows.
      c.recovery.pending_fast_retransmit and not SendBuffer.empty?(c.tx_buffer) -> true
      c.rack.probe_pending and loss_probe_allowed?(c) -> true
      true -> new_seq_to_transmit?(c, cx)
    end
  end

  defp new_seq_to_transmit?(c, cx) do
    effective_mss = effective_mss(c, cx, if(c.options.tsval_generator != nil, do: 12, else: 0))
    data_in_flight = c.remote_last_seq != c.local_seq_no

    cond do
      # A SYN not yet sent.
      c.state in [:syn_sent, :syn_received] and not data_in_flight -> true
      sack_retransmission(c, effective_mss) != nil -> true
      true -> data_or_fin_to_transmit?(c, effective_mss, data_in_flight)
    end
  end

  defp data_or_fin_to_transmit?(c, effective_mss, data_in_flight) do
    tx_len = SendBuffer.len(c.tx_buffer)
    max_send_seq = Seq.add(c.local_seq_no, min(c.remote_win_len, tx_len))

    capped_send =
      if Seq.ge?(max_send_seq, c.remote_last_seq),
        do: Seq.diff(max_send_seq, c.remote_last_seq),
        else: 0

    max_send = min(capped_send, cwnd_remaining(c))
    want_fin = c.state in [:fin_wait_1, :closing, :last_ack]
    held = data_in_flight and not want_fin and nagle_holds?(c, max_send, effective_mss)
    can_fin = want_fin and c.remote_last_seq == Seq.add(c.local_seq_no, tx_len)
    (max_send != 0 and not held) or can_fin
  end

  # Nagle's algorithm in Minshall's variant, as Linux has it: a segment
  # short of a full MSS waits while another such is in flight, unless it
  # carries the last of the data.
  defp nagle_holds?(c, max_send, effective_mss) do
    c.options.nagle and c.nagle_small_segment_end != nil and
      Seq.gt?(c.nagle_small_segment_end, c.local_seq_no) and max_send < effective_mss
  end

  defp delayed_ack_expired?(%__MODULE__{ack_delay_timer: {:waiting, at}}, now), do: at <= now
  defp delayed_ack_expired?(_c, _now), do: true

  defp ack_to_transmit?(%__MODULE__{remote_last_ack: nil}), do: false

  defp ack_to_transmit?(c),
    do: Seq.lt?(c.remote_last_ack, Seq.add(c.remote_seq_no, RecvBuffer.len(c.rx_buffer)))

  # An ACK at least every RMSS octets, as Linux sends.
  defp immediate_ack_to_transmit?(%__MODULE__{remote_last_ack: nil}), do: false

  defp immediate_ack_to_transmit?(c) do
    Seq.lt?(
      Seq.add(c.remote_last_ack, c.remote_mss),
      Seq.add(c.remote_seq_no, RecvBuffer.len(c.rx_buffer))
    )
  end

  # A window that has doubled is worth telling the peer at once.
  defp window_to_update?(%__MODULE__{state: state} = c)
       when state in [:syn_sent, :syn_received, :established, :fin_wait_1, :fin_wait_2] do
    case last_scaled_window(c) do
      nil -> false
      last_win -> scaled_window(c) > 0 and div(scaled_window(c), 2) >= last_win
    end
  end

  defp window_to_update?(_c), do: false

  @doc """
  Asks for one segment to send at `cx.now`. Returns `{:segment, out, c}`
  or `{:nothing, c}`. With `:refuse` the device turns the segment down, as
  a full one would: `{:refused, out, c}`, the connection keeping what it
  did before trying to send, so that it tries again.
  """
  @spec dispatch(t(), cx(), :accept | :refuse) ::
          {:segment, out(), t()} | {:refused, out(), t()} | {:nothing, t()}
  def dispatch(c, cx, device \\ :accept)

  def dispatch(%__MODULE__{tuple: nil} = c, _cx, _device), do: {:nothing, c}

  def dispatch(c, cx, device) do
    {local_addr, _port} = c.tuple.local

    if local_addr in Map.get(cx, :addresses, [local_addr]) do
      c |> expire_timers(cx.now) |> decide(cx, device)
    else
      # The interface lost the address: forgotten without a RST, which
      # could not be sent from it.
      {:nothing, reset(c)}
    end
  end

  defp expire_timers(c, now) do
    # Quiet until now: the timeout counts from the first segment sent.
    c = if c.liveness.remote_last_ts == nil, do: liveness(c, :remote_last_ts, now), else: c

    # An expired retransmission timer supersedes RACK's timers.
    c = if Timer.should_retransmit?(c.timer, now), do: c, else: poll_rack_timers(c, now)

    cond do
      timed_out?(c, now) or keep_alive_exhausted?(c, now) ->
        c |> liveness(:timeout_abort, true) |> set_state(:closed)

      Timer.should_retransmit?(c.timer, now) ->
        retransmit(c, now)

      true ->
        c
    end
  end

  defp retransmit(c, now) do
    in_flight = flight_size(c)

    c =
      case c.timer do
        {:retransmit, _at} ->
          # Resends everything from the last ACK, SACKed data too, so the
          # scoreboard starts afresh (RFC 6675 5.1).
          c = cc(c, &Cubic.on_rto(&1, in_flight))

          %{
            c
            | remote_last_seq: c.local_seq_no,
              recovery: %{c.recovery | recover: nil, recovery_rxt_end: nil, scoreboard: []},
              rack: Rack.on_rewind(c.rack),
              rtte: Rtte.on_rto(c.rtte)
          }

        :fast_retransmit ->
          c = cc(c, &Cubic.on_loss(&1, now, in_flight))
          recovery(c, :pending_fast_retransmit, true)
      end

    # Clears the expired timer even if nothing can be resent now, as with
    # a zero window, so that the deadline does not stay in the past.
    c = set_for_idle(c, now)
    %{c | rtte: Rtte.on_retransmit(c.rtte)}
  end

  # Why a segment would be sent, in smoltcp's order.
  defp decide(c, cx, device) do
    if send_reason?(c, cx) do
      build(c, cx, device)
    else
      # TIME-WAIT over: the connection is forgotten.
      if Timer.should_close?(c.timer, cx.now), do: {:nothing, reset(c)}, else: {:nothing, c}
    end
  end

  defp send_reason?(c, cx) do
    seq_to_transmit?(c, cx) or (ack_to_transmit?(c) and delayed_ack_expired?(c, cx.now)) or
      window_to_update?(c) or c.state == :closed or Timer.should_keep_alive?(c.timer, cx.now) or
      Timer.should_zero_window_probe?(c.timer, cx.now)
  end

  @flags %{
    zero_window_probe: false,
    fast_retransmit: false,
    sack_retransmission: false,
    loss_probe: false,
    probe_retransmission: false,
    small_segment: false
  }

  defp build(%__MODULE__{state: :listen} = c, _cx, _device), do: {:nothing, c}

  defp build(c, cx, device) do
    {local_addr, local_port} = c.tuple.local
    {remote_addr, remote_port} = c.tuple.remote

    # An empty ACK, made more specific as needed.
    repr =
      Tcp.tcp(
        src_port: local_port,
        dst_port: remote_port,
        control: :none,
        seq: c.remote_last_seq,
        ack: Seq.add(c.remote_seq_no, RecvBuffer.len(c.rx_buffer)),
        window: scaled_window(c),
        timestamp: timestamp_reply(c, cx, c.last_remote_tsval)
      )

    {repr, flags} = segment(c.state, c, cx, repr)
    finish(c, cx, device, repr, flags, {local_addr, remote_addr, c.options.hop_limit || 64})
  end

  # An aborted connection with a peer sends a RST.
  defp segment(:closed, _c, _cx, repr), do: {Tcp.tcp(repr, control: :rst), @flags}

  # The SYN, or SYN-ACK; its window is never scaled.
  defp segment(state, c, _cx, repr) when state in [:syn_sent, :syn_received] do
    repr =
      Tcp.tcp(repr,
        control: :syn,
        seq: c.local_seq_no,
        window: min(RecvBuffer.window(c.rx_buffer), 0xFFFF)
      )

    repr =
      if state == :syn_sent,
        do: Tcp.tcp(repr, ack: nil, window_scale: c.remote_win_shift, sack_permitted: true),
        else:
          Tcp.tcp(repr,
            sack_permitted: c.remote_has_sack,
            window_scale: c.remote_win_scale && c.remote_win_shift
          )

    {repr, @flags}
  end

  defp segment(state, c, cx, repr)
       when state in [:established, :fin_wait_1, :closing, :close_wait, :last_ack],
       do: data_segment(c, cx, repr)

  # FIN-WAIT-2 and TIME-WAIT send only ACKs.
  defp segment(_state, _c, _cx, repr), do: {repr, @flags}

  defp data_segment(c, cx, repr) do
    options_len = Tcp.header_len(repr) - @tcp_header_len
    effective_mss = effective_mss(c, cx, options_len)
    {repr, flags, offset} = data(c, cx, repr, effective_mss)
    payload_len = byte_size(Tcp.tcp(repr, :payload))
    flags = %{flags | small_segment: payload_len > 0 and payload_len < effective_mss}

    # The last of the data carries PSH or, with the sending half closed, FIN.
    repr =
      cond do
        offset + payload_len != SendBuffer.len(c.tx_buffer) -> repr
        c.state in [:fin_wait_1, :last_ack, :closing] -> Tcp.tcp(repr, control: :fin)
        payload_len > 0 -> Tcp.tcp(repr, control: :psh)
        true -> repr
      end

    {repr, flags}
  end

  # What data to send: a fast retransmission, a loss probe, a SACK
  # recovery resend, or new data.
  defp data(%__MODULE__{recovery: %{pending_fast_retransmit: true}} = c, _cx, repr, effective_mss) do
    # Stops short of any SACKed data after the segment.
    hole =
      case Scoreboard.next_hole(c.recovery.scoreboard, c.local_seq_no) do
        {start, end_} when start == c.local_seq_no -> Seq.diff(end_, start)
        _ -> @usize_max
      end

    size = effective_mss |> min(SendBuffer.len(c.tx_buffer)) |> min(hole)
    payload = SendBuffer.get(c.tx_buffer, 0, size)
    {Tcp.tcp(repr, seq: c.local_seq_no, payload: payload), %{@flags | fast_retransmit: true}, 0}
  end

  # RFC 8985 7.3: one new segment if the window allows, else a resend of
  # the last one sent; the congestion window does not hold it back.
  defp data(%__MODULE__{rack: %Rack{probe_pending: true}} = c, _cx, repr, effective_mss) do
    tx_len = SendBuffer.len(c.tx_buffer)
    sent = min(flight_size(c), tx_len)
    unsent = max(min(tx_len, c.remote_win_len) - flight_size(c), 0)

    {offset, size, flags} =
      if unsent > 0,
        do: {sent, min(unsent, effective_mss), @flags},
        else:
          {sent - min(effective_mss, sent), min(effective_mss, sent),
           %{@flags | probe_retransmission: true}}

    payload = SendBuffer.get(c.tx_buffer, offset, size)

    {Tcp.tcp(repr, seq: Seq.add(c.local_seq_no, offset), payload: payload),
     %{flags | loss_probe: true}, offset}
  end

  defp data(c, cx, repr, effective_mss) do
    case sack_retransmission(c, effective_mss) do
      {seq, size} ->
        offset = Seq.diff(seq, c.local_seq_no)
        payload = SendBuffer.get(c.tx_buffer, offset, size)
        {Tcp.tcp(repr, seq: seq, payload: payload), %{@flags | sack_retransmission: true}, offset}

      nil ->
        new_data(c, cx, repr, effective_mss)
    end
  end

  defp new_data(c, cx, repr, effective_mss) do
    win_right_edge = Seq.add(c.local_seq_no, c.remote_win_len)

    # Zero when the peer shrank its window under data already sent.
    win_limit =
      if Seq.ge?(win_right_edge, c.remote_last_seq),
        do: Seq.diff(win_right_edge, c.remote_last_seq),
        else: 0

    # A zero-window probe is one octet, and the congestion window does not
    # hold it back: an empty probe would draw no reply.
    size =
      if win_limit == 0 and Timer.should_zero_window_probe?(c.timer, cx.now),
        do: {:probe, min(1, effective_mss)},
        else: win_limit |> min(effective_mss) |> min(cwnd_remaining(c))

    {size, flags} =
      case size do
        {:probe, size} -> {size, %{@flags | zero_window_probe: true}}
        size -> {size, @flags}
      end

    offset = flight_size(c)
    {Tcp.tcp(repr, payload: SendBuffer.get(c.tx_buffer, offset, size)), flags, offset}
  end

  defp finish(c, cx, device, repr, flags, {local_addr, remote_addr, hop_limit}) do
    # Something occupying sequence space draws an ACK anyway; a keep-alive
    # is needed only when nothing does. RFC 1122 has it carry an octet.
    keep_alive = Timer.should_keep_alive?(c.timer, cx.now) and empty_repr?(repr)

    repr =
      if keep_alive,
        do: Tcp.tcp(repr, seq: Seq.sub(Tcp.tcp(repr, :seq), 1), payload: <<0>>),
        else: repr

    # The MSS option, from the interface's MTU (RFC 6691).
    repr =
      if Tcp.tcp(repr, :control) == :syn,
        do: Tcp.tcp(repr, mss: cx.ip_mtu - ip_header_len(local_addr) - @tcp_header_len),
        else: repr

    out = {local_addr, remote_addr, hop_limit, repr}

    case device do
      :refuse -> {:refused, out, c}
      :accept -> {:segment, out, commit(c, cx.now, repr, flags, keep_alive)}
    end
  end

  defp empty_repr?(repr) do
    Tcp.tcp(repr, :control) in [:none, :psh] and Tcp.tcp(repr, :payload) == <<>>
  end

  # What sending `repr` changes.
  defp commit(c, now, repr, flags, keep_alive) do
    c = if flags.fast_retransmit, do: recovery(c, :pending_fast_retransmit, false), else: c
    c = recovery_resent(c, repr, flags)

    # Karn's algorithm: nothing resent gives a round-trip sample.
    c =
      if flags.sack_retransmission or flags.probe_retransmission,
        do: %{c | rtte: Rtte.on_retransmit(c.rtte)},
        else: c

    # A probe waits `interval` for an answer before the next.
    c =
      if keep_alive,
        do:
          rewind_keep_alive(
            liveness(c, :keep_alive_probes, min(c.liveness.keep_alive_probes + 1, 255)),
            now
          ),
        else: c

    c = %{c | ack_delay_timer: :idle}

    # A zero-window probe and a keep-alive leave the rest as it was: the
    # one carries an octet the peer will not take, the other a fake one.
    cond do
      flags.zero_window_probe -> rewind_zero_window_probe(c, now)
      keep_alive -> c
      true -> sent(c, now, repr, flags)
    end
  end

  defp recovery_resent(c, repr, flags) do
    if (flags.fast_retransmit or flags.sack_retransmission) and c.recovery.recover != nil do
      end_ = Seq.add(Tcp.tcp(repr, :seq), byte_size(Tcp.tcp(repr, :payload)))

      rxt_end =
        if c.recovery.recovery_rxt_end, do: Seq.max(c.recovery.recovery_rxt_end, end_), else: end_

      recovery(c, :recovery_rxt_end, rxt_end)
    else
      c
    end
  end

  defp sent(c, now, repr, flags) do
    seq = Tcp.tcp(repr, :seq)
    segment_len = Tcp.segment_len(repr)
    seg_end = Seq.add(seq, segment_len)

    # A fast retransmission does not move the highest sequence number sent.
    c = %{
      c
      | remote_last_seq: Seq.max(c.remote_last_seq, seg_end),
        remote_last_ack: Tcp.tcp(repr, :ack),
        remote_last_win: Tcp.tcp(repr, :window)
    }

    c =
      if flags.small_segment,
        do: %{c | nagle_small_segment_end: small_segment_end(c, seg_end)},
        else: c

    c =
      if segment_len > 0,
        do: sent_sequence(c, now, repr, seq, seg_end),
        else: c

    c = probe_timers(c, now, flags, segment_len)

    # An aborted connection forgets its peer once its RST is out.
    if c.state == :closed, do: wake(%{c | tuple: nil}, @wake_tx), else: c
  end

  defp small_segment_end(%__MODULE__{nagle_small_segment_end: nil}, seg_end), do: seg_end
  defp small_segment_end(c, seg_end), do: Seq.max(c.nagle_small_segment_end, seg_end)

  defp sent_sequence(c, now, repr, seq, seg_end) do
    c =
      if Tcp.tcp(repr, :control) != :syn,
        do: %{c | rack: Rack.record(c.rack, c.local_seq_no, seq, seg_end, now)},
        else: c

    c = %{c | rtte: Rtte.on_send(c.rtte, now, seg_end)}
    c = cc(c, &Cubic.post_transmit(&1, now))

    # RFC 6298 (5.1): whatever occupies sequence space starts the timer if
    # it is not running.
    if Timer.retransmit?(c.timer),
      do: c,
      else: set_for_retransmit(c, now, Rtte.retransmission_timeout(c.rtte))
  end

  # RFC 8985 7.3: a probe's episode lasts until what is in flight now is
  # acknowledged, with the retransmission timer restarted to guard it. 7.2:
  # new data restarts the loss probe timer.
  defp probe_timers(c, now, %{loss_probe: true} = flags, _segment_len) do
    probe = %{end: c.remote_last_seq, retransmitted: flags.probe_retransmission, dsacked: false}
    c = %{c | rack: %{c.rack | probe_pending: false, probe: probe}}
    set_for_retransmit(c, now, Rtte.retransmission_timeout(c.rtte))
  end

  defp probe_timers(c, now, flags, segment_len) do
    if segment_len > 0 and not flags.fast_retransmit and not flags.sack_retransmission,
      do: arm_loss_probe(c, now),
      else: c
  end

  @doc "When the connection next needs `dispatch/3`: `:now`, `{:time, at}` or `:ingress`."
  @spec poll_at(t(), cx()) :: :now | :ingress | {:time, integer()}
  def poll_at(%__MODULE__{tuple: nil}, _cx), do: :ingress

  # Quiet until now, or aborted with a RST to send.
  def poll_at(%__MODULE__{liveness: %{remote_last_ts: nil}}, _cx), do: :now
  def poll_at(%__MODULE__{state: :closed}, _cx), do: :now

  def poll_at(c, cx) do
    if seq_to_transmit?(c, cx) or window_to_update?(c),
      do: :now,
      else: timers_poll_at(c)
  end

  @doc """
  `poll_at/2` for a connection that `dispatch/3` has just found nothing to
  send for, and that has not changed since: it has neither data nor a
  window update waiting, so only its timers count.
  """
  @spec idle_poll_at(t()) :: :now | :ingress | {:time, integer()}
  def idle_poll_at(%__MODULE__{tuple: nil}), do: :ingress
  def idle_poll_at(%__MODULE__{liveness: %{remote_last_ts: nil}}), do: :now
  def idle_poll_at(%__MODULE__{state: :closed}), do: :now
  def idle_poll_at(c), do: timers_poll_at(c)

  defp timers_poll_at(c) do
    Timer.poll_at(c.timer)
    |> earlier(timeout_at(c))
    |> earlier(delayed_ack_at(c))
    |> earlier(rack_at(c))
  end

  defp delayed_ack_at(c) do
    case {ack_to_transmit?(c), c.ack_delay_timer} do
      {false, _timer} -> :ingress
      {true, {:waiting, at}} -> {:time, at}
      {true, _idle_or_immediate} -> :now
    end
  end

  defp timeout_at(
         %__MODULE__{liveness: %{remote_last_ts: last}, options: %{timeout: timeout}} = c
       )
       when last != nil and timeout != nil,
       do: if(awaits_remote?(c), do: {:time, last + timeout}, else: :ingress)

  defp timeout_at(_c), do: :ingress

  defp rack_at(%__MODULE__{rack: %Rack{reo_timeout: nil, probe_at: nil}}), do: :ingress
  defp rack_at(%__MODULE__{rack: %Rack{reo_timeout: nil, probe_at: at}}), do: {:time, at}
  defp rack_at(%__MODULE__{rack: %Rack{reo_timeout: at, probe_at: nil}}), do: {:time, at}
  defp rack_at(%__MODULE__{rack: %Rack{reo_timeout: a, probe_at: b}}), do: {:time, min(a, b)}

  # The earlier of two in `PollAt`'s order: now, then times, then ingress;
  # the first of two equal ones.
  defp earlier(:now, _b), do: :now
  defp earlier(_a, :now), do: :now
  defp earlier(:ingress, b), do: b
  defp earlier(a, :ingress), do: a
  defp earlier({:time, a}, {:time, b}) when b < a, do: {:time, b}
  defp earlier(a, _b), do: a
end
