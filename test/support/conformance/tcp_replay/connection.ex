defmodule SmolNet.Conformance.TcpReplay.Connection do
  @moduledoc false

  # The Elixir connection, `SmolNet.Core.Tcp.Connection`, as a replay
  # subject: it translates the vectors' JSON-shaped states, segments and
  # calls to and from the connection's own terms. Times in the vectors are
  # milliseconds and the connection's microseconds; the interface is the
  # vector's `interface_addresses` and `ip_mtu`, and every connection opened
  # takes the vector's `initial_seq_no`, as smoltcp's test build does.

  @behaviour SmolNet.Conformance.TcpReplay

  require SmolNet.Core.Wire.Tcp

  alias SmolNet.Conformance.TcpVector
  alias SmolNet.Core.Iface
  alias SmolNet.Core.Tcp.Assembler
  alias SmolNet.Core.Tcp.Connection
  alias SmolNet.Core.Tcp.Cubic
  alias SmolNet.Core.Tcp.Rack
  alias SmolNet.Core.Tcp.RecvBuffer
  alias SmolNet.Core.Tcp.Rtte
  alias SmolNet.Core.Tcp.SendBuffer
  alias SmolNet.Core.Wire.Tcp

  @ms 1_000

  @impl true
  def new(context, initial, _args) do
    conn =
      Connection.new(initial["rx_buffer"]["capacity"], initial["tx_buffer"]["capacity"],
        assembler_capacity: context["assembler_capacity"]
      )

    subject = %{
      conn: conn,
      mtu: context["ip_mtu"],
      isn: context["initial_seq_no"],
      addresses: [],
      now: 0
    }

    apply_fields(subject, initial)
  end

  @impl true
  def set(subject, changed, _state), do: apply_fields(subject, changed)

  @impl true
  def input(subject, segment, ip, at) do
    subject = %{subject | now: at * @ms}
    {reply, conn} = Connection.input(subject.conn, cx(subject), ip_pair(ip), repr(segment))
    {reply && segment_map(elem(reply, 3)), %{subject | conn: conn}}
  end

  @impl true
  def dispatch(subject, at, device) do
    subject = %{subject | now: at * @ms}

    case Connection.dispatch(subject.conn, cx(subject), device) do
      {:segment, {_src, _dst, hop_limit, segment}, conn} ->
        {{:segment, segment_map(segment), hop_limit}, %{subject | conn: conn}}

      {:refused, _out, conn} ->
        {:error, %{subject | conn: conn}}

      {:nothing, conn} ->
        {:nothing, %{subject | conn: conn}}
    end
  end

  @impl true
  def call(subject, name, args, at) do
    subject = %{subject | now: at * @ms}
    {result, conn} = run_call(subject.conn, name, args, cx(subject))
    {result, %{subject | conn: conn}}
  end

  defp run_call(conn, "accepts", args, _cx) do
    ip = ip_pair(%{src_addr: args["src_addr"], dst_addr: args["dst_addr"]})
    {Connection.accepts?(conn, ip, repr(TcpVector.segment(args["segment"]))), conn}
  end

  defp run_call(conn, "send_slice", %{"data" => data}, _cx) do
    case Connection.send(conn, hex(data)) do
      {:ok, size, conn} -> {%{"ok" => size}, conn}
      {:error, reason} -> {%{"error" => Atom.to_string(reason)}, conn}
    end
  end

  defp run_call(conn, "recv", %{"len" => len}, _cx) do
    case Connection.recv(conn, len) do
      {:ok, data, offered, conn} ->
        {%{"ok" => %{"offered" => offered, "data" => to_hex(data)}}, conn}

      {:error, reason} ->
        {%{"error" => Atom.to_string(reason)}, conn}
    end
  end

  # A `recv` whose closure is never called, as when it fails first.
  defp run_call(conn, "recv", %{}, _cx) do
    case Connection.recv(conn, 0) do
      {:ok, data, offered, conn} ->
        {%{"ok" => %{"offered" => offered, "data" => to_hex(data)}}, conn}

      {:error, reason} ->
        {%{"error" => Atom.to_string(reason)}, conn}
    end
  end

  defp run_call(conn, "recv_slice", %{"len" => len}, _cx) do
    case Connection.recv_slice(conn, len) do
      {:ok, data, conn} -> {%{"ok" => to_hex(data)}, conn}
      {:error, reason} -> {%{"error" => Atom.to_string(reason)}, conn}
    end
  end

  defp run_call(conn, "peek_slice", %{"len" => len}, _cx),
    do: {%{"ok" => to_hex(Connection.peek(conn, len))}, conn}

  defp run_call(conn, "listen", %{"local" => local}, _cx) do
    case Connection.listen(conn, listen_endpoint(local)) do
      {:ok, conn} -> {%{"ok" => nil}, conn}
      {:error, reason} -> {%{"error" => Atom.to_string(reason)}, conn}
    end
  end

  defp run_call(conn, "connect", %{"local" => local, "remote" => remote}, cx) do
    case Connection.connect(conn, cx, endpoint(remote), listen_endpoint(local)) do
      {:ok, conn} -> {%{"ok" => nil}, conn}
      {:error, reason} -> {%{"error" => Atom.to_string(reason)}, conn}
    end
  end

  defp run_call(conn, "close", _args, _cx), do: {nil, Connection.close(conn)}
  defp run_call(conn, "abort", _args, _cx), do: {nil, Connection.abort(conn)}
  defp run_call(conn, "reset", _args, _cx), do: {nil, Connection.forget(conn)}

  defp run_call(conn, "set_timeout", %{"timeout" => timeout}, _cx),
    do: {nil, Connection.set_timeout(conn, duration(timeout))}

  defp run_call(conn, "set_ack_delay", %{"delay" => delay}, _cx),
    do: {nil, Connection.set_ack_delay(conn, duration(delay))}

  defp run_call(conn, "set_nagle_enabled", %{"enabled" => enabled}, _cx),
    do: {nil, Connection.set_nagle_enabled(conn, enabled)}

  defp run_call(conn, "set_keep_alive", %{"interval" => interval}, _cx),
    do: {nil, Connection.set_keep_alive(conn, duration(interval))}

  defp run_call(conn, "set_keep_alive_config", %{"config" => config}, _cx),
    do: {nil, Connection.set_keep_alive_config(conn, keep_alive(config))}

  defp run_call(conn, "set_hop_limit", %{"hop_limit" => hop_limit}, _cx),
    do: {nil, Connection.set_hop_limit(conn, hop_limit)}

  defp run_call(conn, "set_congestion_control", %{"control" => control}, _cx),
    do: {nil, Connection.set_congestion_control(conn, String.to_existing_atom(control))}

  defp run_call(conn, "set_tsval_generator", %{"tsval" => tsval}, _cx),
    do: {nil, Connection.set_tsval_generator(conn, tsval && {:constant, tsval})}

  defp run_call(conn, "process_path_mtu", args, cx) do
    {outcome, conn} =
      Connection.process_path_mtu(
        conn,
        cx,
        endpoint(args["local"]),
        endpoint(args["remote"]),
        args["seq"],
        args["mtu"]
      )

    {Atom.to_string(outcome), conn}
  end

  defp cx(subject) do
    iface = Iface.new(subject.mtu, subject.addresses, [])

    %{
      now: subject.now,
      ip_mtu: subject.mtu,
      addresses: Enum.map(subject.addresses, &elem(&1, 0)),
      isn: subject.isn,
      source_address: &Iface.source_address(iface, &1)
    }
  end

  ## Vector fields to the connection's

  defp apply_fields(subject, fields) do
    Enum.reduce(fields, subject, fn {name, value}, subject ->
      apply_field(subject, name, value)
    end)
  end

  defp apply_field(subject, "observed", _value), do: subject

  defp apply_field(subject, "interface_addresses", addresses),
    do: %{subject | addresses: Enum.map(addresses, &cidr/1)}

  defp apply_field(subject, "tsval", tsval),
    do: put(subject, :tsval_generator, tsval && {:constant, tsval})

  defp apply_field(subject, "rx_buffer", %{"capacity" => capacity, "data" => data}),
    do: put(subject, :rx_buffer, RecvBuffer.from_data(capacity, hex(data)))

  # A test that takes octets off the front of the send buffer, or adds them
  # to its end, moves the ring as smoltcp's moves; any other change is a
  # new buffer.
  defp apply_field(subject, "tx_buffer", %{"capacity" => capacity, "data" => data}) do
    data = hex(data)
    buffer = subject.conn.tx_buffer
    held = SendBuffer.data(buffer)
    {held_size, size} = {byte_size(held), byte_size(data)}

    buffer =
      cond do
        buffer.capacity != capacity ->
          SendBuffer.from_data(capacity, data)

        size <= held_size and binary_part(held, held_size - size, size) == data ->
          SendBuffer.dequeue(buffer, held_size - size)

        size > held_size and binary_part(data, 0, held_size) == held ->
          buffer |> SendBuffer.enqueue(binary_part(data, held_size, size - held_size)) |> elem(1)

        true ->
          SendBuffer.from_data(capacity, data)
      end

    put(subject, :tx_buffer, buffer)
  end

  defp apply_field(subject, "assembler", ranges) do
    assembler = subject.conn.assembler
    put(subject, :assembler, %{assembler | ranges: Enum.map(ranges, &List.to_tuple/1)})
  end

  defp apply_field(subject, "congestion", congestion),
    do: put(subject, :congestion, cubic(congestion))

  defp apply_field(subject, "rack", rack), do: put(subject, :rack, rack(rack))
  defp apply_field(subject, "rtte", rtte), do: put(subject, :rtte, rtte(rtte))
  defp apply_field(subject, "timer", timer), do: put(subject, :timer, timer(timer))

  defp apply_field(subject, "state", state),
    do: put(subject, :state, String.to_existing_atom(state))

  defp apply_field(subject, "tuple", tuple), do: put(subject, :tuple, tuple(tuple))

  defp apply_field(subject, "listen_endpoint", endpoint),
    do: put(subject, :listen_endpoint, listen_endpoint(endpoint))

  defp apply_field(subject, "keep_alive", keep_alive),
    do: put(subject, :keep_alive, keep_alive(keep_alive))

  defp apply_field(subject, "scoreboard", ranges),
    do: put(subject, :scoreboard, Enum.map(ranges, &List.to_tuple/1))

  defp apply_field(subject, "ack_delay_timer", %{"kind" => "idle"}),
    do: put(subject, :ack_delay_timer, :idle)

  defp apply_field(subject, "ack_delay_timer", %{"kind" => "immediate"}),
    do: put(subject, :ack_delay_timer, :immediate)

  defp apply_field(subject, "ack_delay_timer", %{"kind" => "waiting", "at" => at}),
    do: put(subject, :ack_delay_timer, {:waiting, at * @ms})

  defp apply_field(subject, name, value) when name in ~w(timeout ack_delay),
    do: put(subject, String.to_existing_atom(name), duration(value))

  defp apply_field(subject, name, value) when name in ~w(remote_last_ts challenge_ack_timer),
    do: put(subject, String.to_existing_atom(name), value && value * @ms)

  defp apply_field(subject, name, value), do: put(subject, String.to_existing_atom(name), value)

  @options ~w(timeout keep_alive hop_limit ack_delay nagle tsval_generator)a
  @recovery ~w(pending_fast_retransmit recover recovery_rxt_end scoreboard local_rx_dup_acks
                local_rx_last_ack)a
  @liveness ~w(keep_alive_probes timeout_abort)a

  # The connection keeps its options and its loss recovery state as
  # groups of fields.
  defp put(subject, field, value) when field in @options,
    do: %{
      subject
      | conn: %{subject.conn | options: Map.replace!(subject.conn.options, field, value)}
    }

  defp put(subject, field, value) when field in @recovery,
    do: %{
      subject
      | conn: %{subject.conn | recovery: Map.replace!(subject.conn.recovery, field, value)}
    }

  defp put(subject, field, value) when field in @liveness,
    do: %{
      subject
      | conn: %{subject.conn | liveness: Map.replace!(subject.conn.liveness, field, value)}
    }

  defp put(subject, field, value) do
    true = Map.has_key?(subject.conn, field)
    %{subject | conn: Map.put(subject.conn, field, value)}
  end

  defp duration(nil), do: nil
  defp duration(ms), do: ms * @ms

  defp keep_alive(nil), do: nil

  defp keep_alive(%{"idle" => idle, "interval" => interval, "probes" => probes}),
    do: %{idle: idle * @ms, interval: interval * @ms, probes: probes}

  defp timer(%{"kind" => "idle", "keep_alive_at" => at}), do: {:idle, at && at * @ms}
  defp timer(%{"kind" => "retransmit", "expires_at" => at}), do: {:retransmit, at * @ms}
  defp timer(%{"kind" => "fast_retransmit"}), do: :fast_retransmit
  defp timer(%{"kind" => "close", "expires_at" => at}), do: {:close, at * @ms}

  defp timer(%{"kind" => "zero_window_probe", "expires_at" => at, "delay" => delay}),
    do: {:zero_window_probe, at * @ms, delay * @ms}

  defp rtte(rtte) do
    %Rtte{
      have_measurement: rtte["have_measurement"],
      srtt: rtte["srtt"],
      rttvar: rtte["rttvar"],
      rto: rtte["rto"],
      sample: rtte["sample"] && {rtte["sample"]["at"] * @ms, rtte["sample"]["seq"]},
      max_seq_sent: rtte["max_seq_sent"],
      rto_count: rtte["rto_count"]
    }
  end

  defp cubic(%{"kind" => "none"}), do: nil

  defp cubic(%{"kind" => "cubic"} = cubic) do
    %Cubic{
      w_max: cubic["w_max"],
      cwnd: cubic["cwnd"],
      mss: cubic["mss"],
      ssthresh: size(cubic["ssthresh"]),
      rwnd: cubic["rwnd"],
      k: cubic["k"] * 1.0,
      w_est: cubic["w_est"] * 1.0,
      cwnd_prior: cubic["cwnd_prior"],
      recovery_start: cubic["recovery_start"] && cubic["recovery_start"] * @ms,
      in_fast_recovery: cubic["in_fast_recovery"],
      in_rto_recovery: cubic["in_rto_recovery"],
      rto_slow_start: cubic["rto_slow_start"],
      idle_start: cubic["idle_start"] && cubic["idle_start"] * @ms
    }
  end

  defp size("max"), do: Cubic.usize_max()
  defp size(value), do: value

  defp rack(rack) do
    log = rack["log"]

    ranges =
      for range <- log["ranges"] do
        %{
          end: range["end"],
          at: range["at"] * @ms,
          retransmitted: range["retransmitted"],
          lost: range["lost"],
          doubled: range["doubled"],
          first: range["first"],
          last: range["last"]
        }
      end

    %Rack{
      log: %{start: log["start"], ranges: ranges, sends: log["sends"]},
      xmit:
        rack["xmit"] &&
          %{
            at: rack["xmit"]["at"] * @ms,
            end: rack["xmit"]["end"],
            first: rack["xmit"]["first"],
            last: rack["xmit"]["last"]
          },
      rtt: rack["rtt_us"],
      min_rtt: rack["min_rtt_us"],
      reo_wnd_mult: rack["reo_wnd_mult"],
      reo_wnd_persist: rack["reo_wnd_persist"],
      dsack_round: rack["dsack_round"],
      reo_timeout: rack["reo_timeout"] && rack["reo_timeout"] * @ms,
      probe_at: rack["probe_at"] && rack["probe_at"] * @ms,
      probe_pending: rack["probe_pending"],
      probe:
        rack["probe"] &&
          %{
            end: rack["probe"]["end"],
            retransmitted: rack["probe"]["retransmitted"],
            dsacked: rack["probe"]["dsacked"]
          }
    }
  end

  defp tuple(nil), do: nil

  defp tuple(%{"local" => local, "remote" => remote}),
    do: %{local: endpoint(local), remote: endpoint(remote)}

  defp endpoint(string) do
    [address, port] = Regex.run(~r/^(.*):(\d+)$/, string, capture: :all_but_first)
    {address(address), String.to_integer(port)}
  end

  defp listen_endpoint(%{"addr" => addr, "port" => port}), do: {addr && address(addr), port}

  defp address(string) do
    {:ok, tuple} =
      string
      |> String.trim_leading("[")
      |> String.trim_trailing("]")
      |> String.to_charlist()
      |> :inet.parse_address()

    case tuple do
      {a, b, c, d} -> <<a, b, c, d>>
      words -> for word <- Tuple.to_list(words), into: <<>>, do: <<word::16>>
    end
  end

  defp cidr(string) do
    [address, prefix] = String.split(string, "/")
    {address(address), String.to_integer(prefix)}
  end

  defp ip_pair(%{src_addr: src, dst_addr: dst}), do: {address(src), address(dst)}

  defp hex(data), do: Base.decode16!(data, case: :lower)
  defp to_hex(data), do: Base.encode16(data, case: :lower)

  defp repr(segment) do
    Tcp.tcp(
      src_port: segment.src_port,
      dst_port: segment.dst_port,
      control: segment.control,
      seq: segment.seq,
      ack: segment.ack,
      window: segment.window_len,
      window_scale: segment.window_scale,
      mss: segment.max_seg_size,
      sack_permitted: segment.sack_permitted,
      sack_ranges: segment.sack_ranges,
      timestamp: segment.timestamp,
      payload: segment.payload
    )
  end

  defp segment_map(repr) do
    %{
      src_port: Tcp.tcp(repr, :src_port),
      dst_port: Tcp.tcp(repr, :dst_port),
      control: Tcp.tcp(repr, :control),
      seq: Tcp.tcp(repr, :seq),
      ack: Tcp.tcp(repr, :ack),
      window_len: Tcp.tcp(repr, :window),
      window_scale: Tcp.tcp(repr, :window_scale),
      max_seg_size: Tcp.tcp(repr, :mss),
      sack_permitted: Tcp.tcp(repr, :sack_permitted),
      sack_ranges: Tcp.tcp(repr, :sack_ranges),
      timestamp: Tcp.tcp(repr, :timestamp),
      payload: Tcp.tcp(repr, :payload)
    }
  end

  ## The connection's fields as the vectors have them

  @impl true
  def snapshot(subject) do
    c = subject.conn
    ms = fn us -> us && div(us, @ms) end

    %{
      "state" => Atom.to_string(c.state),
      "timer" => timer_json(c.timer),
      "rtte" => rtte_json(c.rtte),
      "assembler" => for({l, r} <- Assembler.ranges(c.assembler), do: [l, r]),
      "rx_buffer" => %{
        "capacity" => c.rx_buffer.capacity,
        "data" => to_hex(RecvBuffer.data(c.rx_buffer))
      },
      "rx_fin_received" => c.rx_fin_received,
      "tx_buffer" => %{
        "capacity" => c.tx_buffer.capacity,
        "data" => to_hex(SendBuffer.data(c.tx_buffer))
      },
      "timeout" => ms.(c.options.timeout),
      "keep_alive" =>
        c.options.keep_alive &&
          %{
            "idle" => ms.(c.options.keep_alive.idle),
            "interval" => ms.(c.options.keep_alive.interval),
            "probes" => c.options.keep_alive.probes
          },
      "keep_alive_probes" => c.liveness.keep_alive_probes,
      "timeout_abort" => c.liveness.timeout_abort,
      "hop_limit" => c.options.hop_limit,
      "listen_endpoint" => listen_endpoint_json(c.listen_endpoint),
      "tuple" =>
        c.tuple &&
          %{"local" => endpoint_json(c.tuple.local), "remote" => endpoint_json(c.tuple.remote)},
      "local_seq_no" => c.local_seq_no,
      "remote_seq_no" => c.remote_seq_no,
      "remote_last_seq" => c.remote_last_seq,
      "remote_last_ack" => c.remote_last_ack,
      "remote_last_win" => c.remote_last_win,
      "remote_win_shift" => c.remote_win_shift,
      "remote_win_len" => c.remote_win_len,
      "remote_win_scale" => c.remote_win_scale,
      "remote_has_sack" => c.remote_has_sack,
      "remote_mss" => c.remote_mss,
      "path_mtu" => c.path_mtu,
      "remote_last_ts" => ms.(c.remote_last_ts),
      "local_rx_last_seq" => c.local_rx_last_seq,
      "local_rx_last_ack" => c.recovery.local_rx_last_ack,
      "local_rx_dup_acks" => c.recovery.local_rx_dup_acks,
      "pending_fast_retransmit" => c.recovery.pending_fast_retransmit,
      "recover" => c.recovery.recover,
      "scoreboard" => for({l, r} <- c.recovery.scoreboard, do: [l, r]),
      "recovery_rxt_end" => c.recovery.recovery_rxt_end,
      "rack" => rack_json(c.rack),
      "ack_delay" => ms.(c.options.ack_delay),
      "ack_delay_timer" => ack_delay_timer_json(c.ack_delay_timer),
      "challenge_ack_timer" => ms.(c.challenge_ack_timer),
      "nagle" => c.options.nagle,
      "nagle_small_segment_end" => c.nagle_small_segment_end,
      "congestion" => cubic_json(c.congestion),
      "tsval" => Connection.current_tsval(c, cx(subject)),
      "last_remote_tsval" => c.last_remote_tsval,
      "interface_addresses" => Enum.map(subject.addresses, &cidr_json/1),
      "observed" => observed(subject)
    }
  end

  defp observed(subject) do
    c = subject.conn

    %{
      "poll_at" => poll_at_json(Connection.poll_at(c, cx(subject))),
      "is_open" => Connection.open?(c),
      "is_listening" => Connection.listening?(c),
      "is_active" => Connection.active?(c),
      "may_send" => Connection.may_send?(c),
      "may_recv" => Connection.may_recv?(c),
      "can_send" => Connection.can_send?(c),
      "can_recv" => Connection.can_recv?(c),
      "send_queue" => Connection.send_queue(c),
      "recv_queue" => Connection.recv_queue(c),
      "timestamp_enabled" => Connection.timestamp_enabled?(c),
      "aborted_by_timeout" => Connection.aborted_by_timeout?(c)
    }
  end

  defp poll_at_json(:now), do: %{"kind" => "now"}
  defp poll_at_json(:ingress), do: %{"kind" => "ingress"}
  defp poll_at_json({:time, at}), do: %{"kind" => "time", "at" => div(at, @ms)}

  defp timer_json({:idle, at}), do: %{"kind" => "idle", "keep_alive_at" => at && div(at, @ms)}
  defp timer_json({:retransmit, at}), do: %{"kind" => "retransmit", "expires_at" => div(at, @ms)}
  defp timer_json(:fast_retransmit), do: %{"kind" => "fast_retransmit"}
  defp timer_json({:close, at}), do: %{"kind" => "close", "expires_at" => div(at, @ms)}

  defp timer_json({:zero_window_probe, at, delay}),
    do: %{"kind" => "zero_window_probe", "expires_at" => div(at, @ms), "delay" => div(delay, @ms)}

  defp ack_delay_timer_json(:idle), do: %{"kind" => "idle"}
  defp ack_delay_timer_json(:immediate), do: %{"kind" => "immediate"}
  defp ack_delay_timer_json({:waiting, at}), do: %{"kind" => "waiting", "at" => div(at, @ms)}

  defp rtte_json(rtte) do
    %{
      "have_measurement" => rtte.have_measurement,
      "srtt" => rtte.srtt,
      "rttvar" => rtte.rttvar,
      "rto" => rtte.rto,
      "sample" =>
        case rtte.sample do
          {at, seq} -> %{"at" => div(at, @ms), "seq" => seq}
          nil -> nil
        end,
      "max_seq_sent" => rtte.max_seq_sent,
      "rto_count" => rtte.rto_count
    }
  end

  defp cubic_json(nil), do: %{"kind" => "none"}

  defp cubic_json(cubic) do
    %{
      "kind" => "cubic",
      "w_max" => cubic.w_max,
      "cwnd" => cubic.cwnd,
      "mss" => cubic.mss,
      "ssthresh" => if(cubic.ssthresh == Cubic.usize_max(), do: "max", else: cubic.ssthresh),
      "rwnd" => cubic.rwnd,
      "k" => cubic.k,
      "w_est" => cubic.w_est,
      "cwnd_prior" => cubic.cwnd_prior,
      "recovery_start" => cubic.recovery_start && div(cubic.recovery_start, @ms),
      "in_fast_recovery" => cubic.in_fast_recovery,
      "in_rto_recovery" => cubic.in_rto_recovery,
      "rto_slow_start" => cubic.rto_slow_start,
      "idle_start" => cubic.idle_start && div(cubic.idle_start, @ms)
    }
  end

  defp rack_json(rack) do
    log = rack.log

    {ranges, _end} =
      Enum.map_reduce(log.ranges, log.start, fn sent, start ->
        {%{
           "start" => start,
           "end" => sent.end,
           "at" => div(sent.at, @ms),
           "retransmitted" => sent.retransmitted,
           "lost" => sent.lost,
           "doubled" => sent.doubled,
           "first" => sent.first,
           "last" => sent.last
         }, sent.end}
      end)

    %{
      "log" => %{"start" => log.start, "ranges" => ranges, "sends" => log.sends},
      "xmit" =>
        rack.xmit &&
          %{
            "at" => div(rack.xmit.at, @ms),
            "end" => rack.xmit.end,
            "first" => rack.xmit.first,
            "last" => rack.xmit.last
          },
      "rtt_us" => rack.rtt,
      "min_rtt_us" => rack.min_rtt,
      "reo_wnd_mult" => rack.reo_wnd_mult,
      "reo_wnd_persist" => rack.reo_wnd_persist,
      "dsack_round" => rack.dsack_round,
      "reo_timeout" => rack.reo_timeout && div(rack.reo_timeout, @ms),
      "probe_at" => rack.probe_at && div(rack.probe_at, @ms),
      "probe_pending" => rack.probe_pending,
      "probe" =>
        rack.probe &&
          %{
            "end" => rack.probe.end,
            "retransmitted" => rack.probe.retransmitted,
            "dsacked" => rack.probe.dsacked
          }
    }
  end

  defp listen_endpoint_json({address, port}),
    do: %{"addr" => address && address_json(address), "port" => port}

  defp endpoint_json({address, port}), do: "#{address_json(address)}:#{port}"

  defp address_json(<<a, b, c, d>>), do: "#{a}.#{b}.#{c}.#{d}"

  defp address_json(address) do
    words = for <<word::16 <- address>>, do: word
    address = words |> List.to_tuple() |> :inet.ntoa() |> to_string()
    address
  end

  defp cidr_json({address, prefix}), do: "#{address_json(address)}/#{prefix}"

  @impl true
  def compared_fields, do: TcpVector.state_fields()
end
