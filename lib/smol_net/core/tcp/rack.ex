defmodule SmolNet.Core.Tcp.Rack do
  @moduledoc false

  # RACK-TLP (RFC 8985), as SmolNet's smoltcp has it: time-based loss
  # detection and tail loss probes, with its deliberate deviations (a
  # reordering window from the start, at least SRTT of wait, a 10 ms least
  # probe timeout).
  #
  # RACK needs to know when each octet in flight was last sent, and a
  # connection cuts its segments afresh on every send, so the send log
  # keeps that as ranges of sequence space sent together: `start` (where
  # the first range starts) and up to 64 ranges, each
  # `%{end, at, retransmitted, lost, doubled, first, last}`, where `first`
  # and `last` number the sends that made it, in the order they were made,
  # since a millisecond clock cannot order what is sent within one. Past
  # 64 ranges the two sent closest in time are merged.
  #
  # Instants and durations are microseconds.

  alias SmolNet.Core.Tcp.Scoreboard
  alias SmolNet.Core.Tcp.Seq
  require SmolNet.Core.Tcp.Seq

  @log_size 64
  @reo_wnd_persist 16
  @reo_wnd_mult_max 64

  defstruct log: %{start: 0, ranges: [], sends: 0},
            xmit: nil,
            rtt: 0,
            min_rtt: nil,
            reo_wnd_mult: 1,
            reo_wnd_persist: 0,
            dsack_round: nil,
            reo_timeout: nil,
            probe_at: nil,
            probe_pending: false,
            probe: nil

  @type t :: %__MODULE__{}

  def new, do: %__MODULE__{}

  ## The send log

  defp joins?(sent, next) do
    same_run = {next.first, next.last} == {sent.first, sent.last}
    sent_next = not sent.retransmitted and next.first == sent.last + 1

    sent.at == next.at and sent.retransmitted == next.retransmitted and sent.lost == next.lost and
      sent.doubled == next.doubled and (same_run or sent_next)
  end

  # Builds a list of ranges, reversed, merging each with the last when they
  # join.
  defp push([last | rest] = built, sent) do
    if joins?(last, sent),
      do: [
        %{
          last
          | end: sent.end,
            first: min(last.first, sent.first),
            last: max(last.last, sent.last)
        }
        | rest
      ],
      else: [sent | built]
  end

  defp push([], sent), do: [sent]

  # The ranges, each with the sequence number it starts at.
  defp with_starts(%{start: start, ranges: ranges}) do
    {pairs, _end} =
      Enum.map_reduce(ranges, start, fn sent, start -> {{start, sent}, sent.end} end)

    pairs
  end

  defp high(%{start: start, ranges: []}), do: start
  defp high(%{ranges: ranges}), do: List.last(ranges).end

  defp clear(log, start), do: %{log | start: start, ranges: []}

  # Takes a reversed built list, merging the closest in time until it fits.
  defp replace(log, built), do: %{log | ranges: compact(:lists.reverse(built))}

  defp compact(ranges) when length(ranges) <= @log_size, do: ranges

  defp compact(ranges) do
    pairs = Enum.zip(ranges, tl(ranges))

    {_gap, i} =
      pairs
      |> Enum.with_index()
      |> Enum.map(fn {{a, b}, i} ->
        same = a.retransmitted == b.retransmitted and a.lost == b.lost and a.doubled == b.doubled
        {{not same, max(a.at, b.at) - min(a.at, b.at)}, i}
      end)
      |> Enum.min_by(&elem(&1, 0))

    {before, [a, b | after_]} = Enum.split(ranges, i)

    merged = %{
      end: b.end,
      at: max(a.at, b.at),
      retransmitted: a.retransmitted or b.retransmitted,
      lost: a.lost and b.lost,
      doubled: a.doubled or b.doubled,
      first: min(a.first, b.first),
      last: max(a.last, b.last)
    }

    compact(before ++ [merged | after_])
  end

  @doc "Forgets the octets below `ack`."
  def advance_log(%{ranges: []} = log, ack), do: %{log | start: ack}

  def advance_log(log, ack) do
    cond do
      Seq.le?(ack, log.start) -> log
      Seq.ge?(ack, high(log)) -> clear(log, ack)
      true -> %{log | ranges: Enum.drop_while(log.ranges, &Seq.le?(&1.end, ack)), start: ack}
    end
  end

  @doc """
  Records that `[seq, end_)` was sent at `now`, with `una` the lowest
  unacknowledged octet. Octets sent before are a retransmission.
  """
  def record(rack, una, seq, end_, now) do
    log = advance_log(rack.log, una)
    seq = Seq.max(seq, log.start)

    if Seq.le?(end_, seq) do
      %{rack | log: log}
    else
      log = if Seq.gt?(seq, high(log)), do: clear(log, seq), else: log
      send = log.sends + 1
      log = %{log | sends: send}
      high = high(log)

      fresh = %{
        end: end_,
        at: now,
        retransmitted: false,
        lost: false,
        doubled: false,
        first: send,
        last: send
      }

      %{rack | log: record_send(log, seq, end_, now, high, fresh, send)}
    end
  end

  defp record_send(log, seq, end_, now, high, fresh, send) do
    last = List.last(log.ranges)

    cond do
      seq == high and last != nil and joins?(last, fresh) ->
        %{log | ranges: List.replace_at(log.ranges, -1, %{last | end: end_, last: send})}

      seq == high and length(log.ranges) < @log_size ->
        %{log | ranges: log.ranges ++ [fresh]}

      true ->
        built = Enum.reduce(with_starts(log), [], &resend(&1, &2, {seq, end_, now, send}))
        built = if Seq.gt?(end_, high), do: push(built, fresh), else: built
        replace(log, built)
    end
  end

  # A range as a resend of `[seq, end_)` leaves it: untouched outside it,
  # split around it, and the resent part last sent now.
  defp resend({start, sent}, built, {seq, end_, now, send}) do
    if Seq.le?(sent.end, seq) or Seq.ge?(start, end_) do
      push(built, sent)
    else
      built = if Seq.lt?(start, seq), do: push(built, %{sent | end: seq}), else: built

      resent = %{
        end: Seq.min(sent.end, end_),
        at: now,
        retransmitted: true,
        lost: false,
        doubled: not sent.lost,
        first: send,
        last: send
      }

      built = push(built, resent)
      if Seq.gt?(sent.end, end_), do: push(built, sent), else: built
    end
  end

  defp mark_all_lost(log) do
    built =
      Enum.reduce(log.ranges, [], fn sent, built ->
        push(built, %{sent | lost: true, doubled: false})
      end)

    replace(log, built)
  end

  @doc """
  Applies RFC 6675's `IsLost` below `boundary`: the first transmission of
  an octet there is lost.
  """
  def mark_lost_below(rack, boundary) do
    log = rack.log
    built = Enum.reduce(with_starts(log), [], &lost_below(&1, &2, boundary))
    %{rack | log: replace(log, built)}
  end

  # A range sent only once is lost below the boundary, and a resent one no
  # longer has its first transmission in flight. Whether a resend is lost
  # is RACK's to judge.
  defp lost_below({start, sent}, built, boundary) do
    if Seq.ge?(start, boundary) do
      push(built, sent)
    else
      below = %{
        sent
        | end: Seq.min(sent.end, boundary),
          lost: sent.lost or not sent.retransmitted,
          doubled: false
      }

      built = push(built, below)
      if Seq.gt?(sent.end, boundary), do: push(built, sent), else: built
    end
  end

  @doc "Whether any octet in the log was resent or is deemed lost."
  def recovering?(rack), do: Enum.any?(rack.log.ranges, &(&1.retransmitted or &1.lost))

  @doc "RFC 6675 `SetPipe` for the octets in `[una, high)`."
  def pipe(rack, board, una, high) do
    log = rack.log

    unsacked = fn from, to ->
      {from, to} = {Seq.max(from, una), Seq.min(to, high)}

      if Seq.lt?(from, to),
        do: Seq.diff(to, from) - Scoreboard.sacked_between(board, from, to),
        else: 0
    end

    base = unsacked.(una, log.start) + unsacked.(high(log), high)

    Enum.reduce(with_starts(log), base, fn {start, sent}, pipe ->
      copies =
        case {sent.lost, sent.doubled} do
          {true, _} -> 0
          {false, false} -> 1
          {false, true} -> 2
        end

      pipe + copies * unsacked.(start, sent.end)
    end)
  end

  @doc "The lowest run below `high` not SACKed whose last transmission is lost."
  def first_lost(rack, board, high) do
    rack.log
    |> with_starts()
    |> Enum.find_value(fn {start, sent} ->
      sent.lost && Scoreboard.unsacked_run(board, start, Seq.min(sent.end, high))
    end)
  end

  # RACK's loss detection (RFC 8985 6.2 step 5): marks lost each range sent
  # before `xmit`, the latest delivered, once `wait` has passed since it was
  # sent. Returns whether it newly deems an unSACKed octet lost, and when
  # the next such octet not yet lost will be.
  defp detect_log(log, board, now, xmit, wait) do
    {built, newly_lost, next} =
      Enum.reduce(with_starts(log), {[], false, nil}, fn {start, sent}, acc ->
        judge(sent, start, sent_before(sent, start, xmit), acc, {board, now, wait})
      end)

    {replace(log, built), newly_lost, next}
  end

  # Where the part of a range sent before `xmit` ends: all of it if it was
  # in an earlier send, up to `xmit` within the same run, none otherwise.
  # Ranges whose runs overlap, which only merging a full log makes, are not
  # judged.
  defp sent_before(sent, start, xmit) do
    cond do
      sent.last < xmit.first ->
        sent.end

      {sent.first, sent.last} == {xmit.first, xmit.last} ->
        sent.end |> Seq.min(xmit.end) |> Seq.max(start)

      true ->
        start
    end
  end

  defp judge(sent, start, before, {built, newly_lost, next}, _context)
       when sent.lost or before == start,
       do: {push(built, sent), newly_lost, next}

  defp judge(sent, start, before, {built, newly_lost, next}, {board, now, wait}) do
    unsacked = Scoreboard.sacked_between(board, start, before) < Seq.diff(before, start)
    deadline = sent.at + wait

    cond do
      now >= deadline ->
        built = push(built, %{sent | end: before, lost: true, doubled: false})
        built = if Seq.lt?(before, sent.end), do: push(built, sent), else: built
        {built, newly_lost or unsacked, next}

      unsacked ->
        {push(built, sent), newly_lost, min_time(next, deadline)}

      true ->
        {push(built, sent), newly_lost, next}
    end
  end

  defp min_time(nil, time), do: time
  defp min_time(a, b), do: min(a, b)

  ## RACK and TLP

  @doc """
  Prepares for a resend of everything from `SND.UNA`: all in flight is
  lost, the timers stop and any probe episode ends.
  """
  def on_rewind(rack) do
    %{cancel_probe(rack) | log: mark_all_lost(rack.log), reo_timeout: nil, probe: nil}
  end

  def cancel_probe(rack), do: %{rack | probe_at: nil, probe_pending: false}

  @doc """
  RFC 8985 6.2 steps 1 to 3 for an ACK of `[una, ack)` with SACK `blocks`,
  `board` as it stood before the ACK. Returns `{sample, rack}`, the round
  trip sample Karn's algorithm allows, in microseconds, or nil.
  """
  def on_ack(rack, now, board, {una, ack}, blocks) do
    min_rtt = rack.min_rtt
    starts = with_starts(rack.log)
    portions = [{una, ack, true} | Enum.map(blocks, fn {l, r} -> {l, r, false} end)]

    acc = %{latest: nil, cumulative: nil, sack: nil, cumulative_rxt: false, min_rtt: rack.min_rtt}

    acc =
      Enum.reduce(portions, acc, fn portion, acc ->
        Enum.reduce(starts, acc, &newly_delivered(&1, &2, portion, {board, now, min_rtt}))
      end)

    rack = %{rack | min_rtt: acc.min_rtt}

    rack =
      case acc.latest do
        nil ->
          rack

        latest ->
          xmit = if rack.xmit == nil or after?(latest, rack.xmit), do: latest, else: rack.xmit
          %{rack | rtt: now - latest.at, xmit: xmit}
      end

    sample = if acc.cumulative_rxt, do: nil, else: acc.cumulative
    {sample || acc.sack, rack}
  end

  # The part of a range that the portion of the ACK, `[from, to)`, newly
  # reports delivered, if any is not SACKed already.
  defp newly_delivered({start, sent}, acc, {from, to, cumulative}, {board, now, min_rtt}) do
    {a, b} = {Seq.max(start, from), Seq.min(sent.end, to)}

    if Seq.ge?(a, b) or Scoreboard.sacked_between(board, a, b) == Seq.diff(b, a),
      do: acc,
      else: delivered(acc, sent, b, {now - sent.at, cumulative, min_rtt})
  end

  defp delivered(acc, %{retransmitted: true} = sent, b, {rtt, cumulative, min_rtt}) do
    acc = %{acc | cumulative_rxt: acc.cumulative_rxt or cumulative}

    # A resend faster than any first transmission may have been delivered
    # by its first transmission (step 2).
    if min_rtt == nil or rtt < min_rtt, do: acc, else: latest(acc, sent, b)
  end

  defp delivered(acc, sent, b, {rtt, true, _min_rtt}) do
    acc = %{
      acc
      | cumulative: max_sample(acc.cumulative, rtt),
        min_rtt: min_sample(acc.min_rtt, rtt)
    }

    latest(acc, sent, b)
  end

  defp delivered(acc, sent, b, {rtt, false, _min_rtt}) do
    acc = %{acc | sack: max_sample(acc.sack, rtt), min_rtt: min_sample(acc.min_rtt, rtt)}
    latest(acc, sent, b)
  end

  defp latest(acc, sent, b) do
    delivered = %{at: sent.at, end: b, first: sent.first, last: sent.last}

    if acc.latest == nil or after?(delivered, acc.latest),
      do: %{acc | latest: delivered},
      else: acc
  end

  defp max_sample(nil, rtt), do: rtt
  defp max_sample(sample, rtt), do: max(sample, rtt)

  defp min_sample(nil, rtt), do: rtt
  defp min_sample(min, rtt), do: min(min, rtt)

  # `RACK_sent_after`.
  defp after?(a, b), do: a.last > b.last or (a.last == b.last and Seq.gt?(a.end, b.end))

  @doc "A D-SACK raises the reordering window's multiplier, once a round trip."
  def on_dsack(rack, ack, nxt) do
    if rack.dsack_round != nil and Seq.lt?(ack, rack.dsack_round) do
      rack
    else
      %{
        rack
        | dsack_round: nxt,
          reo_wnd_mult: min(rack.reo_wnd_mult + 1, @reo_wnd_mult_max),
          reo_wnd_persist: @reo_wnd_persist
      }
    end
  end

  @doc "Counts down the recoveries that keep a raised multiplier."
  def on_recovery_end(rack) do
    persist = max(rack.reo_wnd_persist - 1, 0)
    mult = if persist == 0, do: 1, else: rack.reo_wnd_mult
    %{rack | reo_wnd_persist: persist, reo_wnd_mult: mult}
  end

  defp reo_wnd(rack, srtt) do
    base = srtt || rack.min_rtt || 0
    wnd = div(base, 4) * rack.reo_wnd_mult
    if srtt, do: min(wnd, srtt), else: wnd
  end

  @doc """
  RACK's loss detection, marking the log. Returns whether it newly deems
  an octet lost that is not SACKed, and arms the reordering timer.
  """
  def detect(%__MODULE__{xmit: nil} = rack, _board, _now, _srtt),
    do: {false, %{rack | reo_timeout: nil}}

  def detect(rack, board, now, srtt) do
    rtt = if srtt, do: max(rack.rtt, srtt), else: rack.rtt
    wait = rtt + reo_wnd(rack, srtt)
    {log, newly_lost, next} = detect_log(rack.log, board, now, rack.xmit, wait)
    {newly_lost, %{rack | log: log, reo_timeout: next}}
  end
end
