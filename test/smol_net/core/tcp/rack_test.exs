defmodule SmolNet.Core.Tcp.RackTest do
  # Ported from smoltcp's RACK unit tests
  # (native/vendor/smoltcp/src/socket/tcp/rack.rs). Instants and durations
  # are microseconds here; `ranges/1` reads send times back as milliseconds
  # so the expected tuples are Rust's literals. The Rust tests drive
  # `SendLog` on its own; here the log lives in the `Rack` struct, so
  # `record`, `mark_lost_below`, `pipe`, `first_lost` and `recovering?`
  # take the rack and `advance` is `Rack.advance_log/2` on `rack.log`.
  # Adapted, since what they call is private: `mark_all_lost` is reached
  # through `Rack.on_rewind/1`, which marks the log the same way;
  # `SendLog::detect(board, now, xmit, wait)` through `Rack.detect/4` with
  # `xmit` set, `rtt` the wait and no SRTT or minimum RTT, so that the
  # reordering window is zero and its `next` is `reo_timeout`; and
  # `reo_wnd` is read off the reordering timer `Rack.detect/4` arms for a
  # one-range log sent at 0 (see `reo_wnd/2`). Nothing is skipped.
  use ExUnit.Case, async: true

  alias SmolNet.Core.Tcp.Rack
  alias SmolNet.Core.Tcp.Scoreboard

  @log_size 64

  defp ms(n), do: n * 1_000

  defp record(rack, una, seq, end_, now), do: Rack.record(rack, una, seq, end_, now)

  defp advance(rack, ack), do: %{rack | log: Rack.advance_log(rack.log, ack)}

  defp with_starts(log) do
    {pairs, _end} =
      Enum.map_reduce(log.ranges, log.start, fn sent, start -> {{start, sent}, sent.end} end)

    pairs
  end

  # What RACK takes as delivered: the octets of the log up to `end_`.
  defp delivered(rack, end_) do
    {_start, sent} =
      Enum.find(with_starts(rack.log), fn {start, sent} -> start < end_ and end_ <= sent.end end)

    %{at: sent.at, end: end_, first: sent.first, last: sent.last}
  end

  # The latest delivered, as {sent at, end}.
  defp latest(%Rack{xmit: nil}), do: nil
  defp latest(%Rack{xmit: xmit}), do: {xmit.at, xmit.end}

  # The log as {start, end, sent at in ms, retransmitted, lost, doubled}.
  defp ranges(rack) do
    for {start, s} <- with_starts(rack.log),
        do: {start, s.end, div(s.at, 1_000), s.retransmitted, s.lost, s.doubled}
  end

  # `SendLog::detect`: with no SRTT and no minimum RTT the reordering
  # window is zero, so the wait is `rtt`. Returns {newly lost, next, rack}.
  defp detect(rack, board, now, xmit, wait) do
    {lost, rack} = Rack.detect(%{rack | xmit: xmit, rtt: wait}, board, now, nil)
    {lost, rack.reo_timeout, rack}
  end

  # `Rack::reo_wnd(srtt)`: one range [0, 10) sent at 0, before the latest
  # delivered, with `RACK.rtt` 0. Detection at 0 then arms the reordering
  # timer for `max(0, srtt) + reo_wnd`.
  defp reo_wnd(rack, srtt) do
    sent = %{end: 10, at: 0, retransmitted: false, lost: false, doubled: false, first: 1, last: 1}

    probe = %{
      rack
      | log: %{start: 0, ranges: [sent], sends: 2},
        xmit: %{at: 0, end: 20, first: 2, last: 2},
        rtt: 0
    }

    {false, probe} = Rack.detect(probe, Scoreboard.new(), 0, srtt)
    probe.reo_timeout - (srtt || 0)
  end

  test "record_merges_what_is_sent_together" do
    rack =
      Rack.new()
      |> record(100, 100, 110, ms(1))
      |> record(100, 110, 115, ms(1))
      |> record(100, 115, 120, ms(1))
      |> record(100, 120, 130, ms(2))

    assert ranges(rack) == [
             {100, 120, 1, false, false, false},
             {120, 130, 2, false, false, false}
           ]

    [first | _] = rack.log.ranges
    assert {first.first, first.last} == {1, 3}
    rack = advance(rack, 125)
    assert ranges(rack) == [{125, 130, 2, false, false, false}]
    rack = advance(rack, 130)
    assert ranges(rack) == []
    refute Rack.recovering?(rack)
  end

  test "record_splits_a_range_for_a_retransmission" do
    rack =
      Rack.new()
      |> record(0, 0, 30, ms(1))
      |> Rack.mark_lost_below(10)
      |> record(0, 0, 10, ms(5))
      |> record(0, 20, 25, ms(6))

    assert ranges(rack) == [
             {0, 10, 5, true, false, false},
             {10, 20, 1, false, false, false},
             {20, 25, 6, true, false, true},
             {25, 30, 1, false, false, false}
           ]

    assert Rack.recovering?(rack)

    # A resend running on into new data records both.
    rack = record(rack, 0, 25, 40, ms(7))

    assert Enum.drop(ranges(rack), 3) == [
             {25, 30, 7, true, false, true},
             {30, 40, 7, false, false, false}
           ]
  end

  test "a_full_log_merges_the_ranges_closest_in_time" do
    rack =
      Enum.reduce(0..(@log_size - 1), Rack.new(), fn i, rack ->
        # Every range 10 ms after the last, except one 1 ms after.
        at = if i == 40, do: 391, else: 10 * i
        record(rack, 0, 10 * i, 10 * i + 10, ms(at))
      end)

    assert length(rack.log.ranges) == @log_size
    rack = record(rack, 0, 640, 650, ms(1_000))
    assert length(rack.log.ranges) == @log_size
    ranges = ranges(rack)
    # Ranges 39 (sent at 390) and 40 (at 391) became one, sent at 391.
    assert Enum.at(ranges, 39) == {390, 410, 391, false, false, false}
    assert Enum.at(ranges, @log_size - 1) == {640, 650, 1_000, false, false, false}
  end

  test "mark_lost_below_leaves_retransmissions_to_rack" do
    rack =
      Rack.new()
      |> record(0, 0, 30, ms(1))
      |> record(0, 10, 20, ms(2))
      |> Rack.mark_lost_below(25)

    assert ranges(rack) == [
             {0, 10, 1, false, true, false},
             {10, 20, 2, true, false, false},
             {20, 25, 1, false, true, false},
             {25, 30, 1, false, false, false}
           ]

    # `mark_all_lost`, by way of `on_rewind`.
    rack = Rack.on_rewind(rack)
    assert Enum.all?(ranges(rack), fn r -> elem(r, 4) and not elem(r, 5) end)
  end

  test "pipe_counts_each_copy_in_flight" do
    {true, board} = Scoreboard.add(Scoreboard.new(), 20, 30)
    rack = record(Rack.new(), 0, 0, 40, ms(1))
    assert Rack.pipe(rack, board, 0, 40) == 30
    # Lost octets do not count; a resend of them counts once, and a
    # resend of octets not deemed lost counts twice.
    rack =
      rack
      |> Rack.mark_lost_below(10)
      |> record(0, 0, 5, ms(2))
      |> record(0, 30, 35, ms(2))

    assert Rack.pipe(rack, board, 0, 40) == 5 + 10 + 10 + 5
    # Octets beyond the log count once.
    assert Rack.pipe(rack, board, 0, 50) == 40
    assert Rack.first_lost(rack, board, 40) == {5, 10}
    assert Rack.first_lost(rack, board, 7) == {5, 7}
  end

  test "detect_marks_what_was_sent_before_the_latest_delivered" do
    board = Scoreboard.new()

    rack =
      Rack.new()
      |> record(0, 0, 10, ms(100))
      |> record(0, 10, 30, ms(110))
      |> record(0, 30, 40, ms(120))

    wait = ms(50)

    # [10, 20) was the latest delivered. What was sent 50 ms before now
    # is lost, and it will be at 160. The octets sent after it, in the
    # same millisecond or later, are not judged.
    xmit = delivered(rack, 20)
    {lost, next, rack} = detect(rack, board, ms(155), xmit, wait)
    assert lost
    assert next == ms(160)

    assert ranges(rack) == [
             {0, 10, 100, false, true, false},
             {10, 30, 110, false, false, false},
             {30, 40, 120, false, false, false}
           ]

    {lost, next, rack} = detect(rack, board, ms(160), xmit, wait)
    assert lost
    assert next == nil
    assert Enum.at(ranges(rack), 1) == {10, 20, 110, false, true, false}
  end

  test "detect_orders_what_is_sent_in_the_same_millisecond" do
    board = Scoreboard.new()
    wait = 0

    rack =
      Rack.new()
      |> record(0, 0, 30, ms(100))
      |> Rack.mark_lost_below(10)
      # In one millisecond, new data goes out, then a resend. The new
      # data's delivery says nothing of the resend, sent after it.
      |> record(0, 30, 40, ms(200))
      |> record(0, 0, 10, ms(200))

    xmit = delivered(rack, 40)
    assert {true, nil, rack} = detect(rack, board, ms(200), xmit, wait)
    refute elem(Enum.at(ranges(rack), 0), 4)
    assert elem(Enum.at(ranges(rack), 1), 4)

    # Data sent after the resend, in the same millisecond, is.
    rack =
      rack
      |> record(0, 40, 50, ms(200))
      |> record(0, 50, 60, ms(200))

    xmit = delivered(rack, 60)
    assert {true, nil, rack} = detect(rack, board, ms(200), xmit, wait)
    assert elem(Enum.at(ranges(rack), 0), 4)
    assert Enum.at(ranges(rack), 3) == {40, 60, 200, false, true, false}
  end

  test "detect_ignores_sacked_octets" do
    {true, board} = Scoreboard.add(Scoreboard.new(), 0, 20)

    rack =
      Rack.new()
      |> record(0, 0, 10, ms(100))
      |> record(0, 10, 20, ms(110))

    wait = ms(50)
    assert {false, nil, _rack} = detect(rack, board, ms(200), delivered(rack, 20), wait)
  end

  test "on_ack_samples_only_first_transmissions" do
    board = Scoreboard.new()

    rack =
      Rack.new()
      |> record(0, 0, 10, ms(100))
      |> record(0, 10, 20, ms(110))
      |> record(0, 20, 30, ms(120))
      |> record(0, 0, 10, ms(130))

    # [20, 30) is SACKed: a sample, and the latest delivered.
    blocks = [{20, 30}]
    {sample, rack} = Rack.on_ack(rack, ms(150), board, {0, 0}, blocks)
    assert sample == ms(30)
    assert latest(rack) == {ms(120), 30}
    assert rack.rtt == ms(30)
    assert rack.min_rtt == ms(30)

    # An ACK of the resent [0, 10) and of [10, 20) takes no sample: the
    # resend may be what arrived, and [10, 20) may have arrived long
    # before.
    {true, board} = Scoreboard.add(Scoreboard.new(), 20, 30)
    {sample, rack} = Rack.on_ack(rack, ms(170), board, {0, 20}, [])
    assert sample == nil
    assert latest(rack) == {ms(130), 10}
    assert rack.rtt == ms(40)
  end

  test "on_ack_skips_a_resend_acknowledged_too_soon" do
    board = Scoreboard.new()

    rack =
      Rack.new()
      |> record(0, 0, 10, ms(100))
      |> record(0, 10, 20, ms(100))

    {_sample, rack} = Rack.on_ack(rack, ms(150), board, {0, 0}, [{10, 20}])
    rack = record(rack, 0, 0, 10, ms(160))

    # Acknowledged 10 ms after the resend, less than the minimum round
    # trip: the first transmission must be what arrived.
    {_sample, rack} = Rack.on_ack(rack, ms(170), board, {0, 10}, [])
    assert latest(rack) == {ms(100), 20}
    assert rack.rtt == ms(50)
  end

  test "reo_wnd_follows_srtt_and_dsacks" do
    rack = %{Rack.new() | min_rtt: ms(40)}
    assert reo_wnd(rack, nil) == ms(10)
    srtt = ms(50)
    assert reo_wnd(rack, srtt) == 12_500

    # A D-SACK raises the multiplier once a round trip, up to SRTT.
    rack = rack |> Rack.on_dsack(0, 100) |> Rack.on_dsack(50, 150)
    assert reo_wnd(rack, srtt) == ms(25)

    rack =
      rack
      |> Rack.on_dsack(100, 200)
      |> Rack.on_dsack(200, 300)
      |> Rack.on_dsack(300, 400)
      |> Rack.on_dsack(400, 500)

    assert reo_wnd(rack, srtt) == ms(50)

    # Sixteen recoveries without one restore it.
    rack = Enum.reduce(1..15, rack, fn _, rack -> Rack.on_recovery_end(rack) end)
    assert rack.reo_wnd_mult == 6
    rack = Rack.on_recovery_end(rack)
    assert reo_wnd(rack, srtt) == 12_500
  end
end
