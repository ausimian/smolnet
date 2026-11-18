defmodule SmolNet.Core.Tcp.CubicTest do
  # Ported from smoltcp's CUBIC unit tests
  # (native/vendor/smoltcp/src/socket/tcp/congestion/cubic.rs). Instants
  # are microseconds here, so `Instant::from_millis(n)` is `ms(n)`, and
  # `on_ack` takes the estimator's smoothed RTT, which is 0 for Rust's
  # `RttEstimator::default()`. `on_dup_ack` drops the `now` and
  # `in_flight` it ignored in Rust, and `on_rto` the `now`. One adaptation:
  # `congestion_avoidance_works` sets `w_max` and calls the private
  # `recompute_k`; here a second `set_mss/2` recomputes K, since the
  # window already is the initial one and `set_mss/2` changes nothing
  # else. `test_cube_root` passes floats, as `cube_root/1` takes only
  # those, and checks against `:math.pow(i, 1 / 3)` where Rust uses
  # `cbrt`. Nothing is skipped.
  use ExUnit.Case, async: true

  import Bitwise

  alias SmolNet.Core.Tcp.Cubic
  alias SmolNet.Core.Tcp.Rtte

  @mss 1024
  @beta 0.7

  defp ms(n), do: n * 1_000

  defp ack(cubic, len, now) do
    in_flight = max(Cubic.window(cubic) - @mss, 0)
    Cubic.on_ack(cubic, now, len, in_flight, Rtte.smoothed_rtt(Rtte.new()))
  end

  defp new_cubic, do: Cubic.set_mss(Cubic.new(), @mss)

  test "congestion_avoidance_works" do
    cubic = new_cubic()
    cubic = Cubic.set_mss(%{cubic | w_max: @mss * 32}, @mss)

    # Post-fast-recovery state: cwnd = ssthresh ≈ w_max * beta.
    cwnd = div(@mss * 32 * 7, 10)
    cubic = %{cubic | cwnd: cwnd, ssthresh: cwnd, recovery_start: ms(0)}

    # CA at small time intervals should grow by less than 1 MSS per ACK.
    cubic =
      Enum.reduce(1..9, cubic, fn i, cubic ->
        initial_cwnd = Cubic.window(cubic)
        cubic = ack(cubic, @mss, ms(i))
        assert Cubic.window(cubic) < initial_cwnd + @mss
        cubic
      end)

    # CA approaches w_max as t approaches K, and exceeds it past K.
    pre = Cubic.window(cubic)
    cubic = Enum.reduce(0..59, cubic, fn i, cubic -> ack(cubic, @mss, ms(i * 100)) end)
    assert Cubic.window(cubic) >= cubic.w_max
    assert Cubic.window(cubic) > pre

    # RFC 9438 §4.2: the target is clamped to 1.5 * cwnd
    pre = Cubic.window(cubic)
    cubic = ack(cubic, @mss, ms(100_000))
    assert Cubic.window(cubic) <= pre + @mss

    # CA should still cap at the receive window once enough ACKs accrue.
    cubic = Enum.reduce(0..199, cubic, fn i, cubic -> ack(cubic, @mss, ms(100_000 + i * 100)) end)
    assert Cubic.window(cubic) == cubic.rwnd
  end

  # Acknowledges `rounds` round trips of 50 ms from `start` ms, a window
  # each, `per_ack` bytes an ACK spread evenly, and returns the growth.
  defp grow_over(cubic, start, rounds, per_ack) do
    before = Cubic.window(cubic)

    cubic =
      Enum.reduce(0..(rounds - 1)//1, cubic, fn round, cubic ->
        acks = div(Cubic.window(cubic), per_ack)

        Enum.reduce(0..(acks - 1)//1, cubic, fn i, cubic ->
          now = start + round * 50 + div(50 * i, acks)
          ack(cubic, per_ack, ms(now))
        end)
      end)

    Cubic.window(cubic) - before
  end

  test "congestion_avoidance_counts_what_each_ack_acknowledges" do
    # #126, #119: an ACK every second segment, or a stretch ACK, must
    # grow the window as fast as an ACK per segment does, on the
    # Reno-friendly line from slow start, and on the curve after a loss.
    for loss <- [false, true] do
      growth = fn per_ack ->
        cubic = Cubic.set_remote_window(new_cubic(), 1 <<< 20)
        cubic = %{cubic | cwnd: 64 * @mss, ssthresh: 64 * @mss}

        cubic =
          if loss do
            cubic = %{cubic | w_max: 100 * @mss}
            cubic = Cubic.on_loss(cubic, ms(0), cubic.cwnd)
            ack(cubic, @mss, ms(1))
          else
            cubic
          end

        grow_over(cubic, 2, 20, per_ack)
      end

      {each, delayed, stretch} = {growth.(@mss), growth.(2 * @mss), growth.(8 * @mss)}
      assert each > 4 * @mss, "loss #{loss}: #{each}"
      assert delayed * 10 >= each * 9, "loss #{loss}: #{delayed} against #{each}"
      assert stretch * 10 >= each * 8, "loss #{loss}: #{stretch} against #{each}"
    end
  end

  test "fast_recovery_works" do
    cubic = %{new_cubic() | cwnd: @mss * 32}

    # duplicate ACKs before fast recovery should do nothing
    initial_cwnd = Cubic.window(cubic)
    cubic = Enum.reduce(1..3, cubic, fn _, cubic -> Cubic.on_dup_ack(cubic, @mss) end)
    assert Cubic.window(cubic) == initial_cwnd

    # we enter fast recovery upon minor loss (three duplicate ACKs).
    # ssthresh = flight_size * beta_cubic, cwnd = ssthresh + 3*MSS, recovery_start = now.
    # w_max = cwnd since the prior w_max (initial 2*MSS) is below cwnd.
    in_flight = div(initial_cwnd, 2)
    expected_ssthresh = trunc(in_flight * @beta)
    cubic = Cubic.on_loss(cubic, ms(0), in_flight)
    assert cubic.ssthresh == expected_ssthresh
    assert cubic.cwnd == expected_ssthresh + 3 * @mss
    assert cubic.w_max == initial_cwnd
    assert cubic.in_fast_recovery
    assert cubic.recovery_start == ms(0)

    # in fast recovery, each dup-ACK should increase the cwnd by 1 MSS
    initial_cwnd = Cubic.window(cubic)

    cubic =
      Enum.reduce(0..2, cubic, fn i, cubic ->
        cubic =
          Enum.reduce(1..3, cubic, fn _, cubic ->
            initial_cwnd = Cubic.window(cubic)
            cubic = Cubic.on_dup_ack(cubic, @mss)
            assert Cubic.window(cubic) == initial_cwnd + @mss
            cubic
          end)

        # multiple loss events (trip-dup-ack) should not trigger additional fast recovery reductions
        initial_cwnd = Cubic.window(cubic)
        initial_ssthresh = cubic.ssthresh
        initial_w_max = cubic.w_max
        cubic = Cubic.on_loss(cubic, ms(i), initial_cwnd)
        assert Cubic.window(cubic) == initial_cwnd
        assert cubic.ssthresh == initial_ssthresh
        assert cubic.w_max == initial_w_max
        cubic
      end)

    assert Cubic.window(cubic) == initial_cwnd + @mss * 9

    # a non-duplicate ACK exits fast recovery and deflates cwnd to ssthresh
    cubic = ack(cubic, @mss, ms(10))
    assert Cubic.window(cubic) == cubic.ssthresh
    refute cubic.in_fast_recovery
  end

  test "zero_length_ack_does_not_exit_fast_recovery" do
    cubic = %{new_cubic() | cwnd: @mss * 32}

    cubic = Cubic.on_loss(cubic, ms(0), cubic.cwnd)
    assert cubic.in_fast_recovery

    cwnd = Cubic.window(cubic)
    ssthresh = cubic.ssthresh

    # Accepted segments that acknowledge no new data (window updates,
    # data segments from the remote) must not end fast recovery or
    # change the window.
    cubic = ack(cubic, 0, ms(1))
    assert cubic.in_fast_recovery
    assert Cubic.window(cubic) == cwnd
    assert cubic.ssthresh == ssthresh

    # The first ACK of new data still exits and deflates.
    cubic = ack(cubic, @mss, ms(2))
    refute cubic.in_fast_recovery
    assert Cubic.window(cubic) == ssthresh
  end

  test "repeated_rto_holds_ssthresh" do
    cubic = %{new_cubic() | cwnd: @mss * 32}

    # First RTO reduces ssthresh based on the flight size.
    cubic = Cubic.on_rto(cubic, @mss * 32)
    ssthresh = cubic.ssthresh
    assert ssthresh == trunc(32.0 * @mss * @beta)

    # Until new data is ACKed, further RTOs are retransmissions of the
    # same segment and must hold ssthresh constant instead of collapsing
    # it towards the minimum.
    cubic = Cubic.on_rto(cubic, @mss)
    assert cubic.ssthresh == ssthresh

    # Once new data is ACKed, the next RTO is a fresh loss detection
    # and reduces ssthresh again.
    cubic = ack(cubic, @mss, ms(2))
    cubic = Cubic.on_rto(cubic, @mss * 4)
    assert cubic.ssthresh == trunc(4.0 * @mss * @beta)
  end

  test "slow_start_works" do
    cubic = %{new_cubic() | cwnd: @mss * 32, ssthresh: @mss * 16}

    # we enter slow start upon major loss (an RTO)
    # window resets to MSS, ssthresh becomes a fraction of the inflight bytes,
    # recovery_start is cleared so any later CA uses a fresh epoch,
    # and w_max is preserved (RFC 9438 §4.8 defers it to the next CA stage).
    w_max_before_rto = cubic.w_max
    inflight = Cubic.window(cubic)
    cubic = Cubic.on_rto(cubic, inflight)
    assert cubic.ssthresh == trunc(inflight * @beta)
    assert Cubic.window(cubic) == @mss
    refute cubic.in_fast_recovery
    assert cubic.recovery_start == nil
    assert cubic.w_max == w_max_before_rto

    # slow start grows by at most the MSS per ack
    initial_cwnd = Cubic.window(cubic)

    cubic =
      Enum.reduce(0..9, cubic, fn i, cubic ->
        initial_cwnd = Cubic.window(cubic)
        cubic = ack(cubic, @mss * 2, ms(i))
        assert Cubic.window(cubic) == initial_cwnd + @mss
        cubic
      end)

    assert Cubic.window(cubic) == initial_cwnd + @mss * 10

    # slow start uses the number of ACKed bytes if they're less than the MSS
    initial_cwnd = Cubic.window(cubic)

    cubic =
      Enum.reduce(0..9, cubic, fn i, cubic ->
        initial_cwnd = Cubic.window(cubic)
        cubic = ack(cubic, div(@mss, 2), ms(10 + i))
        assert Cubic.window(cubic) == initial_cwnd + div(@mss, 2)
        cubic
      end)

    assert Cubic.window(cubic) == initial_cwnd + div(@mss, 2) * 10

    # slow start transitions to congestion avoidance at ssthresh
    initial_cwnd = Cubic.window(cubic)
    cubic = %{cubic | ssthresh: initial_cwnd + @mss}
    cubic = ack(cubic, @mss, ms(30))
    assert Cubic.window(cubic) == initial_cwnd + @mss
    assert cubic.ssthresh == initial_cwnd + @mss
  end

  test "slow_start_counts_what_a_stretch_ack_acknowledges" do
    # #126: a receiver that coalesces segments (GRO, LRO) acknowledges
    # several at once, and slow start must still double each round trip.
    cubic = new_cubic()
    initial = Cubic.window(cubic)
    assert initial == 10 * @mss

    # A round trip of ACKs of five segments each.
    cubic = cubic |> ack(5 * @mss, ms(1)) |> ack(5 * @mss, ms(2))
    assert Cubic.window(cubic) == 2 * initial

    # One ACK for the whole window, as GRO of 64 KiB may send.
    cubic = ack(cubic, 2 * initial, ms(3))
    assert Cubic.window(cubic) == 4 * initial

    # It stops at ssthresh, and the next ACK is congestion avoidance's.
    cubic = %{cubic | ssthresh: Cubic.window(cubic) + 3 * @mss}
    cubic = ack(cubic, 10 * @mss, ms(4))
    assert Cubic.window(cubic) == cubic.ssthresh
    assert cubic.recovery_start == nil
    cubic = ack(cubic, @mss, ms(5))
    assert cubic.recovery_start == ms(5)
  end

  test "slow_start_after_a_timeout_counts_one_mss_an_ack" do
    # RFC 3465 §2.3: after a timeout everything is resent, and one ACK
    # may cover what the receiver held, so each counts one MSS.
    cubic = %{new_cubic() | cwnd: 40 * @mss}
    cubic = Cubic.on_rto(cubic, 40 * @mss)

    cubic =
      Enum.reduce(1..5, cubic, fn i, cubic ->
        cubic = ack(cubic, 8 * @mss, ms(i))
        assert Cubic.window(cubic) == (1 + i) * @mss
        cubic
      end)

    # Reaching ssthresh ends it.
    {cubic, _time} = ack_until_ssthresh(cubic, 5, 8 * @mss, fn _before, _cubic -> :ok end)
    assert Cubic.window(cubic) == cubic.ssthresh
    refute cubic.rto_slow_start
  end

  # Rust's `while cubic.window() < cubic.ssthresh { time += 1; ack(..) }`,
  # with `check` given the window before and the state after each ACK.
  # Returns the state and the time reached.
  defp ack_until_ssthresh(cubic, time, len, check) do
    if Cubic.window(cubic) < cubic.ssthresh do
      before = Cubic.window(cubic)
      cubic = ack(cubic, len, ms(time + 1))
      check.(before, cubic)
      ack_until_ssthresh(cubic, time + 1, len, check)
    else
      {cubic, time}
    end
  end

  # Slow start from the default state, thirty ACKs of an MSS each.
  defp slow_start_30 do
    cubic = new_cubic()
    initial_cwnd = Cubic.window(cubic)
    cubic = Enum.reduce(1..30, cubic, fn time, cubic -> ack(cubic, @mss, ms(time)) end)
    assert Cubic.window(cubic) == initial_cwnd + @mss * 30
    assert Cubic.window(cubic) < cubic.ssthresh
    {cubic, 30}
  end

  test "progress_to_ca_via_rto" do
    # slow start from default state
    {cubic, time} = slow_start_30()

    # rto: cwnd resets to MSS and sstresh reduces
    rto_cwnd = Cubic.window(cubic)
    cubic = Cubic.on_rto(cubic, rto_cwnd)
    assert Cubic.window(cubic) == @mss
    assert cubic.ssthresh == trunc(rto_cwnd * @beta)

    # slow start again until cwnd reaches new ssthresh
    {cubic, time} =
      ack_until_ssthresh(cubic, time, @mss, fn before, cubic ->
        assert Cubic.window(cubic) == before + @mss
      end)

    assert Cubic.window(cubic) >= cubic.ssthresh
    assert Cubic.window(cubic) < cubic.ssthresh + @mss

    # ca: first CA ACK starts a fresh epoch with W_max = cwnd and K = 0.
    time = time + 1
    cwnd_at_ca_entry = Cubic.window(cubic)
    cubic = ack(cubic, @mss, ms(time))
    assert cubic.w_max == cwnd_at_ca_entry
    assert cubic.k == 0.0
    assert Cubic.window(cubic) >= cwnd_at_ca_entry
  end

  test "progress_to_ca_via_loss" do
    # slow start from default state
    {cubic, time} = slow_start_30()

    # dup ACKs: ssthresh = cwnd * beta, cwnd = ssthresh + 3*MSS, recovery_start = now
    time = time + 1
    loss_cwnd = Cubic.window(cubic)
    expected_ssthresh = trunc(loss_cwnd * @beta)
    cubic = Cubic.on_loss(cubic, ms(time), loss_cwnd)
    assert cubic.ssthresh == expected_ssthresh
    assert Cubic.window(cubic) == expected_ssthresh + 3 * @mss
    assert cubic.in_fast_recovery
    assert cubic.recovery_start == ms(time)

    # inflate cwnd on each duplicate ACK
    cubic =
      Enum.reduce(1..9, cubic, fn _, cubic ->
        initial_cwnd = Cubic.window(cubic)
        cubic = Cubic.on_dup_ack(cubic, @mss)
        assert Cubic.window(cubic) == initial_cwnd + @mss
        cubic
      end)

    time = time + 9

    # non-duplicate ACK deflates cwnd to ssthresh
    time = time + 1
    cubic = ack(cubic, @mss, ms(time))
    assert Cubic.window(cubic) == expected_ssthresh
    refute cubic.in_fast_recovery

    # ca: subsequent ACKs follow the cubic curve
    time = time + 1
    initial_cwnd = Cubic.window(cubic)
    cubic = ack(cubic, @mss, ms(time))
    assert Cubic.window(cubic) >= initial_cwnd
  end

  test "initial_window_follows_the_mss" do
    # RFC 6928: min(10 * MSS, max(2 * MSS, 14600)).
    for {mss, window} <- [{536, 5_360}, {1_460, 14_600}, {8_960, 17_920}] do
      cubic = Cubic.set_mss(Cubic.new(), mss)
      assert Cubic.window(cubic) == window, "MSS #{mss}"
      assert cubic.w_max == window
      assert cubic.ssthresh == Cubic.usize_max()
    end
  end

  test "initial_window_is_not_raised_after_a_timeout" do
    # A retransmitted SYN leaves the window at the loss window.
    cubic = Cubic.on_rto(Cubic.new(), 0)
    window = Cubic.window(cubic)
    cubic = Cubic.set_mss(cubic, 1_460)
    assert Cubic.window(cubic) == window

    # So does a window that has grown or shrunk from use.
    cubic = Cubic.set_mss(Cubic.new(), 1_460)
    cubic = Cubic.on_loss(cubic, ms(0), Cubic.window(cubic))
    window = Cubic.window(cubic)
    cubic = Cubic.set_mss(cubic, 1_460)
    assert Cubic.window(cubic) == window
  end

  test "fast_convergence_reduces_w_max" do
    cubic = %{new_cubic() | w_max: @mss * 50, cwnd: @mss * 30}

    # Loss while cwnd < w_max (a new competing flow) should pull w_max down.
    w_max_prev = cubic.w_max
    cubic = Cubic.on_loss(cubic, ms(0), cubic.cwnd)
    assert cubic.w_max < w_max_prev
  end

  test "test_cube_root" do
    {max_err, max_err_at, max_err_found, max_err_expected} =
      Enum.reduce(1..999_999//99, {0.0, 0, 0.0, 0.0}, fn i, worst ->
        found = Cubic.cube_root(i * 1.0)
        expected = :math.pow(i, 1 / 3)
        err = abs(found - expected) / expected
        if err > elem(worst, 0), do: {err, i, found, expected}, else: worst
      end)

    assert max_err < 0.0005,
           "cube_root(#{max_err_at}) = #{max_err_found}, expected ~#{max_err_expected}, rel err #{max_err}"
  end
end
