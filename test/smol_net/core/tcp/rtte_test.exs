defmodule SmolNet.Core.Tcp.RtteTest do
  # Ported from smoltcp's `test_rtt_estimator` and
  # `test_rtt_estimator_min_rto` (native/vendor/smoltcp/src/socket/tcp.rs).
  # Samples are milliseconds, as in smoltcp, but `retransmission_timeout/1`
  # returns microseconds, so each expected RTO is the Rust millisecond
  # literal times 1000; nothing is skipped.
  use ExUnit.Case, async: true

  alias SmolNet.Core.Tcp.Rtte

  test "test_rtt_estimator" do
    rtos = [
      6000,
      5000,
      4252,
      3692,
      3272,
      2956,
      2720,
      2540,
      2408,
      2308,
      2232,
      2176,
      2132,
      2100,
      2076,
      2060,
      2048,
      2036,
      2028,
      2024,
      2020,
      2016,
      2012,
      2012
    ]

    Enum.reduce(rtos, Rtte.new(), fn rto, r ->
      r = Rtte.sample(r, 2000)
      assert Rtte.retransmission_timeout(r) == rto * 1_000
      r
    end)
  end

  test "test_rtt_estimator_min_rto" do
    r = Rtte.new()
    assert Rtte.retransmission_timeout(r) == 1_000_000

    # Steady 10 ms samples converge on SRTT + 4 * RTTVAR well below 200 ms,
    # so the floor holds the RTO there, and not at RFC 6298's 1 s.
    r = Enum.reduce(1..32, r, fn _, r -> Rtte.sample(r, 10) end)
    assert Rtte.retransmission_timeout(r) == 200_000

    # A timeout still backs off from the floor.
    r = Rtte.on_rto(r)
    assert Rtte.retransmission_timeout(r) == 400_000
  end
end
