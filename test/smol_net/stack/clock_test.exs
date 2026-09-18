defmodule SmolNet.Stack.ClockTest do
  use ExUnit.Case, async: true

  alias SmolNet.Stack.Clock

  test "the system clock hands smoltcp a non-negative, advancing instant" do
    # smoltcp compares instants against timers that start at time zero (the challenge-ACK
    # rate limiter among them); the raw BEAM monotonic clock is negative, which would keep
    # those gates shut for good — a peer's keep-alive probe would never be answered.
    first = Clock.System.now()
    assert first >= 0
    assert first < 100 * 365 * 24 * 60 * 60 * 1000
    Process.sleep(5)
    assert Clock.System.now() >= first + 4
  end
end
