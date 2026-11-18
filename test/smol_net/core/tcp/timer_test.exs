defmodule SmolNet.Core.Tcp.TimerTest do
  # Ported from smoltcp's `test_timer_retransmit`
  # (native/vendor/smoltcp/src/socket/tcp.rs). Instants and durations are
  # microseconds here, so `Instant::from_millis(n)` is `n * 1000`; nothing
  # is skipped.
  use ExUnit.Case, async: true

  alias SmolNet.Core.Tcp.Timer

  @rto 100_000

  defp ms(n), do: n * 1_000

  test "test_timer_retransmit" do
    r = Timer.new()
    refute Timer.should_retransmit?(r, 1_000_000)
    r = Timer.set_for_retransmit(r, ms(1000), @rto)
    refute Timer.should_retransmit?(r, ms(1000))
    refute Timer.should_retransmit?(r, ms(1050))
    assert Timer.should_retransmit?(r, ms(1101))
    r = Timer.set_for_retransmit(r, ms(1101), @rto)
    refute Timer.should_retransmit?(r, ms(1101))
    refute Timer.should_retransmit?(r, ms(1150))
    refute Timer.should_retransmit?(r, ms(1200))
    assert Timer.should_retransmit?(r, ms(1301))
    r = Timer.set_for_idle(r, ms(1301), nil)
    refute Timer.should_retransmit?(r, ms(1350))
  end
end
