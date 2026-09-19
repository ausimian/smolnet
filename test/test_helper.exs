alias SmolNet.Test.Timing

# ExUnit's own budgets are liveness bounds: the per-test timeout and the
# default `assert_receive` wait both bound how long a healthy run may take.
# They move with the host so that scaled per-call budgets cannot be cut short
# by an unscaled enclosing one. `refute_receive_timeout` is a quiescence
# budget and keeps its default.
ExUnit.start(
  timeout: Timing.liveness(60_000),
  assert_receive_timeout: Timing.liveness(100)
)
