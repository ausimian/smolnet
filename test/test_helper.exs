alias SmolNet.Stack.Backend
alias SmolNet.Test.Timing

# The suite runs once per backend: SMOLNET_TEST_BACKEND names the one every
# stack starts with unless a test chooses (default `native`, the NIF). Tests
# of the NIF itself are tagged `:nif`, and those needing its debug hooks
# `:debug_nif`; both run only on the NIF backend.
backend_name = System.get_env("SMOLNET_TEST_BACKEND", "native")

backend =
  Enum.find(Backend.names(), &(Atom.to_string(&1) == backend_name)) ||
    raise "unknown SMOLNET_TEST_BACKEND #{inspect(backend_name)}; " <>
            "expected one of #{inspect(Backend.names())}"

Application.put_env(:smolnet, :backend, backend)

# ExUnit's own budgets are liveness bounds: the per-test timeout and the
# default `assert_receive` wait both bound how long a healthy run may take.
# They move with the host so that scaled per-call budgets cannot be cut short
# by an unscaled enclosing one. `refute_receive_timeout` is a quiescence
# budget and keeps its default.
exclude =
  case backend do
    :native ->
      []

    :elixir ->
      [:nif, :debug_nif, :elixir_phase_7]
  end

ExUnit.start(
  exclude: exclude,
  timeout: Timing.liveness(60_000),
  assert_receive_timeout: Timing.liveness(100)
)
