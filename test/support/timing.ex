defmodule SmolNet.Test.Timing do
  @moduledoc false

  # Wall-clock budgets in this suite are one of two kinds, and only one of
  # them may move with the host.
  #
  #   * A *liveness* budget bounds how long the test is willing to wait for
  #     something that must eventually happen: an accept completing, a
  #     datagram arriving, a task returning. Its value is not the property
  #     under test, only a bound on how long a healthy run may take. On a
  #     contended shared runner the stack is correct but slower, so these
  #     budgets are multiplied by `scale/0`.
  #
  #   * A *quiescence* budget bounds how long the test waits to conclude that
  #     something did *not* happen, or asserts that an operation times out
  #     when it should. Here the wall clock is the property under test, so
  #     the budget is fixed: scaling it would weaken the assertion (a longer
  #     wait for an expected timeout) or slow the suite for no signal.
  #     `quiescence/1` marks these call sites so an unscaled literal is a
  #     deliberate choice rather than an oversight.
  #
  # Test files name their budgets as module attributes (`@wait_1s`,
  # `@idle_20ms`) rather than calling this module at each site, so a call site
  # still reads as a timeout and a file's budgets are declared in one place.
  #
  # The multiplier is read from `SMOLNET_TEST_TIMEOUT_SCALE`. It defaults to
  # 1, so a developer's run keeps the original strict budgets and a
  # regression that slows the stack still fails locally. CI sets it because
  # GitHub-hosted runners are preemptible and their compute environment is
  # outside this project's control — the same reasoning that made the NIF
  # wall-clock gate advisory there (see `scripts/nif_budget.exs`). The policy
  # and its rationale are recorded in `docs/adr/0012-test-wall-clock-budgets.md`.

  @env "SMOLNET_TEST_TIMEOUT_SCALE"
  @max_scale 10
  @key {__MODULE__, :scale}

  @doc """
  The multiplier applied to liveness budgets, read once from the environment.
  """
  @spec scale() :: number()
  def scale do
    case :persistent_term.get(@key, nil) do
      nil ->
        scale = read_scale()
        :persistent_term.put(@key, scale)
        scale

      scale ->
        scale
    end
  end

  @doc """
  Scales a budget that bounds how long a healthy run may take to make progress.
  """
  @spec liveness(pos_integer()) :: pos_integer()
  def liveness(budget) when is_integer(budget) and budget > 0 do
    ceil(budget * scale())
  end

  @doc """
  Marks a budget whose wall-clock value is itself under test. Never scaled.
  """
  @spec quiescence(pos_integer()) :: pos_integer()
  def quiescence(budget) when is_integer(budget) and budget > 0, do: budget

  defp read_scale do
    case System.get_env(@env) do
      nil ->
        1

      value ->
        parse_scale(value) ||
          raise "invalid #{@env}: #{inspect(value)} (expected a number in 1..#{@max_scale})"
    end
  end

  defp parse_scale(value) do
    parsed =
      case Float.parse(value) do
        {scale, ""} -> scale
        _other -> nil
      end

    if is_number(parsed) and parsed >= 1 and parsed <= @max_scale, do: parsed
  end
end
