defmodule SmolNet.Test.NativeBudget do
  @moduledoc false

  # Every native call stops at a wall-clock work budget as well as its work
  # limits. A preempted runner can spend the whole budget before a call does
  # the work a test expects of it in one call, and the call then returns
  # `more: true` with that work retained for a continuation. Tests that count
  # calls lift the deadline with `without_deadline/1`. It uses a debug NIF
  # hook, so tag its callers `:debug_nif`.

  alias SmolNet.Native

  # Forced budget checkpoints enough for any one call to end on its own work
  # limits, never on the wall clock or on the caller's spent reduction slice.
  @no_deadline_checkpoints 1_000_000

  @doc "Lifts the wall-clock deadline from the resource's next native call."
  @spec without_deadline(reference()) :: reference()
  def without_deadline(resource) do
    {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(resource, @no_deadline_checkpoints)

    resource
  end
end
