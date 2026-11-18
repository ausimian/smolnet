defmodule SmolNet.Conformance.YieldingNative do
  @moduledoc false

  # The NIF with its wall-clock budget cut to nothing for every call but
  # the polls, so each call yields at its first checkpoint and leaves the
  # rest to the driver's continuation polls.

  alias SmolNet.Native
  alias SmolNet.Stack.Backend, as: Behaviour
  alias SmolNet.Stack.Backend.Native, as: Backend

  defdelegate stack_new(limits, config, now), to: Backend
  defdelegate stack_poll(resource, now), to: Backend

  for {name, arity} <- Behaviour.behaviour_info(:callbacks),
      name not in [:stack_new, :stack_poll] do
    [resource | _rest] = args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(name)(unquote_splicing(args)) do
      {:ok, %{result: :ok}} = Native.test_set_budget_checkpoints(unquote(resource), 1)
      Backend.unquote(name)(unquote_splicing(args))
    end
  end
end
