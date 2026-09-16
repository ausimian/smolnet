defmodule SmolNet.Stack.Ref do
  @moduledoc """
  Opaque reference to a running SmolNet stack bundle.

  The reference deliberately hides the processes which implement the bundle.
  Use the functions on `SmolNet` rather than depending on its fields.
  """

  @enforce_keys [:bundle, :stack, :inet_backends]
  defstruct [:bundle, :stack, :inet_backends]

  @opaque t :: %__MODULE__{
            bundle: pid(),
            stack: pid(),
            inet_backends: pid()
          }

  @doc false
  @spec pids(t()) :: %{bundle: pid(), stack: pid(), inet_backends: pid()}
  def pids(%__MODULE__{} = ref) do
    Map.take(ref, [:bundle, :stack, :inet_backends])
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(_ref, options) do
      concat(["#SmolNet.Stack.Ref<", to_doc(:running, options), ">"])
    end
  end
end
