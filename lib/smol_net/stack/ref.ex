defmodule SmolNet.Stack.Ref do
  @moduledoc """
  Opaque reference to a running SmolNet stack bundle.

  The reference deliberately hides the processes which implement the bundle.
  Use the functions on `SmolNet` rather than depending on its fields.
  """

  alias SmolNet.Stack.IngressGate

  @enforce_keys [:bundle, :stack, :inet_backends, :ingress_gate, :ingress_token, :mtu]
  defstruct [:bundle, :stack, :inet_backends, :ingress_gate, :ingress_token, :mtu]

  @opaque t :: %__MODULE__{
            bundle: pid(),
            stack: pid(),
            inet_backends: pid(),
            ingress_gate: IngressGate.t(),
            ingress_token: reference(),
            mtu: pos_integer()
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
