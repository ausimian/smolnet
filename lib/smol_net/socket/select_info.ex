defmodule SmolNet.Socket.SelectInfo do
  @moduledoc """
  Identifies one pending nonblocking socket operation.

  Select information is single-use. A matching readiness message only asks the
  caller to retry the operation; it does not guarantee completion.
  """

  @operations [:recv, :recvfrom, :accept, :send, :sendto, :connect]

  @enforce_keys [:operation, :ref]
  defstruct [:operation, :ref]

  @type operation :: :recv | :recvfrom | :accept | :send | :sendto | :connect
  @type t :: %__MODULE__{operation: operation(), ref: reference()}

  @doc false
  @spec new(operation(), reference()) :: t()
  def new(operation, reference)
      when operation in @operations and is_reference(reference) do
    %__MODULE__{operation: operation, ref: reference}
  end

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{operation: operation, ref: reference}) do
    operation in @operations and is_reference(reference)
  end

  def valid?(_value), do: false
end
