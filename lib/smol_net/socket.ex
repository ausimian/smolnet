defmodule SmolNet.Socket do
  @moduledoc """
  A stable, lightweight identity for a socket owned by a `SmolNet` stack.

  Socket values are not processes. Operations are serialized by the stack
  process identified in the `:stack` field. The native socket ID and generation
  together prevent delayed readiness from addressing a later socket.
  """

  alias SmolNet.Socket.SelectInfo
  alias SmolNet.Stack

  @max_identity 576_460_752_303_423_487

  @enforce_keys [:stack, :id, :generation]
  defstruct [:stack, :id, :generation]

  @type t :: %__MODULE__{
          stack: pid(),
          id: pos_integer(),
          generation: pos_integer()
        }

  @doc false
  @spec new(pid(), map()) :: t()
  def new(stack, %{id: id, generation: generation})
      when is_pid(stack) and id in 1..@max_identity and generation in 1..@max_identity do
    %__MODULE__{stack: stack, id: id, generation: generation}
  end

  @doc """
  Cancels the exact pending operation described by `select_info`.

  Returns `:ok` when the waiter was removed, `:already_sent` when its one-shot
  readiness notification won the race, or `:not_found` when the reference does
  not identify the pending operation.
  """
  @spec cancel(t(), SelectInfo.t()) ::
          :ok | :already_sent | :not_found | {:error, :closed | :invalid_socket}
  def cancel(%__MODULE__{} = socket, %SelectInfo{} = select_info) do
    if valid?(socket) and SelectInfo.valid?(select_info) do
      Stack.cancel(socket, select_info)
    else
      {:error, :invalid_socket}
    end
  end

  def cancel(_socket, _select_info), do: {:error, :invalid_socket}

  @doc false
  @spec identity(t()) :: {pos_integer(), pos_integer()}
  def identity(%__MODULE__{id: id, generation: generation}), do: {id, generation}

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{stack: stack, id: id, generation: generation}) do
    is_pid(stack) and id in 1..@max_identity and generation in 1..@max_identity
  end

  def valid?(_socket), do: false
end
