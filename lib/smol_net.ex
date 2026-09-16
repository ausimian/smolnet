defmodule SmolNet do
  @moduledoc """
  An OTP-friendly embedded network stack powered by
  [`smoltcp`](https://github.com/smoltcp-rs/smoltcp).

  Each stack is an independent, supervised native network namespace. Phase 1
  exposes stack lifecycle only; packet transport and sockets are added in later
  implementation phases.
  """

  alias SmolNet.Stack

  @doc """
  Starts an empty network stack.

  The returned reference is opaque and owns the complete temporary runtime
  bundle. No packet transport or socket operations are available yet.

  Native work limits can be reduced with the `:limits` option. It accepts a map
  containing any of `:bytes_copied`, `:output_packets`, `:ready_events`, and
  `:maintenance_work`; unspecified values retain their safe defaults.
  """
  @spec start_stack(keyword()) :: {:ok, Stack.Ref.t()} | {:error, term()}
  defdelegate start_stack(options \\ []), to: SmolNet.StackSupervisor, as: :start_stack

  @doc "Stops a stack and its complete runtime bundle."
  @spec stop_stack(Stack.Ref.t()) :: :ok | {:error, :closed}
  defdelegate stop_stack(stack), to: SmolNet.StackSupervisor, as: :stop_stack
end
