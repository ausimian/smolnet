defmodule SmolNet do
  @moduledoc """
  An OTP-friendly embedded network stack powered by
  [`smoltcp`](https://github.com/smoltcp-rs/smoltcp).

  Each stack is an independent, supervised native network namespace. Stacks
  exchange complete raw IPv6 packets with a caller-provided link process.
  """

  alias SmolNet.Stack

  @doc """
  Starts an IPv6 raw-IP network stack.

  The returned reference is opaque and owns the complete temporary runtime
  bundle. Configure packet output with `egress: {pid, link_ref}`. Each emitted
  packet is delivered as `{:smol_stack, link_ref, :egress, packet}`.

  IPv6 addresses use `{{s1, s2, s3, s4, s5, s6, s7, s8}, prefix_length}`.
  Routes use `{destination, prefix_length, gateway}` with addresses in the same
  eight-segment tuple form.

  Native work limits can be reduced with the `:limits` option. It accepts a map
  containing any of `:bytes_copied`, `:output_packets`, `:ready_events`, and
  `:maintenance_work`; unspecified values retain their safe defaults.
  """
  @spec start_stack(keyword()) :: {:ok, Stack.Ref.t()} | {:error, term()}
  defdelegate start_stack(options \\ []), to: SmolNet.StackSupervisor, as: :start_stack

  @doc "Stops a stack and its complete runtime bundle."
  @spec stop_stack(Stack.Ref.t()) :: :ok | {:error, :closed}
  defdelegate stop_stack(stack), to: SmolNet.StackSupervisor, as: :stop_stack

  @doc """
  Asynchronously admits one complete raw IPv6 packet to a stack.

  The packet is validated and counted against the configured bounded ingress
  queue before this function returns. Queue saturation returns
  `{:error, :queue_full}` without sending the packet to the stack process.
  """
  @spec ingress(Stack.Ref.t(), binary()) :: :ok | {:error, atom()}
  defdelegate ingress(stack, packet), to: Stack

  @doc "Returns bounded-ingress, link, timer, and native stack metrics."
  @spec stack_info(Stack.Ref.t()) :: {:ok, map()} | {:error, :closed}
  defdelegate stack_info(stack), to: Stack, as: :info
end
