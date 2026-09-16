defmodule SmolNet do
  @moduledoc """
  An OTP-friendly embedded network stack powered by
  [`smoltcp`](https://github.com/smoltcp-rs/smoltcp).

  Each stack is an independent, supervised native network namespace. Stacks
  exchange complete raw IPv6 packets with a caller-provided link process.

  ## IPv6 TCP endpoints and errors

  Low-level TCP endpoints use `:socket`-style maps:

      %{family: :inet6, addr: {0xfd00, 0, 0, 0, 0, 0, 0, 2}, port: 443}

  `flowinfo` may be omitted or set to zero. A link-local `fe80::/10` address
  requires a positive integer `scope_id` identifying its raw-IP link zone;
  global addresses require `scope_id: 0` (the default).

  Stable validation and bind errors are `:unsupported_family`,
  `:unsupported_socket`, `:unsupported_timeout`, `:invalid_options`,
  `:invalid_address`, `:invalid_port`, `:scope_required`, `:invalid_scope`,
  `:address_in_use`, `:address_not_available`, and
  `:ephemeral_ports_exhausted`. Connection lifecycle errors are
  `:network_unreachable`, `:connection_refused`, `:connection_reset`,
  `:connection_timeout`, `:already_connected`, `:not_bound`,
  `:not_connected`, `:busy`, `:closed`, `:invalid_socket`, and
  `:invalid_socket_state`.
  """

  alias SmolNet.Socket
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
  Hands one complete raw IPv6 packet from the stack's link feeder to the stack.

  Each stack has one serialized feeder. This call returns after the stack owner
  validates and accepts the packet, then native processing runs before the
  stack accepts another message. The feeder must bound its own transport input.
  """
  @spec ingress(Stack.Ref.t(), binary()) :: :ok | {:error, atom()}
  defdelegate ingress(stack, packet), to: Stack

  @doc "Returns ingress, link, timer, and native stack metrics."
  @spec stack_info(Stack.Ref.t()) :: {:ok, map()} | {:error, :closed}
  defdelegate stack_info(stack), to: Stack, as: :info

  @doc "Cancels the exact pending nonblocking operation identified by `select_info`."
  @spec cancel(Socket.t(), :socket.select_info()) ::
          :ok | :already_sent | :not_found | {:error, :closed | :invalid_socket}
  defdelegate cancel(socket, select_info), to: Socket

  @doc """
  Opens a bounded low-level IPv6 TCP stream socket on `stack`.

  Phase 4 supports only `open(:inet6, :stream, :tcp, stack: stack)`.
  IPv4 and other socket kinds fail explicitly.
  """
  @spec open(:inet6 | :inet, :stream, :tcp, keyword()) ::
          {:ok, Socket.t()} | {:error, atom()}
  defdelegate open(domain, type, protocol, options), to: Socket

  @doc """
  Binds an IPv6 TCP socket.

  Port zero allocates from the bounded range 49152..50175. Ports are unique
  within a stack during this phase, and allocation failure is reported as
  `:ephemeral_ports_exhausted`.
  """
  @spec bind(Socket.t(), Socket.sockaddr_in6()) :: :ok | {:error, atom()}
  defdelegate bind(socket, address), to: Socket

  @doc """
  Initiates or finalizes an IPv6 TCP connection without waiting for traffic.

  A pending handshake returns `{:select, select_info}`. After the matching
  one-shot socket message arrives, retry this function. The retry returns
  `:ok`, a stable connection error, or another select hint after a spurious
  wake. Only `:nowait` is supported until synchronous wrappers land in Phase 5.
  """
  @spec connect(Socket.t(), Socket.sockaddr_in6(), :nowait) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  defdelegate connect(socket, address, timeout_or_nowait), to: Socket

  @doc "Returns the bound IPv6 endpoint for a TCP socket."
  @spec sockname(Socket.t()) :: {:ok, Socket.sockaddr_in6()} | {:error, atom()}
  defdelegate sockname(socket), to: Socket

  @doc "Returns the peer IPv6 endpoint while a TCP connection is pending or established."
  @spec peername(Socket.t()) :: {:ok, Socket.sockaddr_in6()} | {:error, atom()}
  defdelegate peername(socket), to: Socket

  @doc "Abortively closes a low-level TCP socket and permanently invalidates its handle."
  @spec close(Socket.t()) :: :ok | {:error, atom()}
  defdelegate close(socket), to: Socket
end
