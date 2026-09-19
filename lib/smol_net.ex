defmodule SmolNet do
  @moduledoc """
  An OTP-friendly embedded network stack powered by
  [`smoltcp`](https://github.com/smoltcp-rs/smoltcp).

  Each stack is an independent, supervised native network namespace. Stacks
  exchange complete raw IPv4 or IPv6 packets with a caller-provided link process.

  ## TCP endpoints and errors

  Low-level TCP endpoints use explicit `:socket`-style maps:

      %{family: :inet, addr: {192, 0, 2, 2}, port: 443}
      %{family: :inet6, addr: {0xfd00, 0, 0, 0, 0, 0, 0, 2}, port: 443}

  `flowinfo` may be omitted or set to zero. A link-local `fe80::/10` address
  requires a positive integer `scope_id` identifying its raw-IP link zone;
  global addresses require `scope_id: 0` (the default).

  Stable validation and bind errors are `:unsupported_family`,
  `:unsupported_socket`, `:invalid_options`, `:invalid_address`,
  `:invalid_port`, `:invalid_backlog`, `:invalid_data`, `:invalid_length`,
  `:invalid_timeout`, `:message_too_large`,
  `:invalid_how`, `:scope_required`, `:invalid_scope`, `:address_in_use`,
  `:address_not_available`, and `:ephemeral_ports_exhausted`. Connection and
  stream lifecycle errors are
  `:network_unreachable`, `:connection_refused`, `:connection_reset`,
  `:connection_timeout`, `:already_connected`, `:not_bound`,
  `:not_connected`, `:busy`, `:closed`, `:invalid_socket`, and
  `:invalid_socket_state`.
  """

  alias SmolNet.Socket
  alias SmolNet.Stack

  import Kernel, except: [send: 2]

  @doc """
  Starts a raw-IP network stack.

  The returned reference is opaque and owns the complete temporary runtime
  bundle. Configure packet output with `egress: {pid, link_ref}`. Each bounded
  native output batch is delivered as
  `{:smol_stack, link_ref, :egress, [packet, ...]}`.

  IPv4 addresses use `{{a, b, c, d}, prefix_length}` and IPv6 addresses use
  `{{s1, s2, s3, s4, s5, s6, s7, s8}, prefix_length}`. Routes use
  `{destination, prefix_length, gateway}`; destination and gateway must have
  the same family. One stack may contain both families.

  Native work limits can be reduced with the `:limits` option. It accepts a map
  containing any of `:bytes_copied`, `:output_packets`, `:ready_events`, and
  `:maintenance_work`; unspecified values retain their safe defaults.

  `SmolNet.Loopback.start_link/1` starts a stack whose egress is a link back
  into itself, returns both references, and needs no external transport.
  """
  @spec start_stack(keyword()) :: {:ok, Stack.Ref.t()} | {:error, term()}
  defdelegate start_stack(options \\ []), to: SmolNet.StackSupervisor, as: :start_stack

  @doc "Stops a stack and its complete runtime bundle."
  @spec stop_stack(Stack.Ref.t()) :: :ok | {:error, :closed}
  defdelegate stop_stack(stack), to: SmolNet.StackSupervisor, as: :stop_stack

  @doc """
  Hands one complete raw IPv4 or IPv6 packet from the stack's link feeder to the stack.

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
  Opens a bounded low-level TCP stream or UDP datagram socket on `stack`.

  Family and kind are explicit and immutable. TCP and UDP support both IPv4
  and IPv6. TCP accepts socket-style `:rcvbuf` and `:sndbuf` options from
  1 KiB through 1 MiB; both default to 64 KiB and remain fixed after open.
  """
  @spec open(:inet6 | :inet, :stream | :dgram, :tcp | :udp, keyword()) ::
          {:ok, Socket.t()} | {:error, atom()}
  defdelegate open(domain, type, protocol, options), to: Socket

  @doc """
  Binds a TCP or UDP socket to an endpoint of its family.

  Port zero allocates from the bounded range 49152..50175. Ports are unique
  within one protocol and address family, so TCP and UDP may share a numeric
  port. Allocation failure is reported as `:ephemeral_ports_exhausted`.
  """
  @spec bind(Socket.t(), Socket.sockaddr_in() | Socket.sockaddr_in6()) ::
          :ok | {:error, atom()}
  defdelegate bind(socket, address), to: Socket

  @doc """
  Turns a bound TCP socket into a reusable bounded listener.

  Backlog must be in `1..128`. The accepted-child queue is capped at that
  value, while the native listening pool is capped at four sockets.
  """
  @spec listen(Socket.t(), pos_integer()) :: :ok | {:error, atom()}
  defdelegate listen(socket, backlog), to: Socket

  @doc "Accepts a TCP child, waiting indefinitely by default."
  @spec accept(Socket.t()) :: {:ok, Socket.t()} | {:error, atom()}
  defdelegate accept(listener), to: Socket

  @doc """
  Accepts with a finite, infinite, or nonblocking timeout.

  `:nowait` returns a read-direction `{:select, select_info}` retry hint. Each
  accepted child has a fresh stable identity and is independent of the
  listener after it is returned.
  """
  @spec accept(Socket.t(), :nowait | timeout()) ::
          {:ok, Socket.t()} | {:select, :socket.select_info()} | {:error, atom()}
  defdelegate accept(listener, timeout_or_nowait), to: Socket

  @doc "Connects a TCP or UDP socket, waiting indefinitely by default."
  @spec connect(Socket.t(), Socket.sockaddr_in() | Socket.sockaddr_in6()) ::
          :ok | {:error, atom()}
  defdelegate connect(socket, address), to: Socket

  @doc """
  Connects a TCP or UDP socket with a finite, infinite, or nonblocking timeout.

  TCP `:nowait` returns a one-shot `{:select, select_info}` retry hint. Finite
  and infinite waits run entirely in the caller and monitor the owning stack.
  UDP connect stores a peer immediately; connected sends must use that peer and
  received datagrams from other peers are discarded.
  """
  @spec connect(Socket.t(), Socket.sockaddr_in() | Socket.sockaddr_in6(), :nowait | timeout()) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  defdelegate connect(socket, address, timeout_or_nowait), to: Socket

  @doc "Sends a complete TCP byte stream, waiting indefinitely by default."
  @spec send(Socket.t(), iodata()) :: :ok | {:error, atom() | {atom(), binary()}}
  defdelegate send(socket, data), to: Socket

  @doc """
  Sends TCP stream data with a finite, infinite, or nonblocking timeout.

  The native stack copies at most one bounded chunk. A nonblocking partial
  result is `{:select, {select_info, unsent_binary}}`; the caller retains and
  retries that remainder. A timed synchronous send that made progress returns
  `{:error, {:timeout, unsent_binary}}`.
  """
  @spec send(Socket.t(), iodata(), :nowait | timeout()) ::
          :ok
          | {:select, {:socket.select_info(), binary()}}
          | {:error, atom() | {atom(), binary()}}
  defdelegate send(socket, data, timeout_or_nowait), to: Socket

  @doc "Receives TCP stream data, waiting indefinitely by default."
  @spec recv(Socket.t(), non_neg_integer()) ::
          {:ok, binary()} | {:error, atom() | {atom(), binary()}}
  defdelegate recv(socket, length), to: Socket

  @doc """
  Receives TCP stream data with a finite, infinite, or nonblocking timeout.

  Positive lengths are exact for synchronous calls unless peer EOF returns the
  final shorter buffered value. Length zero returns one bounded currently
  available chunk. Nonblocking partial exact reads return
  `{:select, {select_info, partial_binary}}`.
  """
  @spec recv(Socket.t(), non_neg_integer(), :nowait | timeout()) ::
          {:ok, binary()}
          | {:select, :socket.select_info()}
          | {:select, {:socket.select_info(), binary()}}
          | {:error, atom() | {atom(), binary()}}
  defdelegate recv(socket, length, timeout_or_nowait), to: Socket

  @doc "Sends one complete UDP datagram, waiting indefinitely by default."
  @spec sendto(Socket.t(), iodata(), Socket.sockaddr_in() | Socket.sockaddr_in6()) ::
          :ok | {:error, atom()}
  defdelegate sendto(socket, data, address), to: Socket

  @doc """
  Sends one complete UDP datagram with a finite, infinite, or nonblocking timeout.

  The datagram is either accepted in full or not accepted. `:nowait` returns a
  write-direction select hint when the bounded native transmit ring is full.
  """
  @spec sendto(
          Socket.t(),
          iodata(),
          Socket.sockaddr_in() | Socket.sockaddr_in6(),
          :nowait | timeout()
        ) ::
          :ok | {:select, :socket.select_info()} | {:error, atom()}
  defdelegate sendto(socket, data, address, timeout_or_nowait), to: Socket

  @doc "Receives one UDP datagram, waiting indefinitely by default."
  @spec recvfrom(Socket.t(), non_neg_integer()) ::
          {:ok, Socket.datagram()} | {:error, atom()}
  defdelegate recvfrom(socket, length), to: Socket

  @doc """
  Receives one UDP datagram with a finite, infinite, or nonblocking timeout.

  Length zero returns the complete datagram. A positive length truncates a
  larger datagram, discards its remainder, and sets `truncated: true`. Source
  and actual local-destination endpoints are always returned.
  """
  @spec recvfrom(Socket.t(), non_neg_integer(), :nowait | timeout()) ::
          {:ok, Socket.datagram()} | {:select, :socket.select_info()} | {:error, atom()}
  defdelegate recvfrom(socket, length, timeout_or_nowait), to: Socket

  @doc """
  Shuts down the read half, write half, or both halves of a TCP socket.

  Write shutdown drives a FIN and rejects later sends while preserving allowed
  reads. Read shutdown rejects later receives.
  """
  @spec shutdown(Socket.t(), :read | :write | :read_write) :: :ok | {:error, atom()}
  defdelegate shutdown(socket, how), to: Socket

  @doc "Returns the bound endpoint for a TCP or UDP socket."
  @spec sockname(Socket.t()) ::
          {:ok, Socket.sockaddr_in() | Socket.sockaddr_in6()} | {:error, atom()}
  defdelegate sockname(socket), to: Socket

  @doc "Returns the peer endpoint for a connected TCP or UDP socket."
  @spec peername(Socket.t()) ::
          {:ok, Socket.sockaddr_in() | Socket.sockaddr_in6()} | {:error, atom()}
  defdelegate peername(socket), to: Socket

  @doc "Closes a socket and permanently invalidates its public handle."
  @spec close(Socket.t()) :: :ok | {:error, atom()}
  defdelegate close(socket), to: Socket
end
