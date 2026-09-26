defmodule SmolNet do
  @moduledoc """
  An OTP-friendly embedded network stack powered by
  [`smoltcp`](https://github.com/smoltcp-rs/smoltcp).

  Each stack is an independent, supervised native network namespace. Stacks
  exchange complete raw IPv4 or IPv6 packets with a caller-provided link process.

  ## Links

  A link carries a stack's packets over an application-defined transport. It
  receives outbound batches as `{:smol_stack, link_ref, :egress, packets}` and
  hands inbound packets back with `ingress/2`. Each side watches the other. The
  stack monitors its link and applies its `:link_down` policy when the link
  exits. A link calls `monitor/1` to receive an ordinary `:DOWN` message when
  its stack stops, whether through `stop_stack/1` or a crash, so it can exit
  instead of running a transport in front of a stack that is gone.

  A link cannot refuse a batch once the stack has sent it. A link with a
  bounded queue can instead start its stack with `:egress_credit` and grant
  more with `grant_egress/3` as it forwards packets. The stack then never sends
  more than the link has granted, and holds the rest back in its sockets.

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
  containing any of `:bytes_copied`, `:input_packets`, `:output_packets`,
  `:ready_events`, and `:maintenance_work`; unspecified values retain their
  safe defaults. `:input_packets` defaults to one and may be raised to 32 for
  bounded batched ingress. `:bytes_copied` bounds, separately, the bytes one
  native call copies for its operation (a send, a receive, or an ingress
  batch) and the bytes of the packets it hands the link, so a send of the
  whole limit still emits its first segments in the same call.

  `:sockets` in the same map is the most sockets the stack holds at once. It
  defaults to 64 and may be raised to 512. It counts native backing sockets:
  one per TCP or UDP socket, one per member of a TCP listener's accept pool
  (up to 4), and one per configured address for a UDP socket bound to a
  wildcard address. A TCP socket that closes first keeps its slot until
  TIME-WAIT ends, about 10 s after the close, so a stack whose sockets close
  first sustains about `sockets / 10` new connections per second. An open
  beyond the limit returns `{:error, :system_limit}`. Each slot holds its
  buffers from open until the slot is freed: a TCP socket's receive and send
  buffers (64 KiB each by default, up to 1 MiB each) and 32 KiB for a UDP
  socket. At the default buffer sizes, 64 TCP sockets hold about 8 MiB and
  512 hold about 64 MiB. Whatever the limit, a stack's socket buffers total at
  most 128 MiB, what 64 TCP sockets with the largest buffers hold; an open
  that would pass that also returns `{:error, :system_limit}`, so 512
  sockets need buffers averaging at most 256 KiB. `stack_info/1` reports the
  slots in use as `native.result.native_socket_count`, the limit as
  `native.result.native_socket_capacity`, the closed TCP sockets still
  holding one as `native.result.closing_tcp_socket_count`, and buffer bytes
  against their cap as `native.result.socket_buffer_bytes` and
  `native.result.socket_buffer_capacity`.

  `:egress_credit` limits how much egress the link must accept. It defaults to
  `:infinity`, which sends every batch as soon as it is ready. A
  `{packets, bytes}` tuple of non-negative integers, each at most
  `0xFFFF_FFFF`, is the credit the stack starts with; see `grant_egress/3`.
  An invalid value is rejected with `:invalid_egress_credit`.

  `SmolNet.Loopback.start_link/1` starts a stack whose egress is a link back
  into itself, returns both references, and needs no external transport.
  """
  @spec start_stack(keyword()) :: {:ok, Stack.Ref.t()} | {:error, term()}
  defdelegate start_stack(options \\ []), to: SmolNet.StackSupervisor, as: :start_stack

  @doc "Stops a stack and its complete runtime bundle."
  @spec stop_stack(Stack.Ref.t()) :: :ok | {:error, :closed}
  defdelegate stop_stack(stack), to: SmolNet.StackSupervisor, as: :stop_stack

  @doc """
  Monitors a stack from the calling process.

  Returns an ordinary monitor reference. When the stack stops for any reason,
  whether through `stop_stack/1`, a crash, or its own `:link_down` policy, the
  caller receives `{:DOWN, ref, :process, object, reason}` once the complete
  runtime bundle has terminated. Match on `ref`: `object` and `reason` describe
  internal processes and are not part of the contract. A stack that has already
  stopped produces the message immediately, as `Process.monitor/1` does for a
  dead process. Remove the monitor with `Process.demonitor/2`.

  A link calls this so that it can exit when its stack stops, letting its
  supervisor rebuild both:

      monitor = SmolNet.monitor(stack)

      receive do
        {:DOWN, ^monitor, :process, _object, _reason} -> exit(:stack_down)
      end
  """
  @spec monitor(Stack.Ref.t()) :: reference()
  defdelegate monitor(stack), to: SmolNet.StackSupervisor

  @doc """
  Hands raw IPv4 or IPv6 packets from the stack's link feeder to the stack.

  Each stack has one serialized feeder. This call returns after the stack owner
  validates and accepts the input, then native processing runs before the stack
  accepts another message. A binary preserves the single-packet `:ok` result.
  A list is admitted atomically and returns `{:ok, packet_count}`; an empty list
  is a no-op. A batch exceeding `:input_packets` or `:bytes_copied` is rejected
  with `{:error, :batch_too_large}`. The feeder must bound its own transport
  input.
  """
  @spec ingress(Stack.Ref.t(), binary()) :: :ok | {:error, atom()}
  @spec ingress(Stack.Ref.t(), [binary()]) ::
          {:ok, non_neg_integer()} | {:error, atom()}
  defdelegate ingress(stack, packet), to: Stack

  @doc """
  Grants a stack started with `:egress_credit` more egress.

  Credit counts both packets and bytes, and grants add up. The stack hands the
  link a packet only while the credit left covers it in both, and every batch
  it sends uses credit up. When credit runs out the stack holds egress back
  rather than dropping it: TCP data stays in the socket's send buffer and UDP
  datagrams in the socket's transmit ring, so senders see the backpressure
  they would see from a slow peer, and nothing is lost. A grant sends whatever
  it releases straight away.

  A link typically grants back what it has forwarded:

      def handle_info({:smol_stack, :my_link, :egress, packets}, state) do
        Enum.each(packets, &transmit(state, &1))
        :ok = SmolNet.grant_egress(state.stack, length(packets), IO.iodata_length(packets))
        {:noreply, state}
      end

  A stack waiting for credit does no work until the next grant, so timers that
  need to send, such as TCP retransmissions, also wait for it. Replies to
  ingress may still queue inside the stack while it waits, up to its
  `:output_packets` limit; once that queue is full, ingress waits for credit
  too.

  `packets` and `bytes` are non-negative integers, each at most `0xFFFF_FFFF`.
  Returns `{:error, :egress_credit_disabled}` for a stack started without
  `:egress_credit`. `stack_info/1` reports the credit left as
  `native.result.egress_credit`.
  """
  @spec grant_egress(Stack.Ref.t(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, :invalid_egress_credit | :egress_credit_disabled | :closed}
  defdelegate grant_egress(stack, packets, bytes), to: Stack

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
