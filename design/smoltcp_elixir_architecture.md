# smoltcp/Elixir Socket Architecture

## Status and purpose

This document records the agreed architecture for exposing one or more `smoltcp`
network stacks to Elixir and, above that, to OTP's `gen_tcp`/`gen_udp` APIs. It
is an architecture handoff for implementation planning, not a claim that the
exact OTP backend callback contract or every return tuple has already been
validated against the target OTP release.

The central design rule is:

> The native stack owns network state and readiness registration; the Elixir
> caller or inet adapter owns application-operation state and OTP semantics.

The design deliberately avoids blocking a BEAM scheduler on a native mutex and
avoids blocking the per-stack owner while waiting for network activity.

## Goals

- Run independent IP stacks over arbitrary raw-IP links.
- Accept and emit raw IPv4/IPv6 packets without an Ethernet layer.
- Support TCP and UDP in the same stack.
- Present a low-level API shaped like Erlang's `socket` API, including
  `:nowait`, readiness notifications, retry, and cancellation.
- Make synchronous operations wrappers over the same nonblocking primitive.
- Provide a thin path to custom `gen_tcp` and `gen_udp` inet backends.
- Keep every normal-scheduler NIF invocation bounded.
- Prevent native lock contention from blocking normal BEAM schedulers.
- Bound native socket buffers and leave arbitrary application backlogs in the
  BEAM process performing the operation.

## Non-goals

- Reimplementing `gen_tcp` policy in Rust.
- Blocking inside a NIF until a connect, send, or receive completes.
- Running a native polling thread.
- Giving each low-level socket its own BEAM process.
- Making `smoltcp::SocketHandle` part of the public socket identity.
- Fully specifying listening/accept semantics in the first outbound-client
  implementation.

## Architecture at a glance

```text
gen_tcp / gen_udp
        │
        ▼
5. OTP backend modules
   callback and socket-term translation only
        │
        ▼
4. inet socket adapter (normally one gen_statem per logical OTP socket)
   active/passive mode, packet framing, owner, timeouts, continuations
        │
        ▼
3. SmolNet public facade + SmolNet.Socket implementation
   :socket-like API and lightweight socket value
        │
        ▼
2. SmolNet.Stack (one GenServer per logical raw-IP link)
   exclusive native-stack access, ingress/egress, timer scheduling
        │
        ▼
1. Rust NIF / native socket engine
   smoltcp, socket table, waiters, wakers, readiness delivery
        │
        ▼
transport-neutral raw-IP link boundary
```

There are two distinct meanings of “socket process” in earlier design notes:

- The low-level `%SmolNet.Socket{}` does **not** require a process. It is a
  lightweight handle containing a stack PID and logical socket ID. The
  application normally operates on it through functions on `SmolNet`.
- The higher inet compatibility layer will normally use one `gen_statem` per
  logical OTP socket because active mode, ownership transfer, packet framing,
  and concurrent operation continuations are naturally process state.

## Layer responsibilities

### 1. Native socket engine

The Rust resource owns one independent network namespace/stack instance:

```rust
struct Stack {
    interface: Interface,
    sockets: SocketSet<'static>,
    device: BeamDevice,
    socket_table: HashMap<SocketId, SocketEntry>,
    ready: ReadyQueue,
    next_socket_id: u64,
}

struct SocketEntry {
    handle: smoltcp::iface::SocketHandle,
    generation: u64,
    recv_waiter: Option<Waiter>,
    send_waiter: Option<Waiter>,
}

struct Waiter {
    pid: LocalPid,
    select_ref: SavedTerm, // conceptual; exact Rustler storage is TBD
    operation: Operation,
}
```

Its responsibilities are:

- own `Interface`, `SocketSet`, and the `Medium::Ip` device;
- map stable logical `SocketId` values to internal smoltcp handles;
- implement bounded try-operations for open, bind, connect, send, receive,
  shutdown, close, and state queries;
- atomically perform “check readiness and, if blocked, install waiter”;
- attach/re-arm smoltcp receive and transmit wakers;
- convert waker calls into native readiness bits or queue entries;
- after the current stack mutation is complete, consume matching waiter slots
  and send select/abort notifications with the current NIF environment;
- collect outbound IP packets from the internal `BeamDevice` adapter;
- report the next `poll_at` deadline.

The native layer does **not** own:

- receive lengths or partial receive accumulation;
- unsent application payloads;
- application timeouts or deadlines;
- `active`, `active: :once`, or `active: N` policy;
- packet framing such as `packet: 1 | 2 | 4 | :line`;
- list/binary conversion;
- controlling-process behavior;
- OTP `{tcp, ...}`, `{udp, ...}`, close, and error message construction.

All native entry points are try-operations. They complete immediately with a
result, error, or select registration. They never wait for future traffic.

### 2. `SmolNet.Stack`: the concurrency boundary

There is one `SmolNet.Stack` process per logical raw-IP link/network namespace.
It is the only BEAM process allowed to invoke NIFs for that native stack
resource.

Representative state:

```elixir
%{
  native: native_stack_resource,
  egress_pid: pid(),
  link_ref: term(),
  link_monitor: reference_or_nil,
  poll_timer: timer_reference_or_nil,
  poll_generation: integer()
}
```

Its responsibilities are:

- serialize all socket operations for the stack through its mailbox;
- accept one raw IP packet or one bounded packet batch from the stack's
  serialized link feeder and pass it to one bounded ingress invocation;
- emit outbound raw IP packets to the configured link-layer recipient;
- schedule and replace the next BEAM timer from native `poll_at` data;
- invoke a bounded native timer poll when that timer fires;
- optionally monitor the link-layer recipient and apply configured link-down
  policy;
- supervise stack lifecycle and reject work after stack shutdown.

It does not retain pending send payloads, receive accumulation, or application
timeouts. It also does not maintain the select waiter registry; that registry is
native so waiter installation and smoltcp waker arming share one atomic critical
section.

### 3. `SmolNet`: public socket interface

The low-level socket value is intentionally small:

```elixir
%SmolNet.Socket{
  stack: stack_pid,
  id: socket_id,
  generation: generation
}
```

The generation may be folded into the ID if IDs are never reused. The important
property is that a delayed readiness event for a closed socket cannot target a
new socket whose smoltcp handle was recycled.

The application-facing API lives directly on `SmolNet` and should be
deliberately close to `:socket`:

```elixir
SmolNet.open(:inet6, :stream, :tcp, stack: stack)
SmolNet.bind(socket, address)
SmolNet.connect(socket, address, timeout_or_nowait)
SmolNet.send(socket, data, timeout_or_nowait)
SmolNet.recv(socket, length, timeout_or_nowait)
SmolNet.sendto(socket, data, address, timeout_or_nowait)
SmolNet.recvfrom(socket, length, timeout_or_nowait)
SmolNet.shutdown(socket, how)
SmolNet.close(socket)
SmolNet.sockname(socket)
SmolNet.peername(socket)
SmolNet.cancel(socket, select_info)
```

The `stack:` option is required for `open/4`. Keeping it in the option list
preserves the familiar `:socket.open(domain, type, protocol, opts)` shape while
still selecting the independent smoltcp stack/network namespace that will own
the socket.

`SmolNet.Socket` contains the implementation and socket struct. Most public
functions on `SmolNet` can be simple delegates, with their public docs and specs
kept on the facade:

```elixir
defmodule SmolNet do
  import Kernel, except: [send: 2]

  defdelegate open(domain, type, protocol, opts), to: SmolNet.Socket
  defdelegate bind(socket, address), to: SmolNet.Socket
  defdelegate connect(socket, address), to: SmolNet.Socket
  defdelegate connect(socket, address, timeout), to: SmolNet.Socket
  defdelegate send(socket, data), to: SmolNet.Socket
  defdelegate send(socket, data, timeout), to: SmolNet.Socket
  defdelegate recv(socket, length), to: SmolNet.Socket
  defdelegate recv(socket, length, timeout), to: SmolNet.Socket
  defdelegate sendto(socket, data, address, timeout), to: SmolNet.Socket
  defdelegate recvfrom(socket, length, timeout), to: SmolNet.Socket
  defdelegate shutdown(socket, how), to: SmolNet.Socket
  defdelegate close(socket), to: SmolNet.Socket
  defdelegate sockname(socket), to: SmolNet.Socket
  defdelegate peername(socket), to: SmolNet.Socket
  defdelegate cancel(socket, select_info), to: SmolNet.Socket
end
```

Where the API provides convenience arities or defaults, define the relevant
delegates explicitly or keep a very small facade wrapper. The facade must not
grow a second copy of socket state or readiness logic. Because `send/2` shares
its name and arity with `Kernel.send/2`, the facade excludes that import and
uses `Kernel.send(pid, message)` explicitly if it ever needs to send a process
message internally.

Calling `:nowait` still uses a short `GenServer.call` to the stack owner. Here,
“nowait” means that the call does not wait for future network activity; it may
wait briefly for normal mailbox scheduling and one bounded NIF invocation.

### 4. inet socket adapter

The inet adapter is expected to be a `gen_statem` per logical OTP socket. It
sees only the `SmolNet` socket API and select/abort messages, not Rust or
smoltcp.

Representative state:

```elixir
%{
  socket: %SmolNet.Socket{},
  owner: controlling_process,
  owner_monitor: monitor_reference,
  active: false | true | :once | integer(),
  mode: :binary | :list,
  packet: :raw | :line | 1 | 2 | 4,
  packet_size: non_neg_integer(),
  recv_buffer: binary(),
  recv_op: nil | operation_continuation(),
  send_op: nil | operation_continuation()
}
```

It owns:

- active/passive receive policy;
- packet framing and buffering above TCP's byte stream;
- binary/list conversion and headers;
- controlling-process monitoring and transfer;
- receive and send deadlines;
- remaining data after partial sends;
- partial data for exact-length receives;
- conversion to OTP socket messages and error conventions;
- serialization/rejection rules for competing operations in one direction.

### 5. OTP backend modules

The custom `gen_tcp`/`gen_udp` backend should be as thin as the target OTP
backend contract permits. It translates OTP socket terms and callbacks into the
inet adapter's operations:

```text
connect, listen, accept
send, recv, sendto, recvfrom
shutdown, close
setopts, getopts
controlling_process
sockname, peername
```

The exact callback set, socket term shape, and dependence on undocumented OTP
internals must be validated against the chosen OTP version during planning.

## Raw-IP link boundary

The library is agnostic about the transport carrying IP packets. A
CoreDeviceProxy connection may be one adapter, but it is outside this library in
the same way that a TUN interface, USB tunnel, test harness, or another packet
source/sink would be outside it.

The library must not contain CoreDeviceProxy types, process names, framing,
reconnection logic, or message conventions. An integration package may translate
between that transport and the generic ingress/egress contracts below.

Each logical raw-IP link maps to an independent stack:

```text
link adapter A ⇄ SmolNet.Stack A ⇄ native Stack A
link adapter B ⇄ SmolNet.Stack B ⇄ native Stack B
link adapter C ⇄ SmolNet.Stack C ⇄ native Stack C
```

The boundary is deliberately two-way:

1. A transport-neutral ingress function accepts one raw IP packet or a bounded
   list of packets for a stack.
2. A configured BEAM recipient receives each outbound raw IP packet, normally
   as a message, and is responsible for writing it to the actual link.

### Inbound contract

The public boundary should be a function rather than a transport-specific
mailbox protocol:

```elixir
SmolNet.ingress(stack, raw_ip_packet)
SmolNet.ingress(stack, [raw_ip_packet, ...])
```

This function synchronously hands the input to the owning `SmolNet.Stack`; it
does not call the NIF from the transport process. Every packet must begin with
an IPv4 or IPv6 header. Link framing, stream reassembly, checksums belonging to
the link, reconnect behavior, extraction of individual IP packets, and upstream
backpressure are responsibilities of the external adapter.

Each stack has exactly one serialized ingress feeder. The call waits until the
stack validates and accepts one packet or atomically accepts one bounded batch,
then returns before bounded native processing runs in a GenServer continuation.
The same feeder may have one later call waiting. If native `more` work remains,
the stack retains that call without acknowledging it until immediate native
polling finishes. Thus at most one admitted call is in native processing and
one later call is waiting at the stack boundary.

### Outbound contract

At stack creation, the caller configures an egress recipient and an opaque link
reference, conceptually:

```elixir
{:ok, stack} =
  SmolNet.Stack.start_link(
    egress: {link_pid, link_ref},
    mtu: mtu,
    addresses: addresses,
    routes: routes
  )
```

For every packet emitted by smoltcp, `SmolNet.Stack` sends:

```elixir
{:smol_stack, link_ref, :egress, raw_ip_packet}
```

The opaque `link_ref` lets one link process serve multiple stack instances
without exposing native socket or stack internals. The exact tag is an API
choice, but the message must identify the logical link and contain one complete
raw IP packet.

Sending this message means “smoltcp emitted this packet”; it does not mean the
external link accepted or transmitted it. If delivery acknowledgement or
bounded egress backpressure is required, that must be added as an explicit link
protocol rather than inferred from BEAM message delivery.

The library may monitor `link_pid`, but link-process failure policy should be
configurable: stop the stack, mark the link down while retaining sockets, or
notify a supervisor. No CoreDeviceProxy-specific reconnect or framing behavior
belongs in `SmolNet.Stack`.

### Native device adapter

The smoltcp-facing device uses `Medium::Ip`. Both ingress and egress are complete
raw IPv4/IPv6 packets, so no Ethernet headers, ARP, or Ethernet neighbor
discovery belong in this layer.

A conceptual device implementation is:

```rust
struct BeamDevice {
    rx: VecDeque<Vec<u8>>,
    tx: VecDeque<Vec<u8>>,
}
```

In practice, ingress enqueues at most the configured `input_packets` and
`bytes_copied` limits, and egress collection is also bounded so no NIF call
drains an unbounded amount of work.
The native `BeamDevice` is an internal adapter between the NIF entry points and
smoltcp; it is not the external transport abstraction.

## Locking and concurrency model

### BEAM ownership is the primary lock

All operations for a stack are serialized by one `SmolNet.Stack` mailbox. Multiple
sockets on one stack ultimately mutate the same `Interface`, `SocketSet`, and
`Device`, so serializing at the per-stack boundary matches smoltcp's natural
ownership model.

Different stack instances remain independent and can progress concurrently on
different BEAM schedulers, regardless of what transports carry their packets.

### Native mutex is defensive only

The native resource may retain a mutex because Rustler resources can otherwise
be misused, but ordinary operations must use a nonblocking acquisition such as
`try_lock`. Failure means an ownership invariant violation (or an explicitly
handled reschedule), never permission to sleep a normal BEAM scheduler on the
mutex.

### Every NIF call is bounded

- Ingress handles at most the configured bounded packet batch.
- Egress uses bounded polling.
- Send copies/enqueues at most a configured chunk or the available TX capacity.
- Receive removes at most the requested/configured amount.
- No native call loops until a socket becomes ready.
- No native call drains an arbitrary mailbox, packet queue, or application
  binary.

If more work remains, it is represented as another BEAM message, caller-owned
continuation, or future readiness notification.

## Native waiter and readiness design

### Waiter slots

Each native socket entry has at most one pending waiter per I/O direction:

```text
read slot  → recv, recvfrom, or accept
write slot → send, sendto, or connect
```

Planning must define the response to a second incompatible registration on the
same slot. The preferred behavior is an explicit `:already_pending`/`:busy`
error, not silent replacement. A retry carrying the same select token may be
treated as continuation/finalization rather than a new competing operation.

Waiters retain only identity and notification information:

```text
socket ID + generation
operation class
recipient PID
unique select reference
```

They do not retain the application's data, requested receive length, or timeout.

### Atomic check-and-arm

For a nowait operation, native code performs the following while it has
exclusive access to the stack:

```text
try operation
    ├─ completed/error → return result
    └─ would block
         ├─ install native waiter
         ├─ register the matching smoltcp one-shot waker
         └─ return {:select_info, operation, reference}
```

This prevents the classic lost-wakeup race where readiness appears after a
failed try but before the waiter is installed.

### Wakers do not send BEAM messages directly

The smoltcp waker callback must stay tiny:

```text
smoltcp waker → set ready bit / append coalesced ready entry
```

It must not construct Erlang terms, send messages, re-enter stack logic, or do
other substantial work while smoltcp is mutating the stack.

At the safe end of the bounded NIF invocation:

```text
finish stack mutation
  → drain/coalesce native ready entries
  → match and consume waiter slots
  → send select/abort notifications using the current NIF Env
  → return outputs and next poll deadline to SmolNet.Stack
```

This keeps callbacks non-reentrant while still letting Rust notify the waiting
process directly. A native background thread and an `OwnedEnv` send path are not
needed.

### Readiness is a retry hint

Wakes are one-shot and may be spurious. A select message means “retry the
operation,” not “the operation is guaranteed to complete.” The retry may:

- complete;
- fail because the socket closed or entered an error state; or
- return another select registration and re-arm the waker.

Consuming the waiter when emitting the notification prevents message storms.
Further packets or ACKs do not generate more notifications until the operation
retries and installs a new waiter.

### Message and select-info shape

The exact public shape should be made as close to `:socket` as practical. A
conceptual form is:

```elixir
{:select, {:select_info, operation, ref}}

{:"$smol_socket", socket_identity, :select, ref}
{:"$smol_socket", socket_identity, :abort, ref, reason}
```

Notifications must include enough stable socket identity to reject stale events.
The inet adapter should match both socket identity and reference.

## NIF result envelope

Readiness messages are sent natively, but every stack-driving call still needs
to return raw-IP egress and timer effects to `SmolNet.Stack`. Conceptually:

```elixir
%{
  result: operation_result,
  output: [raw_ip_packet()],
  poll_at: monotonic_timestamp_or_nil
}
```

This need not be represented as a map in the final ABI. The important invariant
is that after **every** operation that can advance smoltcp, the caller forwards
all bounded output returned by that invocation and replaces the BEAM poll timer
with the latest native deadline.

## Synchronous and nowait semantics

### Fundamental primitive: nowait

Only the nonblocking form needs native support:

```elixir
connect(socket, address, :nowait)
send(socket, data, :nowait)
recv(socket, length, :nowait)
cancel(socket, select_info)
```

Typical results are conceptually:

```elixir
:ok
{:ok, value}
{:error, reason}
{:select, select_info}
{:select, select_info, continuation_value}
```

For a partial stream send, `continuation_value` is the unsent binary. For an
exact-length stream receive, it may be the partial data already received. The
precise tuple layout should follow the selected OTP `:socket` precedent where
possible.

### Synchronous operations are caller-side retry loops

A finite or infinite timeout is implemented in Elixir over `:nowait`:

```text
compute one monotonic deadline
  → try nowait operation
  → if complete, return
  → if select, wait for matching select/abort until remaining deadline
  → on select, retry with continuation state
  → on timeout, cancel native waiter and return timeout
```

The timeout is a single deadline across all retries; it must not restart after a
spurious wake or partial send.

Only the calling BEAM process waits. Neither `SmolNet.Stack` nor a scheduler
thread is held while waiting for network activity.

For inet sockets, the adapter `gen_statem` usually owns this continuation so it
can reconcile synchronous calls with active-mode and OTP ownership rules.

## TCP considerations

- A connected/outbound logical TCP socket maps naturally to one smoltcp TCP
  socket.
- `connect` uses write/connect readiness and is finalized by retrying/querying
  state after notification.
- TCP send is a byte-stream operation. The native layer accepts only what fits;
  the caller/inet adapter retains the remainder.
- TCP receive may return available bytes or partial exact-length data. Packet
  framing remains entirely in the inet adapter.
- EOF, reset, and other state transitions must wake/abort relevant waiters so a
  caller cannot sleep forever after closure.
- Half-close maps to `shutdown`; exact behavior must be checked against both
  smoltcp and OTP expectations.
- A reusable listening socket is not a one-to-one mapping. A smoltcp TCP socket
  transitions from listen into a connection, whereas an OS listener continues
  accepting. Full `listen`/`accept` support therefore needs a native or BEAM
  listener abstraction that maintains a pool/backlog of listening smoltcp
  sockets, queues established children, and replenishes the pool.

The first implementation may explicitly scope itself to outbound TCP if that
matches the product requirement, rather than hiding incomplete listener
semantics.

## UDP considerations

- TCP and UDP sockets share the same `SocketSet` and stack-driving machinery.
- IPv4 and IPv6 UDP use one native record type, waiter path, and adapter state
  machine. Family is immutable per socket and is validated before native
  mutation; the IPv4 `SmolNet.Inet.Udp` callback only selects `:inet`
  resolution.
- UDP preserves datagram boundaries and source/destination metadata.
- `sendto` is all-or-error at the public datagram level unless target OTP
  semantics require another explicit convention; it must not expose a partial
  datagram as if it were a TCP partial send.
- `recvfrom` returns one datagram plus peer address metadata.
- Native UDP packet buffers remain bounded; readiness and retry provide
  backpressure.
- A wildcard logical socket has one exact-address smoltcp backing socket for
  each configured address in its family, bounded by the eight-address stack
  limit. This prevents smoltcp's family-neutral wildcard endpoint from routing
  an IPv4 datagram into an IPv6 socket (or the reverse). The backing sockets
  share one public identity and waiter, and one receive call inspects at most
  the fixed packet-capacity bound across them.
- A zero-length datagram is distinct from no datagram. A positive receive
  length truncates one datagram and discards its remainder. Connected UDP
  rejects a different send destination and filters packets from other peers.
- Maximum payload is the smaller of the 16 KiB payload ring and MTU minus 28
  bytes for IPv4 or 48 bytes for IPv6. IPv4 accepts its standard zero UDP
  checksum; invalid nonzero checksums and invalid IPv6 checksums are discarded.

## Detailed operation sequencing

The sequences below show the required ownership and event flow. Exact function
and tuple names are illustrative.

### Open

```text
caller / inet adapter
  → SmolNet.open(domain, type, protocol, stack: stack)
  → delegated SmolNet.Socket implementation
  → SmolNet.Stack call
  → Native.socket_open(...)
      → try-lock resource; fail fast on invariant violation
      → allocate smoltcp buffers and socket
      → add socket to SocketSet
      → allocate monotonically unique SocketId/generation
      → insert SocketEntry with empty waiter slots
      → perform bounded egress/maintenance if required
      → drain native readiness safely
      → return ID, outbound packets, poll_at
  → SmolNet.Stack forwards outbound packets and replaces timer
  → caller receives {:ok, %SmolNet.Socket{stack: stack_pid, id: id, ...}}
```

If construction fails, no public socket handle is returned and any partially
inserted native state is removed before the NIF returns.

### Connect

First attempt:

```text
caller creates ref
  → SmolNet.connect(socket, peer, :nowait)
  → delegated SmolNet.Socket implementation
  → SmolNet.Stack call includes caller PID + ref
  → Native.tcp_connect(socket_id, peer, pid, ref)
      → validate ID/generation and socket type/state
      → initiate connect if not already initiated
      → bounded egress emits SYN if possible
      → if established, return :ok
      → if failed, return {:error, reason}
      → otherwise install connect waiter in write slot
      → arm transmit/state waker
      → return {:select, select_info}
  → SmolNet.Stack forwards SYN and updates timer
```

Completion:

```text
SYN/ACK raw IP packet
  → external link adapter calls SmolNet.ingress(stack, packet)
  → bounded native ingress
  → smoltcp transitions socket state
  → waker sets native write/state-ready bit
  → after poll, native code consumes matching waiter
  → native code sends select(ref) to registered PID
  → caller retries connect using socket/select context
  → Native.tcp_connect/finalize observes Established
  → :ok
```

Failure such as reset or timeout inside TCP must produce a retryable wake or an
abort message. Application timeout is handled by cancel as described below.

### Send

Immediate or partial attempt:

```text
caller owns full binary and one deadline
  → send_nowait(socket, binary, pid, ref)
  → SmolNet.Stack serializes request
  → Native.tcp_send
      → validate socket
      → enqueue at most available/configured bounded bytes
      → run bounded egress
      → if all accepted, return :ok
      → if remainder exists or no capacity:
          → install send waiter in write slot
          → arm send waker
          → return select_info + unsent slice/remainder boundary
  → SmolNet.Stack forwards generated IP packets and updates timer
  → caller retains unsent binary and waits
```

Continuation:

```text
ACK ingress or timer poll
  → smoltcp frees TX buffer space
  → write waker marks ready
  → native epilogue consumes waiter and sends select(ref)
  → caller retries only the unsent remainder with original deadline
```

The Rust socket table must never retain the arbitrary remaining application
binary. Large BEAM binaries can cross the stack-process boundary by reference;
the bounded native copy occurs only for bytes accepted by smoltcp.

### Receive

```text
caller owns requested length, accumulator, and deadline
  → recv_nowait(socket, length, pid, ref)
  → SmolNet.Stack serializes request
  → Native.tcp_recv
      → validate socket
      → if bytes are available, remove at most bounded/requested bytes
      → if request semantics are satisfied, return {:ok, data}
      → if peer EOF/error is observable, return closed/error
      → otherwise install recv waiter in read slot
      → arm receive waker
      → return select_info, optionally with partial data
  → caller retains partial data and waits
```

On packet ingress, close, or error, the read waker/abort path notifies the
registered PID. The caller retries and must tolerate a spurious wake by receiving
another select registration.

For `length == 0` or “return available data” semantics, the exact behavior must
follow the intended low-level API contract. TCP framing is not performed here.

### UDP send and receive

```text
sendto_nowait
  → validate destination and datagram size
  → enqueue one complete datagram if capacity exists
  → otherwise arm write waiter and return select

recvfrom_nowait
  → dequeue one complete datagram plus source metadata if present
  → otherwise arm read waiter and return select
```

The readiness, timeout, retry, cancellation, packet forwarding, and timer paths
are otherwise identical to TCP.

### Packet ingress

```text
external raw-IP link adapter
  → calls SmolNet.ingress(stack, raw_ip_packet_or_bounded_batch)
  → stack validates and atomically accepts the input from its serialized feeder
  → stack replies, then enters an ingress continuation before another message
  → SmolNet.Stack invokes the single-packet or batch native ingress function
  → native code try-locks stack
  → place the accepted packet(s) in the bounded BeamDevice receive queue
  → run at most `input_packets` bounded ingress operations
  → run bounded maintenance/egress as required
  → smoltcp wakers only mark native readiness
  → collect a bounded set of outbound raw IP packets
  → drain readiness and send matching notifications
  → compute poll_at
  → unlock/return
  → SmolNet.Stack sends each bounded output batch to the configured egress PID
  → SmolNet.Stack replaces its timer from poll_at
  → any native `more` work drains before a waiting feeder call is acknowledged
  → SmolNet.Stack returns to mailbox
```

The native ingress loop is bounded by `input_packets`, `bytes_copied`, output
capacity, and the native time/reduction budget. Unconsumed packets stay in the
bounded receive queue and set `more`, so continuations preserve scheduling
fairness. The feeder still owns upstream transport backpressure and only one
feeder call is processed at a time.

### Readiness delivery and retry

```text
smoltcp operation invokes recv/send waker
  → waker sets/coalesces {SocketId, generation, direction}
  → current bounded native operation completes stack mutation
  → native epilogue drains ready entries
  → for each entry:
      → look up current SocketEntry
      → reject stale generation
      → take matching waiter slot (one-shot)
      → send select(ref) to its PID using current NIF Env
  → caller or inet adapter receives message
  → verify socket identity + ref against current continuation
  → retry original operation
  → operation completes, errors, or atomically installs a fresh waiter
```

Unknown, stale, duplicated, or already-cancelled readiness is harmless and
dropped.

### Timer poll

Every stack-driving result supplies the next native deadline. `SmolNet.Stack`
maintains only the latest timer:

```text
NIF result contains poll_at
  → cancel/obsolete previous timer logically
  → increment timer generation
  → schedule Process.send_after for monotonic remaining delay

timer message arrives
  → discard if generation is stale
  → Native.poll(stack, monotonic_now)
      → bounded maintenance/egress
      → retransmission/timeout state advances
      → wakers mark readiness
      → native epilogue sends notifications
      → return outbound packets + next poll_at
  → forward packets
  → schedule next timer
```

This drives retransmission and protocol timers even when there is no inbound
traffic, without a native thread or busy loop.

### Close

```text
caller / inet adapter initiates close
  → SmolNet.Stack call
  → Native.socket_close(socket_id, generation, mode)
      → validate identity
      → mark/remove entry so no new operation can register
      → take all waiter slots
      → perform abortive or graceful smoltcp close as requested
      → remove smoltcp socket when lifecycle permits
      → queue abort notifications for taken waiters with :closed/reason
      → bounded egress emits FIN/RST where applicable
      → send abort notifications in native epilogue
      → return output + poll_at
  → SmolNet.Stack forwards output and updates timer
  → public handle remains permanently invalid
```

Close must be idempotent at the public API boundary or return a stable already-
closed error. It must never allow late readiness to address a reused socket.

Graceful TCP close may require a separate “closing but retained by stack” native
lifecycle after the public handle is invalidated; planning should make that state
explicit.

### Cancel

Caller timeout or explicit cancellation:

```text
caller deadline expires
  → SmolNet.cancel(socket, select_info)
  → delegated SmolNet.Socket implementation
  → SmolNet.Stack call
  → Native.cancel(socket_id, generation, operation, ref)
      → validate socket and matching waiter slot
      → if exact ref matches, clear waiter and return :ok
      → if waiter was already consumed for notification, return :already_sent
        or equivalent race result
      → if no match, return :not_found/:invalid
  → caller drains/ignores a racing select message by reference
  → synchronous wrapper returns {:error, :timeout}
```

Cancellation need not unregister a smoltcp waker immediately. A later wake may
set a ready bit, but the native epilogue finds no waiter and sends nothing. Any
new operation will re-arm/replace the direction's waker as needed.

The cancel-versus-ready race must have a documented outcome. In all outcomes,
at most one native waiter owns the reference, and stale notifications cannot
complete a different operation.

## Active and passive inet behavior

Passive receive uses the synchronous wrapper/continuation described above and
replies to the waiting OTP caller when framing requirements are met.

Active receive continuously issues nowait receives:

```text
recv_nowait
  ├─ data → frame/convert → send {tcp|udp, socket, data} to owner → continue
  └─ select → store continuation and return to gen_statem mailbox
```

- `active: true` resumes after each delivery.
- `active: :once` produces one logical delivery, then changes to passive.
- `active: N` decrements on logical deliveries and becomes passive at zero.

The adapter must bound how many immediately available chunks/datagrams it drains
in one callback, yielding through its mailbox when necessary.

If the controlling process dies, the adapter closes the low-level socket unless
ownership has been transferred according to OTP rules.

## Error and lifecycle rules

- All entry points validate stack identity, socket ID/generation, kind, and
  legal state before touching smoltcp state.
- Stack shutdown aborts all native waiters and causes all handles to become
  invalid.
- Link-recipient failure follows the configured link-down policy; a terminal
  link failure is propagated to inet adapters if that policy closes the stack.
- Close/error transitions wake or abort both relevant directions.
- A select notification never contains application data and never means success;
  it authorizes a retry.
- Mailbox delivery order must not be used as the only defense against stale
  events; references and socket generations are mandatory.
- Native output and timer effects are handled after every operation, not only
  ingress calls. A connect or send can generate packets immediately.

## Recommended implementation boundaries

Implemented source layout:

```text
lib/
  smol_net.ex                    # public socket-like facade
  smol_net/
    stack.ex
    socket.ex
  smol_net/inet_backend/
    options.ex
    packet.ex
    tcp.ex / tcp4.ex
    udp.ex / udp4.ex

native/smolnet_core/src/
  stack.rs
  device.rs
  socket_table.rs
  waiter.rs
  tcp.rs
  udp.rs

native/smolnet_nif/src/
  lib.rs                         # thin Rustler entry point

native/fuzz/fuzz_targets/
  raw_packets.rs
  endpoint_options.rs
  socket_operations.rs
```

Recommended planning slices:

1. Define the raw-IP link contract and prove one `Medium::Ip` stack can accept
   packets through `SmolNet.ingress/2` and emit packets as
   transport-neutral messages with bounded polling and BEAM-scheduled timers.
2. Add stable socket IDs, lifecycle validation, and outbound TCP open/connect.
3. Add native waiter slots, waker-to-ready-bit plumbing, safe NIF-epilogue sends,
   and cancellation race tests.
4. Implement low-level TCP nowait send/recv plus synchronous Elixir wrappers.
5. Add UDP bind/sendto/recvfrom using the same readiness machinery.
6. Build the inet `gen_statem` for passive TCP first, then active modes, packet
   framing, options, ownership, and UDP delivery.
7. Validate and implement the exact custom OTP backend contract for the pinned
   OTP release.
8. Add listener/accept pooling only if inbound TCP is in scope.

## Required tests and observable invariants

The implementation plan should include at least:

- concurrent socket calls for one stack are serialized without a blocking
  native lock;
- separate stack instances and link adapters progress independently;
- replacing one raw-IP transport adapter with another requires no changes to
  `SmolNet.Stack`, `SmolNet.Socket`, or the native engine;
- the `SmolNet` facade delegates each documented socket operation to
  `SmolNet.Socket` without changing its return or readiness semantics;
- ingress accepts a complete raw IP packet or bounded packet list through the
  public function and never invokes the NIF in the link process;
- each native outbound packet becomes one correctly addressed egress message;
- link-recipient termination follows the configured link-down policy;
- ingress and large sends are split into bounded calls;
- readiness between try and arm cannot be lost;
- spurious wake causes a safe retry/re-arm;
- repeated readiness is coalesced while a one-shot waiter is outstanding;
- second waiter in one direction gets a deterministic error;
- partial send retains the remainder only in Elixir;
- exact-length receive retains partial data only in Elixir;
- timeout uses one deadline across retries;
- cancel-before-ready, ready-before-cancel, and simultaneous races have stable
  outcomes with no cross-operation notification;
- close aborts read/write/connect waiters and makes late readiness harmless;
- recycled smoltcp handles cannot revive stale socket IDs;
- every stack-driving operation forwards output and refreshes `poll_at`;
- retransmissions progress with timer events and no inbound traffic;
- owner death and terminal stack/link failure close or abort sockets correctly;
- TCP and UDP coexist in one `SocketSet` without readiness cross-talk;
- active mode is drain-bounded and cannot monopolize its adapter process.

## Finalized first-release decisions

- Rustler 0.38.0 and smoltcp 0.14.0 are pinned. One-shot wakers store only the
  waiting PID/reference, and every native drive applies explicit work, byte,
  output, readiness, and maintenance limits.
- Public nowait operations return opaque select continuations compatible with
  the library's socket-like API. OTP integration uses custom IPv4 and IPv6
  `:gen_tcp` and `:gen_udp` backend modules for the supported OTP matrix.
- `SmolNet.Stack` owns and monitors the native lifetime. Each inet adapter also
  monitors its controlling process and stack, so socket cleanup and bundle
  failure stay isolated.
- Raw-link egress is `{:smol_stack, link_ref, :egress, packet}`. The configured
  link process owns transport backpressure; link death follows `:stop`,
  `:mark_down`, or notification policy.
- TCP buffers are fixed at 4,096 bytes per direction. UDP rings contain 16
  packet descriptors and 16 KiB per direction. Public sockets and retained TCP
  closes share the configured `ready_events` capacity.
- Established TCP close invalidates the public handle immediately and retains a
  bounded native FIN state for at most 30 seconds. UDP closes immediately.
- The first release includes outbound TCP/UDP, listener/accept pooling, and
  passive and bounded active inet modes for IPv4 and IPv6.
- Stable low-level errors are translated by inet adapters into documented OTP
  atoms. Close/error transitions abort both relevant waiter directions.
- Application timeouts, packet framing, active-mode policy, and unsent or
  partially received application data remain in Elixir; Rust never owns an
  arbitrary application queue.
