# SmolNet

SmolNet is an Elixir library that embeds the Rust
[`smoltcp`](https://github.com/smoltcp-rs/smoltcp) TCP/IP stack behind a
deliberately small Rustler NIF.

Version 0.1.0 is the first release candidate. It provides independent raw-IP
dual-family stacks and bounded IPv4 and IPv6 TCP and UDP operation. TCP is
available through `:gen_tcp`; UDP is available through `:gen_udp`, with passive
and active delivery and normal controlling-process ownership.

## Quick start and raw-link contract

From a source checkout, this self-contained smoke test starts a stack, inspects
it, and shuts down its temporary supervision bundle:

```console
mix run examples/quickstart.exs
```

```elixir
address = {0xFD00, 0, 0, 0, 0, 0, 0, 1}

{:ok, stack} =
  SmolNet.start_stack(
    egress: {self(), :quickstart},
    addresses: [{address, 64}]
  )

{:ok, info} = SmolNet.stack_info(stack)
:running = info.native.result.lifecycle
:ok = SmolNet.stop_stack(stack)
```

`SmolNet.start_stack/1` creates an independent native stack and returns an
opaque reference. A transport-neutral link process supplies complete IPv4 or IPv6
packets with `SmolNet.ingress/2` and receives emitted packets in bounded batches:

```elixir
address = {0xFD00, 0, 0, 0, 0, 0, 0, 1}

{:ok, stack} =
  SmolNet.start_stack(
    egress: {self(), :my_link},
    mtu: 1280,
    addresses: [{address, 64}, {{192, 0, 2, 1}, 24}],
    link_down: :stop
  )

:ok = SmolNet.ingress(stack, complete_ip_packet)

receive do
  {:smol_stack, :my_link, :egress, complete_ip_packets} ->
    :send_them_over_the_external_transport
end
```

`complete_ip_packets` is a non-empty list of complete raw IP packet binaries.
Its length and total data are bounded by the stack's `:output_packets` and
`:bytes_copied` limits. Each native output envelope produces at most one message
and preserves packet order.

Each stack has one serialized link feeder. Ingress waits only until the stack
owner validates and accepts the packet; bounded native processing then runs
before another stack message is accepted. This naturally limits ingress to one
packet being processed and one subsequent feeder call waiting. The feeder owns
backpressure for its external transport. Link-recipient failure can stop the
stack, retain it as marked down, or
notify another process. `SmolNet.stop_stack/1` stops the complete temporary
supervision bundle.

Low-level socket values are lightweight `%SmolNet.Socket{}` structs. Native
socket IDs and generations are globally monotonic, never reused, and capped at
`2^59 - 1` so they remain immediate integers on the supported 64-bit BEAM
targets. Each stack also caps live public socket entries at its `:ready_events`
limit; opening or promoting an accepted child beyond that bound returns or
applies the documented bounded-overflow policy. A listener may own up to four
additional internal pool sockets per public listener identity. A blocked
nonblocking operation returns `{:select_info, operation, reference}`; its
one-shot message has this shape:

```elixir
{:"$smol_socket", {socket.id, socket.generation}, :select, reference}
```

The message is only a retry hint. `SmolNet.cancel/2` removes the exact waiter
and returns `:ok`, `:already_sent`, or `:not_found` according to which side of
the readiness race won.

## Loopback links

`SmolNet.Loopback` is a link process that hands every packet its stack emits
straight back to that same stack. One stack then reaches its own addresses with
no peer, no external transport, and no privileges, which is the shortest path
to a runnable example or test:

```console
mix run examples/loopback.exs
```

```elixir
{:ok, link} =
  SmolNet.Loopback.start_link(
    addresses: [{{127, 0, 0, 1}, 8}, {{0, 0, 0, 0, 0, 0, 0, 1}, 128}]
  )

stack = SmolNet.Loopback.stack(link)
```

The link owns the stack it loops. `SmolNet.Loopback.start_link/1` takes the
`SmolNet.start_stack/1` options apart from `:egress`, which the link supplies,
and an optional `:name` for the link process. Stopping the link stops its stack
through the stack's own `:link_down` policy, and `SmolNet.stop_stack/1` stops
the link.

A loopback link carries packets; it does not invent addresses. A stack answers
only on the addresses it was configured with, so conventional localhost
addresses are a convention here rather than a special case, and any other
address the stack holds loops just as well. Packets re-enter through the public
`SmolNet.ingress/2` and are validated exactly like packets from a real
transport, so the loop is a worked example of the link contract above rather
than a shortcut past it.

## Low-level TCP streams

TCP endpoints use explicit `:socket`-style IPv4 or IPv6 maps. The family is
fixed by `SmolNet.open/4`; mismatched endpoints are rejected rather than
converted. IPv4-mapped IPv6 addresses are intentionally unsupported.
Synchronous calls wait in the
calling process using one monotonic deadline; the stack owner and native
scheduler never wait for traffic. Finite timeouts use milliseconds in
`0..4_294_967_295`. Passing `:nowait` exposes the same one-shot retry primitive
directly.

```elixir
local = {0xFD00, 0, 0, 0, 0, 0, 0, 1}
remote = {0xFD00, 0, 0, 0, 0, 0, 0, 2}

{:ok, stack} =
  SmolNet.start_stack(
    egress: {link_pid, :link_a},
    addresses: [{local, 64}]
  )

{:ok, socket} =
  SmolNet.open(:inet6, :stream, :tcp,
    stack: stack,
    rcvbuf: 192 * 1024,
    sndbuf: 192 * 1024
  )
:ok = SmolNet.bind(socket, %{family: :inet6, addr: local, port: 0})

peer = %{family: :inet6, addr: remote, port: 443}
:ok = SmolNet.connect(socket, peer, 5_000)
:ok = SmolNet.send(socket, ["hello", " world"], 5_000)
{:ok, response} = SmolNet.recv(socket, 128, 5_000)
:ok = SmolNet.shutdown(socket, :write)

{:ok, local_endpoint} = SmolNet.sockname(socket)
{:ok, %{addr: ^remote, port: 443}} = SmolNet.peername(socket)
:ok = SmolNet.close(socket)
```

The nonblocking stream shapes keep every continuation in the caller:

```elixir
{:select, {send_info, unsent}} = SmolNet.send(socket, large_binary, :nowait)
{:select, {recv_info, partial}} = SmolNet.recv(socket, exact_length, :nowait)

# After the matching one-shot message, retry only `unsent`, or request the
# remaining receive length while retaining `partial` in the caller.
:ok = SmolNet.cancel(socket, send_info)
:ok = SmolNet.cancel(socket, recv_info)
```

`recv(socket, 0, timeout)` returns one bounded currently available chunk. A
positive synchronous length accumulates bounded reads until exact completion,
timeout, error, or peer EOF. EOF returns buffered data first; the next receive
returns `{:error, :closed}`. Reset remains `:connection_reset`. If a timeout or
error follows partial progress, send returns the unsent remainder and receive
returns accumulated data in `{reason, continuation}`.

Each TCP socket allocates fixed-size native RX and TX buffers when it opens.
Socket-style `rcvbuf` and `sndbuf` select the respective capacities from 1 KiB
through 1 MiB; both default to 64 KiB and cannot be resized later. The inet
adapter exposes the corresponding `gen_tcp` names, `recbuf` and `sndbuf`.
Accepted sockets inherit the listener's capacities, including newly replenished
listener-pool members. `SmolNet.stack_info/1` reports the defaults and the
configured capacities of each logical TCP socket under
`native.result.tcp_buffer_bytes`.

The two buffers remain allocated while a live socket is closing. Listener-pool
members also allocate both buffers, and all of them count toward the stack's
64-native-socket cap. The defaults therefore permit at most 8 MiB of TCP buffer
memory per full stack; configuring both directions to 1 MiB raises that bound
to 128 MiB. Native calls still copy at most 64 KiB per bounded call, so a larger
buffer fills or drains over multiple polls.

Automatic ports come from the bounded `49152..50175` range. Bind reservations
are unique within one address family, so one dual-family stack may bind the same port once for
IPv4 and once for IPv6; accepted children retain their listener's local port.
Link-local `fe80::/10` endpoints require a positive integer `scope_id`; global
addresses use scope zero. The native layer never stores arbitrary unsent
payloads or exact-receive accumulation. Established close invalidates the
public handle immediately, then retains one bounded native closing record long
enough to drive FIN and retransmission, with a 30-second deadline measured from
the close call; closing records continue to count against the socket limit.
The public module documentation lists the stable validation, timeout,
connection, stream, lifecycle, and handle errors.

IPv4 endpoints use `%{family: :inet, addr: {192, 0, 2, 2}, port: 443}` and
otherwise have the same connect, stream, timeout, cancellation, half-close,
and close behavior. A stack may contain both four-octet IPv4 and eight-segment
IPv6 addresses and routes. Each route's destination and gateway must have the
same family.

Raw IPv4 ingress validates the IHL, exact total length, header checksum, and
MTU before native mutation. Fragmented IPv4 input (a nonzero fragment offset or
the more-fragments flag) is rejected; reassembly is outside this raw-link API.
IPv4 limited broadcast is rejected for interface addresses, gateways, and TCP
endpoints.

## Low-level TCP listeners

A bound stream socket becomes a reusable listener with `SmolNet.listen/2`.
Backlogs are integers in `1..128`. Each listener maintains
`min(backlog, 4)` native listening sockets so one `smoltcp` socket can be
promoted into a connected child without pretending that it remains reusable.
Every promoted child receives a fresh public socket ID and generation, and the
native pool is replenished. Half-open handshakes expire after 30 seconds and
their pool slots are replenished from timer-driven maintenance.
Wildcard listeners remain scoped to their explicit socket family on a
dual-family stack and accept any configured local address in that family.

```elixir
{:ok, listener} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
:ok = SmolNet.bind(listener, %{family: :inet6, addr: local, port: 8080})
:ok = SmolNet.listen(listener, 16)

{:ok, child} = SmolNet.accept(listener, 5_000)
{:ok, request} = SmolNet.recv(child, 0, 5_000)
:ok = SmolNet.send(child, request, 5_000)
```

`SmolNet.accept/2` supports finite and infinite waits plus `:nowait`, using the
same read-direction select/cancel contract as receive. The accepted-child queue
never exceeds the requested backlog. If another connection becomes established
while that queue is full—or the public socket table cannot allocate its child
identity—the newest child is dropped and its pool slot is replenished. The peer
may observe handshake completion first; its next traffic receives a reset from
the now-unmatched connection. Listener scans and replenishment are charged to
the stack's `maintenance_work` bound and expose pool, queue, promotion, refill,
and overflow metrics through `SmolNet.stack_info/1`.

Closing a listener aborts a pending accept and immediately releases queued
children and listening or half-open pool members. Already-returned children are
independent and remain usable. Stack shutdown releases both listener and child
state.

## Low-level IPv4 and IPv6 UDP

Open UDP with `SmolNet.open(family, :dgram, :udp, stack: stack)`, where
`family` is `:inet` or `:inet6`. UDP and TCP have independent port namespaces,
and the two address families are also isolated, so all four combinations may
bind the same numeric port on one stack.

```elixir
local = %{family: :inet6, addr: local_address, port: 0}
peer = %{family: :inet6, addr: peer_address, port: 53}

{:ok, socket} = SmolNet.open(:inet6, :dgram, :udp, stack: stack)
:ok = SmolNet.bind(socket, local)
:ok = SmolNet.sendto(socket, <<0, 1, "query">>, peer, 5_000)

{:ok, datagram} = SmolNet.recvfrom(socket, 0, 5_000)
%{source: source, destination: destination, data: payload, truncated: false} = datagram
```

Each native UDP socket has fixed receive and transmit rings of 16 packet
metadata entries and 16 KiB of payload. The maximum accepted datagram payload
is `min(stack_mtu - header_bytes, 16_384)`, where `header_bytes` is 28 for IPv4
and 48 for IPv6. A larger send returns `:message_too_large`; the `gen_udp`
adapter translates that error to `:emsgsize`. A send is accepted in full or not
at all. When the transmit ring is full, `:nowait` returns a write-direction
select hint and a retry uses the original complete datagram—there is no partial
progress.

A wildcard bind uses one family-specific native backing socket per configured
local address, capped by the stack's eight-address configuration limit. This
keeps IPv4 and IPv6 wildcard sockets on the same port isolated despite
smoltcp's family-neutral wildcard endpoint. All backing sockets share one
logical identity, one transmit ring, and one read waiter; receive inspection
remains capped at 16 datagrams per call. Wildcard sends retain the stack's
destination-aware IPv4 and IPv6 source-address selection.

`recvfrom(socket, 0, timeout)` returns one complete datagram, including a
zero-length datagram. A positive length returns at most that many bytes,
discards the remainder of that datagram, and reports `truncated: true`. Source
and the packet's actual local destination are retained. Invalid nonzero UDP
checksums are discarded by the native stack. IPv4's standard zero checksum is
accepted, while IPv6 requires a checksum. An unreachable destination fails
before queueing with `:network_unreachable`.

UDP uses the same one-shot read/write waiter table, cancellation result, caller
deadline loop, ready-event sweep, timer propagation, and output bounds as TCP.
One read and one write waiter may coexist. Closing a UDP socket invalidates it
immediately and aborts both waiters; UDP has no retained graceful-close state.
`SmolNet.connect/2` stores a default peer, restricts later sends to that peer,
and discards incoming datagrams from other peers.

## `gen_tcp` IPv4 and IPv6 clients and servers

Select the SmolNet backend with `{:tcp_module, SmolNet.InetBackend.Tcp}` and
identify the target stack with `{:smolnet_stack, stack}`. The returned socket
works with the standard `:gen_tcp` and `:inet` client operations implemented by
this phase:

```elixir
options = [
  {:tcp_module, SmolNet.InetBackend.Tcp},
  {:smolnet_stack, stack},
  :inet6,
  :binary,
  {:active, false},
  {:packet, :raw}
]

{:ok, socket} = :gen_tcp.connect(remote, 443, options, 5_000)
:ok = :gen_tcp.send(socket, "request")
{:ok, response} = :gen_tcp.recv(socket, 0, 5_000)
:ok = :gen_tcp.shutdown(socket, :write)
:ok = :gen_tcp.close(socket)
```

For IPv4, use `SmolNet.InetBackend.Tcp4` and `:inet`; all socket operations use
the shared adapter implementation:

```elixir
ipv4_options = [
  {:tcp_module, SmolNet.InetBackend.Tcp4},
  {:smolnet_stack, stack},
  :inet,
  :binary,
  {:active, false}
]

{:ok, socket} = :gen_tcp.connect({192, 0, 2, 2}, 443, ipv4_options, 5_000)
```

`examples/loopback.exs` runs this end to end against a loopback link, so it
needs no peer.

The same options can create a server. `backlog` defaults to 5 and accepts
values in `1..128`; accepted sockets inherit the listener's supported active,
mode, packet, packet-size, adapter-buffer, native `recbuf`/`sndbuf`, and
send-timeout options.

```elixir
server_options = [{:backlog, 16} | options]

{:ok, listener} = :gen_tcp.listen(8080, server_options)
{:ok, socket} = :gen_tcp.accept(listener, 5_000)
{:ok, request} = :gen_tcp.recv(socket, 0, 5_000)
:ok = :gen_tcp.send(socket, request)
```

The adapter supports `:binary` and `:list`, packet modes `:raw`, `:line`, `1`,
`2`, and `4`, and `active: false | true | :once | N` for `N` in
`1..32_767`. Counted active mode counts complete logical packets and emits
`{:tcp_passive, socket}` when exhausted. Active delivery is limited to 16
native reads or logical packets per mailbox turn.

The adapter `buffer` and `packet_size` default to 65,536 bytes and each is
capped at 1 MiB. The separate native `recbuf` and `sndbuf` options default to
65,536 bytes, accept 1 KiB through 1 MiB at connect or listen time, and return
`:einval` from `setopts` because smoltcp cannot resize a live TCP socket.
As with inet, setting `recbuf` also raises `buffer` to at least that size unless
a later `buffer` option explicitly lowers it.
Oversized frames return `:emsgsize` and close the socket because the stream
cannot be resynchronized. One read and one write may proceed at the same time;
a second operation in the same direction returns `:busy`. `send_timeout` and
`send_timeout_close` control a blocked adapter send.

Each OTP socket is a temporary process under its stack bundle. The controlling
process owns active messages, and `:gen_tcp.controlling_process/2` transfers
queued and future messages in order. Owner death, adapter death, or stack
failure closes the low-level socket without affecting independent stack
bundles. A listener has its own adapter state, while each accepted child gets a
separate connected-stream adapter owned by the process that called
`:gen_tcp.accept/2`.

## `gen_udp` IPv4 and IPv6 sockets

Select the IPv6 backend with `{:udp_module, SmolNet.InetBackend.Udp}` and pass
the target stack with `{:smolnet_stack, stack}`:

```elixir
options = [
  {:udp_module, SmolNet.InetBackend.Udp},
  {:smolnet_stack, stack},
  :inet6,
  :binary,
  {:active, false}
]

{:ok, socket} = :gen_udp.open(0, options)
:ok = :gen_udp.send(socket, peer_address, 53, "query")
{:ok, {source_address, source_port, response}} = :gen_udp.recv(socket, 0, 5_000)
:ok = :gen_udp.connect(socket, peer_address, 53)
:ok = :gen_udp.send(socket, "connected query")
:ok = :gen_udp.close(socket)
```

For IPv4, select `SmolNet.InetBackend.Udp4` and `:inet`. The callback supplies
IPv4 name resolution while all socket operations use the same bounded adapter
state machine:

```elixir
ipv4_options = [
  {:udp_module, SmolNet.InetBackend.Udp4},
  {:smolnet_stack, stack},
  :inet,
  :binary,
  {:active, false}
]

{:ok, socket} = :gen_udp.open(0, ipv4_options)
:ok = :gen_udp.send(socket, {192, 0, 2, 2}, 53, "query")
```

Supported UDP socket policy is `:binary` or `:list`,
`active: false | true | :once | N` for `N` in `1..32_767`, and bounded
`buffer`/`recbuf` values from 1 byte through 1 MiB. The buffer caps the payload
returned for both passive and active receives; a larger datagram is truncated
and its remainder discarded, as with the standard callback's unreported receive
truncation. Active messages have the standard
`{:udp, socket, source_address, source_port, packet}` shape; counted mode emits
`{:udp_passive, socket}` when exhausted. Each adapter mailbox turn delivers at
most 16 datagrams before yielding. Packet framing, send timeouts, ancillary
data, multicast, broadcast, raw options, and file descriptors are unsupported
and fail explicitly.

Every UDP socket is a temporary state-machine child under its stack bundle.
Read and write continuations are independent, competing operations in one
direction return `:busy`, controlling-process transfer moves queued and future
active messages, and owner or adapter death closes only that socket. Stack
failure terminates its adapters with `:enetdown` without affecting another
stack bundle.

## Inet option contract

The callback option surface is deliberately finite. Invalid values, options in
the wrong lifecycle phase, and any option not listed as supported return
`:einval`; selecting the other family returns `:eafnosupport`.

| Option | TCP open/listen/connect | TCP runtime | UDP open | UDP runtime |
| --- | --- | --- | --- | --- |
| `:smolnet_stack` | required | fixed | required | fixed |
| `:inet` / `:inet6` | supported | fixed | supported | fixed |
| `:binary` / `:list` / `:mode` | supported | supported | supported | supported |
| `:active` | `false`, `true`, `:once`, or `1..32767` | supported | same | supported |
| `:packet` | `:raw`, `:line`, `1`, `2`, or `4` | supported | unsupported | unsupported |
| `:packet_size` | `0..1048576` | supported | unsupported | unsupported |
| `:buffer` | `1..1048576` | supported | `1..1048576` | supported |
| `:recbuf` | `1024..1048576` | fixed | `1..1048576` | supported |
| `:sndbuf` | `1024..1048576` | fixed | unsupported | unsupported |
| `:send_timeout` / `:send_timeout_close` | supported | supported | unsupported | unsupported |
| `:ip` / `:ifaddr` / `:port` | supported | fixed | supported | fixed |
| `:backlog` | listen only, `1..128` | fixed | unsupported | unsupported |
| `:ipv6_v6only` | IPv6 `true` only | fixed | IPv6 `true` only | fixed |

Ancillary data, multicast, broadcast, OS file descriptors, raw socket options,
and every other TCP or UDP inet option are outside the first-release contract
and fail explicitly.

## Architecture and supervision

Each call to `SmolNet.start_stack/1` creates a temporary supervision bundle
containing one stack owner process and one dynamic supervisor for its inet
adapters. The stack process exclusively owns the native resource and serializes
all access. Native calls use a nonblocking `try_lock`, perform bounded work, and
return immediately; readiness waits, application deadlines, framing, and active
mode remain in Elixir processes. A stack or adapter failure cannot corrupt
another bundle, and owner/link failure follows the documented lifecycle policy.

The Rust workspace separates the reusable `smolnet_core` engine from the thin
`smolnet_nif` Rustler entry point. The engine contains packet, socket, waiter,
timer, and bounds logic. Elixir owns supervision, raw-link delivery, timer
replacement, synchronous retry loops, and OTP inet compatibility. Native output
is delivered after every driving operation, and each returned `poll_at` replaces
the prior BEAM timer.

## Error contract

Public low-level calls return `:ok`, `{:ok, value}`, `{:select, continuation}`,
or `{:error, reason}`. Validation failures are stable atoms such as
`:unsupported_family`, `:invalid_options`, `:invalid_address`, and
`:message_too_large`; lifecycle failures use `:closed`, `:invalid_socket`, or
`:invalid_socket_state`; network failures include `:network_unreachable`,
`:connection_timeout`, `:connection_refused`, and `:connection_reset`.
Concurrent operations in the same readiness direction return `:busy`. Inet
adapters translate these into the documented OTP-style atoms such as `:einval`,
`:eafnosupport`, `:emsgsize`, and `:enetdown`. Treat a select notification only
as permission to retry.

## Migrating to 0.1.0

This is the first public release candidate, so there is no earlier supported
release API to migrate from. Users of development snapshots should update to
the explicit IPv4/IPv6 endpoint maps and inet backend modules shown above,
remove assumptions that socket IDs can be reused, and handle nowait operations
through their returned select continuations. Repository checkouts continue to
compile the NIF from source. Hex consumers on supported GNU/Linux and Apple
Silicon macOS targets download a checksum-pinned precompiled NIF.

## Development

The supported development baseline is Elixir 1.19.5, Erlang/OTP 28.3, and Rust
1.94.0. The compatibility matrix additionally covers the supported Elixir
1.18–1.20 and OTP 27–29 combinations on Linux and macOS.

Run the complete local quality gate before committing:

```console
mix precommit
```

The suite's wall-clock budgets are strict by default so that a change which
slows the stack fails locally. Budgets that only bound how long a healthy run
may take are multiplied by `SMOLNET_TEST_TIMEOUT_SCALE`, which CI sets to `5`
because its shared runners are preemptible; budgets whose expiry is the
assertion are never scaled. Set it locally only to reproduce a CI run:

```console
SMOLNET_TEST_TIMEOUT_SCALE=5 mix test
```

Maintainers cutting a release should follow the complete Publisho, native
asset, checksum, and Hex sequence in [MAINTAINING.md](MAINTAINING.md).

## Native builds

Hex consumers on GNU/Linux x86_64/AArch64 and Apple Silicon macOS download a
precompiled NIF for the exact SmolNet version. Each archive is verified against
the checksum pinned inside the Hex package, validated to contain exactly one
regular NIF file, and then extracted. Linux assets target glibc 2.35 or newer
and may depend only on glibc's standard `libc`, `libm`, `libdl`, `libpthread`,
and `librt` libraries plus `libgcc_s`. The Apple Silicon asset targets macOS
14 or newer and may depend only on `libSystem`. Alpine, other musl systems, and
Intel macOS are unsupported as Hex-package targets.

Repository checkouts and CI always compile from source so native changes cannot
be hidden by a restored or downloaded artifact. Source builds require Rust
1.91 or newer, a platform C linker, and Erlang development files; Rust 1.94.0
is the pinned development version. Hex packages intentionally omit the Rust
workspace. Unsupported Hex-consumer targets fail with a list of supported
targets and should use a source checkout if they need to build locally.

Every native stack call has a 1 ms normal-scheduler target. Native work stops
at a monotonic 750 microsecond deadline, reserving 250 microseconds for result
encoding and handoff to the BEAM. Output packets are allocated as Rustler
`OwnedBinary` values and released into the result without a second payload
copy. Output, readiness and overflow delivery, listener and closing
maintenance, and shutdown retain their cursors or queues when the deadline is
reached; `more: true` asks the stack owner to run the next bounded slice.
Native calls also report their measured scheduler share through
`enif_consume_timeslice`, so repeated continuations yield fairly to other
stack processes and ordinary mailbox traffic.

Ingress remains a single-feeder interface with one admitted packet at a time.
When a deadline continuation still owns that slot, another feeder call can
return `{:error, :busy}` sooner than it did under quota-only batching. Feeders
should treat `:busy` as backpressure and retry after yielding or waiting for
their next input opportunity.

The time budget is backed by deterministic per-call maxima of 65,575 copied
bytes, 32 output packets, 128 readiness events, and 128 maintenance units.
These bounds prevent clock or platform anomalies from creating unbounded work.
A separate hard ceiling allows at most 64 native TCP/UDP backing sockets per
stack, including listener pools and wildcard-UDP expansion across configured
addresses. `SmolNet.stack_info/1` exposes the call target, work budget,
encoding headroom, deadline-yield count, timeslice-exhaustion count, and
maximum observed serialized native-call duration before result encoding.

Local `mix precommit` enforces a 1 ms maximum for complete Elixir-visible NIF
calls, including result encoding. The GitHub-hosted x86_64 Linux quality job
plus the AArch64 Linux and macOS native-budget jobs report the full-call
maximum and p99 as evidence without gating on either, because the compute
environment of a shared runner is outside this project's control. The
deterministic caller-reduction budget stays enforced everywhere. Each
maximum-state scenario uses 100 independently prepared samples outside
`enforce` mode.
Unexpected resource destruction remains synchronously bounded by the fixed
socket, waiter, and packet capacities and is included in the benchmark.

## Troubleshooting

- If a source build fails, confirm Rust 1.91 or newer is active and that the
  platform C linker and Erlang development files are installed.
- If a Hex dependency reports that no precompiled NIF is available, confirm
  the host is glibc-based GNU/Linux on x86_64/AArch64 or Apple Silicon running
  macOS 14 or newer. Other targets require a source checkout; there is no
  implicit source fallback in the Hex package.
- If ingress returns a validation error, supply exactly one complete IPv4 or
  IPv6 packet within the configured MTU. Ethernet frames and fragmented IPv4
  packets are not accepted.
- If an operation times out despite no inbound traffic, ensure the stack owner
  remains alive. SmolNet schedules retransmission timers itself, but the raw
  transport must forward emitted packets and return peer traffic.
- Use `SmolNet.stack_info/1` to inspect lifecycle, bounds, socket counts, and
  listener/queue metrics before reporting a failure.

## Status

Version 0.1.0 is prepared as a release candidate but has not been tagged,
published to Hex, or released. `RELEASE.md` will be introduced only after the
first release has actually been published.

## License

SmolNet is released under the MIT License. See `LICENSE`.
