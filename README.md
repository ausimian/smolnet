# SmolNet

SmolNet is an Elixir library that embeds the Rust
[`smoltcp`](https://github.com/smoltcp-rs/smoltcp) TCP/IP stack behind a
deliberately small Rustler NIF.

The project is under initial development. Phase 5 provides independent raw-IP
IPv6 stacks plus low-level IPv6 TCP open, bind, connect, bounded stream I/O,
half-close, endpoint queries, cancellation, and graceful close. The same
operations support finite, infinite, and nonblocking caller-owned waits.
Listening, the inet adapter, IPv4, and UDP arrive in later phases.

`SmolNet.start_stack/1` creates an independent native stack and returns an
opaque reference. A transport-neutral link process supplies complete IPv6
packets with `SmolNet.ingress/2` and receives each emitted packet as a message:

```elixir
address = {0xFD00, 0, 0, 0, 0, 0, 0, 1}

{:ok, stack} =
  SmolNet.start_stack(
    egress: {self(), :my_link},
    mtu: 1280,
    addresses: [{address, 64}],
    link_down: :stop
  )

:ok = SmolNet.ingress(stack, complete_ipv6_packet)

receive do
  {:smol_stack, :my_link, :egress, complete_ipv6_packet} ->
    :send_it_over_the_external_transport
end
```

Each stack has one serialized link feeder. Ingress waits only until the stack
owner validates and accepts the packet; bounded native processing then runs
before another stack message is accepted. This naturally limits ingress to one
packet being processed and one subsequent feeder call waiting. The feeder owns
backpressure for its external transport. IPv4 is rejected until its planned
phase. Link-recipient failure can stop the stack, retain it as marked down, or
notify another process. `SmolNet.stop_stack/1` stops the complete temporary
supervision bundle.

Low-level socket values are lightweight `%SmolNet.Socket{}` structs. Native
socket IDs and generations are globally monotonic, never reused, and capped at
`2^59 - 1` so they remain immediate integers on the supported 64-bit BEAM
targets. Each stack also caps live socket entries at its `:ready_events` limit;
opening beyond that bound returns `{:error, :system_limit}`. A blocked
nonblocking operation returns `{:select_info, operation, reference}`; its
one-shot message has this shape:

```elixir
{:"$smol_socket", {socket.id, socket.generation}, :select, reference}
```

The message is only a retry hint. `SmolNet.cancel/2` removes the exact waiter
and returns `:ok`, `:already_sent`, or `:not_found` according to which side of
the readiness race won.

## Low-level IPv6 TCP streams

TCP endpoints use `:socket`-style IPv6 maps. Synchronous calls wait in the
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

{:ok, socket} = SmolNet.open(:inet6, :stream, :tcp, stack: stack)
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

Each TCP socket has fixed 4096-byte native RX and TX buffers. Automatic ports
come from the bounded `49152..50175` range and are unique within one stack.
Link-local `fe80::/10` endpoints require a positive integer `scope_id`; global
addresses use scope zero. The native layer never stores arbitrary unsent
payloads or exact-receive accumulation. Established close invalidates the
public handle immediately, then retains one bounded native closing record long
enough to drive FIN and retransmission, with a 30-second deadline measured from
the close call; closing records continue to count against the socket limit.
The public module documentation lists the stable validation, timeout,
connection, stream, lifecycle, and handle errors.

## Development

The supported development baseline is Elixir 1.19.5, Erlang/OTP 28.3, and Rust
1.94.0. The compatibility matrix additionally covers the supported Elixir
1.18–1.20 and OTP 27–29 combinations on Linux and macOS.

Run the complete local quality gate before committing:

```console
mix precommit
```

## Native builds

SmolNet currently builds its NIF from source. Building requires a Rust toolchain
new enough for Rustler and smoltcp; Rust 1.91 is the declared minimum and Rust
1.94.0 is the pinned development version.

The first release targets GNU-libc Linux and macOS. When a supported GNU
architecture has no prebuilt NIF, the Hex package retains `native/Cargo.toml`,
`native/Cargo.lock`, and the complete `smolnet_nif` crate so Rustler can compile
the library during dependency compilation. Alpine and other musl systems are
not yet supported.

## Status

SmolNet has not published its first release. `CHANGELOG.md` records initial
development; `RELEASE.md` will be introduced only after the first release has
been published.

## License

SmolNet is released under the MIT License. See `LICENSE`.
