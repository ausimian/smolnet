# SmolNet

SmolNet is an Elixir library that embeds the Rust
[`smoltcp`](https://github.com/smoltcp-rs/smoltcp) TCP/IP stack behind a
deliberately small Rustler NIF.

The project is under initial development. Phase 4 provides independent raw-IP
IPv6 stacks plus low-level IPv6 TCP open, bind, nonblocking connect, endpoint
queries, cancellation, and abortive close. Stream send/receive, synchronous
wrappers, listening, IPv4, and UDP arrive in later phases.

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

## Low-level IPv6 TCP connect

TCP endpoints use `:socket`-style IPv6 maps. `connect/3` is nonblocking in this
phase: initiate it with `:nowait`, wait for the matching select message, then
retry to finalize.

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
{:select, select_info} = SmolNet.connect(socket, peer, :nowait)
%SmolNet.Socket{id: id, generation: generation} = socket
{:select_info, :connect, select_ref} = select_info

receive do
  {:"$smol_socket", {^id, ^generation}, :select, ^select_ref} ->
    :ok = SmolNet.connect(socket, peer, :nowait)
end

{:ok, local_endpoint} = SmolNet.sockname(socket)
{:ok, %{addr: ^remote, port: 443}} = SmolNet.peername(socket)
:ok = SmolNet.close(socket)
```

Each TCP socket has fixed 4096-byte native RX and TX buffers. Automatic ports
come from the bounded `49152..50175` range and are unique within one stack.
Link-local `fe80::/10` endpoints require a positive integer `scope_id`; global
addresses use scope zero. The public module documentation lists the stable
validation, bind, connection, lifecycle, and handle errors. A native ignored-
handshake timeout is 30 seconds; caller-controlled synchronous deadlines remain
Phase 5 work.

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
