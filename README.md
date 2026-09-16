# SmolNet

SmolNet is an Elixir library that embeds the Rust
[`smoltcp`](https://github.com/smoltcp-rs/smoltcp) TCP/IP stack behind a
deliberately small Rustler NIF.

The project is under initial development. Phase 2 provides independent raw-IP
IPv6 stacks; sockets arrive in later phases. TCP over IPv6 is the first socket
target, followed by TCP over IPv4, then UDP over IPv6 and IPv4.

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

Ingress validates the IPv6 header and declared length before reserving space in
a packet-and-byte-bounded queue. Saturation returns `{:error, :queue_full}`.
IPv4 is rejected until its planned phase. Link-recipient failure can stop the
stack, retain it as marked down, or notify another process. `SmolNet.stop_stack/1`
stops the complete temporary supervision bundle.

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
