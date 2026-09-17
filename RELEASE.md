# Initial release

SmolNet embeds the Rust `smoltcp` network stack in an OTP-friendly Elixir
library with explicitly bounded native work.

### Added

- Independent, supervised IPv4 and IPv6 raw-IP stacks with configurable
  addresses, routes, MTUs, link behavior, and runtime telemetry.
- Low-level TCP and UDP sockets plus `:gen_tcp` and `:gen_udp` compatible
  adapters for clients, listeners, datagrams, active and passive delivery, and
  controlling-process ownership.
- Scheduler-aware NIF execution with bounded queues, buffers, readiness,
  maintenance, shutdown, and packet-processing work.
- Checksum-pinned precompiled NIFs for Linux x86_64, Linux AArch64, and Apple
  Silicon macOS, while repository checkouts continue to build from Rust source.
