### Added

- `SmolNet.start_stack/1` takes a `:backend` option that selects the protocol
  engine, and `stack_info/1` reports it as `:backend`. The default is
  `:native`, the smoltcp NIF; any other value is rejected with
  `:invalid_backend`.
- `backend: :elixir` selects a pure-Elixir protocol engine, still in
  development. It answers what a stack answers by itself, as the NIF does:
  echo replies, Port and Protocol Unreachable, Parameter Problem and a RST
  for a closed port, over IPv4 and IPv6. It has UDP sockets, with the NIF's
  semantics and through the `gen_udp` adapters too: binds (wildcard
  included), ephemeral ports, connect, truncation, `:message_too_large`,
  waiters, `cancel/2` and the `stack_info/1` counts. It has no TCP sockets
  yet: opening one returns `{:error, :not_implemented}`, as does starting it
  with `:egress_credit`.
