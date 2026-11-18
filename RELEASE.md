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
  waiters, `cancel/2` and the `stack_info/1` counts. It has TCP sockets too,
  through the `gen_tcp` and `ssl` adapters as well, sending what the NIF
  sends segment for segment: listeners with a real backlog of up to 128
  handshakes and 128 accepted children, connect that tells a refused
  connection from a reset one, graceful close with a 30 s deadline,
  shutdown, Nagle, keep-alive and the user timeout. Its loss recovery and
  congestion control are the NIF's too, and send what it sends under loss,
  reordering and duplication: NewReno with limited transmit, SACK recovery,
  RACK-TLP, D-SACK and CUBIC. It does not yet act on
  ICMP errors that report a path MTU, nor hold one call's egress to the
  `:output_packets` and `:bytes_copied` limits, and starting it with
  `:egress_credit` returns `{:error, :not_implemented}`.
