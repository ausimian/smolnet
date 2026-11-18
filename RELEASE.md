### Added

- `SmolNet.start_stack/1` takes a `:backend` option that selects the protocol
  engine, and `stack_info/1` reports it as `:backend`. The only engine, and
  the default, is `:native`, the smoltcp NIF; any other value is rejected
  with `:invalid_backend`.
