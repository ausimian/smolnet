### Fixed

- Fixed a bug where a call to `SmolNet.ingress/2` could hang. If the stack was
  still busy with earlier work when a packet arrived, it held the packet and
  planned to process it after its next poll. A socket call made at the same
  time could cancel that poll. The stack then never processed the held packet,
  and the link process that sent it waited forever, or until an unrelated
  timer happened to fire. The stack now processes the held packet even when
  the poll it was waiting for has been cancelled.

### Added

- `SmolNet.Loopback`, a link process that feeds every packet its stack emits
  back into that same stack. One stack then reaches its own addresses with no
  peer, no external transport, and no privileges, which makes a runnable
  example or test out of what previously needed two stacks and a relay. The
  link takes the `SmolNet.start_stack/1` options apart from `:egress`, which it
  supplies, owns the stack it loops, and is stopped by `SmolNet.stop_stack/1`.
  `examples/loopback.exs` runs a complete `gen_tcp` request and response over
  one stack.

### Changed

- The NIF now charges `enif_consume_timeslice` incrementally at work-loop chunk
  boundaries instead of once at the end of a call, and stops early when a charge
  reports the caller's reduction slice as spent. A stack owner that has already
  used most of its slice before re-entering the NIF receives a shorter native
  slice, so other processes on the scheduler are not starved. The 750 microsecond
  work deadline remains a hard ceiling, at least one chunk always runs per call,
  and retained work still continues through the existing `more` protocol.
