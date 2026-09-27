### Added

- A real-network integration harness in the source repository, under
  `integration/`, for long runs of SmolNet against real peers. It is not
  part of the Hex package and does not run in `mix test`. It provides a
  link that carries a stack's packets to a host TUN device through a small
  helper program, with egress credit returned as the device accepts
  packets; `setup.sh` and `teardown.sh` for the device, addresses and
  NAT; a soak runner with per-operation deadlines, metrics sampled to CSV
  with leak detection, a rolling packet capture kept on failure, named
  `tc netem` profiles, and a kernel baseline mode; and a smoke script,
  `sudo mix run integration/smoke.exs`, that pings SmolNet from the host
  and opens TCP connections each way through the device.
