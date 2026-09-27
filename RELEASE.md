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
- A TLS integration script, `sudo mix run integration/tls.exs`, that soaks
  HTTPS downloads and uploads over `:ssl` on SmolNet, single- and
  multi-stream over IPv4 and IPv6, to `speed.cloudflare.com` or to HTTPS
  servers on the host, with SmolNet as the TLS client and the server. Every
  body's length and SHA-256 is checked, any TLS alert fails the run, and
  throughput is reported next to the kernel's on the same path, in the notes
  and as a result in `verdict.json`. `:ssl` cannot yet take SmolNet's
  `:gen_tcp` modules as its transport directly (#97), so the harness wraps
  them.
