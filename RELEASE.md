### Added

- `:ssl` can run over SmolNet with `SmolNet.Inet.Tcp` or `SmolNet.Inet6.Tcp`
  as its `cb_info` transport, as a client (`:ssl.connect/4`, or
  `:ssl.connect/3` on a connected socket) and as a server (`:ssl.listen/2`
  with `:ssl.transport_accept/2` and `:ssl.handshake/2`, or
  `:ssl.handshake/3` on an accepted socket). Both modules now accept the
  `{:header, 0}` option `:ssl` sets, report it from `getopts`, and export
  `port/1`, `monitor/1` and `cancel_monitor/1`, so `:inet.monitor/1` works
  on SmolNet sockets and delivers `{:DOWN, ref, :socket, socket, :closed}`
  when one closes. See the `:gen_tcp` guide.
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
  and as a result in `verdict.json`.

### Fixed

- A TCP write longer than one segment no longer holds its last, partial
  segment back until the peer acknowledges the earlier ones. Nagle's
  algorithm now holds a partial segment only while another partial segment
  is unacknowledged, as Linux does, instead of while any data is. The old
  rule cost such a write a round trip plus the peer's delayed ACK, and did so
  on every TLS 1.3 handshake whose server asks for a post-quantum key share,
  as `speed.cloudflare.com` does: `:ssl`'s second ClientHello is longer than
  a segment. Those handshakes now take as long as over the kernel's stack.
- A `:gen_tcp` or `:gen_udp` call pending on a SmolNet socket whose stack
  fails now always returns `{:error, :enetdown}`. Depending on scheduling,
  it could return `{:error, :closed}` instead, and an active socket's owner
  could get no `tcp_error`, `tcp_closed` or `udp_error` message at all. This
  covers pending receives, sends, accepts and connects. Calls pending when
  `SmolNet.stop_stack/1` stops a stack still fail with `:closed`.
- A TCP sender now fast-retransmits a lost segment to a Linux peer instead
  of waiting at least a second for its retransmission timer. Linux grows
  its advertised window on nearly every ACK early in a connection, and
  SmolNet counted an ACK as a duplicate only if its window was unchanged,
  so none of Linux's duplicate ACKs counted. An ACK whose SACK blocks
  report new data is now a duplicate whatever its window.
