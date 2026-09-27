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
- `:gen_tcp`'s `nodelay` option, which turns Nagle's algorithm off. It is
  accepted at connect, listen and `:inet.setopts/2` and reported by
  `:inet.getopts/2`, and defaults to `false` as in `:gen_tcp`. A socket
  accepted from a listener takes the listener's setting. Before, SmolNet
  rejected the option with `:einval`, so code that turned Nagle off could
  not run over it unchanged. The low-level API gets the same switch as
  `SmolNet.setopt(socket, {:tcp, :nodelay}, true)`, read back with
  `SmolNet.getopt/2`.
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

### Changed

- TCP sockets now run CUBIC congestion control, as Linux's do by default.
  Before, a SmolNet sender put its whole send buffer into the network every
  round trip, however narrow the path, and lost much of it wherever a queue
  was short: through a 20 Mbit/s bottleneck it sent at about 1.5 Mbit/s,
  and with several streams less. A connection now starts from a window of
  ten segments, 14,600 bytes on a typical path, as RFC 6928 and Linux do, and
  grows it from there. After a lost SYN it starts from one segment.
- The minimum TCP retransmission timeout is now 200 ms, as on Linux,
  instead of 1 s. This deliberately departs from RFC 6298, which says the
  minimum SHOULD be 1 s: on paths with round trips of tens of milliseconds,
  that floor cost a whole second, doubling from there, for every loss that
  fast retransmit could not repair, such as a lost retransmission or the end
  of a transfer. The timeout computed from measured round trips still
  applies above the floor, and the first one, before any round trip is
  measured, is still 1 s.
- TCP receive and send buffers (`rcvbuf`/`sndbuf`, and `:gen_tcp`'s
  `recbuf`/`sndbuf`) now default to 256 KiB each instead of 64 KiB. At
  64 KiB, a stream over a 100 ms round trip could not exceed about 5 Mbit/s,
  whatever the path; 256 KiB allows about 20 Mbit/s. They still do not tune
  themselves. Each TCP socket therefore holds 512 KiB by default, 64
  sockets about 32 MiB, and the per-stack 128 MiB buffer cap now fits 256
  TCP sockets at the default sizes: a stack with a raised socket limit that
  opens more than that gets `{:error, :system_limit}` unless it asks for
  smaller buffers. `:gen_tcp`'s `buffer` still defaults to 64 KiB.
- A TCP sender now resends several lost segments per round trip when its
  peer supports SACK, as Linux does, using RFC 6675's SACK scoreboard.
  Before, it resent one lost segment per round trip, so a window that lost
  many segments, as a congested queue or a burst of loss does, took many
  round trips or a retransmission timeout to repair. An ACK that SACKs data
  below a range the peer reported earlier now also counts as a duplicate
  ACK; before, it did not when it also changed the window, which could
  delay a fast retransmission.

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
