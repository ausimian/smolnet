SmolNet's TCP now behaves much more like Linux's on real networks, with
congestion control, modern loss recovery and path MTU discovery, and `:ssl`
runs over SmolNet directly. Several TCP defaults change; see **Changed**,
especially the larger socket buffers, which lower how many TCP sockets a
stack holds by default.

### Added

- `:ssl` over SmolNet: pass `SmolNet.Inet.Tcp` or `SmolNet.Inet6.Tcp` as the
  `cb_info` transport, as a client or a server, including `:ssl.listen/2`.
  Connect to an address and pass the host name as `server_name_indication`;
  `:ssl.connect/4` with a host name is not supported yet. SmolNet sockets
  also work with `:inet.monitor/1`. See the new `:ssl` guide, `ssl.md`.
- `:gen_tcp`'s `nodelay` option, to turn off Nagle's algorithm, and
  `SmolNet.setopt(socket, {:tcp, :nodelay}, true)` in the low-level API.
  Before, SmolNet rejected it with `:einval`.
- `:gen_tcp`'s `keepalive` option, with Linux's default timing: a probe after
  2 hours with nothing received, then one every 75 s, and `:etimedout` after
  9 go unanswered. The low-level API has `{:socket, :keepalive}`. Before,
  SmolNet rejected it with `:einval`.
- A path MTU guide, `path_mtu.md`, on configuring `:mtu` and MSS clamping for
  paths with a narrower hop.

### Changed

- **TCP buffers now default to 256 KiB each, up from 64 KiB,** so one stream
  over a 100 ms round trip can reach about 20 Mbit/s instead of 5. The
  128 MiB per-stack buffer cap now fits 256 TCP sockets at these defaults;
  beyond that, opens fail with `{:error, :system_limit}` unless sockets ask
  for smaller buffers. `:gen_tcp`'s `buffer` stays at 64 KiB.
- TCP now uses CUBIC congestion control with a ten-segment initial window,
  as Linux does, instead of none. Through a 20 Mbit/s bottleneck a sender
  went from about 1.5 Mbit/s to about 18.
- The minimum TCP retransmission timeout is now 200 ms, as on Linux, instead
  of 1 s. This departs from RFC 6298's suggested 1 s.
- TCP loss recovery now matches Linux's: SACK-based recovery of several
  segments per round trip (RFC 6675), tail loss probes and RACK loss
  detection (RFC 8985), limited transmit (RFC 3042), and window growth by
  the bytes each ACK acknowledges. Under bursty loss, and when uploading to
  internet servers, transfers are several times faster.

### Fixed

- A TCP connection whose peer vanishes with data outstanding now fails with
  `:etimedout` after 924.6 s, Linux's default, instead of retransmitting
  forever (RFC 5482's user timeout). Idle connections are never timed out.
  The low-level API reports `:connection_timeout`.
- A TCP send across a hop narrower than `:mtu` no longer stalls forever. A
  connection now does path MTU discovery (RFC 1191, RFC 8201): a validated
  ICMP "Fragmentation Needed" or "Packet Too Big" lowers its segment size,
  and it resends at the new size. Paths that filter these errors still need
  `:mtu` or MSS clamping; see `path_mtu.md`. `stack_info` reports new
  `icmp_too_big_*` and `path_mtu_reductions` counters.
- A TCP write longer than one segment no longer waits a round trip for its
  last partial segment, which slowed some TLS 1.3 handshakes. Nagle's
  algorithm now holds a partial segment only while another is
  unacknowledged, as on Linux.
- A lost segment to a Linux peer is now fast-retransmitted instead of
  waiting for the retransmission timer.
- `:gen_tcp` and `:gen_udp` calls pending when a stack fails, or when its link
  dies under `link_down: :stop`, now return `{:error, :enetdown}`, and an
  active socket's owner gets `tcp_error` or `udp_error` with `:enetdown`.
  Before, they could get `:closed`, as after `SmolNet.stop_stack/1`, which
  still gives `:closed`. A partly sent write returns
  `{:error, {:enetdown, rest}}`.
- Idle sockets no longer hold on to the memory of their last transfer.
  Socket processes, and `SmolNet.Loopback`, now hibernate after 5 s idle.

### Internal

- A real-network integration harness under `integration/`, not part of the
  Hex package, with smoke, TLS, path MTU, idle-connection, chaos, crawl and
  netem-matrix scenarios. They run on every pull request and on nightly and
  weekly schedules; see `integration/README.md`.
