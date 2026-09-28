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
- `:gen_tcp`'s `keepalive` option, so that an idle connection can detect
  a peer that has gone. It is accepted at connect, listen and
  `:inet.setopts/2` and reported by `:inet.getopts/2`, defaults to `false`,
  and a socket accepted from a listener takes the listener's setting.
  Its timing is Linux's default, and fixed: a probe after 2 hours in which
  nothing arrives, then one every 75 s, and the connection fails with
  `:etimedout` once 9 have gone unanswered. Before, SmolNet rejected the
  option with `:einval`, and `:gen_tcp.connect/4` exited with `:badarg`.
  The low-level API gets the same switch as
  `SmolNet.setopt(socket, {:socket, :keepalive}, true)`, where the
  connection fails with `:connection_timeout`.
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
- A path MTU guide, `path_mtu.md`. It covers what SmolNet does when a hop
  on the path is narrower than its `:mtu`, and how to configure a link so
  that this does not matter. Where the path filters the ICMP errors that
  report such a hop, a TCP send across it stalls with no error, and UDP
  datagrams too large for it are lost wherever they are. Setting `:mtu` to
  the narrowest MTU on the path avoids both. Where no `:mtu` fits, as on
  an IPv4 path below 1280, clamping the MSS on the router at the narrow
  link fixes TCP, and UDP datagrams must be kept within the path.
- A path MTU integration script, run as
  `integration/pmtu-topology.sh isolate mix run integration/pmtu.exs`. It
  puts a hop with a smaller MTU between SmolNet and a Linux peer, in
  network namespaces of its own, and needs no sudo where the host allows
  unprivileged user namespaces. It sends TCP and UDP each way over IPv4
  and IPv6, with and without the hop's ICMP errors and MSS clamping, and
  reports each case as adapting, stalling or failing, next to what is
  expected, with captures from both sides of the hop.
- An idle-connection integration script, `sudo mix run integration/idle.exs`,
  that holds up to 240 TCP and TLS connections open for the whole run, idle
  for seconds to hours between echoes, trickling single bytes, bursting after
  long idles and stalling on a closed receive window, while a share of them
  lose their peer silently through nftables. It records how soon SmolNet
  fails each connection to a vanished peer, fails the run unless it fails
  every one it can detect, and fails the run if the stack polls while every
  connection is idle.
- A chaos integration script, run as
  `integration/pmtu-topology.sh isolate mix run integration/chaos.exs`. It
  injects faults into a stack while it carries TLS transfers, connection
  churn and blocked calls. The faults are kills of the TUN helper or the
  link under each `:link_down` policy, `SmolNet.stop_stack/1`, device and
  route flaps, egress credit delayed, trickled or withheld, and socket
  owners killed or swapped. After each one it checks that every blocked
  call returns an error within a deadline, that `SmolNet.monitor/1`
  delivers its `:DOWN`, that nothing of the stack is left running, and
  that a fresh stack works. Its schedule of faults comes from `--seed`, so
  a run can be replayed.
- A crawl integration script, `sudo mix run integration/crawl.exs`, that
  sends TLS `HEAD /` requests to the top 1,000 sites of the day's Tranco
  list, over SmolNet and over the kernel, host by host, round after round,
  at a polite rate. It lists the hosts that fail over SmolNet but not over
  the kernel, grouped by cause, with packets and TCP options cut from the
  capture, and checks that the stack's sockets return to their baseline
  after every round. Before the first round it pushes the stack past its
  socket ceiling against a listener on the host, and checks that opens
  past it fail promptly with `:system_limit` and that the stack recovers.
- A netem matrix integration script, run as
  `integration/netem-topology.sh isolate mix run integration/netem_matrix.exs`.
  It runs bulk TCP transfers under every `tc netem` profile, with SmolNet
  sending and receiving, beside two kernel stacks on a veth impaired the
  same way, and fails on any transfer that is not delivered intact or any
  SmolNet stall longer than a set number of retransmission timeouts.
  Throughput and completion time are recorded next to the kernel's, and
  `integration/netem.md` holds the latest results, with each gap linked
  to the issue that tracks it.

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

- A TCP connection whose peer vanishes while it has data outstanding now
  fails with `:etimedout`, instead of retransmitting forever. A connection
  with data or a FIN unacknowledged, or data its peer's zero window holds
  back, that hears nothing from its peer for 924.6 s is aborted: a pending
  `:gen_tcp.recv/3` or `:gen_tcp.send/2` returns `{:error, :etimedout}`,
  and an active socket's owner gets `{:tcp_error, socket, :etimedout}` then
  `{:tcp_closed, socket}`. This is RFC 5482's user timeout, fixed at how
  long Linux takes to give up by default, and it bounds zero-window probing
  too. An idle connection, with nothing outstanding, is never timed out.
  In the low-level API the error is `:connection_timeout`.
- A TCP send across a hop narrower than the stack's `:mtu` no longer
  stalls forever. SmolNet ignored the router's ICMP "Fragmentation Needed"
  or ICMPv6 "Packet Too Big" error and resent the same full-size segment,
  backing off, without ever failing. A connection now does path MTU
  discovery (RFC 1191, RFC 8201): the error lowers its segment size to fit
  the reported MTU, never below 536 bytes for IPv4 or 1220 for IPv6, and
  it resends the unacknowledged data at once in smaller segments. An error
  must quote data the connection has in flight (RFC 5927), so one forged
  blind is ignored. Each connection learns the path MTU for itself. Where
  the path filters these errors it is still a black hole, which setting
  `:mtu` or clamping the MSS avoids; see `path_mtu.md`. `stack_info`'s
  native counters now include `icmp_too_big_received`,
  `icmp_too_big_rejected` and `path_mtu_reductions`.
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
- A stack that stops because its link died, under the default
  `link_down: :stop`, now fails `:gen_tcp` and `:gen_udp` calls pending on
  its sockets with `{:error, :enetdown}`, and gives an active socket's owner
  `{:tcp_error, socket, :enetdown}` then `{:tcp_closed, socket}`, or
  `{:udp_error, socket, :enetdown}`. So does a stack that crashes. Before,
  they got `{:error, :closed}` and a bare `tcp_closed`, exactly as after
  `SmolNet.stop_stack/1`, so a caller could not tell a lost network from a
  stack the application stopped, and a stream cut short by a dead link read
  as one that had ended. A send that had queued part of its data returns
  `{:error, {:enetdown, rest}}` in every such case. `SmolNet.stop_stack/1`
  and a supervisor's shutdown still give `:closed`. In the low-level API,
  the `:abort` message a stack sends as it stops carries `:link_down` or
  `:stack_down` in these cases instead of `:closed`; blocking calls still
  return `{:error, :closed}`.
- A TCP sender now fast-retransmits a lost segment to a Linux peer instead
  of waiting at least a second for its retransmission timer. Linux grows
  its advertised window on nearly every ACK early in a connection, and
  SmolNet counted an ACK as a duplicate only if its window was unchanged,
  so none of Linux's duplicate ACKs counted. An ACK whose SACK blocks
  report new data is now a duplicate whatever its window.
- An idle `:gen_tcp` or `:gen_udp` socket on SmolNet no longer holds the
  memory of its last transfer. Each socket is a process, and one that does
  no work never collects its garbage, so it kept the binaries of the data
  it last sent or received alive, up to about the size of that transfer,
  for as long as it stayed idle. A `SmolNet.Loopback` link did the same
  with the packets it last forwarded. These processes now hibernate after
  5 seconds with nothing to do, which frees that memory; a connection busy
  with an exchange never waits that long, so never pays for it. `:ssl`'s
  own connection processes over a SmolNet socket behave as they do over
  any other, keeping their last exchange until they hibernate, which
  their `hibernate_after` option controls.
