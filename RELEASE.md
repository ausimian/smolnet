### Changed

- A stack checks the header checksum of each IPv4 packet it receives about
  seven times faster, cutting the work its process does per IPv4 packet by
  about 40%. Bulk TCP over IPv4 on a loopback link runs about 10% faster,
  level with IPv6. A packet with a bad checksum is still refused with
  `{:error, :invalid_packet}`.
- Received TCP data is copied once on its way from a stack to the socket
  owner, instead of two or three times. A 64 KiB `:gen_tcp.recv/2` of data
  that is already queued returns about three times faster, and bulk
  transfers that read large chunks, with `recv(0)` or in active mode, run
  about 20% faster on a loopback link.
- A stack reads the packets handed to `SmolNet.ingress/2` in place instead
  of copying each one first. Only packets it has not processed when its
  work budget for the call runs out are copied, to be processed on the
  next call. Batched ingress costs about 40 ns less per 1,280-byte packet,
  a third less for a batch of 32, and bulk TCP fed in batches runs a few
  percent faster on a loopback link.
- A stack clears only the first 64 bytes of each outgoing packet before
  writing it, instead of the whole packet; every later byte is always
  written. Sending a 9,000-byte UDP datagram takes about 4% less native
  time; at smaller packet sizes the saving is too small to measure.
- A received UDP datagram is copied to the socket owner in one block
  instead of byte by byte. `recvfrom` of an 8 KiB datagram returns about
  three times faster, and of a 1,200-byte datagram about 30% faster.
- A send that uses a stack's whole `bytes_copied` limit, such as a 64 KiB
  `:gen_tcp.send/2` at the default limit, now hands its link the first
  segments in the same native call. Before, they waited for the stack to
  poll itself, which cost one more native call and stack round trip per
  send. On an idle connection over a loopback link the first segment of a
  64 KiB send at MTU 9,000 now leaves about 10% sooner; bulk throughput is
  unchanged. A full-size ingress batch or receive also hands off its
  output at once. `bytes_copied` now bounds what a call copies and the
  packets it hands the link separately, and `max_bytes_copied` in
  `SmolNet.stack_info/1` reports the larger of the two.
- A stack keeps its poll timer when a call leaves the poll deadline where
  it was, instead of cancelling the timer and starting a new one. A stack
  call that does not move the deadline returns about 20% sooner, and a
  bulk TCP transfer over a loopback link starts and cancels about a quarter
  fewer timers. Throughput and send latency are unchanged.

### Fixed

- A `:gen_tcp` socket in active mode could fail to deliver the last bytes
  it received, so the owner waited forever for the end of a transfer. It
  happened when the socket read those bytes at the end of a batch of reads
  and no more data followed. The same could happen after
  `:gen_tcp.controlling_process/2` moved the socket to a new owner as such
  a batch ended.
