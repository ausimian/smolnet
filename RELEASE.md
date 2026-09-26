### Changed

- IPv4 header checksums are checked about seven times faster. This cuts
  the work a stack does per IPv4 packet by about 40% and makes bulk TCP
  over IPv4 about 10% faster on a loopback link. Packets with a bad
  checksum are still rejected with `{:error, :invalid_packet}`.
- Received TCP data is copied once instead of two or three times. A
  64 KiB `:gen_tcp.recv/2` of queued data is about three times faster, and
  bulk transfers that read large chunks are up to about 20% faster.
- `SmolNet.ingress/2` no longer copies every packet in a batch. Only the
  packets left over when a call runs out of work budget are copied. This
  saves about 40 ns per 1,280-byte packet, about a third of the cost of a
  batch of 32.
- Received UDP datagrams are copied in one block instead of byte by byte.
  Receiving an 8 KiB datagram is about three times faster, and a
  1,200-byte datagram about 30% faster.
- Outgoing packets are no longer zero-filled in full; only their first 64
  bytes are cleared. Sending a 9,000-byte UDP datagram takes about 4% less
  time.
- A send that uses the whole `bytes_copied` limit, such as a 64 KiB
  `:gen_tcp.send/2`, now sends its first segments straight away instead
  of waiting for the stack's next poll. On an idle connection at MTU
  9,000, the first segment leaves about 10% sooner.
- `bytes_copied` now limits what a native call copies in and what it
  sends out separately, instead of sharing one budget between them.
  `max_bytes_copied` in `SmolNet.stack_info/1` reports the larger of the
  two.
- A stack keeps its poll timer when the deadline doesn't change, instead
  of cancelling and restarting it. Those calls are about 20% faster, and a
  bulk transfer starts about a quarter fewer timers.

### Fixed

- In active mode, a `:gen_tcp` socket could fail to deliver the last bytes
  it received, leaving its owner waiting forever for the end of a
  transfer. This happened when those bytes were read at the end of a batch
  of reads and no more data followed, including when
  `:gen_tcp.controlling_process/2` had just moved the socket to a new
  owner.
