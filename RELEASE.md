### Changed

- TCP connections now use timestamps (RFC 7323) when the peer offers them, as
  Linux, macOS and Windows peers do. A sender that retransmitted a segment
  needlessly can tell from the timestamps and undo its window reduction. The
  option takes 12 bytes of every segment's header, so a full segment carries
  1448 bytes of data on a 1500-byte MTU instead of 1460, as on Linux.

### Fixed

- Receiving over a path that reorders packets is much faster. On a path with
  20 ms ± 5 ms of delay and no loss, one stream from Linux went from about
  6 Mbit/s to 44 Mbit/s.
  - A SmolNet receiver now holds up to 32 ranges of out-of-order data, up from
    4. Before, it dropped a segment past the fourth, and the sender had to
    send it again.
  - A SmolNet receiver now acknowledges a duplicate segment at once with a
    D-SACK (RFC 2883). A sender that retransmitted it needlessly, as Linux
    does on a path that reorders packets, can then undo its window reduction.
    Before, a Linux sender on such a path held its window at a few segments.
