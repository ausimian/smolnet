### Changed

- The advertised TCP receive window now follows the socket's receive buffer
  (`rcvbuf`/`recbuf`) instead of being held to a single segment, so throughput
  over links with real latency is no longer capped at about one segment per
  round trip. Over a 50 ms round trip, a default 64 KiB buffer now reaches
  about 1 MB/s instead of about 47 KB/s. An embedder that relied on the
  smaller window can lower `recbuf` to get it back.
