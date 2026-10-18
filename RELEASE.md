### Fixed

- A SmolNet receiver now acknowledges a duplicate segment at once with a
  D-SACK (RFC 2883). A sender that retransmitted it needlessly, as Linux does
  on a path that reorders packets, can then undo its window reduction. Before,
  a Linux sender on such a path held its window at a few segments.
