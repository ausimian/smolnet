### Fixed

- A TCP stream that loses several segments from one window now resends just
  the lost segments, one round trip apart, instead of stalling for smoltcp's
  1 s minimum retransmission timeout and then resending everything after the
  first gap. Over loopback, a 4 MiB transfer through a link that drops bursts
  of segments fell from 10–25 s to about 0.2 s. A loss at the very end of a
  transfer can still wait for the 1 s timeout.
