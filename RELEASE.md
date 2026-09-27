### Fixed

- A TCP stream could stop for good after losing a single segment. This
  happened when the link ran out of egress credit just as the stack tried
  to resend the lost segment, and all the data the window allowed was
  already in flight. The resend is now retried once credit is granted.
