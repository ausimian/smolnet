### Fixed

- A TCP stream could stop for good after losing a single segment. This
  happened when the link ran out of egress credit just as the stack tried
  to resend the lost segment, and all the data the window allowed was
  already in flight. The resend is now retried once credit is granted.
- A stack with no egress credit and closing TCP sockets could poll itself
  without end, about a million times a second, and never finish its native
  call. It kept any `SmolNet.ingress/2` call waiting for that time, so a
  link that feeds ingress and grants credit from one process never sent the
  grant that would have ended it, and every stream on the stack stopped. The
  stack now waits for the grant instead.
