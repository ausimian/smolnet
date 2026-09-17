### Changed

- The NIF now charges `enif_consume_timeslice` incrementally at work-loop chunk
  boundaries instead of once at the end of a call, and stops early when a charge
  reports the caller's reduction slice as spent. A stack owner that has already
  used most of its slice before re-entering the NIF receives a shorter native
  slice, so other processes on the scheduler are not starved. The 750 microsecond
  work deadline remains a hard ceiling, at least one chunk always runs per call,
  and retained work still continues through the existing `more` protocol.
