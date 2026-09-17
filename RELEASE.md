### Added

- Expose native lifecycle, call-budget, deadline-yield, scheduler-timeslice,
  and maximum native-call duration telemetry through `SmolNet.stack_info/1`.

### Changed

- Drive native output, readiness, maintenance, and shutdown through
  time-bounded resumable continuations with zero-copy transmit binaries and
  explicit BEAM scheduler accounting. Orderly shutdown now enters an
  observable `:shutting_down` native lifecycle while bounded cleanup drains.
  A feeder can receive `{:error, :busy}` while a deadline continuation retains
  the stack's single ingress slot and should treat it as backpressure.
