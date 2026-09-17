### Added

- Distribute checksum-pinned precompiled NIFs for glibc 2.35+ GNU/Linux on
  x86_64/AArch64 and Apple Silicon on macOS 14+, with strict archive and
  native-dependency validation.
- Expose native lifecycle, call-budget, deadline-yield, scheduler-timeslice,
  and maximum native-call duration telemetry through `SmolNet.stack_info/1`.

### Changed

- Drive native output, readiness, maintenance, and shutdown through
  time-bounded resumable continuations with zero-copy transmit binaries and
  explicit BEAM scheduler accounting. Orderly shutdown now enters an
  observable `:shutting_down` native lifecycle while bounded cleanup drains.
  A feeder can receive `{:error, :busy}` while a deadline continuation retains
  the stack's single ingress slot and should treat it as backpressure.
