# ADR 0011: time-driven native continuations

## Status

Accepted.

## Context

The first release bounded every normal-scheduler NIF path with fixed byte,
packet, readiness, maintenance, socket, and waiter quotas. Those limits made
work deterministic, but they were calibrated on particular hardware. A path
could remain below its fixed quota while taking longer on a slower host, and
copying transmit `Vec<u8>` values into BEAM binaries happened after the native
work being bounded.

The stack owner already understands the envelope's `more` flag and schedules
another poll through its mailbox. The native side therefore needs to stop at a
portable time boundary without losing work, while keeping the fixed bounds as
protection against clock and implementation anomalies.

## Decision

Each serialized native stack call starts a Rust monotonic deadline. The total
normal-scheduler target is 1 millisecond: native work receives 750
microseconds, with 250 microseconds reserved for result construction and BEAM
handoff. Work loops check the deadline between atomic units. The existing byte,
packet, readiness, and maintenance quotas remain hard upper bounds.

Transmit tokens allocate Rustler `OwnedBinary` buffers and smoltcp writes
directly into them. Encoding releases those buffers into the caller's
environment, eliminating the former packet payload copy. The BEAM-owned
buffer is still explicitly zero-initialized before smoltcp fills it. This
matches smoltcp's standard transmit-token buffers: encoders may retain the
initial value of padding or reserved bytes, and no untouched byte can expose
uninitialized native memory. Initializing through `OwnedBinary.as_mut_slice()`
uses Rustler's documented contract for its newly allocated binary memory.

Pending output stays in the device queue. Abort notifications have a retained
queue, ready events are removed one at a time, and overflow scanning preserves
its socket-table cursor. Listener scans and graceful-close cleanup preserve
their existing cursors. Shutdown linearizes the lifecycle as
`running -> shutting_down -> shutdown`, rejects later socket operations, and
removes bounded state incrementally. Any retained work sets `more: true`.
The Elixir shutdown boundary drains those bounded native continuations before
the stack process exits, so abort notifications cannot be stranded in a dying
owner's mailbox. Readiness hints already queued when shutdown begins are
discarded incrementally; their still-armed waiters receive the shutdown abort
instead of a misleading select notification.

Orderly shutdown is the deliberate exception to mailbox-scheduled
continuations: the stack owner synchronously invokes bounded native slices
until cleanup finishes before allowing the process to exit. Fixed native caps
bound the total drain, and a 1,024-call safety guard prevents an implementation
fault from trapping the owner indefinitely. An explicit shutdown attempt is
not repeated from `terminate/2`; if the guard is exhausted, the public stop
path still destroys the bounded resource instead of leaving it live.

`enif_consume_timeslice` charges the calling process for the measured fraction
of the 1 millisecond target. The charge is incremental rather than a single
report at the end of the call: work loops charge at chunk boundaries and stop
early when a charge reports the caller's reduction slice as spent. A stack
owner that has already burned most of its slice on Elixir work before
re-entering therefore receives a correspondingly shorter native slice instead
of a full one. Each charge covers only the time since the previous charge;
charging the elapsed time since the start of the call would re-bill every
earlier chunk.

Rustler's API accepts whole percentages, so the smallest honest charge covers
10 microseconds. Work units are grouped into chunks large enough that the
minimum charge is not a systematic over-report, and at least one chunk always
runs per call so that a chronically reduction-starved owner still makes
progress instead of spinning through its own mailbox. The monotonic deadline
remains a hard ceiling: reductions bound how much the caller is charged, not
how long the call occupies the scheduler thread.

Continuation state is unchanged. Work stopped by either the deadline or a spent
slice retains its cursors, and `more` only reports work actually retained. The
stack owner's self-sent poll messages are ordered behind messages already in
its mailbox, so each bounded slice gives ordinary stack traffic an opportunity
to run. Separate stack owners remain independently schedulable.

Unexpected resource destruction remains synchronous. This is intentionally
different from explicit shutdown: the resource contains at most 64 native
sockets, 128 logical entries and waiters, and 32 queued output packets.
Worst-case destruction is measured as a complete process-exit-to-release path.
A permanent cleanup thread was rejected because its lifetime, NIF unload, and
failure behavior would be more complex than the already small, fixed
destructor bound.

## Verification

Debug NIF tests can force a deadline after an exact number of checkpoints.
They verify that maximum-capacity output, ready-queue overflow, abort delivery,
and shutdown retain work, report `more`, and converge within a bounded number
of polls. Stack-owner tests run a long continuation chain while proving both
ordinary messages to that stack and calls to another stack complete.

Rust unit tests and fuzz targets use `Vec<u8>` output because Rustler
`OwnedBinary` allocation and release require a live BEAM NIF environment. The
Elixir debug-NIF tests therefore cover the production allocation, retention,
release, and destruction path, while the sanitizer-backed Rust fuzz targets
cover the same queue and continuation logic with the `Vec<u8>` substitute.
Failure to allocate a bounded transmit binary is reported as `:native_panic`;
the partially completed native call is not retried. Because the panic occurs
while the resource lock is held, the resource then rejects later operations as
`:ownership_invariant_violation` and is reclaimed when its owner exits.
The sanitizer-backed lifecycle fuzzer drives the production structural
shutdown loop, but cannot exercise BEAM notification delivery or the complete
`Env`-dependent lifecycle transition; deterministic debug-NIF tests cover that
integration path.

The benchmark measures complete Elixir-visible calls, including result
encoding, as well as caller reductions. Local runs enforce the absolute 1
millisecond maximum. The GitHub-hosted x86_64 Linux quality job plus the
AArch64 Linux and macOS native-budget jobs enforce the 1 millisecond p99 and
retain maximum samples as evidence because runner preemption is outside the
NIF's control. In p99 mode, each maximum-state scenario is rebuilt and
measured 100 times rather than being treated as an ungated single sample.
Shutdown and unexpected resource destruction remain part of the maximum-state
benchmark.

The native snapshot exposes the configured call target, work deadline,
encoding headroom, deadline yields, timeslice exhaustion events, and maximum
observed serialized native-call duration before result encoding. A call may now
charge the caller several times, so `timeslice_exhaustions` counts the calls in
which any charge reported the slice as spent rather than the number of charges.

## Consequences

Continuation frequency now adapts to the host rather than assuming every
machine can complete a full quota in the same time. Hard quotas still bound
each work dimension, so a stalled or anomalous clock cannot create an
unbounded loop. Output ownership is transferred to the BEAM without a payload
copy. Time checks and one-at-a-time cursor updates add small overhead to busy
paths in exchange for predictable scheduler occupancy and safely retained
progress.

Calling `enif_consume_timeslice` means message-heavy calls are charged for
both explicit native scheduler usage and BEAM message delivery. The reduction
gate therefore allows at most 4,000 reductions, replacing the former 1,200
ceiling while still rejecting calls that consume more than two ordinary
2,000-reduction slices. Chunked charging rounds each chunk up to a whole
percent, which costs at most one percent of a slice per chunk; the maximum-state
scenarios remain around half the gate.

Slice-aware stopping makes continuations more frequent under load: a caller
with little of its slice left now yields after a chunk rather than draining a
whole readiness quota. Total work per unit of time is unchanged, but it is
spread across more calls, which is the point — other processes on the scheduler
are no longer starved by an owner that re-enters the NIF with an almost empty
slice.

The remaining gap is the encoding tail. `enif_consume_timeslice` is called
before result encoding runs, so up to the 250 microsecond headroom goes
unreported. That is a separate change.
