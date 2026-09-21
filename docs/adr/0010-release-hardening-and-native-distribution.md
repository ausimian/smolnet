# ADR 0010: release hardening and native distribution

## Status

Accepted for the first release candidate.

## Context

The first release must demonstrate that malformed input, concurrency, process
death, resource destruction, and sustained socket churn cannot create an
unbounded native path or leave the BEAM scheduler inside a long NIF call.
Native distribution must not permanently add generated shared libraries to the
source repository's history.

## Boundedness and lifecycle audit

- `StackResource` exclusively owns the native stack behind a nonblocking
  `try_lock`. Its destructor does not acquire that lock, send messages, or wait
  for network progress. It explicitly takes and drops the bounded mutex-owned
  stack, socket buffers, queued packets, and saved waiter terms before updating
  the observable completion counters.
- Public socket-table entries plus retained graceful TCP closes share the
  logical `ready_events` capacity, whose maximum is 128. A separate hard cap of
  64 native backing sockets covers ordinary TCP/UDP sockets, listener-pool
  members, accepted children, graceful closes, and wildcard-UDP expansion.
  Opening, listener expansion, binding, and accepted-child promotion preflight
  both capacities before allocating.
- TCP uses fixed 4,096-byte receive and transmit buffers. UDP uses fixed rings
  of 16 packet descriptors and 16 KiB of payload per direction. Listener pools
  contain at most four native sockets per public listener, accepted queues use
  the requested backlog capped at 128, and wildcard UDP uses at most the eight
  configured interface addresses while remaining within the shared 64-socket
  native cap.
- Waiters retain one saved PID and reference per read direction and per write
  direction. Ready queues, ingress work, output packets, maintenance scans, and
  copied bytes all have explicit per-call limits: at most 128 readiness events,
  128 maintenance units, 32 input packets, 32 output packets, and 65,575 copied
  bytes. Remaining work sets the envelope's `more` flag so the stack owner
  promptly polls again.
- Native configuration decoding accepts at most eight addresses, four routes,
  16 address bytes, and 128 debug ready keys. It decodes incrementally rather
  than allocating an unbounded intermediate list.
- Panics remain inside Rustler's unwind boundary. Native operations expose
  stable atom errors, Elixir public specifications cover their return shapes,
  and resource destruction never sends application data or blocks for network
  progress.

## Verification evidence

Deterministic tests preserve four state-machine seeds and execute 3,000 mixed
open, wait, readiness, cancellation, close, and race operations. Raw-link tests
exercise repeated loss, duplication, held/reordered traffic, delayed ACKs,
resets, full buffers, owner/link death, stack restarts, concurrent close and
cancel, and 100 native-resource release cycles. Existing timer-only TCP tests
prove retransmission without inbound traffic.

The Rust workspace has unit and Clippy gates plus three libFuzzer targets. They
construct real native stacks and drive arbitrary and valid smoltcp ingress and
polling, stack configuration and endpoint validation, and bounded TCP/UDP open,
bind, listen/connect, readiness, stale-handle, graceful-close, and shutdown
sequences. Linux CI runs the Rust suite with AddressSanitizer and gives every
target 10,000 iterations. Failure artifacts are retained by CI.

A local 20,000-call empty-poll sample measured a 1.469 microsecond mean, 1.875
microsecond p99, and 46.000 microsecond maximum. Maximum-path samples measured
592.583 microseconds for 65,575 bytes encoded across 32 output packets, 35.542
microseconds for maximum-MTU ingress, 119.459 microseconds for 64 closing native
sockets, and 185.583 microseconds for 128 readiness deliveries. A conservative
combined call performing output encoding, closing cleanup, and readiness
delivery measured 734.750 microseconds. Eight-address wildcard UDP binding took
32.375 microseconds and maximum TCP listener expansion took 17.416 microseconds.

Shutdown with 64 native UDP handles, 128 active waiters, and 128 retained sent
waiters took at most 95.667 microseconds. Complete process-exit destruction of
the same socket/waiter state plus 32 queued maximum-MTU packets took at most
79.125 microseconds. The resource completion counter advances only after the
inner stack has been explicitly dropped. The executable gate allows at most 1
millisecond and 1,200 caller reductions for every measured NIF call; the highest
observed caller charge was 389 reductions. Local runs enforce both ceilings.
Shared GitHub-hosted runners retain the 1 millisecond measurements as
report-only evidence because VM performance is not stable enough for a hard
wall-clock assertion; the deterministic reduction ceiling remains enforced.
Issue #14 tracks a portable measurement strategy and restoration of the hard
CI wall-clock gate. The destructor gate measures the complete
process-exit-to-resource-release interval rather than claiming it is a NIF
invocation.

## Native distribution decision

Repository checkouts keep source compilation through Rustler and contain no
generated `.so` or `.dylib` files. Hex packages omit the Rust workspace and use
`rustler_precompiled` target naming, checksum verification, and loading for
GNU/Linux x86_64/AArch64 and Apple Silicon macOS assets. The checksum manifest
is generated from the published bytes and shipped inside the immutable Hex
package rather than trusted from the mutable GitHub release.

The consumer compiler adds an archive-validation step before
`rustler_precompiled` extracts anything. An archive must contain exactly the
one expected regular NIF file; links, devices, directories, traversal paths,
absolute paths, duplicate entries, and unexpected files are rejected.

The release workflow builds Linux assets on native Ubuntu 22.04 runners,
establishing glibc 2.35 as the compatibility floor. It rejects imports newer
than that floor and dependencies outside `libc`, `libgcc_s`, `libm`, `libdl`,
`libpthread`, and `librt`. Apple Silicon builds on a native `macos-14` runner,
records a macOS 14 deployment target, and permits only `libSystem`. Each
packaged artifact is loaded by a real BEAM at its platform floor and run
through `health/0`, the public integration suite, and a package-consumer smoke
test. Unsupported Hex-consumer targets fail with an actionable error instead
of falling back to source that is not present in the package.

## Consequences

The release candidate has no intentionally unbounded packet, socket, waiter,
queue, decoder, or native-wait path. Supported glibc Linux and Apple Silicon
macOS consumers do not need a Rust toolchain. Source checkouts remain portable
source builds; musl, Intel macOS Hex packages, and other targets remain
explicitly unsupported.
