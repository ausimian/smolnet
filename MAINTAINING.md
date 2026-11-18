# Maintaining SmolNet

This file covers repository development, native-build internals, performance
budgets, and releases. It is kept with the source distribution but is not
included in the published ExDoc site.

## Development

The supported development baseline is Elixir 1.19.5, Erlang/OTP 28.3, and Rust
1.94.0. The compatibility matrix additionally covers the supported Elixir
1.18–1.20 and OTP 27–29 combinations on Linux and macOS.

Run the complete local quality gate before committing:

```console
mix precommit
```

The suite's wall-clock budgets are strict by default so that a change which
slows the stack fails locally. Budgets that only bound how long a healthy run
may take are multiplied by `SMOLNET_TEST_TIMEOUT_SCALE`, which CI sets to `5`
because its shared runners are preemptible; budgets whose expiry is the
assertion are never scaled. Set it locally only to reproduce a CI run:

```console
SMOLNET_TEST_TIMEOUT_SCALE=5 mix test
```

## Protocol backends

`SmolNet.Stack` is a shell around a protocol engine, its backend: a module
implementing `SmolNet.Stack.Backend`, chosen per stack with the `:backend`
option. Each callback is one stack operation and returns `{reply, state,
effects}`; the shell keeps the state, sends the effects' packets, readiness
and abort messages, and arms the poll timer. There are two: `:native`, the
NIF behind `SmolNet.Stack.Backend.Native`, and `:elixir`,
`SmolNet.Stack.Backend.Elixir`, which wraps the pure core `SmolNet.Core` that
#173 builds in phases to replace the NIF. The Elixir backend's module is
always written in full, never aliased, since an alias named `Elixir` would
shadow the root namespace.

The suite runs once per backend. `SMOLNET_TEST_BACKEND` names the one stacks
start with when a test does not choose (default `native`):

```console
SMOLNET_TEST_BACKEND=native mix test
```

`SmolNet.start_stack(test_determinism: %{...})` fixes what a stack
otherwise draws at random or from the system clock, so that differential
runs can feed two backends the same packets and times and compare what they
emit. It is test-only and unsupported, like `:test_tcp_timers`. Its keys,
each optional:

- `:random_seed`, 0 to 2^64 - 1, seeds the stack's random source. In the
  NIF, smoltcp's PCG draws the IPv4 identification start from it first and
  then each initial sequence number.
- `:timestamp_origin`, 0 to 2^32 - 1, makes the TCP timestamp clock
  `origin + now` in milliseconds, from the stack's own clock, rather than
  the system's monotonic clock from a random start.
- `:ephemeral_port_start`, 49152 to 50175, is the first ephemeral port tried,
  for TCP and UDP alike.

Tests of the NIF itself are tagged `:nif`, and those needing its debug
hooks `:debug_nif`; both run only on the native backend.

The Elixir backend is built in phases, and a test that needs a phase still
to come is tagged with it: `:elixir_phase_7` (keepalive, user timeout,
path MTU, egress credit, link-down policies and the shutdown lifecycle),
as `@moduletag` for a whole file and `@tag` where a file mixes phases.
`test/test_helper.exs` excludes those tags under
`SMOLNET_TEST_BACKEND=elixir` only, so the native run is unaffected, and CI
gates whatever already passes on the Elixir backend. A phase's exit
criterion includes removing its tag from every test and from the exclusion
list; to see what it still has to do:

```console
SMOLNET_TEST_BACKEND=elixir mix test --only elixir_phase_7
```

A test comparing the two backends directly, such as
`test/conformance/stack_replies_test.exs`, needs the NIF and is tagged
`:nif`; the traces it feeds outlive the NIF. The widest of them,
`test/conformance/link_differential_test.exs`, runs random transfers over
impaired links on every pairing of the two backends; a commit tries 30, and
a longer run sets how many:

```console
SMOLNET_DIFFERENTIAL_RUNS=20000 mix test test/conformance/link_differential_test.exs
```

A test that reads a `stack_info/1` key only the NIF reports checks it
through `SmolNet.Test.Snapshot.nif/2`. The compatibility job's `backend`
matrix dimension lists the backends CI runs. Tests of the shell itself use
`SmolNet.Test.NativeDouble`, a scripted backend.

### Benchmarks

`bench/` holds two benchmarks, not part of the suite; each file's header
lists its settings. Run them under `MIX_ENV=prod`, so the NIF is its release
build, with scheduler busy-waiting off and pinned to idle cores:

```console
MIX_ENV=prod ERL_FLAGS="+S 4 +sbwt none +sbwtdcpu none +sbwtdio none" \
  taskset -c 8-11 mix run bench/core.exs
```

- `bench/core.exs` runs the engine alone: two stacks' backend states joined
  in one process by an in-memory link, under a virtual clock, timing only
  the backend calls. `BACKENDS=native,elixir` compares the two engines,
  `GC=tuned` runs with the Stack process's GC flags, and
  `PROFILE=call_count` (or `call_time`, `call_memory`) adds a `:tprof`
  profile. `DIGEST=all` prints a digest of everything the engine returned
  over a fixed set of runs (several streams, both families, closes, losses):
  a change meant to leave behaviour alone, such as an optimisation, must
  leave every digest as it was. Its wire digest covers only the packets
  each end sent, and is the same for both backends; the full digest is
  reproducible for the Elixir engine only, since the NIF yields to
  continuations by wall-clock budget.
- `bench/loopback.exs` runs one stack carrying a transfer to itself over
  `SmolNet.Loopback`, through the socket API, and reports the rate and the
  CPU and GC per MiB; `TRACE=1` adds the Stack process's own time per MiB,
  which bounds the rate.

## Native development builds and work budgets

Repository checkouts and CI always compile the NIF from source so native
changes cannot be hidden by a restored or downloaded artifact. Source builds
require Rust 1.91 or newer, a platform C linker, and Erlang development files;
Rust 1.94.0 is the pinned development version. Hex packages intentionally omit
the Rust workspace.

Every native stack call has a 1 ms normal-scheduler target. Native work stops
at a monotonic 750 microsecond deadline, reserving 250 microseconds for result
encoding and handoff to the BEAM. Output packets are allocated as Rustler
`OwnedBinary` values and released into the result without a second payload
copy. Output, readiness and overflow delivery, listener and closing
maintenance, and shutdown retain their cursors or queues when the deadline is
reached; `more: true` asks the stack owner to run the next bounded slice.
Native calls also report their measured scheduler share through
`enif_consume_timeslice`, so repeated continuations yield fairly to other stack
processes and ordinary mailbox traffic.

Ingress remains a single-feeder interface with one admitted call at a time.
That call carries either one packet or a batch bounded by `input_packets` and
`bytes_copied`. When a deadline continuation still owns that slot, another
feeder call can return `{:error, :busy}`. Feeders should treat `:busy` as
backpressure and retry after yielding or waiting for their next input
opportunity.

The time budget is backed by deterministic per-call maxima of 65,575 copied
bytes, 32 input packets, 32 output packets, 128 readiness events, and 128
maintenance units. These bounds prevent clock or platform anomalies from
creating unbounded work. The byte maximum applies separately to what a call's
operation copies (a send, a receive, or an ingress batch) and to the packets
it hands the link (ADR 0015), so a 64 KiB send still hands off its first
burst in the same call. The maximum TCP send scenario measures a call at
both.
The `sockets` limit caps the native TCP/UDP backing sockets per stack,
including listener pools and wildcard-UDP expansion across configured
addresses. It defaults to 64 and may be raised to 512, the most at which
resource destruction still fits the 1 ms target (ADR 0014); the socket table
admits the same number of entries and twice as many waiters. Socket buffers
are capped at 128 MiB per stack, what 64 sockets with the largest TCP buffers
held before the limit was configurable, so a higher limit never raises the
memory a destructor frees. The budget scenarios run at both maxima, so
raising either means re-measuring them.
`SmolNet.stack_info/1` exposes the call target, work budget,
encoding headroom, deadline-yield count, timeslice-exhaustion count, and
maximum observed serialized native-call duration before result encoding.

Local `mix precommit` enforces a 1 ms maximum for complete Elixir-visible NIF
calls, including result encoding. The GitHub-hosted x86-64 Linux quality job
plus the AArch64 Linux and macOS native-budget jobs report the full-call maximum
and p99 as evidence without gating on either, because the compute environment
of a shared runner is outside this project's control. The deterministic
caller-reduction budget stays enforced everywhere. Each maximum-state scenario
uses 100 independently prepared samples outside `enforce` mode. Unexpected
resource destruction remains synchronously bounded by the maximum socket,
waiter, and packet capacities and is included in the benchmark.

## Vendored smoltcp

`native/vendor/smoltcp` is smoltcp 0.14.0 with SmolNet patches to the TCP
sender: RFC 6582 partial-ACK recovery (ADR 0013), a fix that keeps a fast
retransmission pending when the device refuses it (#80), and Minshall's
variant of Nagle's algorithm, which sends the partial tail of a write longer
than an MSS without waiting for an ACK (#102), RFC 6675's duplicate ACK,
which counts an ACK that SACKs new data whatever its window (#103), a
200 ms minimum RTO in place of RFC 6298's 1 s (#103), CUBIC's RFC 6928
initial window of up to ten segments (#123), RFC 6675's SACK-based loss
recovery, which resends several lost segments per round trip (#119), and
RFC 8985's RACK-TLP, which probes a lost tail and detects a lost
retransmission without waiting for the timer, with round trip samples
during recovery and RFC 3042's limited transmit (#138). A
further patch, to the interface as well as the sender, adds path MTU
discovery: ICMP "Fragmentation Needed" and "Packet Too Big" lower a
connection's segment size, and it resends at once (#128). Two more patch
its timers: the timeout applies only while a connection has something
outstanding, which makes it RFC 5482's user timeout (#132), and keep-alive
has Linux's idle time, probe interval and probe count (#133). One patches
the receiver: it acknowledges a duplicate segment at once with an RFC 2883
D-SACK, so that a sender can undo a spurious retransmission (#168). The commit that
added the directory holds the crates.io package unmodified, so
`git log -p -- native/vendor/smoltcp` after that commit is the complete set of
SmolNet changes. Keep any further patch small, covered by tests in the
vendored crate, and suitable for offering upstream.

`mix precommit` runs the vendored crate's library tests, with the CUBIC
controller SmolNet enables, so that its tests run too. The TCP socket
tests' harness also records each test as a test vector when
`SMOLTCP_VECTOR_DIR` is set (`src/socket/tcp/vectors.rs`, test-only), and
precommit checks that the committed vectors are current; see
[Conformance assets](#conformance-assets). To run the tests alone:

```console
cargo test --manifest-path native/vendor/smoltcp/Cargo.toml --lib --features socket-tcp-cubic --target-dir native/target/vendor
```

To move to a new smoltcp release, first check whether it already carries
the equivalent of every patch above. If it does, delete
`native/vendor/smoltcp`, restore the registry dependency in
`native/smolnet_core/Cargo.toml`, and remove the precommit step. Otherwise, replace the directory with the new release's
crates.io package from `~/.cargo/registry/src`, omitting `.cargo-ok` and
`Cargo.lock`, and commit that on its own. Then reapply the patch, update the
pinned version, and run `cargo update -p smoltcp` in both `native/` and
`native/fuzz/`.

## Conformance assets

`test/conformance/` holds what the pure-Elixir engine (#173) is checked
against after the NIF and the vendored smoltcp are deleted, generated from
them while they exist. Its README documents each asset, its format and how
to regenerate it:

- `tcp_vectors/`: smoltcp's 238 TCP socket tests as JSON vectors, from
  `elixir scripts/tcp_vectors.exs`, replayed by
  `SmolNet.Conformance.TcpReplay`. `mix precommit` fails when they are
  stale, so a change to the vendored TCP code or its tests is committed with
  regenerated vectors.
- `traces/`: what the NIF backend did, call for call, in a set of
  scenarios, from `MIX_ENV=test mix run scripts/backend_traces.exs`, and
  replayed on another backend by `SmolNet.Conformance.Trace`. The test suite
  replays them on its backend, so a change to the NIF's behaviour is
  committed with re-recorded traces.
- `SmolNet.Conformance.LinkSim`, two backends over a simulated lossy,
  reordering and duplicating link in virtual time, and StreamData properties
  in place of the NIF's fuzz targets.

## Fixed TCP timers, and the test-only override

SmolNet's user timeout (924.6 s) and keep-alive timing (2 h, 75 s, 9
probes) are Linux's defaults and fixed, with no user-facing options:
`USER_TIMEOUT_MILLIS` and the `KEEPALIVE_*` constants in
`native/smolnet_core/src/tcp.rs`. `SmolNet.Stack.Options` holds a copy, as
does the idle scenario, so change all three together. The code takes
them from a stack's `TcpTimers`, so that they can become options later.

`SmolNet.start_stack(test_tcp_timers: %{...})` overrides them for one
stack, with any of `:user_timeout`, `:keepalive_idle` and
`:keepalive_interval`, in milliseconds up to a day, and
`:keepalive_probes`, 1 to 255. It exists so that tests and the idle
scenario's `--user-timeout` and `--keepalive-*` switches can see the timers
work within a short run. It is not documented in `SmolNet.start_stack/1`
and is not a supported option: do not rely on it outside tests. Unit tests
that need the real timings move a stack's clock on instead, with
`SmolNet.Test.ManualClock` and `SmolNet.Test.ClockedPair`.

## Native release assets

SmolNet has two deliberately separate native build paths:

- A repository checkout contains `native/` and always compiles the NIF from
  source through Rustler.
- A Hex package omits `native/`. Its extra compiler downloads the matching
  platform archive, verifies the SHA-256 pinned in
  `checksum-Elixir.SmolNet.Native.exs`, validates the archive allowlist, and
  lets `rustler_precompiled` extract and load it.

The package supports `x86_64-unknown-linux-gnu`,
`aarch64-unknown-linux-gnu`, and `aarch64-apple-darwin` with NIF ABI 2.15.
Linux assets are built and tested on native Ubuntu 22.04 runners, making glibc
2.35 the supported floor. Their dependency allowlist is `libc.so.6`,
`libgcc_s.so.1`, `libm.so.6`, `libdl.so.2`, `libpthread.so.0`, and
`librt.so.1`, plus the architecture's glibc dynamic loader. The Apple Silicon
asset is built with a macOS 14 deployment target on a native `macos-14` runner
and may depend only on the system `libSystem` and `libiconv` libraries.

## Release runbook: Publisho, native assets, then Hex

This order is mandatory. Publisho establishes the version and tag, the tag
builds immutable inputs for the checksum manifest, and that manifest must be
inside the Hex package before it is published.

Before the first release, add a repository Actions secret named `HEX_API_KEY`.
Use a dedicated Hex API key with API write permission. The `smolnet` package
cannot be selected as a key scope until its first version exists, so the
initial release uses a short-lived bootstrap key with `API: Write` and no
package selection. After the first publication, generate a replacement key
restricted to `smolnet`, update the repository secret, and revoke the bootstrap
key.

### 1. Prepare and validate `main`

Land all release changes, including `RELEASE.md`, on `main`. From a clean,
up-to-date `main` checkout, run:

```console
git pull --ff-only
mix precommit
mix publisho <patch|minor|major|stable> --dry-run
```

The NIF workflow publishes stable bare-semver tags only. Do not use Publisho's
`alpha`, `beta`, or `rc` levels with this workflow.

### 2. Create the release commit and tag

Run the same stable level without `--dry-run`:

```console
mix publisho <patch|minor|major|stable>
```

Publisho updates the `@version` in `mix.exs`, moves `RELEASE.md` into
`CHANGELOG.md`, clears the next-release notes, creates the `Version <version>`
commit, and creates an annotated bare-semver tag. It does not push either one.
Inspect them before continuing:

```console
git show --stat --oneline HEAD
git tag --points-at HEAD
```

### 3. Push the commit and tag

Push the version commit first, then its tag:

```console
git push origin main
git push origin <version>
```

The tag starts `.github/workflows/release-nif.yml`. It creates a draft GitHub
release and builds the GNU/Linux x86_64, GNU/Linux AArch64, and Apple Silicon
macOS assets. Each asset is inspected and exercised through the integration
and package-consumer tests. A failed matrix cell leaves the release in draft;
the workflow cannot publish a partial release.

Wait for the workflow to finish. The remaining release steps are performed by
that same workflow; do not publish the package locally during a successful
run.

### 4. CI generates the checksum manifest

After publishing the complete GitHub release, the same tag workflow runs:

```console
mix rustler_precompiled.download SmolNet.Native --all --print
```

That task downloads and hashes the actual public asset bytes, then passes
`checksum-Elixir.SmolNet.Native.exs` to the final job as the
`hex-checksum-manifest` workflow artifact. It does not copy or trust the
release `.sha256` sidecars. The final job confirms that the manifest contains
one entry for each of the three target archives.

The checksum file is intentionally ignored by Git and must come from the exact
tag workflow being published.

### 5. CI validates and publishes the Hex package

The `publish-hex` job downloads the checksum artifact, builds an unpacked Hex
preview, and verifies that it contains:

- `checksum-Elixir.SmolNet.Native.exs` with all three hashes;
- the Elixir sources and release documentation; and
- no `native/` tree, `.so`, or `.dylib` files.

It then publishes the package and documentation non-interactively:

```console
mix hex.publish --yes
```

The tag workflow succeeds only after Hex publication succeeds. The retained
manifest artifact provides an audit and recovery copy of the generated release
material.

### 6. Verify a real Hex consumer

In a throwaway project on each supported platform, add the published SmolNet
version, run `mix deps.get`, and verify:

```console
mix run -e 'IO.inspect(SmolNet.Native.health())'
```

The result must be `:ok`, and the compile log should show a precompiled asset
rather than a Rust build.

Once Hex is published, changing a GitHub asset causes consumer checksum
verification to fail. Rebuilds therefore require a new version; never replace
an asset behind an already-published Hex checksum manifest.

## Recovering a failed Hex publication

If the GitHub release and checksum job succeeded but `publish-hex` failed,
correct the repository secret and rerun only the failed job. It will download
the checksum artifact from that same workflow run and rebuild the package from
the tagged commit.

If the artifact has expired, download the three assets from the public GitHub
release and regenerate the manifest from a checkout of the unchanged tag:

```console
mix rustler_precompiled.download SmolNet.Native --all --print
HEX_API_KEY=<dedicated-write-key> mix hex.publish --yes
```

Use local publication only as recovery, and only after confirming the tag,
GitHub release, and package version are identical. A version already accepted
by Hex is immutable; do not move or reuse its tag.

## Rebuilding without a release

Run the NIF workflow manually against the desired branch. Manual dispatches
store all tarballs and sidecars as 90-day workflow artifacts and never create,
modify, or publish a GitHub release:

```console
gh workflow run release-nif.yml --repo ausimian/smolnet --ref main
```

To recover a failed tag run, correct the source on a new version and repeat the
normal release flow. Do not move or reuse a published tag. A still-draft
release for the same unshipped tag may be rerun; successful matrix cells upload
with `--clobber`, and publication occurs only after the complete matrix passes.
