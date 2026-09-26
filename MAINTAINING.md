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

`native/vendor/smoltcp` is smoltcp 0.14.0 with a SmolNet patch that adds RFC
6582 partial-ACK recovery to the TCP sender (ADR 0013). The commit that added
the directory holds the crates.io package unmodified, so
`git log -p -- native/vendor/smoltcp` after that commit is the complete set of
SmolNet changes. Keep any further patch small, covered by tests in the
vendored crate, and suitable for offering upstream.

`mix precommit` runs the vendored crate's library tests. To run them alone:

```console
cargo test --manifest-path native/vendor/smoltcp/Cargo.toml --lib --target-dir native/target/vendor
```

To move to a new smoltcp release, first check whether it already recovers
from partial ACKs. If it does, delete `native/vendor/smoltcp`, restore the
registry dependency in `native/smolnet_core/Cargo.toml`, and remove the
precommit step. Otherwise, replace the directory with the new release's
crates.io package from `~/.cargo/registry/src`, omitting `.cargo-ok` and
`Cargo.lock`, and commit that on its own. Then reapply the patch, update the
pinned version, and run `cargo update -p smoltcp` in both `native/` and
`native/fuzz/`.

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
