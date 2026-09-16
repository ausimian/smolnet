# SmolNet phased implementation plan

## 1. Purpose

This plan turns the `smoltcp`/Elixir socket architecture into an incremental
implementation sequence for an Elixir library backed by a Rust NIF. It builds
from the native stack boundary outward and preserves these product priorities:

1. TCP is completed before UDP begins.
2. IPv6 is implemented and proven before IPv4 is added.
3. Nonblocking low-level primitives precede synchronous wrappers.
4. The transport-neutral socket API precedes the inet adapter and OTP backend.
5. Every phase is reviewed by a regular, non-adversarial sub-agent; defects are
   fixed, while feature suggestions are deferred.
6. Each phase is squashed locally before its first push to GitHub.

The intended local repository is a bare-plus-sibling-worktrees container:

```text
~/Code/smolnet/
├── .bare/                       # shared bare Git repository
├── .git                         # `gitdir: ./.bare` pointer file
├── main/                        # protected main branch worktree
└── phase-NN-short-name/         # one sibling worktree per active phase
```

The container root has a detached `HEAD` and is not a development checkout.
All editing, builds, tests, commits, and reviews happen inside a branch
worktree. Build caches such as `_build/`, `.elixir_ls/`, and Rust `target/` are
per-worktree and may need rebuilding when a worktree is first created.

The intended remote is:

```text
git@github.com:ausimian/smolnet.git
```

The Mix application and Hex package are named `smolnet` (`:smolnet`), while the
public Elixir namespace remains `SmolNet`. This is conventional: package and
application atoms use lowercase names, while Elixir modules use aliases. Source
files therefore continue to use snake_case paths such as `lib/smol_net.ex` and
`lib/smol_net/socket.ex`.

The intended Mix metadata is conceptually:

```elixir
def project do
  [
    app: :smolnet,
    package: [name: "smolnet"]
  ]
end
```

This does not constrain the public module name; `defmodule SmolNet` remains the
root facade.

The repository should be created under the user's `ausimian` account, not under
an organization unless the user explicitly changes that choice.

## 2. Scope and release target

The first complete release covered by this plan provides:

- independent `smoltcp` stacks over transport-neutral raw-IP links;
- raw IPv6 and IPv4 ingress/egress using `Medium::Ip`;
- stable logical socket identities independent of `smoltcp::SocketHandle`;
- bounded Rust NIF calls with no scheduler-blocking native lock acquisition;
- waiter registration, one-shot readiness, cancellation, and stale-event
  protection;
- TCP client and server sockets, including connect, send, receive, shutdown,
  close, listen, and accept;
- UDP bind, send, and receive;
- synchronous APIs implemented over the same `:nowait` primitives;
- a per-socket inet adapter implementing passive/active modes, ownership,
  framing, options, and timeouts;
- custom `gen_tcp` and `gen_udp` callback modules for supported OTP releases;
- Linux and macOS CI for the compatible Elixir/OTP combinations in Section 7.

The first release does **not** include:

- Ethernet framing, ARP, or an Ethernet device;
- CoreDeviceProxy-specific types, message shapes, reconnect logic, or framing;
- a native polling thread;
- blocking NIF operations;
- TLS, DNS, multicast, raw sockets, SCTP, or Unix-domain sockets;
- every obscure inet option; unsupported options must return a documented,
  stable error rather than being silently ignored;
- application payload queues, packet framing, or active-mode policy in Rust.

## 3. Architectural invariants

These invariants are release blockers, not implementation preferences.

### 3.1 Ownership and concurrency

- One `SmolNet.Stack` GenServer owns one native stack resource and is the only
  BEAM process allowed to invoke NIF functions for that resource.
- One serialized link process feeds ingress to each stack. Its synchronous
  handoff returns when the stack accepts a packet, and the link owns upstream
  transport backpressure.
- Calls for one stack are serialized by its mailbox. Separate stacks can make
  progress independently.
- A native mutex is defensive only and uses `try_lock`; a normal scheduler must
  never sleep while waiting for it.
- A low-level `%SmolNet.Socket{}` is a value, not a process. The higher inet
  adapter normally uses one `gen_statem` per logical OTP socket.

### 3.2 Bounded work

- Every NIF invocation has explicit limits for input packets, output packets,
  copied bytes, readiness events, and protocol polling work.
- Ingress processes one complete raw IP packet per accepted feeder call. At
  most one packet is in native processing and one later feeder call is waiting.
- The NIF never loops until a socket becomes ready and never owns an arbitrary
  unsent application payload.
- If bounded work remains, it is represented by another BEAM message,
  continuation, or readiness notification.

### 3.3 Readiness and operation state

- The native layer atomically tries an operation and installs its waiter before
  releasing exclusive stack access.
- Each socket has at most one read-direction waiter and one write-direction
  waiter. A competing registration receives a deterministic `:busy`-class
  error.
- Wakers only set/coalesce native readiness. BEAM messages are sent in the safe
  NIF epilogue after the current stack mutation.
- Readiness is a one-shot retry hint, never proof that the operation will now
  complete.
- Select references and socket generations make duplicate, cancelled, and late
  notifications harmless.
- Elixir owns operation deadlines, partial receives, and unsent remainders.

### 3.4 Stack-driving side effects

- Every operation that can advance `smoltcp` returns bounded egress packets and
  the next `poll_at` deadline, not only ingress calls.
- `SmolNet.Stack` forwards every returned packet and replaces its timer using a
  monotonically increasing timer generation.
- Retransmission and protocol timers progress without inbound traffic.

### 3.5 Protocol ordering

- IPv4 support must not be slipped into an earlier IPv6 phase as incidental
  scope.
- UDP implementation begins only after the complete TCP acceptance gate passes
  on IPv6 and IPv4.
- Shared infrastructure may be protocol-neutral, but each phase only exposes
  the protocols explicitly listed in its scope.

### 3.6 Supervision and runtime lifecycle

The application root is one empty `DynamicSupervisor`, locally registered as
`SmolNet.Supervisor`. `SmolNet.Application.start/2` starts and returns the
standard `DynamicSupervisor` directly; there is no `SmolNet.Supervisor` module
and an otherwise empty wrapper supervisor adds no value.

Each call that creates a stack starts one temporary per-stack bundle supervisor
under the root:

```text
SmolNet.Supervisor                       # name of root DynamicSupervisor
└── SmolNet.StackSupervisor              # temporary static supervisor; module
    ├── SmolNet.Stack                    # temporary significant GenServer
    └── DynamicSupervisor                # anonymous, significant; id :inet_backends
        ├── SmolNet.InetBackend.Tcp       # temporary gen_statem
        ├── SmolNet.InetBackend.Udp       # temporary gen_statem
        └── ...
```

`SmolNet.StackSupervisor` is the only supervisor backed by a project module. It
uses `strategy: :one_for_one` and
`auto_shutdown: :any_significant`. Both structural children are marked
`significant: true` and `restart: :temporary`:

- if the stack exits for any reason, the bundle shuts down the inet supervisor
  and every adapter below it;
- if the inet supervisor itself exits, the bundle shuts down the stack rather
  than leaving a live stack that can no longer own OTP sockets;
- an individual inet adapter exit affects only that logical socket and is never
  restarted;
- after the bundle exits, the root dynamic supervisor does not restart it.

“All processes are temporary” applies to every runtime stack bundle, stack,
inet supervisor, and inet adapter. The application-root dynamic supervisor is
the application master child itself, so it has no parent child specification or
restart value; it lives for the application lifetime.

The per-stack supervisor is itself added to the root with
`restart: :temporary`. This is required as well as desirable: OTP warns against
making an automatically shutting-down supervisor a permanent child because it
would be restarted immediately. Child supervisors use `shutdown: :infinity` so
their descendants are not orphaned by a supervisor shutdown timeout.

Start the stack child before the inet dynamic supervisor. Normal/manual bundle
shutdown then occurs in reverse start order: inet adapters terminate while the
stack is still available for bounded close operations, followed by the stack.
If the stack has already failed, automatic bundle teardown simply terminates
the adapters without trying to resurrect or synchronously drain the stack.

Stack creation returns an opaque `%SmolNet.Stack.Ref{}` containing the bundle,
stack-server, and anonymous inet-backend DynamicSupervisor PIDs resolved once
from the static supervisor.
Public callers must not discover children repeatedly or depend on their raw
child IDs. Low-level `%SmolNet.Socket{}` values retain the stack server identity
needed for serialized operations; OTP backend creation uses the `inet_backends`
supervisor identity from the stack reference.

Supervision owns cleanup, but monitors still provide operation semantics:

- every inet adapter monitors its stack server and exits with a stable
  stack-down reason if it observes `:DOWN` before supervisor teardown reaches
  it;
- a low-level synchronous operation monitors the stack while waiting for a
  select notification, so abrupt stack loss returns `{:error, :closed}` (or the
  finalized equivalent) instead of waiting until its deadline;
- monitor handling is idempotent with supervisor shutdown and must never attempt
  to restart a process or recreate a native socket.

No child performs blocking or unbounded work in `init/1`. Initial callbacks
only validate cheap local arguments, install minimal state/monitors, and return.
Native resource creation, stack calls, and other bounded setup happen in
`handle_continue/2`. The public start function waits for an explicit readiness
acknowledgement outside supervisor initialization; initialization failure stops
the complete bundle before returning an error. Expensive setup must be split
into bounded continuations or an appropriately scheduled NIF, never hidden in
`init/1`.

This design uses OTP's “work unit of cooperating children” semantics directly.
The official supervisor documentation defines `auto_shutdown: :any_significant`
for this purpose and requires significant children to be `:transient` or
`:temporary`: [OTP supervisor automatic shutdown](https://www.erlang.org/doc/system/sup_princ.html#automatic-shutdown).

## 4. Proposed source layout

```text
.github/
  workflows/
    ci.yml
.credo.exs
.formatter.exs
config/
  config.exs
lib/
  smol_net.ex
  smol_net/
    application.ex
    native.ex                    # sole `use Rustler` module and NIF stubs
    stack.ex
    stack_supervisor.ex          # only project-defined Supervisor module
    stack/
      ref.ex                     # opaque stack runtime reference
    socket.ex
    inet_backend/
      tcp.ex                     # gen_tcp backend and TCP inet gen_statem
      udp.ex                     # gen_udp backend and UDP inet gen_statem
      options.ex
      packet.ex
native/
  Cargo.toml                     # Cargo workspace manifest
  Cargo.lock                     # workspace lockfile
  smolnet_nif/
    Cargo.toml
    src/
      lib.rs
      stack.rs
      device.rs
      socket_table.rs
      waiter.rs
      tcp.rs
      udp.rs
test/
  support/
    raw_ip_link.ex
    peer_stack.ex
  smol_net/
  integration/
  property/
CHANGELOG.md
LICENSE
README.md
mix.exs
mix.lock
rust-toolchain.toml
```

`RELEASE.md` is intentionally absent from the initial layout. It is introduced
only after the first release has been published, when it becomes the working
notes file for the next release cycle.

Test-only modules must not leak transport-specific behavior into production
modules. A deterministic in-memory raw-IP link should be the default integration
harness; OS TUN/TAP tests may be optional because they require privileges and
behave differently across CI runners.

## 5. Phase delivery workflow

The following gate applies to **every phase**, including documentation and
hardening phases.

### 5.1 Start

1. Update the `~/Code/smolnet/main` worktree, then check GitHub's open issues
   for work that matches the phase or any newly discovered defect.
2. Surface a matching issue before implementation and either use it or record
   why the phase remains separate.
3. From the repository container, create a sibling worktree and branch named
   `phase-NN-short-name` from the latest protected `main`, for example with
   `git worktree add phase-NN-short-name -b phase-NN-short-name refs/heads/main`.
   Never commit directly in the `main` worktree.
4. Copy the phase's tasks and acceptance criteria into its GitHub issue or PR
   checklist. Any scope change requires an explicit plan update.

### 5.2 Implement and verify locally

1. Work in small local commits while the phase is in progress.
2. Add tests with each behavioral change, including negative and race cases.
3. Update public docs, types/specs, architecture decision records, and
   `CHANGELOG.md` in the same phase as the behavior. After the first release is
   published, also update `RELEASE.md` for subsequent-release work.
4. Run targeted tests during development, then run the repository's complete
   `mix precommit` alias. The alias must cover compilation with warnings as
   errors, unused dependency checking, formatting, strict Credo, Dialyzer,
   ExDoc warnings, ExUnit, and the required Rust formatting/lint/test checks.

### 5.3 Regular sub-agent review

1. Give a fresh sub-agent the phase goal, architecture invariants, base ref,
   complete branch diff, and acceptance criteria.
2. Request a **regular, non-adversarial code review**. The reviewer should check
   correctness, races, resource lifetime, error paths, bounds, tests, and scope.
3. Classify each finding:
   - **Bug/regression/missing required test:** fix it in this phase.
   - **Required acceptance criterion missing:** finish it in this phase.
   - **New feature, enhancement, or speculative refactor:** do not implement it;
     record it in a clearly labelled follow-up list or issue if useful.
   - **Incorrect finding:** respond with evidence and make no code change.
4. Re-run focused tests and `mix precommit` after fixes.
5. Ask the reviewer to re-check corrected defects. The gate closes only when no
   unresolved correctness finding remains. A review request must never be used
   to expand phase scope.

### 5.4 Squash, push, and merge

1. Before the branch has ever been pushed, squash its local implementation and
   review-fix commits into one coherent Conventional Commit.
2. Preserve required AI co-author trailers in the squashed commit and keep the
   subject at 72 characters or fewer.
3. Run `mix precommit` against the exact squashed tree.
4. Push the single-commit branch to GitHub, open a PR, and require the complete
   CI matrix. Do not push WIP history.
5. Merge only after CI is green and required review findings are resolved. Use
   a merge method that keeps one logical phase commit on `main` (normally
   squash-merge if GitHub policy may add PR metadata).
6. Update the `main` worktree, remove the merged phase worktree with
   `git worktree remove`, and delete its local branch only after confirming the
   merge. Never remove a worktree merely to discard uncommitted changes.

If a remote correction is unavoidable after the first push, make the fix,
re-squash, re-run the full gate, and use `--force-with-lease`; never use an
unqualified force push.

## 6. Cross-cutting test strategy

### 6.1 Test layers

- **Rust unit tests:** ID/generation allocation, table lifecycle, waiter slots,
  ready-queue coalescing, packet queue bounds, address conversion, and error
  mapping.
- **Elixir unit tests:** option parsing, select-info validation, deadlines,
  packet framing, active counters, list/binary conversion, and public delegates.
- **NIF boundary tests:** invalid resource, bad ID/generation, wrong socket kind,
  lock contention, oversized input, panic containment, and result envelope.
- **Deterministic integration tests:** two in-memory stacks joined by a
  controllable raw-IP link with packet drop, duplication, reordering, delay,
  and clock advancement.
- **Race tests:** readiness versus registration/cancel/close, stale timer
  messages, owner death, stack death, link death, and recycled internal handles.
- **Property/state-machine tests:** legal operation sequences preserve table,
  waiter, and ownership invariants; malformed packets and options never crash
  the VM.
- **Stress/fairness tests:** many sockets on one stack, several independent
  stacks, large sends, ingress bursts, and continuously readable active sockets.

### 6.2 Required observability

Test-only instrumentation should make these assertions possible without
depending on timing luck:

- maximum bytes and packets processed by one NIF call;
- current native socket and waiter counts;
- ready entries coalesced and notifications delivered/dropped;
- current timer generation and scheduled native deadline;
- socket generation and lifecycle state;
- stack mailbox pressure and ingress rejection counts;
- output packet count per operation.

Instrumentation must be disabled or cheap in production and must not expose raw
native handles as public identity.

### 6.3 Native artifact size and GNU/Linux portability

Treat NIF size as a measured product property. The first release supports
GNU-libc Linux and deliberately defers musl. Its prebuilt baseline is:

| Architecture | GNU-libc artifact |
|---|---:|
| `x86_64` | required |
| `aarch64` | required |

Other GNU-libc Linux architectures may compile from source when Rust, Rustler,
and OTP support the target. They become prebuilt targets only after native CI
coverage exists. Alpine and other musl systems are explicitly unsupported in
the first release rather than receiving an untested or ABI-mismatched artifact.

The size investigation must:

- keep one NIF crate/shared library so Rust and Rustler runtime code is not
  duplicated across several `.so` files;
- disable `smoltcp` default features and enable only `std`/`alloc`, `medium-ip`,
  the current IP family, and the current socket protocols. Do not enable
  `phy-raw_socket` or `phy-tuntap_interface`, which pull in `smoltcp`'s direct
  `libc` feature;
- compare `opt-level = "z"`, `"s"`, and `3` with LTO, one codegen unit, and
  symbol stripping, recording both stripped and compressed sizes and any
  latency/throughput cost;
- retain unwinding (`panic = "unwind"`). `panic = "abort"` can turn a Rust panic
  into a whole-VM crash and is not an acceptable size optimization;
- use `cargo bloat` or an equivalent symbol report to identify actual size
  owners before accepting complexity for marginal savings;
- inspect every release artifact's dynamic dependencies and required symbol
  versions (`readelf`/`objdump`/`ldd` or equivalents), saving the report in CI;
- build GNU artifacts against Rust's supported old baseline (currently glibc
  2.17 for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`) and test
  them on old-baseline and current distributions;
- retain a documented GNU-libc source-build fallback for architectures without
  a precompiled artifact.

Musl and a libc-free `no_std` NIF are deferred. The latter would require
replacing ordinary Rustler assumptions plus allocator, panic, and
dynamic-library support; that effort is unlikely to make the first-release NIF
smaller enough to justify a separate native runtime.

References: [Rust linkage and C runtime modes](https://doc.rust-lang.org/reference/linkage.html),
[Rust Linux target baselines](https://doc.rust-lang.org/rustc/platform-support.html),
and [`smoltcp` feature flags](https://docs.rs/crate/smoltcp/latest/features).

### 6.4 Project metadata and static-analysis conventions

Borrow the reusable quality conventions from the CAL Elixir build setup without
making this a CAL project. This is a public GitHub/Hex library, so it must not
contain a CyberAssessmentLabs Hex organization, GitLab build components,
private-Hex credentials, CAL webhooks, or organization-specific release jobs.

`mix.exs` should include:

- `@version` as the single version source and
  `@source_url "https://github.com/ausimian/smolnet"`;
- `version: System.get_env("VERSION_OVERRIDE", @version)`;
- a public `package/0` with the selected license, files required to build the
  Rust NIF from source (`lib`, `native`, and relevant root metadata), and a
  GitHub link, with no `organization:` entry;
- `docs: [source_ref: @version, source_url: @source_url]`;
- `test_coverage: [tool: ExCoveralls]` plus preferred test environments for
  ExCoveralls tasks;
- Dialyzer configuration with no blanket ignore file. Add a narrowly justified
  ignore only after demonstrating a third-party false positive;
- a `cli/0` callback with `preferred_envs: [precommit: :test, ...]`.

Initial development dependencies are:

```elixir
{:credo, "~> 1.7", only: [:dev, :test], runtime: false},
{:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
{:ex_doc, ">= 0.0.0", only: [:dev, :test], runtime: false},
{:excoveralls, "~> 0.18", only: :test},
{:publisho, "~> 1.0", only: :dev, runtime: false}
```

`ex_doc` is available in `:test` as well as `:dev` because the authoritative
precommit alias runs in `MIX_ENV=test` and builds documentation with warnings as
errors. Version requirements should be resolved to current compatible releases
during setup and committed in `mix.lock`.

The project exposes one complete, non-mutating local gate:

```elixir
precommit: [
  "compile --warnings-as-errors",
  "deps.unlock --check-unused",
  "format --check-formatted",
  "credo --strict",
  "dialyzer",
  "docs --warnings-as-errors",
  "cmd cargo fmt --manifest-path native/Cargo.toml --all -- --check",
  "cmd cargo clippy --manifest-path native/Cargo.toml --workspace --all-targets -- -D warnings",
  "cmd cargo test --manifest-path native/Cargo.toml --workspace",
  "test"
]
```

Run `mix precommit` before every commit. Do not substitute a hand-picked subset
of checks. The alias uses `deps.unlock --check-unused` instead of the mutating
`--unused`, and `format --check-formatted` instead of rewriting files, so a
successful validation cannot silently modify the candidate commit. Elixir 1.18
supports `--check-unused`, matching the oldest supported version.

Supporting files created during bootstrap are:

- `.credo.exs`, generated from Credo's default configuration and then reviewed;
- `.formatter.exs`, including project and dependency-provided formatters;
- `CHANGELOG.md`, containing `<!-- %% CHANGELOG_ENTRIES %% -->`;
- no `RELEASE.md` before the first successful publication; after that event,
  create it with Keep a Changelog headings and maintain it on every feature
  branch for the next release;
- `LICENSE`, after the user selects the public license;
- package README and ExDoc landing-page configuration;
- coverage configuration only when needed to set exclusions and an
  evidence-based minimum threshold.

Generate documentation with `mix docs --warnings-as-errors`, fail Dialyzer on
warnings, and produce Cobertura coverage in CI with
`mix coveralls.cobertura`. ExDoc documents `--warnings-as-errors`, while
Dialyxir exits non-zero for warnings unless explicitly told to ignore status:
[ExDoc task](https://hexdocs.pm/ex_doc/Mix.Tasks.Docs.html),
[Dialyxir task](https://dialyxir.hexdocs.pm/Mix.Tasks.Dialyzer.html), and
[ExCoveralls](https://excoveralls.hexdocs.pm/readme.html).

## 7. GitHub Actions CI matrix

Elixir's official compatibility table gives these supported intersections for
the requested versions:

| Elixir | OTP 29 | OTP 28 | OTP 27 |
|---|---:|---:|---:|
| 1.20 | yes | yes | yes |
| 1.19 | no | yes | yes |
| 1.18 | no | no | yes |

This produces six language/runtime pairs. Running each on Linux and macOS
produces **12 compatibility jobs**. A thirteenth canonical quality job runs the
complete `mix precommit`, coverage, and package checks once on Elixir 1.19.5 /
OTP 28.3 / Linux. Unsupported cross-products must not be generated and then
ignored with `exclude`; an explicit `include` list makes accidental expansion
obvious.

The initial workflow should be structurally equivalent to:

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  test:
    name: ${{ matrix.os }} / Elixir ${{ matrix.elixir }} / OTP ${{ matrix.otp }}
    runs-on: ${{ matrix.os }}
    timeout-minutes: 30
    strategy:
      fail-fast: false
      matrix:
        include:
          - {os: ubuntu-24.04, elixir: "1.20", otp: "29"}
          - {os: ubuntu-24.04, elixir: "1.20", otp: "28"}
          - {os: ubuntu-24.04, elixir: "1.20", otp: "27"}
          - {os: ubuntu-24.04, elixir: "1.19", otp: "28"}
          - {os: ubuntu-24.04, elixir: "1.19", otp: "27"}
          - {os: ubuntu-24.04, elixir: "1.18", otp: "27"}
          - {os: macos-15, elixir: "1.20", otp: "29"}
          - {os: macos-15, elixir: "1.20", otp: "28"}
          - {os: macos-15, elixir: "1.20", otp: "27"}
          - {os: macos-15, elixir: "1.19", otp: "28"}
          - {os: macos-15, elixir: "1.19", otp: "27"}
          - {os: macos-15, elixir: "1.18", otp: "27"}

    steps:
      - uses: actions/checkout@v4

      - uses: erlef/setup-beam@v1
        with:
          elixir-version: ${{ matrix.elixir }}
          otp-version: ${{ matrix.otp }}

      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy

      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: native

      - uses: actions/cache@v4
        with:
          path: |
            deps
            _build
          key: >-
            mix-${{ runner.os }}-${{ matrix.elixir }}-${{ matrix.otp }}-
            ${{ hashFiles('mix.lock', 'native/Cargo.lock') }}

      - run: mix local.hex --force
      - run: mix local.rebar --force
      - run: mix deps.get
      - run: mix compile --warnings-as-errors
      - run: mix test

  quality:
    name: precommit / coverage / package
    runs-on: ubuntu-24.04
    timeout-minutes: 45

    steps:
      - uses: actions/checkout@v4

      - uses: erlef/setup-beam@v1
        with:
          elixir-version: "1.19.5"
          otp-version: "28.3"

      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy

      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: native

      - uses: actions/cache@v4
        with:
          path: |
            deps
            _build
            priv/plts
          key: >-
            quality-${{ runner.os }}-1.19.5-28.3-
            ${{ hashFiles('mix.lock', 'native/Cargo.lock') }}

      - run: mix local.hex --force
      - run: mix local.rebar --force
      - run: mix deps.get
      - run: mix precommit
      - run: mix coveralls.cobertura
      - run: mix hex.build
```

Implementation notes:

- Pin action revisions to immutable commit SHAs before the first public push;
  the tags above make the intended actions readable during planning.
- Resolve and record exact patch releases in a maintenance file if fully
  reproducible builds are required. The compatibility rows remain the same.
- The development toolchain follows the organizational default (currently
  Elixir 1.19.5 and OTP 28.3), while the library constraint remains compatible
  with Elixir 1.18 through 1.20.
- Keep the 12 compatibility jobs focused on cross-version native compilation
  and ExUnit behavior. The canonical quality job owns Credo, Dialyzer, ExDoc,
  Rust formatting/Clippy/unit tests, coverage output, and the Hex package dry
  build through the complete precommit contract and its adjacent CI checks.
- Cache `priv/plts` only after configuring Dialyxir to keep project PLTs there;
  invalidate the cache with the dependency locks and toolchain versions.
- Add a separate, non-matrix security/supply-chain job later if needed; do not
  pretend it substitutes for the 12 compile-and-test jobs.
- If a GitHub macOS image is retired, replace it deliberately with a supported
  pinned macOS image. Do not drop macOS coverage or unsupportedly change the
  language/runtime pairs.

Compatibility source: [Elixir compatibility and deprecations](https://elixir.hexdocs.pm/main/compatibility-and-deprecations.html).
CI setup reference: [`erlef/setup-beam`](https://github.com/erlef/setup-beam).

## 8. Phase summary and dependency order

| Phase | Outcome | Protocol/family exposed |
|---:|---|---|
| 0 | Contracts, repository, NIF skeleton, CI | none |
| 1 | Native stack resource and bounded ABI | none |
| 2 | Raw-IP link, timers, independent stacks | IPv6 packets only |
| 3 | Stable sockets, waiters, readiness, cancellation | protocol-neutral internals |
| 4 | Low-level TCP connection lifecycle | IPv6 TCP client |
| 5 | Low-level stream I/O and synchronous wrappers | IPv6 TCP client |
| 6 | inet adapter and `gen_tcp` client backend | IPv6 TCP client |
| 7 | Listener/accept pooling and server behavior | complete IPv6 TCP |
| 8 | IPv4 and dual-family TCP parity | complete TCP |
| 9 | UDP stack, adapter, and backend | IPv6 UDP |
| 10 | IPv4 UDP parity | complete TCP and UDP |
| 11 | Hardening, documentation, and release | release candidate |

Phases are sequential. A later phase may be designed in advance, but its public
behavior must not land before all earlier acceptance gates pass.

## 9. Detailed phases

### Phase 0 — Contract discovery, repository, and build skeleton

**Goal:** establish a reproducible project and resolve external contracts before
production logic is built.

#### Work

- Confirm repository visibility, license, and initial semantic version. The Mix
  application and Hex package name are fixed as `smolnet`/`:smolnet`; the root
  Elixir namespace is fixed as `SmolNet`.
- Create `~/Code/smolnet` initially as a standard Git repository on a
  non-protected bootstrap branch. Create a minimal seed commit, rename that
  local branch to `main` only after the commit exists, and leave the working
  tree clean. This gives the conversion a single local branch and avoids an
  obsolete bootstrap worktree.
- Before adding project files, apply the `convert-to-worktree` procedure. Its
  pre-flight must confirm that `.git` is a directory and `git status
  --porcelain` is empty. Convert `.git` to `.bare`, set `core.bare=true`, create
  the `.git` pointer, create a `main/` sibling worktree, and remove the old root
  checkout files only after explicitly listing the deletion set.
- Verify the conversion with `git worktree list`. The container root's detached
  `HEAD` is intentional; no project work may occur there. Create
  `phase-00-project-setup/` from `main` as a sibling branch worktree and
  scaffold the OTP application and Rustler NIF crate inside it.
- Make `SmolNet.Application.start/2` return a standard, empty
  `DynamicSupervisor` registered as `SmolNet.Supervisor`. Do not create a
  `SmolNet.Supervisor` module, registry, or wrapper supervisor without a
  demonstrated need.
- Use `@version` and `@source_url` in `mix.exs`; use `@version` as the version
  source of truth, support `VERSION_OVERRIDE`, and point source metadata to
  `https://github.com/ausimian/smolnet`.
- Set the Elixir constraint to cover 1.18, 1.19, and 1.20 without admitting an
  untested future major release.
- Pin Rustler, `smoltcp`, Rust edition/MSRV, and the Rust toolchain after checking
  their supported combinations. Record the decision and rejected alternatives
  in an ADR.
- Enable only the necessary initial `smoltcp` features: `Medium::Ip`, IPv6, and
  TCP-related support. Do not enable IPv4 or UDP merely because a default
  feature set includes them.
- Add `SmolNet.Native` as the sole NIF-loading module. Use Rustler's current
  `native/Cargo.toml` workspace layout, with `smolnet_nif` as one member and a
  workspace-level `native/Cargo.lock`.
- Build a trivial NIF health call and ensure native loading failures produce a
  clear Elixir error.
- Build the health NIF with a small-artifact profile matrix (`z`, `s`, and `3`),
  LTO, one codegen unit, and symbol stripping. Record unstripped, stripped, and
  compressed sizes, compile times, exported symbols, and dynamic dependencies.
- Prove GNU-libc `cdylib` builds for `x86_64` and `aarch64`, load each in BEAM
  containers/runners at the oldest supported glibc baseline and on a current
  distribution, and include a GNU-libc source-build fallback.
- Record musl and libc-free/`no_std` builds as explicitly deferred. Spend the
  phase's optimization budget on dependency features, code generation, LTO,
  stripping, and symbol-level size analysis instead.
- Define public type vocabulary, error atoms, monotonic time units, address
  representation, select-info shape, and the native result envelope.
- Inspect OTP 27, 28, and 29 source for the exact `gen_tcp`/`gen_udp` dispatch
  contract, including `{tcp_module, Module}` and `{udp_module, Module}` option
  handling, callback arities, socket term expectations, and use of `:inet`
  helpers. Capture version differences as executable contract fixtures.
- Create `mix precommit`, the 12-job CI matrix, branch protection expectations,
  public Hex `package/0`, ExDoc configuration, ExCoveralls configuration,
  `CHANGELOG.md`, `.credo.exs`, formatting, lint, Dialyzer, and dependency audit
  setup. Include Credo, Dialyxir, ExDoc, ExCoveralls, and Publisho development
  dependencies without CAL organization metadata. Do not create `RELEASE.md`
  during bootstrap.
- Before creating the remote, complete the phase review and squash the Phase 0
  branch. Then create `ausimian/smolnet` with the bare repository as `origin`,
  push the minimal seed `main`, protect it, push the single Phase 0 branch
  commit, and merge it only through a green reviewed PR.

The OTP callback mechanism is source-level integration, not a stable public
behaviour declaration. Treat source verification on each supported OTP major as
mandatory. The [`gen_tcp` source](https://github.com/erlang/otp/blob/master/lib/kernel/src/gen_tcp.erl)
documents callback dispatch through `{tcp_module, module()}` but does not remove
the need for version-specific contract tests.

#### Verification and acceptance criteria

- A clean clone can fetch dependencies, compile the NIF, load it, and run one
  health test on the default development toolchain.
- `~/Code/smolnet` contains only `.bare`, the `.git` pointer, and registered
  sibling worktrees; `git worktree list` reports `main` and the active Phase 0
  worktree at the expected paths. The root is not used as a checkout.
- Application startup creates exactly the named, empty root DynamicSupervisor
  and performs no native allocation, stack creation, network work, or blocking
  call in `init/1`.
- All 12 compatibility jobs compile and test the bootstrap project on actual
  GitHub-hosted Linux and macOS runners, and the canonical quality job passes.
- The dependency ADR records exact versions, MSRV, enabled features, ownership
  of upgrades, and the reason each dependency is accepted.
- The artifact ADR records the measured size matrix, selected release profile,
  exported/dynamic symbols, GNU-libc architecture support matrix, source
  fallback, and explicit musl deferral.
- Minimal GNU-libc health artifacts load successfully on `x86_64` and `aarch64`
  BEAM environments at both the oldest supported and current glibc baselines.
- Contract fixtures enumerate required TCP and UDP callback functions for OTP
  27/28/29 and fail clearly if a supported OTP release changes them.
- `mix precommit` is the single complete local gate and succeeds from a clean
  checkout. It runs warnings-as-errors compilation, unused-lock checking,
  formatting verification, strict Credo, Dialyzer, ExDoc warnings-as-errors,
  Rust format/Clippy/tests, and ExUnit.
- `mix docs --warnings-as-errors` builds complete public documentation with no
  warnings or broken internal links, and package metadata includes every source
  file needed to compile the NIF from a Hex tarball.
- `mix coveralls.cobertura` produces a valid coverage report; any minimum
  threshold is based on the bootstrap baseline rather than chosen arbitrarily.
- `mix hex.build` succeeds locally as a dry packaging check and its contents
  contain no CAL organization metadata, private credentials, build output,
  caches, or unrelated development files.
- No TCP, UDP, IPv4, or link behavior is accidentally exposed.
- `CHANGELOG.md` records the project skeleton under an `### Added` heading, and
  `RELEASE.md` does not exist before the first release is published.

#### Regular review focus

The sub-agent reviews reproducibility, dependency/features minimization, CI
pairings, OTP contract evidence, NIF size evidence, GNU-libc compatibility
claims, public Hex metadata, ExDoc, static-analysis coverage, the complete
precommit alias, project conventions, and accidental scope. Fix
build/configuration and portability defects; defer requests for musl,
additional protocols or architectures, and unrelated release automation.

---

### Phase 1 — Native stack resource and bounded ABI

**Goal:** create the safe native ownership boundary without packet transport or
public sockets.

#### Work

- Implement the Rust resource containing `Interface`, `SocketSet`, `BeamDevice`,
  logical socket table placeholder, ready queue, counters, and lifecycle state.
- Keep initialization and teardown exception-safe; partial initialization must
  not leak native allocations.
- Add defensive `try_lock` acquisition and a deterministic invariant-violation
  error. Never use blocking lock acquisition from a normal-scheduler NIF.
- Define configurable hard bounds for bytes copied, emitted packets, ready
  events, and maintenance work in one invocation.
- Implement the common NIF result envelope containing operation result, bounded
  output, next `poll_at`, and a “more bounded work remains” indication if needed.
- Add monotonic time conversion with checked overflow/underflow behavior.
- Add Rust panic containment consistent with Rustler conventions; a bad input
  must return an error and a native panic must not unwind across the NIF ABI.
- Introduce `SmolNet.Application` and the minimal `SmolNet.Stack` lifecycle,
  while leaving ingress and sockets unavailable.
- Implement `SmolNet.StackSupervisor` as the only project-defined Supervisor
  module and as a temporary child of the root dynamic supervisor. Give it a
  temporary significant `SmolNet.Stack` worker followed by an anonymous,
  temporary significant `DynamicSupervisor` child with ID `:inet_backends`,
  using `auto_shutdown: :any_significant`.
- Set supervisor-child shutdown to `:infinity`, keep stack-worker cleanup
  bounded, and make every future inet adapter child specification temporary.
- Return an opaque `%SmolNet.Stack.Ref{}` after resolving the bundle, stack, and
  `:inet_backends` DynamicSupervisor PIDs once and after receiving an explicit
  stack-ready reply.
- Keep every `init/1` callback cheap and nonblocking. Perform native stack
  allocation in `handle_continue/2`; if it fails, terminate the bundle and
  return a stable creation error to the original caller.
- Add stack monitors to the waiting/start path so a crash cannot leave
  `start_stack` or a future low-level select wait blocked.

#### Verification and acceptance criteria

- Repeated create/destroy cycles release all native resources under sanitizing
  or leak-detection tests available to the platform.
- A forced lock-contention test returns immediately with the chosen invariant
  error; it does not stall a BEAM scheduler.
- Boundary and malformed-input tests cannot crash the VM.
- Time conversions cover zero, large values, expired deadlines, and overflow.
- Instrumented tests prove every ABI collection respects its configured bound.
- Two empty native stacks can be created and destroyed independently.
- Stack termination for `:normal`, `:shutdown`, and abnormal reasons tears down
  its instance supervisor, inet supervisor, and all temporary adapter fixtures;
  none is restarted or orphaned.
- Unexpected anonymous inet-backend DynamicSupervisor termination tears down
  the stack and bundle.
- Individual temporary adapter termination does not terminate its siblings or
  restart the adapter.
- Manual bundle shutdown stops adapters before the still-live stack, then stops
  the stack; forced stack loss still removes all adapters.
- Tests prove application, bundle, stack, anonymous DynamicSupervisor, and
  adapter `init/1` callbacks do not invoke the NIF, synchronously call another
  child, or perform unbounded work.
- A failed `handle_continue/2` native initialization returns an error and leaves
  no bundle, worker, native resource, or dynamic-supervisor child behind.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews ABI safety, resource destruction, mutex behavior, panic
paths, integer conversions, explicit bounds, significant-child semantics,
shutdown order, temporary child specs, initialization handshakes, and orphan
risks. Fix correctness and lifecycle leaks; defer packet processing and socket
functionality.

---

### Phase 2 — IPv6 raw-IP link, egress, and timer driving

**Goal:** prove one and many IPv6 `Medium::Ip` stacks can advance through the
transport-neutral BEAM boundary.

#### Work

- Implement `BeamDevice` receive/transmit tokens for complete raw IP packets.
- Add `SmolNet.Stack.start_link/1` options for egress `{pid, link_ref}`, MTU,
  IPv6 addresses, routes, limits, and link-down policy.
- Implement a synchronous single-feeder `SmolNet.Stack.ingress/2` handoff.
  Validate type, minimum header, IPv6 version nibble, declared payload length,
  and MTU in the stack owner before native work.
- Reply when the stack accepts the packet, then process one packet per bounded
  native ingress continuation before accepting another stack message. The
  serialized feeder and its one outstanding call replace an ingress queue.
- Emit one `{:smol_stack, link_ref, :egress, packet}` message per returned
  packet. Treat delivery as emission, not transport acknowledgement.
- Implement native `poll_at`, BEAM `Process.send_after`, timer generations,
  stale timer rejection, and bounded timer polling.
- Monitor the egress PID and implement the three designed policies (`:stop`,
  `:mark_down`, and `{:notify, pid}` or their final documented equivalents).
- Build the deterministic in-memory IPv6 peer/link harness with controllable
  clock and packet faults.
- Reject IPv4 ingress explicitly in this phase.

#### Verification and acceptance criteria

- An IPv6 packet accepted through public ingress is processed by the stack
  process; the link process never invokes a NIF directly.
- Each native output packet produces exactly one correctly tagged egress
  message with the configured opaque `link_ref`.
- Invalid, truncated, oversized, and IPv4 packets are rejected without native
  mutation.
- Single-feeder backpressure permits at most one packet in native processing
  and one subsequent ingress call waiting in the stack mailbox.
- Retransmission/timer activity advances under a deterministic clock with no
  inbound traffic; stale timer generations have no effect.
- Egress recipient termination exercises every configured link-down policy.
- Two stacks on separate links exchange packets or advance timers without
  blocking one another.
- Substituting a second test link implementation requires no change to native,
  stack, or future socket modules.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews the single-feeder bound, raw-packet validation, timer
races, egress attribution, link monitoring, fairness, and transport leakage.
Fix lost or duplicated packet/timer behavior; defer sockets and IPv4.

---

### Phase 3 — Stable socket identity, waiters, readiness, and cancellation

**Goal:** implement the protocol-neutral lifecycle and race machinery required
before TCP operations.

#### Work

- Add monotonically unique `SocketId` allocation and a generation strategy that
  remains safe if any ID can ever be reused.
- Implement `SocketEntry` validation by ID, generation, kind, and lifecycle.
- Add one read waiter slot and one write waiter slot per socket, holding only
  PID, select reference, operation class, and stable socket identity.
- Define the final `%SmolNet.Socket{stack, id, generation}` value and the
  `{:select_info, operation, reference}` tagged tuple used by `:socket`.
- Implement atomic try-and-arm support inside the native exclusive section.
- Connect `smoltcp` one-shot wakers to a coalescing ready queue. Wakers must not
  allocate BEAM terms or send messages directly.
- Drain bounded readiness in the NIF epilogue, consume matching waiter slots,
  and send stable select/abort message shapes using the current environment.
- Implement deterministic `:busy` behavior for a second incompatible waiter.
- Implement exact-reference cancellation, including `:ok`, `:already_sent`,
  and `:not_found`-class race results.
- Abort both waiter directions during close and all waiters during stack
  shutdown. Make late and duplicate ready entries harmless.
- Add a test-only synthetic socket/readiness driver; do not expose it publicly.

#### Verification and acceptance criteria

- A readiness event occurring at every instrumented point around try-and-arm is
  either observed immediately or produces exactly one retry notification.
- Spurious wake leads to a safe retry and re-arm.
- Repeated readiness coalesces while one waiter is outstanding.
- A competing waiter in the same direction gets the documented error; one read
  and one write waiter may coexist without cross-talk.
- Cancel-before-ready, ready-before-cancel, and simultaneous races have stable
  outcomes and cannot notify a later operation.
- Close and stack shutdown abort pending waiters exactly once.
- Reusing an internal `smoltcp` handle cannot revive an old logical socket.
- Ready-queue overflow follows a safe, documented policy without leaving a
  caller asleep forever.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews lost-wakeup windows, stale identity, reference matching,
queue bounds, cancellation linearization, close races, and direct message sends
from wakers. Fix every race or liveness defect; defer real TCP operations.

---

### Phase 4 — IPv6 low-level TCP open, bind, and connect

**Goal:** establish outbound IPv6 TCP connections through the public low-level
API.

#### Work

- Add bounded native TCP buffers and insert/remove TCP sockets in `SocketSet`.
- Implement `SmolNet.open(:inet6, :stream, :tcp, stack: stack)` through the
  facade, socket module, stack owner, and NIF.
- Implement IPv6 bind and ephemeral-port allocation with deterministic
  collision/exhaustion errors.
- Implement `connect(..., :nowait)` as initiate-or-finalize logic using the
  write waiter slot and the generic readiness path.
- Validate link-local scope/zone requirements and all IPv6 address/port inputs.
- Map connection states and errors to a documented low-level atom vocabulary.
- Forward SYN, ACK, RST, and timer-driven egress from every operation result.
- Implement `sockname/1`, `peername/1`, abortive close, and invalid-handle
  behavior for the supported states.
- Add a deterministic IPv6 TCP peer fixture that can accept, refuse, ignore,
  reset, and delay handshakes.

#### Verification and acceptance criteria

- Immediate and delayed IPv6 connections complete through nowait/retry.
- Refused, reset, unreachable, malformed-address, and native-timeout states map
  to stable documented errors and wake a registered caller.
- Duplicate connect attempts and illegal state transitions return deterministic
  errors without corrupting the socket table.
- `sockname` and `peername` reflect the correct lifecycle states.
- Every connect-generated packet is forwarded and every returned `poll_at`
  replaces the stack timer.
- Closing during a handshake aborts the connect waiter; late SYN/ACK cannot
  revive the socket.
- Many connecting sockets on one stack remain bounded and serialized; separate
  stacks progress independently.
- IPv4 open/connect remains explicitly unsupported.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews TCP state mapping, port allocation, output/timer handling,
connect finalization, IPv6 validation, buffer bounds, and close during connect.
Fix handshake/lifecycle defects; defer stream I/O, listening, IPv4, and UDP.

---

### Phase 5 — IPv6 TCP stream I/O and synchronous wrappers

**Goal:** provide correct bounded send/receive semantics without retaining
application operation state in Rust or `SmolNet.Stack`.

#### Work

- Implement native `tcp_send` with a per-call copy limit and available-TX
  limit. Return the unsent boundary/remainder to Elixir; never retain it in the
  socket entry.
- Implement native `tcp_recv` with requested and per-call limits. Return partial
  bytes to Elixir; never retain exact-length accumulation in Rust.
- Implement nowait `SmolNet.send/3` and `SmolNet.recv/3` using the generic write
  and read waiter slots.
- Implement synchronous finite/infinite-timeout wrappers as caller-side loops
  using one monotonic absolute deadline across all partial completions and
  spurious wakes.
- On timeout, cancel the exact select reference and safely ignore/drain any
  racing notification.
- Specify `recv(socket, 0, ...)`, EOF-after-buffered-data, reset, partial send,
  and half-close behavior.
- Implement `shutdown/2` and graceful close lifecycle, retaining native closing
  state only as long as required to drive FIN/retransmission.
- Bound the amount of immediately available work retried before yielding.

#### Verification and acceptance criteria

- Small sends/receives complete immediately; large sends require multiple
  bounded retries while the complete original payload arrives in order.
- Native state never retains the arbitrary unsent application binary.
- Exact-length receive accumulates only in the caller and uses bounded native
  reads.
- Timeout duration is one deadline, not restarted by partial progress or a
  spurious wake.
- A timeout-versus-ready race returns one stable result and cannot satisfy a
  future operation.
- EOF returns buffered data according to the chosen contract before reporting
  closed; resets and half-close match documented behavior.
- Write shutdown emits/drives FIN as required and rejects later sends without
  preventing allowed reads.
- Large sends and continuously readable sockets do not monopolize a normal
  scheduler or the stack process.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews payload ownership, partial-operation accounting, deadline
math, cancel races, EOF/reset/half-close semantics, graceful-close retention,
and scheduler fairness. Fix data loss, duplication, hangs, and unbounded loops;
defer inet options and listeners.

---

### Phase 6 — IPv6 TCP inet adapter and `gen_tcp` client backend

**Goal:** expose outbound IPv6 TCP through OTP-shaped behavior while keeping
policy in Elixir.

#### Work

- Implement `SmolNet.InetBackend.Tcp` as both the thin `gen_tcp` backend module
  and the `gen_statem` module used by each logical TCP socket process. Keep its
  public backend entry points as thin translations into per-socket process
  calls; it does not share a module or state machine with UDP.
- Give each TCP socket process an owner monitor, active mode, representation
  mode, packet mode/size, receive buffer, and independent read/write
  continuations.
- Start every TCP socket process as a `restart: :temporary` child of its stack's
  anonymous `:inet_backends` DynamicSupervisor. Its `init/1` only installs local
  state and monitors; bounded socket open/connect setup begins in
  `handle_continue/2`.
- Monitor the stack server in every adapter. Treat `:DOWN` as terminal and exit
  with a stable stack-down reason, while remaining idempotent with bundle-driven
  shutdown.
- Implement passive `recv`, send, close, shutdown, `sockname`, `peername`,
  `controlling_process`, `setopts`, and `getopts` first.
- Add `:binary`/`:list` conversion and packet modes `:raw`, `:line`, `1`, `2`,
  and `4`, including `packet_size` enforcement.
- Add `active: false | true | :once | N`; count logical framed deliveries, not
  native chunks. Bound each drain and yield through the adapter mailbox.
- Define serialization or `:busy` responses for competing operations in the
  same direction while allowing compatible opposite-direction work.
- Implement owner transfer atomically enough that data/close/error messages
  cannot be delivered to the wrong owner. Owner death closes the low-level
  socket.
- Implement the OTP 27/28/29 TCP callback contract on
  `SmolNet.InetBackend.Tcp`, including
  IPv6 client connect and socket-term translation. Version-gate only where
  executable contract fixtures prove a difference.
- Match standard `{tcp, socket, data}`, `{tcp_closed, socket}`,
  `{tcp_error, socket, reason}`, and passive-state message behavior.

#### Verification and acceptance criteria

- Passive raw binary and list clients connect, send, receive, time out, shut
  down, and close through public `gen_tcp` calls using the custom backend.
- All supported packet modes correctly span arbitrary native chunk boundaries;
  invalid/oversized frames produce the documented error.
- Active true, once, and N deliver correctly tagged logical packets, transition
  modes correctly, and remain drain-bounded under sustained input.
- Controlling-process transfer has no misdelivered data across a deterministic
  race test; owner death closes and aborts operations.
- Concurrent read/write works; competing reads or writes follow the documented
  rule without overwriting a continuation.
- Adapter timeouts do not block the stack owner or a scheduler.
- Stack failure removes every adapter under that stack without affecting other
  stack bundles; no adapter is restarted, orphaned, or left waiting on a select
  deadline.
- Killing one adapter closes only its logical socket and does not terminate the
  stack, inet supervisor, or sibling adapters.
- Contract tests pass through actual `gen_tcp` entry points on OTP 27, 28, and
  29 rather than calling the callback module directly.
- IPv4 and listening remain unsupported with explicit errors.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews the thin backend-entry-point/process boundary within
`SmolNet.InetBackend.Tcp`, `gen_statem`
transitions, framing across chunks, active drain bounds, ownership races, OTP
tuple compatibility, callback dispatch, and continuation isolation. Fix
compatibility and state-machine defects; defer listeners, IPv4, extra packet
modes, and UDP.

---

### Phase 7 — IPv6 TCP listen and accept

**Goal:** complete IPv6 TCP support with reusable listeners and bounded accept
backlogs.

#### Work

- Design the listener as an explicit abstraction over a bounded pool of
  listening `smoltcp` TCP sockets; do not pretend one connected smoltcp socket
  remains a reusable listener.
- Define listen pool size, accepted-child queue, application backlog, refill
  policy, overflow behavior, and fairness limits.
- Allocate a new stable public identity for each accepted child and replenish
  the native listening pool after a connection is promoted.
- Implement read-direction accept waiters and cancellation using the existing
  readiness mechanism.
- Implement `SmolNet.listen/2`, `SmolNet.accept/2`, and inet adapter state for a
  listener distinct from a connected stream.
- Implement the OTP TCP callback's `listen` and `accept` paths and option
  inheritance for accepted sockets.
- Preserve controlling-process, active-mode, packet, mode, and buffer semantics
  expected for the accepted socket.
- Handle listener close, stack shutdown, queued children, half-open handshakes,
  and late packets without leaks or stale events.

#### Verification and acceptance criteria

- A listener accepts sequential and concurrent IPv6 clients and remains usable
  after each accept.
- Pool replenishment is bounded, observable, and cannot starve established
  sockets on the same stack.
- Backlog saturation follows the documented policy with no unbounded queue.
- Accept timeout/cancel/close races deliver at most one outcome and do not lose
  a subsequently accepted child.
- Closing a listener aborts pending accept, handles queued children according to
  contract, and releases every pool member.
- Accepted sockets inherit supported options and use independent stable IDs.
- Public `gen_tcp.listen`/`accept` interoperability tests pass on OTP 27/28/29.
- Full IPv6 TCP client/server echo, framing, active/passive, shutdown, and reset
  tests pass under packet loss and timer-driven retransmission.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews listener pool lifecycle, backlog bounds, child promotion,
option inheritance, accept races, cleanup, and fairness. Fix leaks, dropped
accepts, stale notifications, and OTP incompatibility; defer IPv4 and UDP.

---

### Phase 8 — IPv4 and dual-family TCP parity

**Goal:** add IPv4 only after IPv6 TCP is complete, without regressing IPv6 or
mixing socket identities/readiness across families.

#### Work

- Enable the required `smoltcp` IPv4 feature and extend stack configuration,
  route validation, and raw packet admission for IPv4 headers/checksums/lengths.
- Add `:inet` address conversion, bind, ephemeral ports, connect, listen,
  accept, send/receive, names, shutdown, and close through every layer.
- Decide and document whether one stack may hold both IPv6 and IPv4 addresses
  and routes (expected: yes) and whether IPv4-mapped IPv6 addresses are
  supported (default: explicit unsupported unless required by OTP contract).
- Keep address-family selection explicit at open time. Do not silently convert
  between IPv4 and IPv6.
- Extend the deterministic link/peer harness for IPv4 and mixed-family traffic.
- Add `gen_tcp` callback dispatch for `:inet` while preserving the completed
  IPv6 path.

#### Verification and acceptance criteria

- Every IPv6 TCP acceptance test from Phases 4–7 still passes unchanged.
- Equivalent IPv4 client/server, passive/active, framing, ownership, timeout,
  cancellation, shutdown, and close tests pass.
- IPv4 header length, total length, checksum, fragmentation policy, MTU, and
  invalid-address cases are documented and tested.
- One dual-family stack can host IPv6 and IPv4 TCP sockets simultaneously with
  no readiness, routing, port, or identity cross-talk.
- Family-mismatched bind/connect calls fail deterministically.
- Separate IPv4 and IPv6 raw link traffic is emitted with correct packet version
  and link reference.
- Actual `gen_tcp` entry-point contract tests pass for both families on OTP
  27/28/29.
- This phase's acceptance establishes the **complete TCP gate**; UDP work may
  begin only after its review closes.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews IPv4 parsing and checksums, dual-family routing/ports,
family mismatches, regression of IPv6, and cross-family readiness. Fix parity or
isolation defects; defer UDP and unrelated dual-stack conveniences.

---

### Phase 9 — IPv6 UDP low-level API, inet adapter, and `gen_udp` backend

**Goal:** add IPv6 datagrams using the already proven stack and readiness
machinery.

#### Work

- Add bounded native UDP packet metadata and payload buffers to the shared
  `SocketSet`.
- Implement IPv6 UDP open, bind, `sendto(..., :nowait)`,
  `recvfrom(..., :nowait)`, names, connected-UDP behavior if required by the
  validated contract, and close.
- Preserve datagram boundaries and source/destination metadata. Public send is
  all-or-error; never expose a partially sent datagram as stream progress.
- Define oversize, zero-length, truncation, no-route, buffer-full, and checksum
  behavior.
- Reuse generic waiter, cancellation, deadline, output, and timer paths without
  creating UDP-specific readiness machinery.
- Implement `SmolNet.InetBackend.Udp` as both the distinct, thin `gen_udp`
  backend module and the `gen_statem` module used by each temporary per-socket
  process under the stack's anonymous `:inet_backends` DynamicSupervisor.
- Implement UDP message shapes, active modes, ownership, options, and drain
  bounds in the UDP socket process while sharing narrowly scoped option,
  ownership, deadline, and error helpers with TCP where semantics truly match.
- Implement the OTP 27/28/29 UDP callback contract on
  `SmolNet.InetBackend.Udp` and exercise it through real `gen_udp` entry points.

#### Verification and acceptance criteria

- IPv6 datagrams retain boundaries and peer metadata through immediate,
  nowait/retry, synchronous, passive, and active paths.
- A datagram is either accepted completely or not accepted; a retry never
  duplicates a previously accepted datagram.
- Zero-length, maximum accepted, oversized, truncated-buffer, and buffer-full
  cases match the documented public contract.
- Timeout/cancel/close races use the shared waiter rules and cannot notify a
  later operation.
- Active true, once, and N count datagrams and are drain-bounded.
- TCP and UDP coexist on one IPv6 stack under load with no socket-table,
  readiness, buffer, or error cross-talk.
- Actual `gen_udp` entry-point contract tests pass on OTP 27/28/29.
- IPv4 UDP remains explicitly unsupported.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews datagram atomicity, metadata, buffer ownership, truncation
contract, shared readiness reuse, TCP regression, and OTP message shapes. Fix
loss, duplication, cross-talk, and compatibility defects; defer IPv4 UDP and
additional datagram features.

---

### Phase 10 — IPv4 UDP parity and complete protocol matrix

**Goal:** finish the requested family/protocol combinations after IPv6 UDP is
stable.

#### Work

- Extend low-level UDP, adapter, and callback paths from Phase 9 to `:inet`.
- Add IPv4 UDP checksum/address/route/error handling without branching around
  the shared boundedness and readiness infrastructure.
- Run combined TCP/UDP and IPv6/IPv4 workloads on one stack and on several
  independent stacks.
- Audit every public function for family/kind/state validation before it touches
  native state.
- Complete the supported/unsupported inet option table for TCP and UDP.

#### Verification and acceptance criteria

- Every IPv6 UDP acceptance test passes unchanged and equivalent IPv4 tests
  pass.
- TCP IPv6, TCP IPv4, UDP IPv6, and UDP IPv4 sockets coexist on one `SocketSet`
  with correct routing and no readiness cross-talk.
- Mixed stress tests remain within native buffer, single-feeder ingress, egress
  batch, adapter drain, and mailbox thresholds.
- Wrong-protocol and wrong-family calls return deterministic errors and leave
  the target socket usable.
- Public `gen_tcp` and `gen_udp` entry-point suites pass for both address
  families on all supported OTP majors.
- A second raw-IP transport adapter passes the complete suite without changes to
  `SmolNet.Stack`, `SmolNet.Socket`, inet adapters, or native engine.
- The full 12-job CI matrix and `mix precommit` pass.

#### Regular review focus

The sub-agent reviews parity, family/protocol dispatch, combined-load bounds,
cross-talk, option tables, and regression of all earlier TCP behavior. Fix
functional or isolation defects; defer any protocol or option outside the
declared first-release scope.

---

### Phase 11 — Hardening, documentation, and release candidate

**Goal:** prove operational safety and publish a coherent first release without
adding features.

#### Work

- Run long deterministic stress/soak scenarios with loss, duplication,
  reordering, delayed ACKs, connection resets, full buffers, owner death, link
  death, stack restarts, and concurrent close/cancel.
- Run property/state-machine suites with high iteration counts and preserve
  seeds for every failure.
- Run Rust sanitizers or equivalent native checks on supported local/CI runners;
  add fuzz targets for raw packets, option conversion, and operation sequences.
- Measure maximum NIF wall time and reductions/latency impact under configured
  limits. Tighten limits or yielding where the evidence requires it.
- Repeat the Phase 0 artifact-size experiment with the complete NIF. Use symbol
  attribution to remove unused features and code, compare `z`/`s`/`3`, and set
  a checked-in size budget separately for each target rather than obscuring
  target differences in one number.
- Audit every native allocation, saved term/PID lifetime, resource destructor,
  error atom, and public spec.
- Write the README quick start, architecture overview, raw-link integration
  guide, supported inet options, error semantics, supervision guidance,
  migration notes, and troubleshooting guide.
- Produce checksummed GNU-libc NIF artifacts for `x86_64` and `aarch64`. Select
  by architecture before load, reject unknown targets, and retain an explicit
  GNU-libc source-build fallback. Consider `rustler_precompiled`, but accept it
  only after its loader, checksum, release, and fallback behavior pass the
  project tests.
- Test artifacts on the oldest supported glibc baseline and a current
  distribution. Save dynamic-dependency and required-symbol-version reports
  alongside CI artifacts. Musl remains outside the release matrix.
- Finalize the first release directly in `CHANGELOG.md` and verify package
  metadata. Do not create `RELEASE.md` for the release candidate. Release/tag
  only after explicit user approval; after publication is confirmed, introduce
  `RELEASE.md` on a later branch as the notes file for the second release.

#### Verification and acceptance criteria

- All phase acceptance suites pass in the 12-job CI matrix from a clean clone.
- No known unbounded mailbox, native collection, operation loop, or adapter
  drain remains.
- Stress/property tests produce no deadlock, lost wake, cross-operation
  notification, memory growth outside documented bounds, VM crash, or data
  corruption.
- Timer-only TCP progress and recovery are demonstrated with inbound traffic
  paused.
- Documentation examples compile and run against the public API.
- Final stripped and compressed sizes for both GNU/Linux targets are recorded,
  each stays within its evidence-based Phase 0 budget (or has an explicitly
  reviewed explanation), and CI prevents unnoticed size regression.
- GNU-libc artifacts load and pass smoke/integration tests on matching `x86_64`
  and `aarch64` systems at the documented glibc baseline.
- `smoltcp` has no direct `libc` dependency in the selected feature graph; any
  remaining C-runtime dependency comes from the Rust target/runtime and is
  documented per artifact.
- The supported feature/option matrix matches executable tests; omissions are
  explicit rather than silent.
- `CHANGELOG.md` uses Keep a Changelog headings and contains only user-visible
  outcomes. `RELEASE.md` remains absent throughout first-release preparation
  and appears only after that release has actually been published.
- The release candidate is one locally squashed phase commit before push, has a
  clean regular review, and is ready for an explicitly authorized tag/release.

#### Regular review focus

The sub-agent reviews release blockers only: safety, correctness, reproducible
builds, binary-size evidence, GNU-libc dependency reports, target selection,
documentation truthfulness, packaging, and unmet acceptance criteria. New
features, musl support, extra options, unsupported architectures, performance
experiments without a failing target, and unrelated refactors are logged but
not implemented.

## 10. Initial API contract to stabilize

The exact return tuples are finalized in Phase 0 against OTP precedent, but the
public surface should remain close to:

```elixir
SmolNet.open(domain, type, protocol, stack: stack)
SmolNet.bind(socket, address)
SmolNet.connect(socket, address, timeout_or_nowait)
SmolNet.listen(socket_or_options, backlog_or_options)
SmolNet.accept(listener, timeout_or_nowait)
SmolNet.send(socket, data, timeout_or_nowait)
SmolNet.recv(socket, length, timeout_or_nowait)
SmolNet.sendto(socket, data, address, timeout_or_nowait)
SmolNet.recvfrom(socket, length, timeout_or_nowait)
SmolNet.shutdown(socket, how)
SmolNet.close(socket)
SmolNet.sockname(socket)
SmolNet.peername(socket)
SmolNet.cancel(socket, select_info)
```

The facade owns docs/specs and delegates behavior to `SmolNet.Socket`. Because
`send/2` conflicts with `Kernel.send/2`, the facade excludes that import and
uses `Kernel.send/2` explicitly for process messages.

## 11. Decisions that must be closed by specific phases

| Decision | Owner phase | Required evidence |
|---|---:|---|
| Rustler, `smoltcp`, Rust/MSRV versions | 0 | compatibility ADR and CI build |
| OTP 27/28/29 callback arities and socket term | 0 | source-backed executable fixtures |
| Select/result/message shapes | 0–3 | contract tests and race tests |
| Stack bundle supervision and readiness handshake | 1 | significant-child teardown and nonblocking-init tests |
| NIF bytes/packets/readiness bounds | 1 | instrumented maximum-work tests |
| Single-feeder ingress backpressure and bound | 2 | serialized handoff and continuation-order tests |
| Egress message tag/link-down policy | 2 | adapter substitution and death tests |
| Socket ID/generation strategy | 3 | stale-handle model and tests |
| Waiter conflict and cancellation results | 3 | deterministic race matrix |
| TCP buffer defaults and error mapping | 4–5 | peer interoperability tests |
| Exact-length/zero-length receive semantics | 5 | public contract tests |
| Graceful close retention limit | 5 | timer/lifecycle tests |
| Supported TCP options and framing | 6 | OTP comparison suite |
| Listener pool/backlog limits | 7 | saturation/fairness tests |
| Dual-family and mapped-address policy | 8 | mixed-family suite |
| UDP truncation/zero-length/oversize rules | 9 | datagram boundary suite |
| Release/package/prebuilt-NIF policy | 11 | reproducibility and package audit |

No decision in this table may move application timeouts, active policy, packet
framing, or arbitrary send queues into Rust.

## 12. Definition of done for the project

The implementation is complete when:

- Phases 0–11 are merged sequentially through protected-branch PRs.
- Every phase has a recorded regular sub-agent review, responses to all review
  findings, fixed defects, and explicitly deferred feature suggestions.
- Every phase was reduced to one coherent local commit before its first push.
- Local development uses the `.bare` plus sibling-worktrees layout; no project
  files or commits are created in the `~/Code/smolnet` container root.
- All 12 supported OS/Elixir/OTP compatibility jobs and the canonical
  quality/coverage/package job are required and green.
- IPv6 TCP, IPv4 TCP, IPv6 UDP, and IPv4 UDP work through low-level APIs and
  actual OTP `gen_tcp`/`gen_udp` entry points.
- The required concurrency, boundedness, cancellation, lifecycle, timer,
  ownership, transport neutrality, and cross-talk invariants are observable in
  automated tests.
- The public documentation accurately states supported behaviors and errors.
- No release/tag is created until the user explicitly authorizes publishing.
