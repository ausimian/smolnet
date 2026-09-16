# ADR 0001: dependency and toolchain baseline

## Status

Accepted for the project skeleton.

## Decision

- Elixir 1.19.5 and Erlang/OTP 28.3 are the development baseline. The library
  supports Elixir 1.18–1.20 on the compatible OTP 27–29 combinations in CI.
- Rust 1.94.0 is pinned for development and CI. The crate declares Rust 1.91 as
  its minimum because both Rustler 0.38 and smoltcp 0.14 require it.
- Rustler 0.38.0 provides the Elixir/NIF boundary.
- smoltcp 0.14.0 is built with default features disabled. Only `std`,
  `medium-ip`, `proto-ipv6`, and `socket-tcp` are enabled initially.
- Credo, Dialyxir, ExDoc, ExCoveralls, and Publisho provide local quality,
  documentation, coverage, and eventual release tooling.

| Dependency | Why accepted | Rejected alternative |
|---|---|---|
| Rustler 0.38.0 | Current NIF API, OTP 29 support, and the newer module-local compiler configuration path | 0.37.x has an older supported NIF surface and predates the 0.38 configuration guidance |
| smoltcp 0.14.0 | Current stable stack, Rust 1.91 MSRV aligned with Rustler, and individually selectable protocol features | 0.13.x is already superseded; default features would add IPv4, UDP, Ethernet, host interfaces, and logging before their phases |
| Credo 1.7 | Strict, established Elixir static analysis | A custom lint script would duplicate ecosystem checks and require local maintenance |
| Dialyxir 1.4 | Standard Mix integration for Dialyzer | Running Dialyzer directly would require bespoke PLT and warning handling |
| ExDoc 0.40 | Standard Elixir documentation generator with warnings-as-errors | Hand-built documentation would not validate references against the compiled API |
| ExCoveralls 0.18 | Supports local and Cobertura coverage output | A CI-specific coverage wrapper would make local and CI behavior diverge |
| Publisho 1.0 | Provides the planned post-first-release version/tag workflow | Ad-hoc release scripts would duplicate version and changelog mutation logic |

Project maintainers own dependency updates. Each update must arrive through a
normal reviewed branch and demonstrate compatibility, feature-graph, NIF-size,
and full `mix precommit` results. Rustler, smoltcp, Rust, Elixir, and OTP updates
also require the relevant cross-version CI evidence before merge.

## Consequences

The initial smoltcp feature graph excludes IPv4, UDP, Ethernet, host raw
socket/TUN helpers, logging, and smoltcp's optional `libc` feature. Rustler
itself depends on the Rust `libc` crate on Unix, and the first release targets
GNU libc; musl builds are deferred.

Dependency versions are locked. Updates require an explicit compatibility,
binary-size, and feature-graph review.
