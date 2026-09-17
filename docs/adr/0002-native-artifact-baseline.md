# ADR 0002: native artifact baseline

## Status

Accepted for the first release candidate.

## Context

Phase 0 established a 302 KiB Apple silicon skeleton baseline. The completed
native stack now implements bounded dual-family TCP and UDP, so the release
candidate needs a new production measurement.

## Measurements

The complete x86_64 GNU/Linux NIF was evaluated with Rust 1.94.0, fat LTO, one
codegen unit, symbol stripping, and unwind panic behavior:

| Optimization | ELF bytes | gzip bytes |
|---|---:|---:|
| `z` | 707,872 | 337,655 |
| `s` | 718,328 | 345,059 |
| `3` | 836,680 | 402,203 |

After removing debug-only NIF entry points and applying the final resource
bounds, representative stripped release builds measured 710,672 bytes for
x86_64 GNU/Linux, 617,432 bytes for AArch64 GNU/Linux, and 568,896 bytes for
Apple silicon macOS. Their gzip sizes were 339,265, 322,912, and 286,643 bytes
respectively. These are measurements, not files distributed from the source
repository; the release-assets workflow will re-establish the Linux values on
matching native runners.

## Decision

Use `opt-level = "z"` for release builds. It is smaller than `s` and `3` for the
completed stack. Keep fat LTO, one codegen unit, symbol stripping, and panic
unwinding. Do not commit generated shared libraries; distribute future
precompiled builds as verified release assets. Re-run the matrix when native
behavior or dependencies materially change.
