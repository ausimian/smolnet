# ADR 0002: native artifact baseline

## Status

Accepted for the project skeleton.

## Context

The initial NIF exports only `health/0`, but links Rustler and the deliberately
restricted smoltcp feature set. Its size is a useful baseline for later phases.

## Measurements

Measured on Apple silicon macOS with Rust 1.94.0, fat LTO, one codegen unit,
symbol stripping, and unwind panic behavior:

| Optimization | Mach-O bytes | gzip bytes |
|---|---:|---:|
| `z` | 302,240 | 137,855 |
| `s` | 302,160 | 136,269 |
| `3` | 335,168 | 146,800 |

The stripped `s` artifact exports two global symbols and dynamically links only
Apple's `libSystem` and `libiconv` platform libraries. Linux dynamic-linkage and
glibc-baseline measurements remain CI work because they cannot be inferred from
a macOS artifact.

## Decision

Use `opt-level = "s"` for release builds. It is marginally smaller uncompressed
than `z` and materially smaller when compressed, while `3` is larger. Keep fat
LTO, one codegen unit, symbol stripping, and panic unwinding. Re-run the matrix
when native behavior or dependencies materially change.
