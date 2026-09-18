# ADR 0012: wall-clock budgets in the test suite

## Status

Accepted.

## Context

The suite bounds many operations with a fixed millisecond budget: an accept
that must complete, a datagram that must arrive, a task that must return.
Those constants were chosen on developer hardware. GitHub-hosted runners are
shared and preemptible, so the same correct stack can miss a budget that has
never been close to tight locally. Two such failures were observed on
identical code, on different platforms, in different tests: a full TCP
accept/connect handshake bounded at 1 second (`loopback_test.exs`), and a
`recvfrom` bounded at 5 seconds inside the deliberately suspended-stack
retransmission ring (`ipv4_udp_test.exs`).

Inflating the constants was rejected. A budget that is generous everywhere
stops reporting the regression it exists to catch, and the ring test's margin
is load-bearing: it has to cover retry timing, not just delivery.

Tuning constants case by case was also rejected. There are roughly 57 fixed
budgets in the suite and no shared vocabulary for what any of them mean, so
each new flake would be argued from scratch.

## Decision

Every wall-clock budget in the suite is classified, and only one class moves
with the host.

- A **liveness** budget bounds how long the test is willing to wait for
  something that must eventually happen. Its value is not the property under
  test, only a bound on how long a healthy run may take. These are written
  `SmolNet.Test.Timing.liveness(budget)` and multiplied by a scale factor.
- A **quiescence** budget bounds how long the test waits to conclude that
  something did *not* happen, or asserts that an operation times out when it
  should. Here the wall clock is the property under test. These are written
  `SmolNet.Test.Timing.quiescence(budget)` and are never scaled: scaling them
  would weaken the assertion and slow the suite for no signal.

The scale factor is read from `SMOLNET_TEST_TIMEOUT_SCALE`, which must be a
number between 1 and 10. It defaults to 1, so a developer's run keeps the
original strict budgets and a regression that slows the stack still fails
locally. CI sets it to 5, for the same reason the NIF wall-clock gate is
advisory there (ADR 0011): the runners' compute environment is outside this
project's control.

`ExUnit`'s own per-test timeout and its default `assert_receive` wait are
liveness budgets and are scaled in `test/test_helper.exs`, so a scaled
per-call budget cannot be cut short by an unscaled enclosing one.
`refute_receive_timeout` is a quiescence budget and keeps its default.

Test files name their scaled budgets as module attributes
(`@wait_1s`, `@idle_20ms`) rather than calling `Timing` at each site, so a
call site still reads as a timeout and the file's budgets are declared in one
place.

The two tests with observed failures adopt this first, across the whole of
`loopback_test.exs` and `ipv4_udp_test.exs`. The remaining files keep their
literals until a failure or a change gives a reason to convert them; the
policy is what a conversion follows, not a mandate to rewrite the suite.

## Verification

`SMOLNET_TEST_TIMEOUT_SCALE` is validated on read, so a typo fails the run
rather than silently leaving budgets strict. Running the suite without the
variable exercises the original constants unchanged, which is the local
default and the guard against a regression hiding behind a generous budget.

## Consequences

A contended runner gets headroom exactly where waiting longer cannot mask a
defect, and nowhere else. No test is skipped, excluded, or made
unconditionally permissive, and the tests that assert timing still fail when
the timing regresses.

The cost is that a genuine liveness regression is detected more slowly on CI
than locally: a hang now takes up to five times as long to be reported there.
That is accepted because the local run, the one a contributor iterates
against, keeps the strict bound.
