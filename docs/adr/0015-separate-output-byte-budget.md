# ADR 0015: separate output byte budget

## Status

Accepted. Refines the byte quota of ADR 0011.

## Context

Every native call that drives the stack had one `bytes_copied` budget for
both directions. The bytes the call's operation copied (a send into the
socket buffer, a receive out of it, or an ingress batch) were subtracted
first, and the packets handed to the link could use only what was left.

`bytes_copied` defaults to 64 KiB, which is also the size of a typical
`:gen_tcp` write, so a full-size send left no output budget (#67). The send's
own egress pass still built the first burst of segments, but they stayed in
the device queue. The call reported `more: true`, the stack owner sent itself
a poll, and the segments left one mailbox round trip and one more native
call later. Every full-size send paid that hop, and a busy mailbox made it
longer. A full-budget ingress, or a large receive whose window update was
due, waited the same way.

Since ADR 0011, handing a queued packet to the link does not copy it: the
`OwnedBinary` smoltcp wrote into is released into the result. The output
budget bounds how much the call hands off and encodes, not bytes it copies
again.

## Decision

- Output has its own `bytes_copied` budget. A call hands its link queued
  packets up to `output_packets` packets and `bytes_copied` bytes whatever its
  operation copied, the same output a poll may hand off.
- The operation's own copy keeps its existing bound of `bytes_copied`, so a
  call copies at most `bytes_copied` bytes in each direction.
- `max_bytes_copied` in `stack_info/1` reports the larger of the two per
  call, so it is still bounded by, and comparable with, the `bytes_copied`
  limit.

The alternatives in #67 were rejected:

- Allowing one burst of `output_packets` frames whatever their size would
  let a call at a large MTU hand off up to 32 frames of 65,575 bytes, far
  more than any call could before, and the output byte bound would no longer
  mean anything.
- Asking senders to keep writes below `bytes_copied` would push a native
  accounting detail onto every adapter and caller, and a write of exactly the
  default size is the common case.

## Verification

`scripts/nif_budget.exs` adds a maximum TCP send scenario: an established
IPv6 connection with 1 MiB buffers at MTU 2,049, where one send of 65,575
bytes also hands off 32 frames totalling 65,568 bytes, both output maxima. It
cannot be reached without this change. On an Apple silicon Mac with the debug
NIF it took p99 686 µs and at most 687 µs over 100 samples, most of which is
building the 32 frames, work the send call already did when they stayed
queued. With the release NIF the same send took about 16.5 µs at the
median, against about 15.2 µs when it handed nothing off.

## Consequences

A full-size send, a full-budget ingress, and a large receive hand their
link the first burst in the same call. Measured between two native stacks,
a 64 KiB send at MTU 9,000 now takes one native call instead of two to hand
off its seven full segments, and at MTU 1,280 two instead of three for its
53 segments, since those beyond 32 frames still go out in a continuation.

One call can now copy up to twice `bytes_copied`: a full budget in and a
full budget of packets out. The frames themselves are built by the egress
pass whether or not they are handed off, so the added work is taking them
from the queue and encoding them, which the maximum output encoding scenario
already measures.
