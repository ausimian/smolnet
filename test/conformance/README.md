# Conformance assets

What the pure-Elixir engine (#173) is checked against once the smoltcp NIF
is gone. They are generated from the NIF and the vendored smoltcp while
those still exist, and committed, so that they outlive `native/`.

## TCP test vectors

`tcp_vectors/` holds one JSON file per socket test in the vendored
smoltcp's `src/socket/tcp.rs`: what each test fed a TCP socket, what the
socket sent and answered, and its state after every step. They replace
those tests when the Rust crate is deleted; `SmolNet.Conformance.TcpReplay`
replays them against an implementation.

### Regenerating

```console
elixir scripts/tcp_vectors.exs           # rewrite tcp_vectors/
elixir scripts/tcp_vectors.exs --check   # fail if they are stale
```

The script runs the vendored crate's socket tests with `SMOLTCP_VECTOR_DIR`
set, which makes the test harness write a vector per test (see
`native/vendor/smoltcp/src/socket/tcp/vectors.rs`). It uses the same
features as `mix precommit` (the crate's defaults plus
`socket-tcp-cubic`), and `mix precommit` runs `--check`, so a change to the
vendored TCP code or its tests fails until the vectors are regenerated and
committed. The output is deterministic: no clock, no randomness, and floats
written as the shortest decimal that reads back exactly.

### What is and is not covered

`tcp_vectors/index.json` lists all 238 socket tests:

- `origin`: `upstream` for smoltcp 0.14.0's tests that no SmolNet patch
  changed (whitespace aside), `upstream_modified` for those a patch changed,
  and `smolnet` for those the patches added. It is computed from the
  pristine import, commit `db218b3`, so a shallow clone keeps the committed
  values. Phase 5 of #173 replays the `upstream` subset.
- `vector`: whether the test has a vector. 230 do. Five are compiled out of
  SmolNet's feature set (Reno, SYN-ACK pausing, segmentation offload), and
  three test `RttEstimator` and `Timer` directly, without a socket; the
  `note` says which.
- `note` on a test with a vector: a caveat. `test_buffer_wraparound_tx`
  depends on where the send ring buffer wraps, which a state does not
  record, so a subject without smoltcp's ring cannot pass it as recorded.
- `panics`: the test expects a panic (`#[should_panic]`). Its vector stops
  before the call that panics, and has `panicked: true`.

The unit tests of `rack.rs`, `scoreboard.rs`, `congestion/cubic.rs` and the
`storage` and `wire` modules are not vectors: they test internals directly
and are to be ported as unit tests. So is what a socket test asserts of an
internal function (`has_dsack`, `pipe`, `loss_probe_allowed`,
`window_to_update`); `test_dsack_reply_is_read_as_dsack` does nothing else,
and its vector has no steps.

### Format

A vector is one JSON object (version 1):

| Field | Meaning |
| --- | --- |
| `format`, `version` | `"smolnet.tcp_vector"`, `1`. Any change to the format bumps the version. |
| `test` | The Rust test's name. |
| `time_unit` | `"ms"`: every instant and duration below is in milliseconds unless its name ends `_us`. |
| `context` | How the test harness builds a socket: `ip_mtu` (1500), `assembler_capacity` (smoltcp's default build has 4 ranges, where the NIF has 32), `initial_seq_no` (10000: the tests stub the ISN). |
| `panicked` | The test panicked, as `#[should_panic]` tests do. |
| `sockets` | Each socket the test made, in order (`index`). Most tests make one; the others make a second as a reference to compare against. |

A socket is its `initial` state, the state `Socket::new` gives it with the
test's buffer sizes and no delayed ACK, and its `steps`. The harness
records the socket's state before and after every step; a step's `state`
holds the fields the step changed, so the state after step *n* is
`initial` with the `state` of steps 0 to *n* merged in. Steps are:

| `op` | Fields | Meaning |
| --- | --- | --- |
| `set` | `state` | The test changed the socket outside the recorded operations: it wrote fields directly, as the fixtures do to put a socket in a state, or made a call the harness does not record. Replay takes the changed fields as given. |
| `input` | `at`, `src_addr`, `dst_addr`, `segment`, `reply` | A segment from the remote arrives at `at` (`send!` in the tests), in an IP packet between the two addresses. `reply` is the segment the socket answered with at once, or null. |
| `dispatch` | `at`, `result`, `segment`, `hop_limit`, `psh_compared`, `refused` | The socket is asked for one segment to send (`recv!`). `result` is `segment` (it sent `segment`, in an IP packet with `hop_limit`), `nothing`, or `error`. `refused`, when present and true, means the test refused the segment as a full device would: `result` is then `error` and `segment` is what the socket tried to send. `psh_compared` is false where the test ignored the PSH flag, as most do. |
| `call` | `at`, `name`, `args`, `result` | A socket call, by its smoltcp name: `accepts` (whether the socket takes a segment between `src_addr` and `dst_addr`), `send_slice`, `recv`, `recv_slice`, `peek_slice`, `listen`, `connect`, `close`, `abort`, `reset`, `process_path_mtu` and the `set_*` options. `result` is `{"ok": value}` or `{"error": name}` for calls that can fail, the value otherwise (null for none). `recv`'s `len` is how many bytes the test's closure took; its result has them as `data`, and the bytes it was offered, which depend on the ring buffer's wrap, as `offered`. |

Every step also has `state`.

A segment is an object with `src_port`, `dst_port`, `control` (`none`,
`psh`, `syn`, `fin` or `rst`), `seq` and `ack` (unsigned 32-bit wire
values, `ack` null when the ACK flag is clear), `window_len` (the 16-bit
field, before scaling), `window_scale`, `max_seg_size` (null when the option
is absent), `sack_permitted`, `sack_ranges` (`[left, right]` pairs),
`timestamp` (`{tsval, tsecr}` or null) and `payload` (hex).

A state holds the socket's fields under their smoltcp names. Sequence
numbers are unsigned 32-bit, instants absolute milliseconds, optional
values null when absent, and a `usize::MAX` (CUBIC's initial `ssthresh`)
the string `"max"`.

| Field | Meaning |
| --- | --- |
| `state` | RFC 793 state, snake case (`syn_received`, `fin_wait_1`, ...). |
| `tuple`, `listen_endpoint` | `{local, remote}` as `address:port` strings; `{addr, port}`. |
| `local_seq_no`, `remote_last_seq` | `SND.UNA` and `SND.NXT`. |
| `remote_seq_no`, `remote_last_ack`, `remote_last_win` | `RCV.NXT`; the last ACK number and window sent. |
| `remote_win_len`, `remote_win_shift`, `remote_win_scale`, `remote_mss`, `remote_has_sack` | The peer's window (scaled, bytes), our and its window scale, its MSS, and SACK permitted. |
| `rx_buffer`, `tx_buffer` | `{capacity, data}`: the bytes queued, in order, as hex. |
| `assembler` | Out-of-order data held, as `[start, end)` offsets from `RCV.NXT`. |
| `rx_fin_received` | A FIN has arrived. |
| `timer` | `{kind, ...}`: `idle` (`keep_alive_at`), `retransmit` (`expires_at`), `fast_retransmit`, `zero_window_probe` (`expires_at`, `delay`), `close` (`expires_at`). |
| `rtte` | RTT estimator: `have_measurement`, `srtt`, `rttvar`, `rto`, `sample` (`{at, seq}` being timed), `max_seq_sent`, `rto_count`. |
| `timeout`, `keep_alive`, `keep_alive_probes`, `timeout_abort` | User timeout; keep-alive `{idle, interval, probes}`, probes sent, and whether the connection was given up. |
| `ack_delay`, `ack_delay_timer` | Delayed-ACK delay; timer `idle`, `waiting` (`at`) or `immediate`. |
| `nagle`, `nagle_small_segment_end` | Nagle enabled; the end of the small segment in flight (Minshall). |
| `local_rx_last_seq`, `local_rx_last_ack`, `local_rx_dup_acks` | The last segment's sequence and ACK numbers; the duplicate ACK count. |
| `pending_fast_retransmit`, `recover`, `recovery_rxt_end`, `scoreboard` | Fast recovery: RFC 6582 `recover`, RFC 6675 `HighRxt`, SACKed `[left, right)` ranges. |
| `rack` | RACK-TLP: the send `log` (`start`, `ranges` with `start`, `end`, `at`, `retransmitted`, `lost`, `doubled`, `first`, `last`, and `sends`), `xmit`, `rtt_us`, `min_rtt_us`, `reo_wnd_mult`, `reo_wnd_persist`, `dsack_round`, `reo_timeout`, `probe_at`, `probe_pending`, `probe`. |
| `congestion` | `{kind: "none"}`, or CUBIC's (`w_max`, `cwnd`, `mss`, `ssthresh`, `rwnd`, `k`, `w_est`, `cwnd_prior`, `recovery_start`, `in_fast_recovery`, `in_rto_recovery`, `rto_slow_start`, `idle_start`) or Reno's fields. |
| `tsval`, `last_remote_tsval` | The value the timestamp generator gives (the tests use a constant), or null when timestamps are off; the peer's last TSval. |
| `path_mtu`, `hop_limit`, `challenge_ack_timer`, `remote_last_ts` | Path MTU from ICMP; hop limit; the challenge-ACK rate limit; when the socket last heard from the peer. |
| `interface_addresses` | The interface's addresses as CIDR strings. Not the socket's: a test that changes them changes what the socket may send from. |
| `observed` | What the public queries answer: `poll_at` (`{kind: "now" \| "ingress"}` or `{kind: "time", at}`), `is_open`, `is_listening`, `is_active`, `may_send`, `may_recv`, `can_send`, `can_recv`, `send_queue`, `recv_queue`, `timestamp_enabled`, `aborted_by_timeout`. |

The segments, call results and the RFC-level fields are what another
implementation should match. `rack.log`, the `assembler` layout, `recv`'s
`offered` and the CUBIC floats are smoltcp's own bookkeeping: a subject
compares them only if it keeps the same.

### Replaying

`SmolNet.Conformance.TcpReplay` replays each socket of a vector against a
subject, a module implementing its behaviour: `new/3` from the context and
initial state, `set/3`, `input/4`, `dispatch/3`, `call/4`, and `snapshot/1`
with `compared_fields/0` for the fields it checks after every step. A
socket's replay stops at its first mismatch, which names the test, socket,
step and what differed. Segments compare whole, except a dispatched
segment's PSH flag where the test ignored it.

The only subject today is `TcpReplay.Recorded`, which answers from the
vector itself; `tcp_vectors_test.exs` replays every vector through it,
checks that each vector is consistent (`TcpReplay.validate/1`), and that
the runner reports a perturbed reply, segment, result or field. The Elixir
connection (`SmolNet.Core.Tcp.Connection`) plugs in as another subject.
