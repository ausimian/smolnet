//! SmolNet test vectors: a record of what the socket tests feed a socket and
//! what it does in reply, written as JSON when `SMOLTCP_VECTOR_DIR` names a
//! directory, so that another TCP implementation can replay them. Test-only:
//! nothing here is compiled into the library.
//!
//! Each test that builds a socket through the test harness writes
//! `<dir>/<test name>.json`. The format is documented in SmolNet's
//! `test/conformance/README.md`; bump `VERSION` on any change to it.

use std::cell::RefCell;
use std::format;
use std::path::PathBuf;
use std::string::{String, ToString};
use std::vec::Vec;

use super::{AckDelayTimer, Context, PollAt, Socket, State, Timer, congestion};
use crate::socket::tcp::RttEstimator;
use crate::time::{Duration, Instant};
use crate::wire::{IpListenEndpoint, TcpControl, TcpRepr, TcpSeqNumber};

pub(super) const FORMAT: &str = "smolnet.tcp_vector";
pub(super) const VERSION: u32 = 1;

/// The environment variable naming the directory vectors are written to.
const DIR_VARIABLE: &str = "SMOLTCP_VECTOR_DIR";

// =============================================================================================//
// JSON values, built as strings. Objects keep their fields in the order given.
// =============================================================================================//

pub(super) fn null() -> String {
    "null".to_string()
}

pub(super) fn bool(value: bool) -> String {
    value.to_string()
}

pub(super) fn uint(value: u64) -> String {
    value.to_string()
}

pub(super) fn int(value: i64) -> String {
    value.to_string()
}

/// A float as the shortest decimal that reads back as the same value.
pub(super) fn float(value: f64) -> String {
    if value.is_finite() {
        format!("{value:?}")
    } else {
        string(&format!("{value}"))
    }
}

/// A `usize` that may be `usize::MAX`, which stands for "unbounded" and is
/// written as the string `"max"` so that readers need no 64-bit integers.
pub(super) fn size(value: usize) -> String {
    if value == usize::MAX {
        string("max")
    } else {
        uint(value as u64)
    }
}

pub(super) fn string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub(super) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 2);
    out.push('"');
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out.push('"');
    out
}

pub(super) fn array(values: impl IntoIterator<Item = String>) -> String {
    let values: Vec<String> = values.into_iter().collect();
    format!("[{}]", values.join(","))
}

pub(super) fn object(fields: &[(&str, String)]) -> String {
    let fields: Vec<String> = fields
        .iter()
        .map(|(name, value)| format!("{}:{}", string(name), value))
        .collect();
    format!("{{{}}}", fields.join(","))
}

pub(super) fn option<T>(value: Option<T>, f: impl FnOnce(T) -> String) -> String {
    value.map_or_else(null, f)
}

/// A sequence number as the unsigned 32-bit value on the wire.
pub(super) fn seq(value: TcpSeqNumber) -> String {
    uint(value.0 as u32 as u64)
}

/// An instant in milliseconds.
pub(super) fn instant(value: Instant) -> String {
    int(value.total_millis())
}

/// A duration in milliseconds.
pub(super) fn duration(value: Duration) -> String {
    uint(value.total_millis())
}

// =============================================================================================//
// Segments and state
// =============================================================================================//

pub(super) fn control(control: TcpControl) -> &'static str {
    match control {
        TcpControl::None => "none",
        TcpControl::Psh => "psh",
        TcpControl::Syn => "syn",
        TcpControl::Fin => "fin",
        TcpControl::Rst => "rst",
    }
}

pub(super) fn segment(repr: &TcpRepr) -> String {
    object(&[
        ("src_port", uint(repr.src_port.into())),
        ("dst_port", uint(repr.dst_port.into())),
        ("control", string(control(repr.control))),
        ("seq", seq(repr.seq_number)),
        ("ack", option(repr.ack_number, seq)),
        ("window_len", uint(repr.window_len.into())),
        (
            "window_scale",
            option(repr.window_scale, |v| uint(v.into())),
        ),
        (
            "max_seg_size",
            option(repr.max_seg_size, |v| uint(v.into())),
        ),
        ("sack_permitted", bool(repr.sack_permitted)),
        (
            "sack_ranges",
            array(
                repr.sack_ranges
                    .iter()
                    .flatten()
                    .map(|&(left, right)| array([uint(left.into()), uint(right.into())])),
            ),
        ),
        (
            "timestamp",
            option(repr.timestamp, |ts| {
                object(&[
                    ("tsval", uint(ts.tsval.into())),
                    ("tsecr", uint(ts.tsecr.into())),
                ])
            }),
        ),
        ("payload", hex(repr.payload)),
    ])
}

fn state_name(state: State) -> &'static str {
    match state {
        State::Closed => "closed",
        State::Listen => "listen",
        State::SynSent => "syn_sent",
        State::SynReceived => "syn_received",
        State::Established => "established",
        State::FinWait1 => "fin_wait_1",
        State::FinWait2 => "fin_wait_2",
        State::CloseWait => "close_wait",
        State::Closing => "closing",
        State::LastAck => "last_ack",
        State::TimeWait => "time_wait",
    }
}

fn timer(timer: &Timer) -> String {
    match *timer {
        Timer::Idle { keep_alive_at } => object(&[
            ("kind", string("idle")),
            ("keep_alive_at", option(keep_alive_at, instant)),
        ]),
        Timer::Retransmit { expires_at } => object(&[
            ("kind", string("retransmit")),
            ("expires_at", instant(expires_at)),
        ]),
        Timer::FastRetransmit => object(&[("kind", string("fast_retransmit"))]),
        Timer::ZeroWindowProbe { expires_at, delay } => object(&[
            ("kind", string("zero_window_probe")),
            ("expires_at", instant(expires_at)),
            ("delay", duration(delay)),
        ]),
        Timer::Close { expires_at } => object(&[
            ("kind", string("close")),
            ("expires_at", instant(expires_at)),
        ]),
    }
}

fn rtte(rtte: &RttEstimator) -> String {
    object(&[
        ("have_measurement", bool(rtte.have_measurement)),
        ("srtt", uint(rtte.srtt.into())),
        ("rttvar", uint(rtte.rttvar.into())),
        ("rto", uint(rtte.rto.into())),
        (
            "sample",
            option(rtte.timestamp, |(at, sent)| {
                object(&[("at", instant(at)), ("seq", seq(sent))])
            }),
        ),
        ("max_seq_sent", option(rtte.max_seq_sent, seq)),
        ("rto_count", uint(rtte.rto_count.into())),
    ])
}

fn ring(buffer: &super::SocketBuffer) -> String {
    let mut data = Vec::with_capacity(buffer.len());
    while data.len() < buffer.len() {
        let chunk = buffer.get_allocated(data.len(), buffer.len() - data.len());
        data.extend_from_slice(chunk);
    }
    object(&[
        ("capacity", uint(buffer.capacity() as u64)),
        ("data", hex(&data)),
    ])
}

pub(super) fn listen_endpoint(endpoint: &IpListenEndpoint) -> String {
    object(&[
        (
            "addr",
            option(endpoint.addr, |addr| string(&addr.to_string())),
        ),
        ("port", uint(endpoint.port.into())),
    ])
}

fn poll_at(poll_at: PollAt) -> String {
    match poll_at {
        PollAt::Now => object(&[("kind", string("now"))]),
        PollAt::Time(at) => object(&[("kind", string("time")), ("at", instant(at))]),
        PollAt::Ingress => object(&[("kind", string("ingress"))]),
    }
}

fn ack_delay_timer(timer: AckDelayTimer) -> String {
    match timer {
        AckDelayTimer::Idle => object(&[("kind", string("idle"))]),
        AckDelayTimer::Waiting(at) => object(&[("kind", string("waiting")), ("at", instant(at))]),
        AckDelayTimer::Immediate => object(&[("kind", string("immediate"))]),
    }
}

fn congestion(controller: &congestion::AnyController) -> String {
    match controller {
        congestion::AnyController::None(_) => object(&[("kind", string("none"))]),
        #[cfg(feature = "socket-tcp-reno")]
        congestion::AnyController::Reno(reno) => reno.vector_json(),
        #[cfg(feature = "socket-tcp-cubic")]
        congestion::AnyController::Cubic(cubic) => cubic.vector_json(),
    }
}

/// A socket's state, field by field, each a JSON value.
pub(super) type Snapshot = Vec<(&'static str, String)>;

/// Everything about a socket that a test can set or observe, as named JSON
/// values. Fields are the socket's own, under its names; `observed` holds
/// what its public queries answer.
pub(super) fn snapshot(socket: &Socket, cx: &mut Context) -> Snapshot {
    let s = socket;
    let observed = object(&[
        ("poll_at", poll_at(s.poll_at(cx))),
        ("is_open", bool(s.is_open())),
        ("is_listening", bool(s.is_listening())),
        ("is_active", bool(s.is_active())),
        ("may_send", bool(s.may_send())),
        ("may_recv", bool(s.may_recv())),
        ("can_send", bool(s.can_send())),
        ("can_recv", bool(s.can_recv())),
        ("send_queue", uint(s.send_queue() as u64)),
        ("recv_queue", uint(s.recv_queue() as u64)),
        ("timestamp_enabled", bool(s.timestamp_enabled())),
        ("aborted_by_timeout", bool(s.aborted_by_timeout())),
    ]);

    std::vec![
        ("state", string(state_name(s.state))),
        ("timer", timer(&s.timer)),
        ("rtte", rtte(&s.rtte)),
        (
            "assembler",
            array(
                s.assembler
                    .iter_data()
                    .map(|(left, right)| array([uint(left as u64), uint(right as u64)])),
            ),
        ),
        ("rx_buffer", ring(&s.rx_buffer)),
        ("rx_fin_received", bool(s.rx_fin_received)),
        ("tx_buffer", ring(&s.tx_buffer)),
        ("timeout", option(s.timeout, duration)),
        (
            "keep_alive",
            option(s.keep_alive, |k| {
                object(&[
                    ("idle", duration(k.idle)),
                    ("interval", duration(k.interval)),
                    ("probes", option(k.probes, |p| uint(p.into()))),
                ])
            }),
        ),
        ("keep_alive_probes", uint(s.keep_alive_probes.into())),
        ("timeout_abort", bool(s.timeout_abort)),
        ("hop_limit", option(s.hop_limit, |v| uint(v.into()))),
        ("listen_endpoint", listen_endpoint(&s.listen_endpoint)),
        (
            "tuple",
            option(s.tuple, |t| {
                object(&[
                    ("local", string(&t.local.to_string())),
                    ("remote", string(&t.remote.to_string())),
                ])
            }),
        ),
        ("local_seq_no", seq(s.local_seq_no)),
        ("remote_seq_no", seq(s.remote_seq_no)),
        ("remote_last_seq", seq(s.remote_last_seq)),
        ("remote_last_ack", option(s.remote_last_ack, seq)),
        ("remote_last_win", uint(s.remote_last_win.into())),
        ("remote_win_shift", uint(s.remote_win_shift.into())),
        ("remote_win_len", uint(s.remote_win_len as u64)),
        (
            "remote_win_scale",
            option(s.remote_win_scale, |v| uint(v.into()))
        ),
        ("remote_has_sack", bool(s.remote_has_sack)),
        ("remote_mss", uint(s.remote_mss as u64)),
        ("path_mtu", option(s.path_mtu, |v| uint(v as u64))),
        ("remote_last_ts", option(s.remote_last_ts, instant)),
        ("local_rx_last_seq", option(s.local_rx_last_seq, seq)),
        ("local_rx_last_ack", option(s.local_rx_last_ack, seq)),
        ("local_rx_dup_acks", uint(s.local_rx_dup_acks.into())),
        ("pending_fast_retransmit", bool(s.pending_fast_retransmit)),
        ("recover", option(s.recover, seq)),
        ("scoreboard", s.scoreboard.vector_json()),
        ("recovery_rxt_end", option(s.recovery_rxt_end, seq)),
        ("rack", s.rack.vector_json()),
        ("ack_delay", option(s.ack_delay, duration)),
        ("ack_delay_timer", ack_delay_timer(s.ack_delay_timer)),
        ("challenge_ack_timer", instant(s.challenge_ack_timer)),
        ("nagle", bool(s.nagle)),
        (
            "nagle_small_segment_end",
            option(s.nagle_small_segment_end, seq)
        ),
        ("congestion", congestion(&s.congestion_controller)),
        (
            "tsval",
            option(s.tsval_generator, |generate| uint(generate().into()))
        ),
        ("last_remote_tsval", uint(s.last_remote_tsval.into())),
        (
            "interface_addresses",
            array(cx.ip_addrs().iter().map(|cidr| string(&cidr.to_string()))),
        ),
        ("observed", observed),
    ]
}

/// The fields of `after` that differ from `before`, which list the same
/// fields in the same order.
fn diff(before: &Snapshot, after: &Snapshot) -> Snapshot {
    before
        .iter()
        .zip(after)
        .filter(|((_, a), (_, b))| a != b)
        .map(|(_, field)| field.clone())
        .collect()
}

// =============================================================================================//
// The recorder
// =============================================================================================//

/// A result for a call step: `{"ok": value}` or `{"error": "name"}`.
pub(super) fn result<T, E: core::fmt::Debug>(
    result: &Result<T, E>,
    ok: impl FnOnce(&T) -> String,
) -> String {
    match result {
        Ok(value) => object(&[("ok", ok(value))]),
        Err(error) => object(&[("error", string(&snake(&format!("{error:?}"))))]),
    }
}

pub(super) fn snake(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

thread_local! {
    /// The sockets this test has recorded so far, by the order they were
    /// made, as JSON.
    static SOCKETS: RefCell<Vec<(usize, String)>> = const { RefCell::new(Vec::new()) };
    /// How many sockets this test has made.
    static MADE: RefCell<usize> = const { RefCell::new(0) };
}

/// Records one test socket's steps, and writes the test's vector when it is
/// dropped. Inert unless `SMOLTCP_VECTOR_DIR` is set.
pub(super) struct Recorder {
    inner: Option<Inner>,
}

struct Inner {
    dir: PathBuf,
    test: String,
    index: usize,
    context: String,
    initial: Snapshot,
    last: Snapshot,
    steps: Vec<String>,
}

impl Recorder {
    pub(super) fn new(socket: &Socket, cx: &mut Context) -> Recorder {
        let Some(dir) = std::env::var_os(DIR_VARIABLE) else {
            return Recorder { inner: None };
        };
        let thread = std::thread::current();
        let test = thread
            .name()
            .and_then(|name| name.rsplit("::").next())
            .unwrap_or("unnamed")
            .to_string();
        let index = MADE.with(|made| {
            let mut made = made.borrow_mut();
            *made += 1;
            *made - 1
        });
        let context = object(&[
            ("ip_mtu", uint(cx.ip_mtu() as u64)),
            (
                "assembler_capacity",
                uint(crate::config::ASSEMBLER_MAX_SEGMENT_COUNT as u64),
            ),
            ("initial_seq_no", seq(Socket::random_seq_no(cx))),
        ]);
        let initial = snapshot(socket, cx);
        Recorder {
            inner: Some(Inner {
                dir: dir.into(),
                test,
                index,
                context,
                last: initial.clone(),
                initial,
                steps: Vec::new(),
            }),
        }
    }

    /// Notes the socket's state before a step, recording a `set` step if
    /// the test changed it since the last one, and returns that state.
    pub(super) fn before(&mut self, socket: &Socket, cx: &mut Context) -> Option<Snapshot> {
        let inner = self.inner.as_mut()?;
        let now = snapshot(socket, cx);
        let changed = diff(&inner.last, &now);
        if !changed.is_empty() {
            inner.steps.push(object(&[
                ("op", string("set")),
                ("state", object(&changed)),
            ]));
            inner.last = now.clone();
        }
        Some(now)
    }

    /// Records a step: its fields, then the state it changed.
    pub(super) fn step(
        &mut self,
        before: Option<Snapshot>,
        socket: &Socket,
        cx: &mut Context,
        mut fields: Vec<(&str, String)>,
    ) {
        let (Some(inner), Some(before)) = (self.inner.as_mut(), before) else {
            return;
        };
        let after = snapshot(socket, cx);
        fields.push(("state", object(&diff(&before, &after))));
        inner.steps.push(object(&fields));
        inner.last = after;
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let socket = format!(
            "{{\"index\":{},\"initial\":{},\"steps\":[\n{}\n]}}",
            inner.index,
            object(&inner.initial),
            inner.steps.join(",\n"),
        );
        let sockets = SOCKETS.with(|sockets| {
            let mut sockets = sockets.borrow_mut();
            sockets.push((inner.index, socket));
            sockets.sort_by_key(|(index, _)| *index);
            sockets
                .iter()
                .map(|(_, socket)| socket.clone())
                .collect::<Vec<_>>()
        });
        let vector = format!(
            "{{\"format\":{},\"version\":{},\"test\":{},\"time_unit\":\"ms\",\"context\":{},\"panicked\":{},\"sockets\":[\n{}\n]}}\n",
            string(FORMAT),
            VERSION,
            string(&inner.test),
            inner.context,
            std::thread::panicking(),
            sockets.join(",\n"),
        );
        let path = inner.dir.join(format!("{}.json", inner.test));
        std::fs::write(&path, vector).expect("cannot write a TCP test vector");
    }
}
