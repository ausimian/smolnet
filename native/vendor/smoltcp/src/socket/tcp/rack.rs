//! RACK-TLP (RFC 8985): time-based loss detection and tail loss probes.
//!
//! RACK deems an unacknowledged segment lost once a segment sent after it
//! has been delivered and a reordering window has passed, so it detects a
//! lost retransmission, or losses too few to leave three segments SACKed
//! above them, without waiting for the retransmission timer. That needs to
//! know when each octet in flight was last sent. The socket segments its
//! send buffer afresh on every transmission, so there is no per-segment
//! record to hang that on: `SendLog` keeps it instead, as ranges of
//! sequence space sent together.

use super::scoreboard::Scoreboard;
use crate::time::{Duration, Instant};
use crate::wire::TcpSeqNumber;

/// The most ranges the log holds. Octets first sent at the same instant
/// share one, and SmolNet's clock counts milliseconds, so this is about how
/// many milliseconds a flight takes to send. Past it, the two neighbouring
/// ranges sent closest in time are merged, as if all of their octets had
/// been sent at the later time, which can only delay RACK's verdict on
/// them.
const LOG_SIZE: usize = 64;

/// How many ranges one change can add before the log is compacted: a
/// retransmission inside a range splits it in three.
const LOG_SLACK: usize = 3;

/// A range of sequence space, and its last transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sent {
    /// The end of the range, which starts where the previous one ends.
    end: TcpSeqNumber,
    /// When the range was last sent.
    at: Instant,
    /// The last transmission was a retransmission.
    retransmitted: bool,
    /// The last transmission is deemed lost.
    lost: bool,
    /// The last transmission resent octets whose earlier transmission was
    /// not deemed lost, so RFC 6675's `pipe` counts them twice.
    doubled: bool,
    /// The first and last of the sends, numbered in the order the socket
    /// made them, that the last transmission took: one, or a run of
    /// consecutive sends of new data, whose octets went in sequence order.
    /// A millisecond clock cannot order what is sent within a millisecond;
    /// these can.
    first: u64,
    last: u64,
}

impl Sent {
    const EMPTY: Sent = Sent {
        end: TcpSeqNumber(0),
        at: Instant::ZERO,
        retransmitted: false,
        lost: false,
        doubled: false,
        first: 0,
        last: 0,
    };

    /// Whether `next`, starting where `self` ends, can share its range: it
    /// is another part of the same run, or new data sent next.
    fn joins(&self, next: &Sent) -> bool {
        let same_run = (next.first, next.last) == (self.first, self.last);
        let sent_next = !self.retransmitted && next.first == self.last + 1;
        self.at == next.at
            && self.retransmitted == next.retransmitted
            && self.lost == next.lost
            && self.doubled == next.doubled
            && (same_run || sent_next)
    }

    /// The octets of this range up to `end`, as delivered.
    fn delivered(&self, end: TcpSeqNumber) -> Xmit {
        Xmit {
            at: self.at,
            end,
            first: self.first,
            last: self.last,
        }
    }
}

/// A delivered segment, as RACK records the latest one: when it was sent,
/// its end, and the run of sends it was part of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Xmit {
    at: Instant,
    end: TcpSeqNumber,
    first: u64,
    last: u64,
}

impl Xmit {
    /// Whether `self` was sent after `other` (RFC 8985 `RACK_sent_after`):
    /// in a later send, or later in the same one.
    fn after(&self, other: &Xmit) -> bool {
        self.last > other.last || (self.last == other.last && self.end > other.end)
    }
}

/// Builds a new list of ranges, merging each with the last when they match.
struct Builder {
    ranges: [Sent; LOG_SIZE + LOG_SLACK],
    len: usize,
}

impl Builder {
    fn new() -> Self {
        Builder {
            ranges: [Sent::EMPTY; LOG_SIZE + LOG_SLACK],
            len: 0,
        }
    }

    fn push(&mut self, sent: Sent) {
        if let Some(last) = self.ranges[..self.len].last_mut()
            && last.joins(&sent)
        {
            last.end = sent.end;
            last.first = last.first.min(sent.first);
            last.last = last.last.max(sent.last);
        } else {
            self.ranges[self.len] = sent;
            self.len += 1;
        }
    }
}

/// When each octet from `SND.UNA` up to the highest one sent was last sent.
#[derive(Debug, Clone, Copy)]
pub(super) struct SendLog {
    /// Where the first range starts: `SND.UNA`, as far as the log knows.
    start: TcpSeqNumber,
    ranges: [Sent; LOG_SIZE],
    len: usize,
    /// How many sends the log has recorded.
    sends: u64,
}

impl SendLog {
    pub(super) const fn new() -> Self {
        SendLog {
            start: TcpSeqNumber(0),
            ranges: [Sent::EMPTY; LOG_SIZE],
            len: 0,
            sends: 0,
        }
    }

    /// The ranges, each with the sequence number it starts at.
    fn iter(&self) -> impl Iterator<Item = (TcpSeqNumber, &Sent)> {
        let starts = core::iter::once(self.start).chain(self.ranges.iter().map(|sent| sent.end));
        starts.zip(&self.ranges[..self.len])
    }

    /// The end of the last range: the highest octet sent, plus one.
    fn high(&self) -> TcpSeqNumber {
        self.ranges[..self.len]
            .last()
            .map_or(self.start, |sent| sent.end)
    }

    fn clear(&mut self, start: TcpSeqNumber) {
        self.start = start;
        self.len = 0;
    }

    /// Replaces the ranges with `built`, merging the closest in time until
    /// they fit.
    fn replace(&mut self, mut built: Builder) {
        while built.len > LOG_SIZE {
            let ranges = &built.ranges[..built.len];
            let gap = |i: usize| {
                let (a, b) = (&ranges[i], &ranges[i + 1]);
                let same = a.retransmitted == b.retransmitted
                    && a.lost == b.lost
                    && a.doubled == b.doubled;
                (!same, a.at.max(b.at) - a.at.min(b.at))
            };
            let i = (0..built.len - 1).min_by_key(|&i| gap(i)).unwrap_or(0);
            let (a, b) = (built.ranges[i], built.ranges[i + 1]);
            // A merged range counts as in flight if either half does, so
            // `pipe` errs high, and it is lost only if both halves are.
            built.ranges[i] = Sent {
                end: b.end,
                at: a.at.max(b.at),
                retransmitted: a.retransmitted || b.retransmitted,
                lost: a.lost && b.lost,
                doubled: a.doubled || b.doubled,
                first: a.first.min(b.first),
                last: a.last.max(b.last),
            };
            built.ranges.copy_within(i + 2..built.len, i + 1);
            built.len -= 1;
        }
        self.ranges[..built.len].copy_from_slice(&built.ranges[..built.len]);
        self.len = built.len;
    }

    /// Forgets the octets below `ack`, which the remote has acknowledged.
    pub(super) fn advance(&mut self, ack: TcpSeqNumber) {
        if self.len == 0 {
            self.start = ack;
            return;
        }
        if ack <= self.start {
            return;
        }
        if ack >= self.high() {
            self.clear(ack);
            return;
        }
        let passed = self.ranges[..self.len]
            .iter()
            .take_while(|sent| sent.end <= ack)
            .count();
        self.ranges.copy_within(passed..self.len, 0);
        self.len -= passed;
        self.start = ack;
    }

    /// Records that `[seq, end)` was sent at `now`, with `una` the lowest
    /// unacknowledged octet. Octets sent before are a retransmission.
    pub(super) fn record(
        &mut self,
        una: TcpSeqNumber,
        seq: TcpSeqNumber,
        end: TcpSeqNumber,
        now: Instant,
    ) {
        self.advance(una);
        let seq = seq.max(self.start);
        if end <= seq {
            return;
        }
        if seq > self.high() {
            // Not contiguous with what the log holds, which it should always
            // be. Start afresh rather than guess at the octets between.
            self.clear(seq);
        }

        self.sends += 1;
        let send = self.sends;
        let high = self.high();
        let fresh = Sent {
            end,
            at: now,
            retransmitted: false,
            lost: false,
            doubled: false,
            first: send,
            last: send,
        };
        if seq == high {
            // New data only, the usual case: extend the last range, or add one.
            if let Some(last) = self.ranges[..self.len].last_mut()
                && last.joins(&fresh)
            {
                last.end = end;
                last.last = send;
                return;
            }
            if self.len < LOG_SIZE {
                self.ranges[self.len] = fresh;
                self.len += 1;
                return;
            }
        }

        let mut built = Builder::new();
        for (start, &sent) in self.iter() {
            if sent.end <= seq || start >= end {
                built.push(sent);
                continue;
            }
            if start < seq {
                built.push(Sent { end: seq, ..sent });
            }
            built.push(Sent {
                end: sent.end.min(end),
                at: now,
                retransmitted: true,
                lost: false,
                doubled: !sent.lost,
                first: send,
                last: send,
            });
            if sent.end > end {
                built.push(sent);
            }
        }
        if end > high {
            built.push(fresh);
        }
        self.replace(built);
    }

    /// Marks every range lost, as a retransmission timeout does (RFC 8985
    /// 6.3).
    pub(super) fn mark_all_lost(&mut self) {
        let mut built = Builder::new();
        for (_, &sent) in self.iter() {
            built.push(Sent {
                lost: true,
                doubled: false,
                ..sent
            });
        }
        self.replace(built);
    }

    /// Applies RFC 6675's `IsLost`, which holds below `boundary`: the first
    /// transmission of an octet there is lost. A range sent only once is
    /// lost, and a retransmitted one no longer has its earlier transmission
    /// in flight. Whether a retransmission is lost is RACK's to judge.
    pub(super) fn mark_lost_below(&mut self, boundary: TcpSeqNumber) {
        let mut built = Builder::new();
        for (start, &sent) in self.iter() {
            if start >= boundary {
                built.push(sent);
                continue;
            }
            let below = Sent {
                end: sent.end.min(boundary),
                lost: sent.lost || !sent.retransmitted,
                doubled: false,
                ..sent
            };
            built.push(below);
            if sent.end > boundary {
                built.push(sent);
            }
        }
        self.replace(built);
    }

    /// Whether any octet in the log was retransmitted or is deemed lost:
    /// loss recovery, of whatever kind, is under way.
    pub(super) fn recovering(&self) -> bool {
        self.ranges[..self.len]
            .iter()
            .any(|sent| sent.retransmitted || sent.lost)
    }

    /// RFC 6675 `SetPipe`, for the octets in `[una, high)`: each one not
    /// SACKed counts once, unless its last transmission is deemed lost, and
    /// twice if that was a resend of a transmission not deemed lost. An
    /// octet the log does not cover counts once.
    pub(super) fn pipe(&self, board: &Scoreboard, una: TcpSeqNumber, high: TcpSeqNumber) -> usize {
        let unsacked = |from: TcpSeqNumber, to: TcpSeqNumber| {
            let (from, to) = (from.max(una), to.min(high));
            if from < to {
                (to - from) - board.sacked_between(from, to)
            } else {
                0
            }
        };
        let mut pipe = unsacked(una, self.start) + unsacked(self.high(), high);
        for (start, sent) in self.iter() {
            let copies = match (sent.lost, sent.doubled) {
                (true, _) => 0,
                (false, false) => 1,
                (false, true) => 2,
            };
            pipe += copies * unsacked(start, sent.end);
        }
        pipe
    }

    /// The lowest run of octets below `high` that are not SACKed and whose
    /// last transmission is deemed lost, as `(start, end)`.
    pub(super) fn first_lost(
        &self,
        board: &Scoreboard,
        high: TcpSeqNumber,
    ) -> Option<(TcpSeqNumber, TcpSeqNumber)> {
        self.iter()
            .filter(|(_, sent)| sent.lost)
            .find_map(|(start, sent)| board.unsacked_run(start, sent.end.min(high)))
    }

    /// RACK's loss detection (RFC 8985 6.2 step 5): marks lost each range
    /// sent before `xmit`, the latest delivered, once `wait`, `RACK.rtt`
    /// plus the reordering window, has passed since it was sent. Returns
    /// whether it newly deems an octet lost that is not SACKed, and when the
    /// next such octet not yet lost will be.
    fn detect(
        &mut self,
        board: &Scoreboard,
        now: Instant,
        xmit: Xmit,
        wait: Duration,
    ) -> (bool, Option<Instant>) {
        let (mut newly_lost, mut next) = (false, None::<Instant>);
        let mut built = Builder::new();
        for (start, &sent) in self.iter() {
            // Sent before: in an earlier send, or earlier in the same run.
            // Ranges whose runs overlap, which only merging a full log makes,
            // are not judged.
            let before = if sent.last < xmit.first {
                sent.end
            } else if (sent.first, sent.last) == (xmit.first, xmit.last) {
                sent.end.min(xmit.end).max(start)
            } else {
                start
            };
            if sent.lost || before == start {
                built.push(sent);
                continue;
            }
            let unsacked = board.sacked_between(start, before) < before - start;
            let deadline = sent.at + wait;
            if now >= deadline {
                newly_lost |= unsacked;
                built.push(Sent {
                    end: before,
                    lost: true,
                    doubled: false,
                    ..sent
                });
                if before < sent.end {
                    built.push(sent);
                }
            } else {
                if unsacked {
                    next = Some(next.map_or(deadline, |next| next.min(deadline)));
                }
                built.push(sent);
            }
        }
        self.replace(built);
        (newly_lost, next)
    }
}

/// The state of a loss probe episode (RFC 8985 7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Probe {
    /// RFC 8985 `TLP.end_seq`: the highest sequence number sent when the
    /// probe was.
    pub(super) end: TcpSeqNumber,
    /// The probe resent data rather than sending new data.
    pub(super) retransmitted: bool,
    /// A D-SACK has arrived since the probe was sent, so a retransmitted
    /// probe may have repaired nothing.
    pub(super) dsacked: bool,
}

/// RFC 8985's state: RACK's, the log it reads, and TLP's.
#[derive(Debug, Clone, Copy)]
pub(super) struct Rack {
    pub(super) log: SendLog,
    /// `RACK.xmit_ts` and `RACK.end_seq`: the latest sent of the segments
    /// delivered.
    xmit: Option<Xmit>,
    /// `RACK.rtt`: the round trip of the latest segment delivered.
    rtt: Duration,
    /// `RACK.min_RTT`, over first transmissions.
    min_rtt: Option<Duration>,
    /// `RACK.reo_wnd_mult`, raised once a round trip by D-SACKs.
    reo_wnd_mult: u32,
    /// `RACK.reo_wnd_persist`: how many more recoveries keep the raised
    /// multiplier.
    reo_wnd_persist: u8,
    /// `RACK.dsack_round`: `SND.NXT` when the multiplier was last raised.
    dsack_round: Option<TcpSeqNumber>,
    /// When RACK's reordering timer expires, if it is armed.
    pub(super) reo_timeout: Option<Instant>,
    /// When the loss probe timer expires, if it is armed.
    pub(super) probe_at: Option<Instant>,
    /// A loss probe is due and has not been sent.
    pub(super) probe_pending: bool,
    /// The loss probe episode under way, if any.
    pub(super) probe: Option<Probe>,
}

/// `RACK.reo_wnd_persist`'s starting value.
const REO_WND_PERSIST: u8 = 16;

/// How far D-SACKs may raise the reordering window's multiplier. The window
/// is at most SRTT whatever the multiplier.
const REO_WND_MULT_MAX: u32 = 64;

impl Rack {
    pub(super) const fn new() -> Self {
        Rack {
            log: SendLog::new(),
            xmit: None,
            rtt: Duration::ZERO,
            min_rtt: None,
            reo_wnd_mult: 1,
            reo_wnd_persist: 0,
            dsack_round: None,
            reo_timeout: None,
            probe_at: None,
            probe_pending: false,
            probe: None,
        }
    }

    /// Prepares for a resend of everything from `SND.UNA`, as after a
    /// retransmission timeout or a path MTU reduction: the log deems
    /// everything in flight lost (RFC 8985 6.3), the timers stop, and any
    /// probe episode ends.
    pub(super) fn on_rewind(&mut self) {
        self.log.mark_all_lost();
        self.reo_timeout = None;
        self.cancel_probe();
        self.probe = None;
    }

    /// Disarms the loss probe timer, and any probe due.
    pub(super) fn cancel_probe(&mut self) {
        self.probe_at = None;
        self.probe_pending = false;
    }

    /// RFC 8985 6.2 steps 1 to 3, for an ACK that acknowledges `[una, ack)`
    /// cumulatively and SACKs `blocks`, with `board` as it stood before the
    /// ACK: updates the latest segment delivered, `RACK.rtt` and
    /// `RACK.min_RTT` from the octets that the ACK newly reports delivered.
    ///
    /// Returns a round trip sample that Karn's algorithm allows, from the
    /// earliest sent of those octets that were sent only once, as Linux
    /// takes one: from those acknowledged cumulatively, unless the ACK also
    /// acknowledges a retransmission so, when they may have arrived well
    /// before; otherwise from those newly SACKed.
    pub(super) fn on_ack(
        &mut self,
        now: Instant,
        board: &Scoreboard,
        (una, ack): (TcpSeqNumber, TcpSeqNumber),
        blocks: &[(TcpSeqNumber, TcpSeqNumber)],
    ) -> Option<Duration> {
        let min_rtt = self.min_rtt;
        let mut latest: Option<Xmit> = None;
        let (mut cumulative_sample, mut sack_sample) = (None::<Duration>, None::<Duration>);
        let mut cumulative_retransmission = false;
        let portions = core::iter::once((una, ack, true))
            .chain(blocks.iter().map(|&(left, right)| (left, right, false)));
        for (from, to, cumulative) in portions {
            for (start, sent) in self.log.iter() {
                let (a, b) = (start.max(from), sent.end.min(to));
                if a >= b || board.sacked_between(a, b) == b - a {
                    continue;
                }
                let rtt = now - sent.at;
                if sent.retransmitted {
                    cumulative_retransmission |= cumulative;
                    // Its first transmission may be what arrived (step 2).
                    if min_rtt.is_none_or(|min_rtt| rtt < min_rtt) {
                        continue;
                    }
                } else {
                    let sample = if cumulative {
                        &mut cumulative_sample
                    } else {
                        &mut sack_sample
                    };
                    *sample = Some(sample.map_or(rtt, |sample| sample.max(rtt)));
                    self.min_rtt = Some(self.min_rtt.map_or(rtt, |min| min.min(rtt)));
                }
                let delivered = sent.delivered(b);
                if latest.is_none_or(|latest| delivered.after(&latest)) {
                    latest = Some(delivered);
                }
            }
        }
        if let Some(latest) = latest {
            self.rtt = now - latest.at;
            if self.xmit.is_none_or(|xmit| latest.after(&xmit)) {
                self.xmit = Some(latest);
            }
        }
        cumulative_sample
            .filter(|_| !cumulative_retransmission)
            .or(sack_sample)
    }

    /// RFC 8985 6.2 step 4's D-SACK rule: a D-SACK raises the reordering
    /// window's multiplier, at most once a round trip, and keeps it raised
    /// for the next 16 recoveries. `ack` is the cumulative ACK and `nxt`
    /// `SND.NXT`.
    pub(super) fn on_dsack(&mut self, ack: TcpSeqNumber, nxt: TcpSeqNumber) {
        if self.dsack_round.is_some_and(|round| ack < round) {
            return;
        }
        self.dsack_round = Some(nxt);
        self.reo_wnd_mult = (self.reo_wnd_mult + 1).min(REO_WND_MULT_MAX);
        self.reo_wnd_persist = REO_WND_PERSIST;
    }

    /// Counts down the recoveries that keep a raised multiplier.
    pub(super) fn on_recovery_end(&mut self) {
        self.reo_wnd_persist = self.reo_wnd_persist.saturating_sub(1);
        if self.reo_wnd_persist == 0 {
            self.reo_wnd_mult = 1;
        }
    }

    /// The reordering window: a quarter of `srtt`, or of the minimum round
    /// trip before there is one, times the multiplier, and at most `srtt`.
    ///
    /// RFC 8985 6.2 step 4 takes a quarter of the minimum round trip, and
    /// none at all during recovery, or once three segments are SACKed,
    /// until reordering has been seen, since RACK stands alone there. Here
    /// RFC 6675's `IsLost` still marks the losses that duplicate ACKs show,
    /// and RACK only adds what they cannot, so it can afford a margin from
    /// the start. Without one, a path that holds packets back for a delay
    /// had RACK resend hundreds of segments needlessly in a connection's
    /// first round trips, before any sign of reordering, each one D-SACKed,
    /// and cut the window each time; smoltcp has no undo. The minimum is no
    /// base on such a path either: the packets it lets through first drag it
    /// down.
    fn reo_wnd(&self, srtt: Option<Duration>) -> Duration {
        let base = srtt.or(self.min_rtt).unwrap_or(Duration::ZERO);
        let wnd = base / 4 * self.reo_wnd_mult;
        srtt.map_or(wnd, |srtt| wnd.min(srtt))
    }

    /// RACK's loss detection, marking the log. Returns whether it newly deems
    /// an octet lost that is not SACKed, and arms the reordering timer for
    /// the next one it would.
    pub(super) fn detect(
        &mut self,
        board: &Scoreboard,
        now: Instant,
        srtt: Option<Duration>,
    ) -> bool {
        let Some(xmit) = self.xmit else {
            self.reo_timeout = None;
            return false;
        };
        // A reordered ACK can make `RACK.rtt` far shorter than the round
        // trip of the segments it judges, so the wait is at least SRTT, plus
        // the window.
        let rtt = srtt.map_or(self.rtt, |srtt| self.rtt.max(srtt));
        let wait = rtt + self.reo_wnd(srtt);
        let (newly_lost, next) = self.log.detect(board, now, xmit, wait);
        self.reo_timeout = next;
        newly_lost
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn seq(n: i32) -> TcpSeqNumber {
        TcpSeqNumber(n)
    }

    fn ms(n: i64) -> Instant {
        Instant::from_millis(n)
    }

    // What RACK takes as delivered: the octets of the log up to `end`.
    fn delivered(log: &SendLog, end: i32) -> Xmit {
        let end = seq(end);
        let (_, sent) = log
            .iter()
            .find(|(start, sent)| *start < end && end <= sent.end)
            .unwrap();
        sent.delivered(end)
    }

    // The latest delivered, as (sent at, end).
    fn latest(rack: &Rack) -> Option<(Instant, TcpSeqNumber)> {
        rack.xmit.map(|xmit| (xmit.at, xmit.end))
    }

    // The log as (start, end, sent at, retransmitted, lost, doubled).
    fn ranges(log: &SendLog) -> Vec<(i32, i32, i64, bool, bool, bool)> {
        log.iter()
            .map(|(start, s)| {
                let at = s.at.total_millis();
                (start.0, s.end.0, at, s.retransmitted, s.lost, s.doubled)
            })
            .collect()
    }

    #[test]
    fn record_merges_what_is_sent_together() {
        let mut log = SendLog::new();
        log.record(seq(100), seq(100), seq(110), ms(1));
        log.record(seq(100), seq(110), seq(115), ms(1));
        log.record(seq(100), seq(115), seq(120), ms(1));
        log.record(seq(100), seq(120), seq(130), ms(2));
        assert_eq!(
            ranges(&log),
            [
                (100, 120, 1, false, false, false),
                (120, 130, 2, false, false, false)
            ]
        );
        assert_eq!((log.ranges[0].first, log.ranges[0].last), (1, 3));
        log.advance(seq(125));
        assert_eq!(ranges(&log), [(125, 130, 2, false, false, false)]);
        log.advance(seq(130));
        assert!(ranges(&log).is_empty());
        assert!(!log.recovering());
    }

    #[test]
    fn record_splits_a_range_for_a_retransmission() {
        let mut log = SendLog::new();
        log.record(seq(0), seq(0), seq(30), ms(1));
        log.mark_lost_below(seq(10));
        log.record(seq(0), seq(0), seq(10), ms(5));
        log.record(seq(0), seq(20), seq(25), ms(6));
        assert_eq!(
            ranges(&log),
            [
                (0, 10, 5, true, false, false),
                (10, 20, 1, false, false, false),
                (20, 25, 6, true, false, true),
                (25, 30, 1, false, false, false)
            ]
        );
        assert!(log.recovering());

        // A resend running on into new data records both.
        log.record(seq(0), seq(25), seq(40), ms(7));
        assert_eq!(
            ranges(&log)[3..],
            [
                (25, 30, 7, true, false, true),
                (30, 40, 7, false, false, false)
            ]
        );
    }

    #[test]
    fn a_full_log_merges_the_ranges_closest_in_time() {
        let mut log = SendLog::new();
        for i in 0..LOG_SIZE as i32 {
            // Every range 10 ms after the last, except one 1 ms after.
            let at = if i == 40 { 391 } else { 10 * i as i64 };
            log.record(seq(0), seq(10 * i), seq(10 * i + 10), ms(at));
        }
        assert_eq!(log.len, LOG_SIZE);
        log.record(seq(0), seq(640), seq(650), ms(1_000));
        assert_eq!(log.len, LOG_SIZE);
        let ranges = ranges(&log);
        // Ranges 39 (sent at 390) and 40 (at 391) became one, sent at 391.
        assert_eq!(ranges[39], (390, 410, 391, false, false, false));
        assert_eq!(ranges[LOG_SIZE - 1], (640, 650, 1_000, false, false, false));
    }

    #[test]
    fn mark_lost_below_leaves_retransmissions_to_rack() {
        let mut log = SendLog::new();
        log.record(seq(0), seq(0), seq(30), ms(1));
        log.record(seq(0), seq(10), seq(20), ms(2));
        log.mark_lost_below(seq(25));
        assert_eq!(
            ranges(&log),
            [
                (0, 10, 1, false, true, false),
                (10, 20, 2, true, false, false),
                (20, 25, 1, false, true, false),
                (25, 30, 1, false, false, false)
            ]
        );
        log.mark_all_lost();
        assert!(ranges(&log).iter().all(|r| r.4 && !r.5));
    }

    #[test]
    fn pipe_counts_each_copy_in_flight() {
        let mut board = Scoreboard::new();
        board.add(seq(20), seq(30));
        let mut log = SendLog::new();
        log.record(seq(0), seq(0), seq(40), ms(1));
        assert_eq!(log.pipe(&board, seq(0), seq(40)), 30);
        // Lost octets do not count; a resend of them counts once, and a
        // resend of octets not deemed lost counts twice.
        log.mark_lost_below(seq(10));
        log.record(seq(0), seq(0), seq(5), ms(2));
        log.record(seq(0), seq(30), seq(35), ms(2));
        assert_eq!(log.pipe(&board, seq(0), seq(40)), 5 + 10 + 10 + 5);
        // Octets beyond the log count once.
        assert_eq!(log.pipe(&board, seq(0), seq(50)), 40);
        assert_eq!(log.first_lost(&board, seq(40)), Some((seq(5), seq(10))));
        assert_eq!(log.first_lost(&board, seq(7)), Some((seq(5), seq(7))));
    }

    #[test]
    fn detect_marks_what_was_sent_before_the_latest_delivered() {
        let board = Scoreboard::new();
        let mut log = SendLog::new();
        log.record(seq(0), seq(0), seq(10), ms(100));
        log.record(seq(0), seq(10), seq(30), ms(110));
        log.record(seq(0), seq(30), seq(40), ms(120));
        let wait = Duration::from_millis(50);

        // [10, 20) was the latest delivered. What was sent 50 ms before now
        // is lost, and it will be at 160. The octets sent after it, in the
        // same millisecond or later, are not judged.
        let xmit = delivered(&log, 20);
        let (lost, next) = log.detect(&board, ms(155), xmit, wait);
        assert!(lost);
        assert_eq!(next, Some(ms(160)));
        assert_eq!(
            ranges(&log),
            [
                (0, 10, 100, false, true, false),
                (10, 30, 110, false, false, false),
                (30, 40, 120, false, false, false)
            ]
        );
        let (lost, next) = log.detect(&board, ms(160), xmit, wait);
        assert!(lost);
        assert_eq!(next, None);
        assert_eq!(ranges(&log)[1], (10, 20, 110, false, true, false));
    }

    #[test]
    fn detect_orders_what_is_sent_in_the_same_millisecond() {
        let board = Scoreboard::new();
        let wait = Duration::ZERO;
        let mut log = SendLog::new();
        log.record(seq(0), seq(0), seq(30), ms(100));
        log.mark_lost_below(seq(10));

        // In one millisecond, new data goes out, then a resend. The new
        // data's delivery says nothing of the resend, sent after it.
        log.record(seq(0), seq(30), seq(40), ms(200));
        log.record(seq(0), seq(0), seq(10), ms(200));
        let xmit = delivered(&log, 40);
        assert_eq!(log.detect(&board, ms(200), xmit, wait), (true, None));
        assert!(!ranges(&log)[0].4);
        assert!(ranges(&log)[1].4);

        // Data sent after the resend, in the same millisecond, is.
        log.record(seq(0), seq(40), seq(50), ms(200));
        log.record(seq(0), seq(50), seq(60), ms(200));
        let xmit = delivered(&log, 60);
        assert_eq!(log.detect(&board, ms(200), xmit, wait), (true, None));
        assert!(ranges(&log)[0].4);
        assert_eq!(ranges(&log)[3], (40, 60, 200, false, true, false));
    }

    #[test]
    fn detect_ignores_sacked_octets() {
        let mut board = Scoreboard::new();
        board.add(seq(0), seq(20));
        let mut log = SendLog::new();
        log.record(seq(0), seq(0), seq(10), ms(100));
        log.record(seq(0), seq(10), seq(20), ms(110));
        let wait = Duration::from_millis(50);
        assert_eq!(
            log.detect(&board, ms(200), delivered(&log, 20), wait),
            (false, None)
        );
    }

    #[test]
    fn on_ack_samples_only_first_transmissions() {
        let mut rack = Rack::new();
        let board = Scoreboard::new();
        rack.log.record(seq(0), seq(0), seq(10), ms(100));
        rack.log.record(seq(0), seq(10), seq(20), ms(110));
        rack.log.record(seq(0), seq(20), seq(30), ms(120));
        rack.log.record(seq(0), seq(0), seq(10), ms(130));

        // [20, 30) is SACKed: a sample, and the latest delivered.
        let blocks = [(seq(20), seq(30))];
        let sample = rack.on_ack(ms(150), &board, (seq(0), seq(0)), &blocks);
        assert_eq!(sample, Some(Duration::from_millis(30)));
        assert_eq!(latest(&rack), Some((ms(120), seq(30))));
        assert_eq!(rack.rtt, Duration::from_millis(30));
        assert_eq!(rack.min_rtt, Some(Duration::from_millis(30)));

        // An ACK of the resent [0, 10) and of [10, 20) takes no sample: the
        // resend may be what arrived, and [10, 20) may have arrived long
        // before.
        let mut board = Scoreboard::new();
        board.add(seq(20), seq(30));
        let sample = rack.on_ack(ms(170), &board, (seq(0), seq(20)), &[]);
        assert_eq!(sample, None);
        assert_eq!(latest(&rack), Some((ms(130), seq(10))));
        assert_eq!(rack.rtt, Duration::from_millis(40));
    }

    #[test]
    fn on_ack_skips_a_resend_acknowledged_too_soon() {
        let mut rack = Rack::new();
        let board = Scoreboard::new();
        rack.log.record(seq(0), seq(0), seq(10), ms(100));
        rack.log.record(seq(0), seq(10), seq(20), ms(100));
        rack.on_ack(ms(150), &board, (seq(0), seq(0)), &[(seq(10), seq(20))]);
        rack.log.record(seq(0), seq(0), seq(10), ms(160));

        // Acknowledged 10 ms after the resend, less than the minimum round
        // trip: the first transmission must be what arrived.
        rack.on_ack(ms(170), &board, (seq(0), seq(10)), &[]);
        assert_eq!(latest(&rack), Some((ms(100), seq(20))));
        assert_eq!(rack.rtt, Duration::from_millis(50));
    }

    #[test]
    fn reo_wnd_follows_srtt_and_dsacks() {
        let mut rack = Rack::new();
        rack.min_rtt = Some(Duration::from_millis(40));
        assert_eq!(rack.reo_wnd(None), Duration::from_millis(10));
        let srtt = Some(Duration::from_millis(50));
        assert_eq!(rack.reo_wnd(srtt), Duration::from_micros(12_500));

        // A D-SACK raises the multiplier once a round trip, up to SRTT.
        rack.on_dsack(seq(0), seq(100));
        rack.on_dsack(seq(50), seq(150));
        assert_eq!(rack.reo_wnd(srtt), Duration::from_millis(25));
        rack.on_dsack(seq(100), seq(200));
        rack.on_dsack(seq(200), seq(300));
        rack.on_dsack(seq(300), seq(400));
        rack.on_dsack(seq(400), seq(500));
        assert_eq!(rack.reo_wnd(srtt), Duration::from_millis(50));

        // Sixteen recoveries without one restore it.
        for _ in 0..15 {
            rack.on_recovery_end();
        }
        assert_eq!(rack.reo_wnd_mult, 6);
        rack.on_recovery_end();
        assert_eq!(rack.reo_wnd(srtt), Duration::from_micros(12_500));
    }
}
