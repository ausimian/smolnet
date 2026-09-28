//! The SACK scoreboard of RFC 6675: the octets above the cumulative ACK that
//! the remote has reported in SACK blocks.

use crate::wire::TcpSeqNumber;

/// The most disjoint ranges the scoreboard holds. Once it is full, the lowest
/// range is forgotten. Its octets then look unacknowledged, which can cost a
/// needless retransmission but never withholds a needed one.
const SCOREBOARD_SIZE: usize = 32;

/// RFC 6675 `DupThresh`.
pub(super) const DUP_THRESH: usize = 3;

#[derive(Debug, Clone, Copy)]
pub(super) struct Scoreboard {
    /// Disjoint, non-adjacent `[left, right)` ranges in ascending order.
    ranges: [(TcpSeqNumber, TcpSeqNumber); SCOREBOARD_SIZE],
    len: usize,
}

impl Scoreboard {
    pub(super) const fn new() -> Self {
        Scoreboard {
            ranges: [(TcpSeqNumber(0), TcpSeqNumber(0)); SCOREBOARD_SIZE],
            len: 0,
        }
    }

    pub(super) fn clear(&mut self) {
        self.len = 0;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn ranges(&self) -> &[(TcpSeqNumber, TcpSeqNumber)] {
        &self.ranges[..self.len]
    }

    /// Records that the remote holds `[left, right)`, and returns whether any
    /// of those octets was not recorded already.
    pub(super) fn add(&mut self, mut left: TcpSeqNumber, mut right: TcpSeqNumber) -> bool {
        if left >= right || self.ranges().iter().any(|&(l, r)| l <= left && right <= r) {
            return false;
        }

        let mut merged = [(TcpSeqNumber(0), TcpSeqNumber(0)); SCOREBOARD_SIZE + 1];
        let mut len = 0;
        let mut inserted = false;
        for &(l, r) in self.ranges() {
            if r < left {
                merged[len] = (l, r);
            } else if right < l {
                if !inserted {
                    merged[len] = (left, right);
                    len += 1;
                    inserted = true;
                }
                merged[len] = (l, r);
            } else {
                // Overlapping or adjacent: absorb it into the new range.
                left = left.min(l);
                right = right.max(r);
                continue;
            }
            len += 1;
        }
        if !inserted {
            merged[len] = (left, right);
            len += 1;
        }

        let forgotten = len.saturating_sub(SCOREBOARD_SIZE);
        self.len = len - forgotten;
        self.ranges[..self.len].copy_from_slice(&merged[forgotten..len]);
        true
    }

    /// Forgets the octets below `ack`, which the remote now acknowledges
    /// cumulatively.
    pub(super) fn advance(&mut self, ack: TcpSeqNumber) {
        let passed = self.ranges().iter().take_while(|&&(_, r)| r <= ack).count();
        self.ranges.copy_within(passed..self.len, 0);
        self.len -= passed;
        if let Some(first) = self.ranges[..self.len].first_mut()
            && first.0 < ack
        {
            first.0 = ack;
        }
    }

    /// The number of recorded octets in `[from, to)`.
    pub(super) fn sacked_between(&self, from: TcpSeqNumber, to: TcpSeqNumber) -> usize {
        self.ranges()
            .iter()
            .map(|&(l, r)| {
                let (l, r) = (l.max(from), r.min(to));
                if l < r { r - l } else { 0 }
            })
            .sum()
    }

    /// RFC 6675 `IsLost`, as a boundary: an octet the remote has not reported
    /// is lost if it lies below the returned sequence number, because at least
    /// `DupThresh` ranges, or more than `(DupThresh - 1) * mss` reported
    /// octets, lie above it.
    pub(super) fn lost_below(&self, mss: usize) -> Option<TcpSeqNumber> {
        let mut sacked = 0;
        for (count, &(l, r)) in self.ranges().iter().rev().enumerate() {
            sacked += r - l;
            if count + 1 >= DUP_THRESH || sacked > (DUP_THRESH - 1) * mss {
                return Some(l);
            }
        }
        None
    }

    /// The lowest gap at or above `from` that lies below a recorded range, as
    /// `(start, end)`.
    pub(super) fn next_hole(&self, from: TcpSeqNumber) -> Option<(TcpSeqNumber, TcpSeqNumber)> {
        let mut start = from;
        for &(l, r) in self.ranges() {
            if start < l {
                return Some((start, l));
            }
            start = start.max(r);
        }
        None
    }

    /// The lowest run of octets in `[from, to)` that no recorded range
    /// covers, as `(start, end)`.
    pub(super) fn unsacked_run(
        &self,
        from: TcpSeqNumber,
        to: TcpSeqNumber,
    ) -> Option<(TcpSeqNumber, TcpSeqNumber)> {
        let mut start = from;
        for &(l, r) in self.ranges() {
            if start >= to || l >= to {
                break;
            }
            if start < l {
                return Some((start, l));
            }
            start = start.max(r);
        }
        (start < to).then_some((start, to))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn seq(n: i32) -> TcpSeqNumber {
        TcpSeqNumber(n)
    }

    fn ranges(board: &Scoreboard) -> Vec<(i32, i32)> {
        board.ranges().iter().map(|&(l, r)| (l.0, r.0)).collect()
    }

    #[test]
    fn add_merges_overlapping_and_adjacent_ranges() {
        let mut board = Scoreboard::new();
        assert!(board.add(seq(10), seq(20)));
        assert!(board.add(seq(30), seq(40)));
        assert_eq!(ranges(&board), [(10, 20), (30, 40)]);

        // Adjacent on the left, overlapping on the right.
        assert!(board.add(seq(20), seq(35)));
        assert_eq!(ranges(&board), [(10, 40)]);

        // A range already held reports nothing new; an empty one neither.
        assert!(!board.add(seq(12), seq(40)));
        assert!(!board.add(seq(50), seq(50)));
        assert_eq!(ranges(&board), [(10, 40)]);
    }

    #[test]
    fn add_reports_a_range_below_a_higher_one() {
        // #115: a range filled in below the highest one is new.
        let mut board = Scoreboard::new();
        assert!(board.add(seq(4), seq(5)));
        assert!(board.add(seq(1), seq(2)));
        assert!(!board.add(seq(4), seq(5)));
        assert_eq!(ranges(&board), [(1, 2), (4, 5)]);
    }

    #[test]
    fn add_forgets_the_lowest_range_when_full() {
        let mut board = Scoreboard::new();
        for i in 0..SCOREBOARD_SIZE as i32 {
            assert!(board.add(seq(10 * i + 10), seq(10 * i + 15)));
        }
        assert!(board.add(seq(1_000), seq(1_005)));
        assert_eq!(board.len, SCOREBOARD_SIZE);
        assert_eq!(board.ranges()[0], (seq(20), seq(25)));
        assert_eq!(
            board.ranges()[SCOREBOARD_SIZE - 1],
            (seq(1_000), seq(1_005))
        );
    }

    #[test]
    fn advance_drops_and_trims_passed_ranges() {
        let mut board = Scoreboard::new();
        board.add(seq(10), seq(20));
        board.add(seq(30), seq(40));
        board.advance(seq(20));
        assert_eq!(ranges(&board), [(30, 40)]);
        board.advance(seq(35));
        assert_eq!(ranges(&board), [(35, 40)]);
        board.advance(seq(40));
        assert!(board.is_empty());
    }

    #[test]
    fn sequence_numbers_wrap() {
        let mut board = Scoreboard::new();
        board.add(seq(i32::MAX - 5), seq(i32::MAX));
        board.add(seq(i32::MIN), seq(i32::MIN + 5));
        assert_eq!(
            ranges(&board),
            [(i32::MAX - 5, i32::MAX), (i32::MIN, i32::MIN + 5)]
        );
        assert!(board.add(seq(i32::MAX), seq(i32::MIN)));
        assert_eq!(ranges(&board), [(i32::MAX - 5, i32::MIN + 5)]);
        assert_eq!(
            board.sacked_between(seq(i32::MAX - 10), seq(i32::MIN + 10)),
            11
        );
    }

    #[test]
    fn lost_below_counts_ranges_or_octets_above() {
        let mss = 10;
        let mut board = Scoreboard::new();
        board.add(seq(100), seq(110));
        board.add(seq(120), seq(130));
        assert_eq!(board.lost_below(mss), None);

        // Three ranges above an octet make it lost.
        board.add(seq(140), seq(150));
        assert_eq!(board.lost_below(mss), Some(seq(100)));

        // So do more than two segments' worth of octets in fewer ranges.
        let mut board = Scoreboard::new();
        board.add(seq(100), seq(121));
        assert_eq!(board.lost_below(mss), Some(seq(100)));
        let mut board = Scoreboard::new();
        board.add(seq(100), seq(120));
        assert_eq!(board.lost_below(mss), None);
    }

    #[test]
    fn next_hole_skips_recorded_ranges() {
        let mut board = Scoreboard::new();
        assert_eq!(board.next_hole(seq(0)), None);
        board.add(seq(10), seq(20));
        board.add(seq(30), seq(40));
        assert_eq!(board.next_hole(seq(0)), Some((seq(0), seq(10))));
        assert_eq!(board.next_hole(seq(10)), Some((seq(20), seq(30))));
        assert_eq!(board.next_hole(seq(25)), Some((seq(25), seq(30))));
        assert_eq!(board.next_hole(seq(30)), None);
        assert_eq!(board.sacked_between(seq(15), seq(35)), 10);
    }

    #[test]
    fn unsacked_run_finds_gaps_and_the_tail() {
        let mut board = Scoreboard::new();
        assert_eq!(board.unsacked_run(seq(0), seq(5)), Some((seq(0), seq(5))));
        board.add(seq(10), seq(20));
        board.add(seq(30), seq(40));
        assert_eq!(board.unsacked_run(seq(0), seq(50)), Some((seq(0), seq(10))));
        assert_eq!(
            board.unsacked_run(seq(10), seq(50)),
            Some((seq(20), seq(30)))
        );
        assert_eq!(
            board.unsacked_run(seq(22), seq(25)),
            Some((seq(22), seq(25)))
        );
        assert_eq!(
            board.unsacked_run(seq(35), seq(50)),
            Some((seq(40), seq(50)))
        );
        assert_eq!(board.unsacked_run(seq(12), seq(18)), None);
        assert_eq!(board.unsacked_run(seq(30), seq(40)), None);
        assert_eq!(board.unsacked_run(seq(5), seq(5)), None);
    }
}
