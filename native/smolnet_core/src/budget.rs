use rustler::Env;
use std::time::{Duration, Instant};

pub const CALL_TARGET: Duration = Duration::from_micros(1_000);
pub const ENCODING_HEADROOM: Duration = Duration::from_micros(250);
pub const WORK_BUDGET: Duration = Duration::from_micros(750);

/// Number of work units charged as a single chunk.
///
/// `enif_consume_timeslice` accepts whole percentages of the 1 millisecond
/// call target, so the smallest honest charge covers 10 microseconds and costs
/// 40 reductions. Charging per work unit would therefore bill 128 * 40 = 5,120
/// reductions for the roughly 100 microseconds a full readiness quota takes.
/// Units are grouped so that a chunk is comfortably longer than the 10
/// microseconds the minimum charge represents.
pub const CHARGE_CHUNK: usize = 16;

#[derive(Clone, Debug)]
pub struct CallBudget {
    started_at: Instant,
    deadline: Instant,
    last_charged_at: Instant,
    forced_checkpoints: Option<usize>,
    forced_charges: Option<usize>,
    yielded: bool,
    slice_exhausted: bool,
}

impl CallBudget {
    pub fn start(forced_checkpoints: Option<usize>) -> Self {
        let started_at = Instant::now();

        Self {
            started_at,
            deadline: started_at + WORK_BUDGET,
            last_charged_at: started_at,
            forced_checkpoints,
            forced_charges: None,
            yielded: false,
            slice_exhausted: false,
        }
    }

    /// Charges the calling process for the native time consumed since the
    /// previous charge and reports whether its reduction slice is now spent.
    ///
    /// The percentage is always a delta. Charging the elapsed time since the
    /// start of the call would re-bill every earlier chunk and over-report the
    /// slice by a large factor.
    pub fn charge(&mut self, env: Env<'_>) -> bool {
        let now = Instant::now();
        let delta = now.saturating_duration_since(self.last_charged_at);
        self.last_charged_at = now;

        // ERTS clamps out-of-range percentages silently in release builds but
        // asserts and aborts in a debug-built emulator, and Rustler passes the
        // value straight through to the FFI symbol. Clamp here.
        let percent = percent_of_call_target(delta);
        let charged = rustler::schedule::consume_timeslice(env, percent);
        let exhausted = match self.forced_charges.as_mut() {
            Some(0) => true,
            Some(remaining) => {
                *remaining -= 1;
                charged
            }
            None => charged,
        };

        self.slice_exhausted |= exhausted;
        exhausted
    }

    /// Test hook: report the caller's reduction slice as exhausted once
    /// `charges` charges have been made during this call. Real charges are
    /// still issued so the reduction accounting under test stays honest.
    pub fn force_slice_exhaustion_after(&mut self, charges: usize) {
        self.forced_charges = Some(charges);
    }

    pub fn checkpoint(&mut self) -> bool {
        let within_budget = match self.forced_checkpoints.as_mut() {
            Some(0) => false,
            Some(remaining) => {
                *remaining -= 1;
                true
            }
            // The deadline stays a hard ceiling: reductions bound how much the
            // caller may be charged, not how long this thread occupies the
            // scheduler, so a process entering with a fresh slice would
            // otherwise run for a full slice plus the encoding tail.
            None => !self.slice_exhausted && Instant::now() < self.deadline,
        };

        self.yielded |= !within_budget;
        within_budget
    }

    pub fn elapsed_nanoseconds(&self) -> u64 {
        u64::try_from(self.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    pub fn yielded(&self) -> bool {
        self.yielded
    }

    /// Whether any charge made during this call reported the caller's
    /// reduction slice as exhausted.
    pub fn slice_exhausted(&self) -> bool {
        self.slice_exhausted
    }
}

fn percent_of_call_target(duration: Duration) -> i32 {
    let nanoseconds = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
    let target = CALL_TARGET.as_nanos() as u64;
    let percent = nanoseconds.saturating_mul(100).div_ceil(target);

    i32::try_from(percent.clamp(1, 100)).expect("timeslice percentage is at most 100")
}

#[cfg(test)]
mod tests {
    use super::{
        CALL_TARGET, CHARGE_CHUNK, CallBudget, ENCODING_HEADROOM, WORK_BUDGET,
        percent_of_call_target,
    };
    use std::time::Duration;

    #[test]
    fn reserves_explicit_result_encoding_headroom() {
        assert_eq!(WORK_BUDGET + ENCODING_HEADROOM, CALL_TARGET);
    }

    #[test]
    fn forced_checkpoints_expire_deterministically() {
        let mut budget = CallBudget::start(Some(2));

        assert!(budget.checkpoint());
        assert!(budget.checkpoint());
        assert!(!budget.checkpoint());
        assert!(budget.yielded());
        assert!(!budget.slice_exhausted());
    }

    #[test]
    fn charged_percentages_stay_within_the_range_erts_accepts() {
        assert_eq!(percent_of_call_target(Duration::ZERO), 1);
        assert_eq!(percent_of_call_target(Duration::from_nanos(1)), 1);
        assert_eq!(percent_of_call_target(Duration::from_micros(10)), 1);
        assert_eq!(percent_of_call_target(Duration::from_nanos(10_001)), 2);
        assert_eq!(percent_of_call_target(CALL_TARGET), 100);
        assert_eq!(percent_of_call_target(Duration::from_secs(1)), 100);
        assert_eq!(percent_of_call_target(Duration::MAX), 100);
    }

    #[test]
    fn the_whole_work_budget_fits_in_a_bounded_number_of_chunk_charges() {
        // The minimum charge covers one percent of the call target, so the
        // rounding error a call can accumulate is one percent per charge. With
        // the readiness quota of 128 units, chunked charging rounds up at most
        // eight times plus the final flush.
        let smallest_charge = CALL_TARGET / 100;
        assert_eq!(smallest_charge, Duration::from_micros(10));
        assert_eq!(128usize.div_ceil(CHARGE_CHUNK) + 1, 9);
        assert!(smallest_charge * 9 < WORK_BUDGET + ENCODING_HEADROOM);
    }
}
