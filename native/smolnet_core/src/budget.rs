use std::time::{Duration, Instant};

pub const CALL_TARGET: Duration = Duration::from_micros(1_000);
pub const ENCODING_HEADROOM: Duration = Duration::from_micros(250);
pub const WORK_BUDGET: Duration = Duration::from_micros(750);

#[derive(Clone, Debug)]
pub struct CallBudget {
    started_at: Instant,
    deadline: Instant,
    forced_checkpoints: Option<usize>,
    yielded: bool,
}

impl CallBudget {
    pub fn start(forced_checkpoints: Option<usize>) -> Self {
        let started_at = Instant::now();

        Self {
            started_at,
            deadline: started_at + WORK_BUDGET,
            forced_checkpoints,
            yielded: false,
        }
    }

    pub fn checkpoint(&mut self) -> bool {
        let within_budget = match self.forced_checkpoints.as_mut() {
            Some(0) => false,
            Some(remaining) => {
                *remaining -= 1;
                true
            }
            None => Instant::now() < self.deadline,
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

    pub fn timeslice_percent(&self) -> i32 {
        let elapsed = self.elapsed_nanoseconds();
        let target = CALL_TARGET.as_nanos() as u64;
        let percent = elapsed.saturating_mul(100).div_ceil(target);
        i32::try_from(percent.clamp(1, 100)).expect("timeslice percentage is at most 100")
    }
}

#[cfg(test)]
mod tests {
    use super::{CALL_TARGET, CallBudget, ENCODING_HEADROOM, WORK_BUDGET};

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
        assert!((1..=100).contains(&budget.timeslice_percent()));
    }
}
