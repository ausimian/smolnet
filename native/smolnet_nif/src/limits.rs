use rustler::NifMap;

#[derive(Clone, Copy, Debug, NifMap, PartialEq, Eq)]
pub struct Limits {
    pub bytes_copied: usize,
    pub output_packets: usize,
    pub ready_events: usize,
    pub maintenance_work: usize,
}

impl Limits {
    pub fn valid(self) -> bool {
        (1..=16 * 1024 * 1024).contains(&self.bytes_copied)
            && (1..=1_024).contains(&self.output_packets)
            && (1..=4_096).contains(&self.ready_events)
            && (1..=4_096).contains(&self.maintenance_work)
    }

    pub fn constrain(self, requested: Work) -> (Work, bool) {
        let completed = Work {
            bytes_copied: requested.bytes_copied.min(self.bytes_copied),
            output_packets: requested.output_packets.min(self.output_packets),
            ready_events: requested.ready_events.min(self.ready_events),
            maintenance_work: requested.maintenance_work.min(self.maintenance_work),
        };

        (completed, completed != requested)
    }
}

#[derive(Clone, Copy, Debug, Default, NifMap, PartialEq, Eq)]
pub struct Work {
    pub bytes_copied: usize,
    pub output_packets: usize,
    pub ready_events: usize,
    pub maintenance_work: usize,
}

#[cfg(test)]
mod tests {
    use super::{Limits, Work};

    const LIMITS: Limits = Limits {
        bytes_copied: 10,
        output_packets: 2,
        ready_events: 3,
        maintenance_work: 4,
    };

    #[test]
    fn every_work_dimension_is_bounded() {
        let requested = Work {
            bytes_copied: 11,
            output_packets: 3,
            ready_events: 4,
            maintenance_work: 5,
        };

        let (completed, more) = LIMITS.constrain(requested);

        assert_eq!(
            completed,
            Work {
                bytes_copied: 10,
                output_packets: 2,
                ready_events: 3,
                maintenance_work: 4,
            }
        );
        assert!(more);
    }

    #[test]
    fn work_at_or_below_limits_is_complete() {
        let requested = Work {
            bytes_copied: 9,
            output_packets: 1,
            ready_events: 2,
            maintenance_work: 3,
        };

        assert_eq!(LIMITS.constrain(requested), (requested, false));
    }

    #[test]
    fn rejects_zero_and_unreasonably_large_limits() {
        assert!(
            !Limits {
                bytes_copied: 0,
                ..LIMITS
            }
            .valid()
        );

        assert!(
            !Limits {
                output_packets: 1_025,
                ..LIMITS
            }
            .valid()
        );
    }
}
