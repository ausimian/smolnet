use smoltcp::time::Instant;

pub fn instant_from_millis(millis: i64) -> Result<Instant, ()> {
    let micros = millis.checked_mul(1_000).ok_or(())?;

    Ok(Instant::from_micros(micros))
}

pub fn time_until(now_millis: i64, deadline_millis: i64) -> Result<u64, ()> {
    if deadline_millis <= now_millis {
        return Ok(0);
    }

    let difference = deadline_millis.checked_sub(now_millis).ok_or(())?;
    u64::try_from(difference).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::{instant_from_millis, time_until};

    #[test]
    fn converts_zero_and_negative_monotonic_instants() {
        assert_eq!(instant_from_millis(0).unwrap().total_millis(), 0);
        assert_eq!(instant_from_millis(-10).unwrap().total_millis(), -10);
    }

    #[test]
    fn rejects_instant_conversion_overflow() {
        assert!(instant_from_millis(i64::MAX).is_err());
        assert!(instant_from_millis(i64::MIN).is_err());
    }

    #[test]
    fn returns_zero_for_expired_deadlines() {
        assert_eq!(time_until(10, 10), Ok(0));
        assert_eq!(time_until(10, 9), Ok(0));
    }

    #[test]
    fn converts_large_valid_deadlines_and_rejects_overflow() {
        assert_eq!(time_until(0, i64::MAX), Ok(i64::MAX as u64));
        assert_eq!(time_until(i64::MIN, i64::MAX), Err(()));
    }
}
