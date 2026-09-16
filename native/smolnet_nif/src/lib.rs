mod device;
mod limits;
mod socket_table;
mod stack;
mod time;
mod waiter;

use limits::{Limits, Work};
use rustler::{Atom, Decoder, Encoder, Env, ResourceArc, Term};
use stack::{ResourceCounts, StackResource};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        native_panic,
        ownership_invariant_violation,
        time_overflow,
        invalid_limits,
        running
    }
}

#[rustler::nif]
fn health() -> Atom {
    atoms::ok()
}

#[rustler::nif]
fn stack_new<'a>(env: Env<'a>, limits_term: Term<'a>, now_millis: i64) -> Term<'a> {
    guarded(env, || {
        let limits = Limits::decode(limits_term).map_err(|_| atoms::invalid_limits())?;
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        let resource = StackResource::new(limits, now).map_err(|_| atoms::invalid_limits())?;
        Ok((atoms::ok(), stack::Envelope::created(resource)))
    })
}

#[rustler::nif]
fn stack_snapshot<'a>(env: Env<'a>, resource: ResourceArc<StackResource>) -> Term<'a> {
    guarded(env, || {
        resource
            .with_stack(|stack| (atoms::ok(), stack.snapshot()))
            .map_err(|_| atoms::ownership_invariant_violation())
    })
}

#[rustler::nif]
fn stack_time_until<'a>(env: Env<'a>, now_millis: i64, deadline_millis: i64) -> Term<'a> {
    match time::time_until(now_millis, deadline_millis) {
        Ok(remaining) => (atoms::ok(), remaining).encode(env),
        Err(()) => (atoms::error(), atoms::time_overflow()).encode(env),
    }
}

#[rustler::nif]
fn resource_counts() -> ResourceCounts {
    StackResource::resource_counts()
}

#[rustler::nif]
fn test_contention<'a>(env: Env<'a>, resource: ResourceArc<StackResource>) -> Term<'a> {
    match resource.test_contention() {
        Ok(()) => atoms::ok().encode(env),
        Err(()) => (atoms::error(), atoms::ownership_invariant_violation()).encode(env),
    }
}

#[rustler::nif]
fn test_bounded_work<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    requested: Work,
) -> Term<'a> {
    guarded(env, || {
        resource
            .with_stack(|stack| (atoms::ok(), stack.apply_bounded_work(requested)))
            .map_err(|_| atoms::ownership_invariant_violation())
    })
}

fn guarded<'a, T, F>(env: Env<'a>, operation: F) -> Term<'a>
where
    T: Encoder,
    F: FnOnce() -> Result<T, Atom>,
{
    match catch_operation(operation) {
        Ok(Ok(value)) => value.encode(env),
        Ok(Err(reason)) => (atoms::error(), reason).encode(env),
        Err(_) => (atoms::error(), atoms::native_panic()).encode(env),
    }
}

fn catch_operation<T, E, F>(operation: F) -> Result<Result<T, E>, ()>
where
    F: FnOnce() -> Result<T, E>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)).map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::catch_operation;

    #[test]
    fn catches_panics_before_they_reach_the_nif_abi() {
        let result = catch_operation(|| -> Result<(), ()> { panic!("contained panic") });
        assert_eq!(result, Err(()));
    }
}

rustler::init!("Elixir.SmolNet.Native");
