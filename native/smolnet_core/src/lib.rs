mod device;
mod limits;
mod socket_table;
mod stack;
mod tcp;
mod time;
mod udp;
mod waiter;

#[cfg(feature = "fuzzing")]
pub mod fuzzing;

use limits::Limits;
#[cfg(debug_assertions)]
use limits::Work;
use rustler::{
    Atom, Binary, Decoder, Encoder, Env, Error, ListIterator, LocalPid, NewBinary, NifMap,
    NifResult, Reference, ResourceArc, Term,
};
use socket_table::SocketError;
use stack::{Envelope, ResourceCounts, StackConfig, StackError, StackResource};
use tcp::{ShutdownHow, TcpEndpoint};
#[cfg(debug_assertions)]
use waiter::{ArmPoint, Direction};
use waiter::{Operation, SocketIdentity};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        native_panic,
        ownership_invariant_violation,
        time_overflow,
        invalid_limits,
        invalid_stack_config,
        invalid_packet,
        packet_too_large,
        invalid_socket,
        wrong_socket_kind,
        invalid_socket_state,
        invalid_operation,
        busy,
        system_limit,
        unsupported_socket,
        unsupported_family,
        message_too_large,
        invalid_address,
        invalid_port,
        invalid_backlog,
        scope_required,
        invalid_scope,
        address_in_use,
        address_not_available,
        ephemeral_ports_exhausted,
        network_unreachable,
        already_connected,
        not_bound,
        not_connected,
        connection_refused,
        connection_reset,
        connection_timeout,
        address,
        addresses,
        destination,
        gateway,
        mtu,
        port,
        prefix_length,
        routes,
        scope_id,
        running,
        shutdown,
        ready,
        select,
        abort,
        closed,
        end_of_stream,
        already_sent,
        not_found,
        smol_socket = "$smol_socket"
    }
}

#[rustler::nif]
fn health() -> Atom {
    atoms::ok()
}

#[rustler::nif]
fn stack_new<'a>(
    env: Env<'a>,
    limits_term: Term<'a>,
    config_term: Term<'a>,
    now_millis: i64,
) -> Term<'a> {
    guarded(env, || {
        let limits = Limits::decode(limits_term).map_err(|_| atoms::invalid_limits())?;
        let config = StackConfig::decode(config_term).map_err(|_| atoms::invalid_stack_config())?;
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        let resource = StackResource::new(limits, config, now).map_err(stack_error_atom)?;
        Ok((atoms::ok(), stack::Envelope::created(resource)))
    })
}

#[rustler::nif]
fn stack_ingress<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    packet: Binary<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| {
                stack
                    .ingress(packet.as_slice(), now)
                    .map(|effects| stack.finish_call(env, atoms::ok(), effects, Vec::new()))
            })
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(stack_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn stack_poll<'a>(env: Env<'a>, resource: ResourceArc<StackResource>, now_millis: i64) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| {
                let effects = stack.poll(now);
                stack.finish_call(env, atoms::ok(), effects, Vec::new())
            })
            .map_err(|_| atoms::ownership_invariant_violation())
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn stack_shutdown<'a>(env: Env<'a>, resource: ResourceArc<StackResource>) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.shutdown(env))
            .map_err(|_| atoms::ownership_invariant_violation())
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn socket_cancel<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    operation: Operation,
    reference: Reference<'a>,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.cancel(env, identity, operation, reference))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn socket_validate<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.socket_validate(env, identity))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_open<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    family: crate::tcp::AddressFamily,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.tcp_open(env, family))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_bind<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    endpoint_term: Term<'a>,
) -> Term<'a> {
    let result = catch_operation(|| {
        let endpoint = decode_tcp_endpoint(endpoint_term).map_err(socket_error_atom)?;
        resource
            .with_stack(|stack| stack.tcp_bind(env, identity, endpoint))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_listen<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    backlog: usize,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.tcp_listen(env, identity, backlog, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_accept<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    pid: LocalPid,
    reference: Reference<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.tcp_accept(env, identity, pid, reference, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_connect<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    endpoint_term: Term<'a>,
    pid: LocalPid,
    reference: Reference<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let endpoint = decode_tcp_endpoint(endpoint_term).map_err(socket_error_atom)?;
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.tcp_connect(env, identity, endpoint, pid, reference, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_send<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    data: Binary<'a>,
    pid: LocalPid,
    reference: Reference<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.tcp_send(env, identity, data.as_slice(), pid, reference, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_recv<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    length: usize,
    pid: LocalPid,
    reference: Reference<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.tcp_recv(env, identity, length, pid, reference, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_shutdown<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    how: ShutdownHow,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.tcp_shutdown(env, identity, how, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_sockname<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.tcp_sockname(env, identity))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_peername<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.tcp_peername(env, identity))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn tcp_close<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.tcp_close(env, identity, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn udp_open<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    family: crate::tcp::AddressFamily,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.udp_open(env, family))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn udp_bind<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    endpoint_term: Term<'a>,
) -> Term<'a> {
    let result = catch_operation(|| {
        let endpoint = decode_tcp_endpoint(endpoint_term).map_err(socket_error_atom)?;
        resource
            .with_stack(|stack| stack.udp_bind(env, identity, endpoint))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn udp_connect<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    endpoint_term: Term<'a>,
) -> Term<'a> {
    let result = catch_operation(|| {
        let endpoint = decode_tcp_endpoint(endpoint_term).map_err(socket_error_atom)?;
        resource
            .with_stack(|stack| stack.udp_connect(env, identity, endpoint))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn udp_sendto<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    endpoint_term: Term<'a>,
    data: Binary<'a>,
    pid: LocalPid,
    reference: Reference<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let endpoint = decode_tcp_endpoint(endpoint_term).map_err(socket_error_atom)?;
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| {
                stack.udp_sendto(
                    env,
                    identity,
                    endpoint,
                    data.as_slice(),
                    pid,
                    reference,
                    now,
                )
            })
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn udp_recvfrom<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    length: usize,
    pid: LocalPid,
    reference: Reference<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result: Result<Result<Envelope<Term<'a>>, Atom>, ()> = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.udp_recvfrom(env, identity, length, pid, reference, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn udp_sockname<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.udp_sockname(env, identity))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn udp_peername<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.udp_peername(env, identity))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[rustler::nif]
fn udp_close<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        resource
            .with_stack(|stack| stack.udp_close(env, identity, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
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

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_contention<'a>(env: Env<'a>, resource: ResourceArc<StackResource>) -> Term<'a> {
    match resource.test_contention() {
        Ok(()) => atoms::ok().encode(env),
        Err(()) => (atoms::error(), atoms::ownership_invariant_violation()).encode(env),
    }
}

#[cfg(debug_assertions)]
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

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_maximum_work<'a>(env: Env<'a>, resource: ResourceArc<StackResource>) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.test_maximum_work())
            .map_err(|_| atoms::ownership_invariant_violation())
    });

    encode_envelope_result(env, result)
}

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_combined_maximum_work<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    keys_term: Term<'a>,
    now_millis: i64,
) -> Term<'a> {
    let result = catch_operation(|| {
        let now = time::instant_from_millis(now_millis).map_err(|_| atoms::time_overflow())?;
        let keys = decode_bounded_list(keys_term, Limits::MAX_READY_EVENTS)
            .map_err(|_| atoms::system_limit())?;
        resource
            .with_stack(|stack| stack.test_combined_maximum_work(env, keys, now))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_prepare_closing<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    count: usize,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.test_prepare_closing(count))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_prepare_maximum_drop<'a>(env: Env<'a>, resource: ResourceArc<StackResource>) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.test_prepare_maximum_drop())
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_socket_open<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    internal_handle: u64,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.test_socket_open(env, internal_handle))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[cfg(debug_assertions)]
#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn test_socket_wait<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    wait: TestWait<'a>,
) -> Term<'a> {
    let TestWait {
        direction,
        operation,
        pid,
        reference,
        arm_point,
        wake_count,
        completed,
    } = wait;

    let result = catch_operation(|| {
        resource
            .with_stack(|stack| {
                stack.test_socket_wait(
                    env, identity, direction, operation, pid, reference, arm_point, wake_count,
                    completed,
                )
            })
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[cfg(debug_assertions)]
#[derive(NifMap)]
struct TestWait<'a> {
    direction: Direction,
    operation: Operation,
    pid: LocalPid,
    reference: Reference<'a>,
    arm_point: ArmPoint,
    wake_count: usize,
    completed: bool,
}

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_socket_ready<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    keys_term: Term<'a>,
) -> Term<'a> {
    let result = catch_operation(|| {
        let keys = decode_bounded_list(keys_term, Limits::MAX_READY_EVENTS)
            .map_err(|_| atoms::system_limit())?;
        resource
            .with_stack(|stack| stack.test_socket_ready(env, keys))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
}

#[cfg(debug_assertions)]
#[rustler::nif]
fn test_socket_close<'a>(
    env: Env<'a>,
    resource: ResourceArc<StackResource>,
    identity: SocketIdentity,
    wake_direction: Option<Direction>,
) -> Term<'a> {
    let result = catch_operation(|| {
        resource
            .with_stack(|stack| stack.test_socket_close(env, identity, wake_direction))
            .map_err(|_| atoms::ownership_invariant_violation())?
            .map_err(socket_error_atom)
    });

    encode_envelope_result(env, result)
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

#[derive(NifMap)]
struct EncodedEnvelope<'a> {
    result: Term<'a>,
    output: Vec<Term<'a>>,
    poll_at: Option<i64>,
    more: bool,
}

fn encode_envelope_result<'a, T>(
    env: Env<'a>,
    result: Result<Result<Envelope<T>, Atom>, ()>,
) -> Term<'a>
where
    T: Encoder,
{
    match result {
        Ok(Ok(envelope)) => {
            let output = envelope
                .output
                .into_iter()
                .map(|packet| NewBinary::from_iter(env, packet.into_iter()).into())
                .collect();

            (
                atoms::ok(),
                EncodedEnvelope {
                    result: envelope.result.encode(env),
                    output,
                    poll_at: envelope.poll_at,
                    more: envelope.more,
                },
            )
                .encode(env)
        }
        Ok(Err(reason)) => (atoms::error(), reason).encode(env),
        Err(()) => (atoms::error(), atoms::native_panic()).encode(env),
    }
}

fn socket_error_atom(error: SocketError) -> Atom {
    match error {
        SocketError::Closed => atoms::closed(),
        SocketError::EndOfStream => atoms::end_of_stream(),
        SocketError::InvalidSocket => atoms::invalid_socket(),
        SocketError::WrongKind => atoms::wrong_socket_kind(),
        SocketError::InvalidState => atoms::invalid_socket_state(),
        SocketError::InvalidOperation => atoms::invalid_operation(),
        SocketError::Busy => atoms::busy(),
        SocketError::SystemLimit => atoms::system_limit(),
        SocketError::MessageTooLarge => atoms::message_too_large(),
        SocketError::InvalidAddress => atoms::invalid_address(),
        SocketError::InvalidPort => atoms::invalid_port(),
        SocketError::InvalidBacklog => atoms::invalid_backlog(),
        SocketError::ScopeRequired => atoms::scope_required(),
        SocketError::InvalidScope => atoms::invalid_scope(),
        SocketError::AddressInUse => atoms::address_in_use(),
        SocketError::AddressNotAvailable => atoms::address_not_available(),
        SocketError::EphemeralPortsExhausted => atoms::ephemeral_ports_exhausted(),
        SocketError::NetworkUnreachable => atoms::network_unreachable(),
        SocketError::AlreadyConnected => atoms::already_connected(),
        SocketError::NotBound => atoms::not_bound(),
        SocketError::NotConnected => atoms::not_connected(),
        SocketError::ConnectionRefused => atoms::connection_refused(),
        SocketError::ConnectionReset => atoms::connection_reset(),
        SocketError::ConnectionTimeout => atoms::connection_timeout(),
    }
}

fn decode_tcp_endpoint(term: Term<'_>) -> Result<TcpEndpoint, SocketError> {
    let address = term
        .map_get(atoms::address())
        .and_then(|value| decode_bounded_list(value, 16))
        .map_err(|_| SocketError::InvalidAddress)?;
    let port = term
        .map_get(atoms::port())
        .and_then(|value| value.decode::<i64>())
        .map_err(|_| SocketError::InvalidPort)?;
    let scope_id = term
        .map_get(atoms::scope_id())
        .and_then(|value| value.decode::<i64>())
        .map_err(|_| SocketError::InvalidScope)?;

    Ok(TcpEndpoint {
        address,
        port,
        scope_id,
    })
}

pub(crate) fn decode_bounded_list<'a, T>(term: Term<'a>, limit: usize) -> NifResult<Vec<T>>
where
    T: Decoder<'a>,
{
    let iterator = term.decode::<ListIterator<'a>>()?;
    let mut values = Vec::with_capacity(limit);

    for item in iterator {
        if values.len() == limit {
            return Err(Error::BadArg);
        }

        values.push(item.decode::<T>()?);
    }

    Ok(values)
}

fn stack_error_atom(error: StackError) -> Atom {
    match error {
        StackError::InvalidLimits => atoms::invalid_limits(),
        StackError::InvalidStackConfig => atoms::invalid_stack_config(),
        StackError::InvalidPacket => atoms::invalid_packet(),
        StackError::PacketTooLarge => atoms::packet_too_large(),
        StackError::OwnershipInvariantViolation => atoms::ownership_invariant_violation(),
    }
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
