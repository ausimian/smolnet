use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, TryLockError};

use rustler::{Atom, Encoder, Env, LocalPid, NifMap, Reference, Resource, ResourceArc, Term};
use smoltcp::iface::{Config, Interface, PollResult, Route, SocketSet};
use smoltcp::socket::tcp::{self, ConnectError};
use smoltcp::time::Duration;
use smoltcp::time::Instant;
use smoltcp::wire::{
    HardwareAddress, IpAddress, IpCidr, IpEndpoint, IpProtocol, Ipv6Address, Ipv6Cidr,
    Ipv6ExtHeader, Ipv6Packet, TcpPacket,
};

use crate::device::BeamDevice;
use crate::limits::{Limits, Work};
use crate::socket_table::{
    CancelResult, InstallResult, ReadyResult, SocketError, SocketKind, SocketTable,
    WaiterRegistration,
};
use crate::tcp::{
    self as tcp_support, ConnectFailure, ConnectPhase, EncodedEndpoint, TcpEndpoint, TcpRecord,
    ValidatedEndpoint,
};
use crate::waiter::{
    ArmPoint, Direction, Operation, ReadinessCounters, ReadyKey, ReadyQueue, SocketIdentity, Waiter,
};

static NEXT_STACK_ID: AtomicU64 = AtomicU64::new(1);
static CREATED: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

pub struct StackResource {
    inner: Mutex<NativeStack>,
}

#[rustler::resource_impl]
impl Resource for StackResource {}

impl StackResource {
    pub fn new(
        limits: Limits,
        config: StackConfig,
        now: Instant,
    ) -> Result<ResourceArc<Self>, StackError> {
        if !limits.valid() {
            return Err(StackError::InvalidLimits);
        }

        let stack = NativeStack::new(limits, config, now)?;
        let resource = ResourceArc::new(Self {
            inner: Mutex::new(stack),
        });

        CREATED.fetch_add(1, Ordering::Relaxed);
        ACTIVE.fetch_add(1, Ordering::Relaxed);
        Ok(resource)
    }

    pub fn with_stack<T>(&self, operation: impl FnOnce(&mut NativeStack) -> T) -> Result<T, ()> {
        match self.inner.try_lock() {
            Ok(mut guard) => Ok(operation(&mut guard)),
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => Err(()),
        }
    }

    pub fn test_contention(&self) -> Result<(), ()> {
        let _guard = self.inner.try_lock().map_err(|_| ())?;
        self.with_stack(|_| ())
    }

    pub fn resource_counts() -> ResourceCounts {
        ResourceCounts {
            created: CREATED.load(Ordering::Relaxed),
            dropped: DROPPED.load(Ordering::Relaxed),
            active: ACTIVE.load(Ordering::Relaxed),
        }
    }
}

impl Drop for StackResource {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::Relaxed);
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct NativeStack {
    id: u64,
    interface: Interface,
    sockets: SocketSet<'static>,
    device: BeamDevice,
    socket_table: SocketTable,
    tcp_records: BTreeMap<u64, TcpRecord>,
    tcp_connections: BTreeMap<ConnectionKey, SocketIdentity>,
    next_ephemeral_port: u16,
    ready: ReadyQueue,
    ready_sweep: bool,
    ready_sweep_cursor: Option<ReadyKey>,
    counters: Counters,
    lifecycle: Lifecycle,
    limits: Limits,
    mtu: usize,
    route_prefixes: Vec<Ipv6Cidr>,
}

impl NativeStack {
    fn new(limits: Limits, stack_config: StackConfig, now: Instant) -> Result<Self, StackError> {
        stack_config.validate(limits)?;
        let route_prefixes = stack_config
            .routes
            .iter()
            .map(RouteConfig::to_cidr)
            .collect();

        let mut device = BeamDevice::new(stack_config.mtu);
        let mut interface_config = Config::new(HardwareAddress::Ip);
        interface_config.random_seed = NEXT_STACK_ID.fetch_add(1, Ordering::Relaxed);
        let id = interface_config.random_seed;
        let mut interface = Interface::new(interface_config, &mut device, now);

        interface.update_ip_addrs(|addresses| {
            for address in &stack_config.addresses {
                addresses
                    .push(IpCidr::Ipv6(address.to_cidr()))
                    .expect("validated address capacity");
            }
        });

        interface.routes_mut().update(|routes| {
            for route in &stack_config.routes {
                routes
                    .push(route.to_route())
                    .expect("validated route capacity");
            }
        });

        Ok(Self {
            id,
            interface,
            sockets: SocketSet::new(Vec::new()),
            device,
            socket_table: SocketTable::new(limits.ready_events, limits.ready_events),
            tcp_records: BTreeMap::new(),
            tcp_connections: BTreeMap::new(),
            next_ephemeral_port: tcp_support::EPHEMERAL_PORT_FIRST,
            ready: ReadyQueue::new((limits.ready_events / 2).max(1)),
            ready_sweep: false,
            ready_sweep_cursor: None,
            counters: Counters::default(),
            lifecycle: Lifecycle::Running,
            limits,
            mtu: stack_config.mtu,
            route_prefixes,
        })
    }

    pub fn snapshot(&self) -> Envelope<Snapshot> {
        let (receive_packets, transmit_packets) = self.device.queued_packets();
        let (read_waiters, write_waiters) = self.socket_table.waiter_counts();

        Envelope {
            result: Snapshot {
                id: self.id,
                limits: self.limits,
                socket_count: self.socket_table.len(),
                native_socket_count: self.sockets.iter().count(),
                tcp_socket_count: self.tcp_records.len(),
                tcp_buffer_bytes: tcp_support::BUFFER_BYTES,
                waiter_count: self.socket_table.waiter_count(),
                read_waiter_count: read_waiters,
                write_waiter_count: write_waiters,
                ready_count: self.ready.len(),
                ready_overflow_pending: self.ready_sweep || self.ready.has_pending(),
                readiness: self.ready.counters(),
                receive_packets,
                transmit_packets,
                mtu: self.mtu,
                ip_address_count: self.interface.ip_addrs().len(),
                lifecycle: self.lifecycle.as_atom(),
                counters: self.counters,
            },
            output: Vec::new(),
            poll_at: None,
            more: false,
        }
    }

    pub fn apply_bounded_work(&mut self, requested: Work) -> Envelope<Work> {
        let (completed, more) = self.limits.constrain(requested);
        self.counters.observe(completed);

        Envelope {
            result: completed,
            output: Vec::new(),
            poll_at: None,
            more,
        }
    }

    pub fn tcp_open(&mut self, env: Env<'_>) -> Result<Envelope<SocketIdentity>, SocketError> {
        self.ensure_running()?;

        let handle = self.sockets.add(tcp_support::socket());
        let identity = match self.socket_table.insert(SocketKind::Tcp, 0) {
            Ok(identity) => identity,
            Err(error) => {
                self.sockets.remove(handle);
                return Err(error);
            }
        };

        self.tcp_records
            .insert(identity.id, TcpRecord::new(identity, handle));

        Ok(self.finish_call(env, identity, Effects::empty(), Vec::new()))
    }

    pub fn tcp_bind(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        endpoint: TcpEndpoint,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        let endpoint = endpoint.bind_endpoint()?;
        let record = self.tcp_record(identity)?;

        if record.phase != ConnectPhase::Open {
            return Err(SocketError::InvalidState);
        }

        if !endpoint.address.is_unspecified() && !self.has_ipv6_address(endpoint.address) {
            return Err(SocketError::AddressNotAvailable);
        }

        let port = if endpoint.port == 0 {
            self.allocate_ephemeral_port()?
        } else {
            if self.port_in_use(endpoint.port, Some(identity)) {
                return Err(SocketError::AddressInUse);
            }

            endpoint.port
        };

        let local = IpEndpoint::new(IpAddress::Ipv6(endpoint.address), port);
        let record = self.tcp_record_mut(identity)?;
        record.phase = ConnectPhase::Bound;
        record.local = Some(local);
        record.local_scope_id = endpoint.scope_id;

        Ok(self.finish_call(env, crate::atoms::ok(), Effects::empty(), Vec::new()))
    }

    pub fn tcp_connect<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        endpoint: TcpEndpoint,
        pid: LocalPid,
        reference: Reference<'a>,
        now: Instant,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        let remote = endpoint.remote_endpoint()?;

        let record = *self.tcp_record(identity)?;

        match record.phase {
            ConnectPhase::Connected => return Err(SocketError::AlreadyConnected),
            ConnectPhase::Failed(failure) => return Err(failure.socket_error()),
            ConnectPhase::Connecting => {
                if record.remote != Some(remote.ip_endpoint())
                    || record.remote_scope_id != remote.scope_id
                {
                    return Err(SocketError::InvalidState);
                }

                return self.finalize_or_arm_connect(env, identity, pid, reference, now);
            }
            ConnectPhase::Open | ConnectPhase::Bound => {}
        }

        if !self.reachable(remote.address) {
            return Err(SocketError::NetworkUnreachable);
        }

        let local = match record.local {
            Some(local) => local,
            None => IpEndpoint::new(
                IpAddress::Ipv6(Ipv6Address::UNSPECIFIED),
                self.allocate_ephemeral_port()?,
            ),
        };

        if self.port_in_use(local.port, Some(identity)) {
            return Err(SocketError::AddressInUse);
        }

        let local_listen = ValidatedEndpoint {
            address: match local.addr {
                IpAddress::Ipv6(address) => address,
            },
            port: local.port,
            scope_id: record.local_scope_id,
        }
        .listen_endpoint();

        let connect_result = {
            let socket = self.sockets.get_mut::<tcp::Socket<'static>>(record.handle);
            socket.set_timeout(Some(Duration::from_millis(
                tcp_support::CONNECT_TIMEOUT_MILLIS,
            )));
            socket.connect(self.interface.context(), remote.ip_endpoint(), local_listen)
        };

        match connect_result {
            Ok(()) => {}
            Err(ConnectError::InvalidState) => return Err(SocketError::InvalidState),
            Err(ConnectError::Unaddressable) => return Err(SocketError::NetworkUnreachable),
        }

        let selected_local = self
            .sockets
            .get::<tcp::Socket<'static>>(record.handle)
            .local_endpoint()
            .expect("a connecting TCP socket has a local endpoint");
        let remote_endpoint = remote.ip_endpoint();
        let connection_key = ConnectionKey::new(selected_local, remote_endpoint);

        {
            let record = self.tcp_record_mut(identity)?;
            record.phase = ConnectPhase::Connecting;
            record.local = Some(selected_local);
            record.remote = Some(remote_endpoint);
            record.remote_scope_id = remote.scope_id;
        }

        self.tcp_connections.insert(connection_key, identity);
        self.arm_connect(env, identity, record.handle, pid, reference)?;
        let effects = self
            .drive(now, None)
            .expect("TCP connect without ingress cannot fail");
        let result = (crate::atoms::select(), Operation::Connect, reference).encode(env);

        Ok(self.finish_call(env, result, effects, Vec::new()))
    }

    pub fn tcp_sockname(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
    ) -> Result<Envelope<EncodedEndpoint>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        let record = self.tcp_record(identity)?;
        let local = record.local.ok_or(SocketError::NotBound)?;
        let result = EncodedEndpoint::new(local, record.local_scope_id);

        Ok(self.finish_call(env, result, Effects::empty(), Vec::new()))
    }

    pub fn tcp_peername(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
    ) -> Result<Envelope<EncodedEndpoint>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        let record = self.tcp_record(identity)?;

        if !matches!(
            record.phase,
            ConnectPhase::Connecting | ConnectPhase::Connected
        ) {
            return Err(SocketError::NotConnected);
        }

        let remote = record.remote.ok_or(SocketError::NotConnected)?;
        let result = EncodedEndpoint::new(remote, record.remote_scope_id);

        Ok(self.finish_call(env, result, Effects::empty(), Vec::new()))
    }

    pub fn tcp_close(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        now: Instant,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        let record = *self.tcp_record(identity)?;

        self.sockets
            .get_mut::<tcp::Socket<'static>>(record.handle)
            .abort();
        let effects = self
            .drive(now, None)
            .expect("TCP close without ingress cannot fail");
        self.sockets.remove(record.handle);
        self.remove_tcp_record(record);

        let waiters = self.socket_table.close(identity)?;
        let aborts = waiters
            .into_iter()
            .map(|(direction, waiter)| PendingNotification {
                identity,
                direction,
                waiter,
            })
            .collect();

        Ok(self.finish_call(env, crate::atoms::ok(), effects, aborts))
    }

    pub fn ingress(&mut self, packet: &[u8], now: Instant) -> Result<Effects, StackError> {
        self.validate_packet(packet)?;
        let reset = self.inbound_reset(packet);
        self.device
            .enqueue_receive(packet.to_vec())
            .map_err(|_| StackError::OwnershipInvariantViolation)?;
        self.counters.ingress_packets += 1;
        let effects = self.drive(now, Some(packet.len()))?;
        self.record_inbound_reset(reset);
        Ok(effects)
    }

    pub fn poll(&mut self, now: Instant) -> Effects {
        self.counters.poll_calls += 1;
        self.drive(now, None)
            .expect("poll without ingress cannot fail")
    }

    pub fn finish_call<T>(
        &mut self,
        env: Env<'_>,
        result: T,
        mut effects: Effects,
        aborts: Vec<PendingNotification>,
    ) -> Envelope<T> {
        let mut readiness_work = 0usize;

        for notification in aborts {
            debug_assert!(readiness_work < self.limits.ready_events);
            self.send_abort(env, &notification.waiter, notification.identity);
            readiness_work += 1;
        }

        if self.ready.take_overflow() {
            self.ready_sweep = true;
            self.ready_sweep_cursor = None;
        }

        let remaining = self.limits.ready_events.saturating_sub(readiness_work);
        let queued = self.ready.drain(remaining);

        for key in queued {
            self.deliver_ready(env, key);
            readiness_work += 1;
        }

        let remaining = self.limits.ready_events.saturating_sub(readiness_work);

        if self.ready_sweep && remaining > 0 {
            let scan = self
                .socket_table
                .scan_ready(self.ready_sweep_cursor, remaining, remaining);
            let scan_cost = scan.entries_scanned.max(scan.keys.len());

            for key in scan.keys {
                self.deliver_ready(env, key);
            }

            readiness_work += scan_cost;
            self.ready_sweep = !scan.complete;
            self.ready_sweep_cursor = self.ready_sweep.then_some(scan.cursor).flatten();
        }

        effects.more = effects.more || self.ready.has_pending() || self.ready_sweep;
        self.counters.observe(Work {
            ready_events: readiness_work,
            maintenance_work: effects.maintenance_work,
            ..Work::default()
        });

        Envelope {
            result,
            output: effects.output,
            poll_at: effects.poll_at,
            more: effects.more,
        }
    }

    pub fn shutdown(&mut self, env: Env<'_>) -> Envelope<Atom> {
        if matches!(self.lifecycle, Lifecycle::Shutdown) {
            return Envelope::empty(crate::atoms::ok());
        }

        // Linearize shutdown before draining the bounded table. Any socket
        // operation serialized after this point is rejected as closed.
        self.lifecycle = Lifecycle::Shutdown;

        for record in std::mem::take(&mut self.tcp_records).into_values() {
            self.sockets.remove(record.handle);
        }
        self.tcp_connections.clear();

        let waiters = self.socket_table.close_all();
        debug_assert!(waiters.len() <= self.limits.ready_events);

        for (identity, _direction, waiter) in &waiters {
            self.send_abort(env, waiter, *identity);
        }

        self.ready.clear();
        self.ready_sweep = false;
        self.ready_sweep_cursor = None;
        self.counters.observe(Work {
            ready_events: waiters.len(),
            ..Work::default()
        });

        Envelope::empty(crate::atoms::ok())
    }

    #[cfg(debug_assertions)]
    pub fn test_socket_open(
        &mut self,
        env: Env<'_>,
        internal_handle: u64,
    ) -> Result<Envelope<SocketIdentity>, SocketError> {
        self.ensure_running()?;
        let identity = self
            .socket_table
            .insert(SocketKind::Synthetic, internal_handle)?;
        Ok(self.finish_call(env, identity, Effects::empty(), Vec::new()))
    }

    #[cfg(debug_assertions)]
    #[allow(clippy::too_many_arguments)]
    pub fn test_socket_wait<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        direction: Direction,
        operation: Operation,
        pid: LocalPid,
        reference: Reference<'a>,
        arm_point: ArmPoint,
        wake_count: usize,
        completed: bool,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        self.ensure_running()?;

        if operation.direction() != direction {
            return Err(SocketError::InvalidOperation);
        }

        let flag = self
            .socket_table
            .ready_flag(identity, SocketKind::Synthetic, direction)?;
        let key = ReadyKey {
            identity,
            direction,
        };
        let waker = self.ready.waker(key, flag);

        if arm_point == ArmPoint::BeforeTry {
            wake_repeatedly(&waker, wake_count);
        }

        let result = if completed {
            crate::atoms::ready().encode(env)
        } else {
            if arm_point == ArmPoint::BetweenTryAndArm {
                wake_repeatedly(&waker, wake_count);
            }

            let install = self.socket_table.install_waiter(
                env,
                WaiterRegistration {
                    identity,
                    expected_kind: SocketKind::Synthetic,
                    direction,
                    pid,
                    operation,
                    reference,
                },
            )?;

            if arm_point == ArmPoint::AfterArm && install == InstallResult::Armed {
                wake_repeatedly(&waker, wake_count);
            }

            (crate::atoms::select(), operation, reference).encode(env)
        };

        Ok(self.finish_call(env, result, Effects::empty(), Vec::new()))
    }

    #[cfg(debug_assertions)]
    pub fn test_socket_ready(
        &mut self,
        env: Env<'_>,
        keys: Vec<ReadyKey>,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;

        if keys.len() > self.limits.ready_events {
            return Err(SocketError::SystemLimit);
        }

        let mut wakers = Vec::with_capacity(keys.len());

        for key in keys {
            let flag =
                self.socket_table
                    .ready_flag(key.identity, SocketKind::Synthetic, key.direction)?;
            wakers.push(self.ready.waker(key, flag));
        }

        for waker in wakers {
            waker.wake();
        }

        Ok(self.finish_call(env, crate::atoms::ok(), Effects::empty(), Vec::new()))
    }

    #[cfg(debug_assertions)]
    pub fn test_socket_close(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        wake_direction: Option<Direction>,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;

        if let Some(direction) = wake_direction {
            let flag = self
                .socket_table
                .ready_flag(identity, SocketKind::Synthetic, direction)?;
            self.ready
                .waker(
                    ReadyKey {
                        identity,
                        direction,
                    },
                    flag,
                )
                .wake();
        }

        let waiters = self.socket_table.close(identity)?;
        let aborts = waiters
            .into_iter()
            .map(|(direction, waiter)| PendingNotification {
                identity,
                direction,
                waiter,
            })
            .collect();

        Ok(self.finish_call(env, crate::atoms::ok(), Effects::empty(), aborts))
    }

    pub fn cancel<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        operation: Operation,
        reference: Reference<'a>,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;

        let result = match self
            .socket_table
            .cancel(env, identity, operation, reference)?
        {
            CancelResult::Cancelled => crate::atoms::ok(),
            CancelResult::AlreadySent => crate::atoms::already_sent(),
            CancelResult::NotFound => crate::atoms::not_found(),
        };

        Ok(self.finish_call(env, result, Effects::empty(), Vec::new()))
    }

    fn finalize_or_arm_connect<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        pid: LocalPid,
        reference: Reference<'a>,
        now: Instant,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        let record = *self.tcp_record(identity)?;
        let state = self
            .sockets
            .get::<tcp::Socket<'static>>(record.handle)
            .state();

        match state {
            tcp::State::Established => {
                self.sockets
                    .get_mut::<tcp::Socket<'static>>(record.handle)
                    .set_timeout(None);
                self.tcp_record_mut(identity)?.phase = ConnectPhase::Connected;
                let result = crate::atoms::ok().encode(env);
                let effects = self.current_effects(now);
                Ok(self.finish_call(env, result, effects, Vec::new()))
            }
            tcp::State::SynSent | tcp::State::SynReceived => {
                self.arm_connect(env, identity, record.handle, pid, reference)?;
                let effects = self
                    .drive(now, None)
                    .expect("TCP connect retry without ingress cannot fail");
                let result = (crate::atoms::select(), Operation::Connect, reference).encode(env);
                Ok(self.finish_call(env, result, effects, Vec::new()))
            }
            tcp::State::Closed => {
                let failure = match record.phase {
                    ConnectPhase::Failed(failure) => failure,
                    _ => ConnectFailure::TimedOut,
                };
                self.tcp_record_mut(identity)?.phase = ConnectPhase::Failed(failure);
                Err(failure.socket_error())
            }
            _ => Err(SocketError::InvalidState),
        }
    }

    fn arm_connect<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        handle: smoltcp::iface::SocketHandle,
        pid: LocalPid,
        reference: Reference<'a>,
    ) -> Result<(), SocketError> {
        let flag = self
            .socket_table
            .ready_flag(identity, SocketKind::Tcp, Direction::Write)?;
        self.socket_table.install_waiter(
            env,
            WaiterRegistration {
                identity,
                expected_kind: SocketKind::Tcp,
                direction: Direction::Write,
                pid,
                operation: Operation::Connect,
                reference,
            },
        )?;

        let waker = self.ready.waker(
            ReadyKey {
                identity,
                direction: Direction::Write,
            },
            flag,
        );
        self.sockets
            .get_mut::<tcp::Socket<'static>>(handle)
            .register_send_waker(&waker);
        Ok(())
    }

    fn tcp_record(&self, identity: SocketIdentity) -> Result<&TcpRecord, SocketError> {
        self.tcp_records
            .get(&identity.id)
            .filter(|record| record.identity == identity)
            .ok_or(SocketError::InvalidSocket)
    }

    fn tcp_record_mut(&mut self, identity: SocketIdentity) -> Result<&mut TcpRecord, SocketError> {
        self.tcp_records
            .get_mut(&identity.id)
            .filter(|record| record.identity == identity)
            .ok_or(SocketError::InvalidSocket)
    }

    fn remove_tcp_record(&mut self, record: TcpRecord) {
        if let (Some(local), Some(remote)) = (record.local, record.remote) {
            self.tcp_connections
                .remove(&ConnectionKey::new(local, remote));
        }

        self.tcp_records.remove(&record.identity.id);
    }

    fn has_ipv6_address(&self, address: Ipv6Address) -> bool {
        self.interface
            .ip_addrs()
            .iter()
            .any(|cidr| cidr.address() == IpAddress::Ipv6(address))
    }

    fn reachable(&self, address: Ipv6Address) -> bool {
        let ip_address = IpAddress::Ipv6(address);

        self.interface
            .ip_addrs()
            .iter()
            .any(|cidr| cidr.contains_addr(&ip_address))
            || self
                .route_prefixes
                .iter()
                .any(|cidr| cidr.contains_addr(&address))
    }

    fn port_in_use(&self, port: u16, excluding: Option<SocketIdentity>) -> bool {
        self.tcp_records.values().any(|record| {
            Some(record.identity) != excluding
                && record.local.is_some_and(|local| local.port == port)
        })
    }

    fn allocate_ephemeral_port(&mut self) -> Result<u16, SocketError> {
        let used = self
            .tcp_records
            .values()
            .filter_map(|record| record.local.map(|local| local.port))
            .collect::<BTreeSet<_>>();
        let port = tcp_support::allocate_ephemeral(
            &used,
            self.next_ephemeral_port,
            tcp_support::EPHEMERAL_PORT_FIRST,
            tcp_support::EPHEMERAL_PORT_LAST,
        )?;
        self.next_ephemeral_port = if port == tcp_support::EPHEMERAL_PORT_LAST {
            tcp_support::EPHEMERAL_PORT_FIRST
        } else {
            port + 1
        };
        Ok(port)
    }

    fn current_effects(&mut self, now: Instant) -> Effects {
        Effects {
            output: Vec::new(),
            poll_at: self
                .interface
                .poll_at(now, &self.sockets)
                .map(|instant| instant.total_millis()),
            more: false,
            maintenance_work: 0,
        }
    }

    fn inbound_reset(&self, packet: &[u8]) -> Option<(SocketIdentity, ConnectFailure)> {
        let ipv6 = Ipv6Packet::new_checked(packet).ok()?;
        let mut next_header = ipv6.next_header();
        let mut transport = ipv6.payload();

        if next_header == IpProtocol::HopByHop {
            let extension = Ipv6ExtHeader::new_checked(transport).ok()?;
            next_header = extension.next_header();
            let header_len = (usize::from(extension.header_len()) + 1) * 8;
            transport = transport.get(header_len..)?;
        }

        if next_header != IpProtocol::Tcp {
            return None;
        }

        let tcp = TcpPacket::new_checked(transport).ok()?;

        if !tcp.rst() {
            return None;
        }

        let key = ConnectionKey {
            local_address: ipv6.dst_addr().octets(),
            local_port: tcp.dst_port(),
            remote_address: ipv6.src_addr().octets(),
            remote_port: tcp.src_port(),
        };
        let identity = *self.tcp_connections.get(&key)?;
        let record = self.tcp_record(identity).ok()?;
        let failure = match self
            .sockets
            .get::<tcp::Socket<'static>>(record.handle)
            .state()
        {
            tcp::State::Established => ConnectFailure::Reset,
            tcp::State::SynSent | tcp::State::SynReceived => ConnectFailure::Refused,
            _ => return None,
        };

        Some((identity, failure))
    }

    fn record_inbound_reset(&mut self, reset: Option<(SocketIdentity, ConnectFailure)>) {
        let Some((identity, failure)) = reset else {
            return;
        };
        let Ok(record) = self.tcp_record(identity).copied() else {
            return;
        };

        if self
            .sockets
            .get::<tcp::Socket<'static>>(record.handle)
            .state()
            == tcp::State::Closed
            && let Ok(record) = self.tcp_record_mut(identity)
        {
            record.phase = ConnectPhase::Failed(failure);
        }
    }

    fn ensure_running(&self) -> Result<(), SocketError> {
        match self.lifecycle {
            Lifecycle::Running => Ok(()),
            Lifecycle::Shutdown => Err(SocketError::Closed),
        }
    }

    fn deliver_ready(&mut self, env: Env<'_>, key: ReadyKey) {
        match self.socket_table.take_ready_waiter(key) {
            ReadyResult::Notify(waiter) => {
                let delivered = env
                    .send(
                        &waiter.pid,
                        (
                            crate::atoms::smol_socket(),
                            (key.identity.id, key.identity.generation),
                            crate::atoms::select(),
                            waiter.reference(env),
                        ),
                    )
                    .is_ok();

                self.observe_notification(delivered);
                self.socket_table
                    .remember_sent(key.identity, key.direction, waiter);
            }
            ReadyResult::NoWaiter | ReadyResult::Coalesced | ReadyResult::Stale => {
                self.counters.readiness_dropped += 1;
            }
        }
    }

    fn send_abort(&mut self, env: Env<'_>, waiter: &Waiter, identity: SocketIdentity) {
        let delivered = env
            .send(
                &waiter.pid,
                (
                    crate::atoms::smol_socket(),
                    (identity.id, identity.generation),
                    crate::atoms::abort(),
                    waiter.reference(env),
                    crate::atoms::closed(),
                ),
            )
            .is_ok();

        self.observe_notification(delivered);
    }

    fn observe_notification(&mut self, delivered: bool) {
        if delivered {
            self.counters.notifications_delivered += 1;
        } else {
            self.counters.notifications_dropped += 1;
        }
    }

    fn drive(&mut self, now: Instant, ingress_bytes: Option<usize>) -> Result<Effects, StackError> {
        self.device.begin_call(self.limits.output_packets);
        let input_bytes = ingress_bytes.unwrap_or(0);
        let output_byte_limit = self.limits.bytes_copied.saturating_sub(input_bytes);
        let mut output = self
            .device
            .take_transmit(self.limits.output_packets, output_byte_limit);
        let mut output_bytes: usize = output.iter().map(Vec::len).sum();

        if self.device.has_receive() {
            let _ = self
                .interface
                .poll_ingress_single(now, &mut self.device, &mut self.sockets);
        }

        self.interface.poll_maintenance(now);

        let mut maintenance_work = 0usize;
        let mut egress_may_remain = false;

        while maintenance_work < self.limits.maintenance_work
            && self.device.queued_packets().1 < self.limits.output_packets
        {
            maintenance_work += 1;

            if matches!(
                self.interface
                    .poll_egress(now, &mut self.device, &mut self.sockets),
                PollResult::None
            ) {
                egress_may_remain = false;
                break;
            }

            egress_may_remain = true;
        }

        let remaining_packets = self.limits.output_packets.saturating_sub(output.len());
        let remaining_bytes = output_byte_limit.saturating_sub(output_bytes);
        let additional_output = self
            .device
            .take_transmit(remaining_packets, remaining_bytes);
        output_bytes += additional_output.iter().map(Vec::len).sum::<usize>();
        output.extend(additional_output);
        let output_packets = output.len();
        let more = self.device.has_receive() || self.device.has_transmit() || egress_may_remain;
        let poll_at = if more {
            Some(now.total_millis())
        } else {
            self.interface
                .poll_at(now, &self.sockets)
                .map(|instant| instant.total_millis())
        };

        self.counters.observe(Work {
            bytes_copied: input_bytes + output_bytes,
            output_packets,
            ready_events: 0,
            maintenance_work,
        });
        self.counters.emitted_packets += output_packets;

        Ok(Effects {
            output,
            poll_at,
            more,
            maintenance_work,
        })
    }

    fn validate_packet(&mut self, packet: &[u8]) -> Result<(), StackError> {
        if packet.len() < 40 {
            self.counters.rejected_packets += 1;
            return Err(StackError::InvalidPacket);
        }

        match packet[0] >> 4 {
            4 => {
                self.counters.rejected_packets += 1;
                return Err(StackError::UnsupportedFamily);
            }
            6 => {}
            _ => {
                self.counters.rejected_packets += 1;
                return Err(StackError::InvalidPacket);
            }
        }

        let payload_length = usize::from(u16::from_be_bytes([packet[4], packet[5]]));
        let declared_length = 40usize
            .checked_add(payload_length)
            .ok_or(StackError::InvalidPacket)?;

        if declared_length != packet.len() {
            self.counters.rejected_packets += 1;
            return Err(StackError::InvalidPacket);
        }

        if packet.len() > self.mtu || packet.len() > self.limits.bytes_copied {
            self.counters.rejected_packets += 1;
            return Err(StackError::PacketTooLarge);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, NifMap)]
pub struct StackConfig {
    mtu: usize,
    addresses: Vec<AddressConfig>,
    routes: Vec<RouteConfig>,
}

impl StackConfig {
    fn validate(&self, limits: Limits) -> Result<(), StackError> {
        if !(1_280..=65_575).contains(&self.mtu)
            || self.mtu > limits.bytes_copied
            || self.addresses.len() > 8
            || self.routes.len() > 4
            || self.addresses.iter().any(|address| !address.valid())
            || self.routes.iter().any(|route| !route.valid())
        {
            return Err(StackError::InvalidStackConfig);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, NifMap)]
struct AddressConfig {
    address: Vec<u8>,
    prefix_length: u8,
}

impl AddressConfig {
    fn valid(&self) -> bool {
        self.address.len() == 16 && self.prefix_length <= 128 && !multicast(&self.address)
    }

    fn to_cidr(&self) -> Ipv6Cidr {
        Ipv6Cidr::new(ipv6_address(&self.address), self.prefix_length)
    }
}

#[derive(Clone, Debug, NifMap)]
struct RouteConfig {
    destination: Vec<u8>,
    prefix_length: u8,
    gateway: Vec<u8>,
}

impl RouteConfig {
    fn valid(&self) -> bool {
        self.destination.len() == 16
            && self.gateway.len() == 16
            && self.prefix_length <= 128
            && !multicast(&self.destination)
            && !multicast(&self.gateway)
            && !unspecified(&self.gateway)
    }

    fn to_route(&self) -> Route {
        Route {
            cidr: IpCidr::Ipv6(self.to_cidr()),
            via_router: IpAddress::Ipv6(ipv6_address(&self.gateway)),
            preferred_until: None,
            expires_at: None,
        }
    }

    fn to_cidr(&self) -> Ipv6Cidr {
        Ipv6Cidr::new(ipv6_address(&self.destination), self.prefix_length)
    }
}

fn ipv6_address(bytes: &[u8]) -> Ipv6Address {
    let octets: [u8; 16] = bytes.try_into().expect("validated IPv6 address length");
    Ipv6Address::from_octets(octets)
}

fn multicast(bytes: &[u8]) -> bool {
    bytes.first() == Some(&0xff)
}

fn unspecified(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

#[derive(Debug, PartialEq, Eq)]
pub enum StackError {
    InvalidLimits,
    InvalidStackConfig,
    InvalidPacket,
    UnsupportedFamily,
    PacketTooLarge,
    OwnershipInvariantViolation,
}

pub struct Effects {
    pub output: Vec<Vec<u8>>,
    pub poll_at: Option<i64>,
    pub more: bool,
    maintenance_work: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ConnectionKey {
    local_address: [u8; 16],
    local_port: u16,
    remote_address: [u8; 16],
    remote_port: u16,
}

impl ConnectionKey {
    fn new(local: IpEndpoint, remote: IpEndpoint) -> Self {
        let IpAddress::Ipv6(local_address) = local.addr;
        let IpAddress::Ipv6(remote_address) = remote.addr;

        Self {
            local_address: local_address.octets(),
            local_port: local.port,
            remote_address: remote_address.octets(),
            remote_port: remote.port,
        }
    }
}

impl Effects {
    fn empty() -> Self {
        Self {
            output: Vec::new(),
            poll_at: None,
            more: false,
            maintenance_work: 0,
        }
    }
}

pub struct PendingNotification {
    identity: SocketIdentity,
    #[allow(dead_code)]
    direction: Direction,
    waiter: Waiter,
}

#[derive(Clone, Copy)]
enum Lifecycle {
    Running,
    Shutdown,
}

impl Lifecycle {
    fn as_atom(self) -> Atom {
        match self {
            Self::Running => crate::atoms::running(),
            Self::Shutdown => crate::atoms::shutdown(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, NifMap)]
pub struct Counters {
    max_bytes_copied: usize,
    max_output_packets: usize,
    max_ready_events: usize,
    max_maintenance_work: usize,
    ingress_packets: usize,
    rejected_packets: usize,
    emitted_packets: usize,
    poll_calls: usize,
    notifications_delivered: usize,
    notifications_dropped: usize,
    readiness_dropped: usize,
}

impl Counters {
    fn observe(&mut self, work: Work) {
        self.max_bytes_copied = self.max_bytes_copied.max(work.bytes_copied);
        self.max_output_packets = self.max_output_packets.max(work.output_packets);
        self.max_ready_events = self.max_ready_events.max(work.ready_events);
        self.max_maintenance_work = self.max_maintenance_work.max(work.maintenance_work);
    }
}

#[derive(NifMap)]
pub struct Envelope<T> {
    pub result: T,
    pub output: Vec<Vec<u8>>,
    pub poll_at: Option<i64>,
    pub more: bool,
}

impl<T> Envelope<T> {
    pub fn empty(result: T) -> Self {
        Self {
            result,
            output: Vec::new(),
            poll_at: None,
            more: false,
        }
    }
}

impl Envelope<ResourceArc<StackResource>> {
    pub fn created(resource: ResourceArc<StackResource>) -> Self {
        Self {
            result: resource,
            output: Vec::new(),
            poll_at: None,
            more: false,
        }
    }
}

#[derive(NifMap)]
pub struct Snapshot {
    id: u64,
    limits: Limits,
    socket_count: usize,
    native_socket_count: usize,
    tcp_socket_count: usize,
    tcp_buffer_bytes: usize,
    waiter_count: usize,
    read_waiter_count: usize,
    write_waiter_count: usize,
    ready_count: usize,
    ready_overflow_pending: bool,
    readiness: ReadinessCounters,
    receive_packets: usize,
    transmit_packets: usize,
    mtu: usize,
    ip_address_count: usize,
    lifecycle: Atom,
    counters: Counters,
}

#[derive(NifMap)]
pub struct ResourceCounts {
    created: usize,
    dropped: usize,
    active: usize,
}

fn wake_repeatedly(waker: &std::task::Waker, count: usize) {
    for _ in 0..count {
        waker.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use smoltcp::time::Instant;

    use super::{NativeStack, StackConfig, StackResource};
    use crate::limits::Limits;

    const LIMITS: Limits = Limits {
        bytes_copied: 1_280,
        output_packets: 1,
        ready_events: 1,
        maintenance_work: 1,
    };

    fn config() -> StackConfig {
        StackConfig {
            mtu: 1_280,
            addresses: Vec::new(),
            routes: Vec::new(),
        }
    }

    #[test]
    fn contention_never_waits_for_the_mutex() {
        let resource = StackResource {
            inner: Mutex::new(NativeStack::new(LIMITS, config(), Instant::ZERO).unwrap()),
        };
        assert_eq!(resource.test_contention(), Err(()));
    }

    #[test]
    fn separate_resources_have_separate_stack_ids() {
        let first = NativeStack::new(LIMITS, config(), Instant::ZERO).unwrap();
        let second = NativeStack::new(LIMITS, config(), Instant::ZERO).unwrap();

        assert_ne!(first.id, second.id);
    }
}
