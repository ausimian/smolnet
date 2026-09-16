use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, TryLockError};

use rustler::{
    Atom, Encoder, Env, LocalPid, NewBinary, NifMap, Reference, Resource, ResourceArc, Term,
};
use smoltcp::iface::{Config, Interface, PollResult, Route, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, ConnectError, ListenError};
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
    self as tcp_support, ConnectFailure, ConnectPhase, EncodedEndpoint, ListenerRecord,
    ShutdownHow, TcpEndpoint, TcpRecord, ValidatedEndpoint,
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
    tcp_listeners: BTreeMap<u64, ListenerRecord>,
    tcp_connections: BTreeMap<ConnectionKey, SocketIdentity>,
    closing_tcp: BTreeSet<u64>,
    closing_deadlines: BTreeSet<(Instant, u64)>,
    closing_sweep_cursor: Option<u64>,
    closing_cleanup_pending: bool,
    closing_cleanup_resweep: bool,
    maintenance_cleanup_turn: bool,
    next_ephemeral_port: u16,
    listener_scan_cursor: Option<ListenerMemberKey>,
    listener_scan_pending: bool,
    listener_scan_resweep: bool,
    listener_maintenance_turn: bool,
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
            tcp_listeners: BTreeMap::new(),
            tcp_connections: BTreeMap::new(),
            closing_tcp: BTreeSet::new(),
            closing_deadlines: BTreeSet::new(),
            closing_sweep_cursor: None,
            closing_cleanup_pending: false,
            closing_cleanup_resweep: false,
            maintenance_cleanup_turn: false,
            next_ephemeral_port: tcp_support::EPHEMERAL_PORT_FIRST,
            listener_scan_cursor: None,
            listener_scan_pending: false,
            listener_scan_resweep: false,
            listener_maintenance_turn: true,
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
        let listener_pool_socket_count = self
            .tcp_listeners
            .values()
            .map(|listener| listener.pool.len())
            .sum();
        let listener_pool_target_count = self
            .tcp_listeners
            .values()
            .map(|listener| listener.pool_target)
            .sum();
        let accepted_queue_count = self
            .tcp_listeners
            .values()
            .map(|listener| listener.accepted.len())
            .sum();
        let listener_backlog_capacity = self
            .tcp_listeners
            .values()
            .map(|listener| listener.backlog)
            .sum();

        Envelope {
            result: Snapshot {
                id: self.id,
                limits: self.limits,
                socket_count: self.socket_table.len(),
                native_socket_count: self.sockets.iter().count(),
                tcp_socket_count: self.tcp_records.len(),
                tcp_listener_count: self.tcp_listeners.len(),
                listener_pool_socket_count,
                listener_pool_target_count,
                accepted_queue_count,
                listener_backlog_capacity,
                closing_tcp_socket_count: self.closing_tcp.len(),
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

        if self.tcp_records.len() >= self.limits.ready_events {
            return Err(SocketError::SystemLimit);
        }

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

    pub fn socket_validate(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        Ok(self.finish_call(env, crate::atoms::ok(), Effects::empty(), Vec::new()))
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

    pub fn tcp_listen(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        backlog: usize,
        now: Instant,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;

        if !(1..=tcp_support::LISTENER_BACKLOG_MAX).contains(&backlog) {
            return Err(SocketError::InvalidBacklog);
        }

        if self.tcp_listeners.contains_key(&identity.id) {
            return Err(SocketError::InvalidState);
        }

        let record = *self.tcp_record(identity)?;

        if record.phase != ConnectPhase::Bound {
            return Err(SocketError::NotBound);
        }

        let local = record.local.ok_or(SocketError::NotBound)?;
        let IpAddress::Ipv6(address) = local.addr;
        let endpoint = ValidatedEndpoint {
            address,
            port: local.port,
            scope_id: record.local_scope_id,
        };
        let pool_target = backlog.min(tcp_support::LISTENER_POOL_MAX);
        let mut handles = vec![record.handle];

        for _ in 1..pool_target {
            handles.push(self.sockets.add(tcp_support::socket()));
        }

        for handle in &handles {
            let socket = self.sockets.get_mut::<tcp::Socket<'static>>(*handle);
            socket.set_timeout(Some(Duration::from_millis(
                tcp_support::CONNECT_TIMEOUT_MILLIS,
            )));

            match socket.listen(endpoint.listen_endpoint()) {
                Ok(()) => {}
                Err(ListenError::InvalidState) => return Err(SocketError::InvalidState),
                Err(ListenError::Unaddressable) => return Err(SocketError::InvalidAddress),
            }
        }

        self.remove_tcp_record(record);
        self.tcp_listeners.insert(
            identity.id,
            ListenerRecord::new(identity, endpoint, local, backlog, handles),
        );
        self.request_listener_scan();

        let effects = self
            .drive(now, None)
            .expect("TCP listen without ingress cannot fail");
        Ok(self.finish_call(env, crate::atoms::ok(), effects, Vec::new()))
    }

    pub fn tcp_accept<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        pid: LocalPid,
        reference: Reference<'a>,
        now: Instant,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;

        if self
            .tcp_listeners
            .get(&identity.id)
            .is_none_or(|listener| listener.identity != identity)
        {
            return if self.tcp_records.contains_key(&identity.id) {
                Err(SocketError::InvalidState)
            } else {
                Err(SocketError::InvalidSocket)
            };
        }

        if self
            .socket_table
            .has_waiter(identity, SocketKind::Tcp, Direction::Read)?
        {
            return Err(SocketError::Busy);
        }

        self.request_listener_scan();
        let (scan_work, scan_more) = self.refresh_listeners(self.limits.maintenance_work);
        let accepted = self
            .tcp_listeners
            .get_mut(&identity.id)
            .and_then(|listener| listener.accepted.pop_front());

        let result = if let Some(child) = accepted {
            (crate::atoms::ok(), child).encode(env)
        } else {
            self.arm_listener_waiter(env, identity, pid, reference)?;
            (crate::atoms::select(), Operation::Accept, reference).encode(env)
        };

        let mut effects = self.current_effects(now);
        effects.more |= scan_more;
        effects.maintenance_work = scan_work;
        Ok(self.finish_call(env, result, effects, Vec::new()))
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

        if let Some(listener) = self
            .tcp_listeners
            .get(&identity.id)
            .filter(|listener| listener.identity == identity)
        {
            let result = EncodedEndpoint::new(listener.local, listener.endpoint.scope_id);
            return Ok(self.finish_call(env, result, Effects::empty(), Vec::new()));
        }

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

        if self
            .tcp_listeners
            .get(&identity.id)
            .is_some_and(|listener| listener.identity == identity)
        {
            return Err(SocketError::NotConnected);
        }

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

    pub fn tcp_send<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        data: &[u8],
        pid: LocalPid,
        reference: Reference<'a>,
        now: Instant,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        let record = *self.tcp_record(identity)?;

        if self
            .socket_table
            .has_waiter(identity, SocketKind::Tcp, Direction::Write)?
        {
            return Err(SocketError::Busy);
        }

        self.ensure_connected(record)?;

        if record.write_shutdown {
            return Err(SocketError::Closed);
        }

        let copy_limit = data.len().min(self.limits.bytes_copied);
        let needs_waiter = {
            let socket = self.sockets.get::<tcp::Socket<'static>>(record.handle);

            if !socket.may_send() {
                return Err(SocketError::Closed);
            }

            copy_limit.min(socket.send_capacity() - socket.send_queue()) < data.len()
        };

        if needs_waiter {
            self.socket_table.ensure_waiter_capacity()?;
        }

        let (accepted, immediately_writable) = {
            let socket = self.sockets.get_mut::<tcp::Socket<'static>>(record.handle);

            let accepted = socket
                .send_slice(&data[..copy_limit])
                .map_err(|_| SocketError::Closed)?;
            (accepted, socket.can_send())
        };

        let result = if accepted == data.len() {
            crate::atoms::ok().encode(env)
        } else {
            self.arm_tcp_waiter(
                env,
                identity,
                record.handle,
                Direction::Write,
                Operation::Send,
                pid,
                reference,
                immediately_writable,
            )?;
            (crate::atoms::select(), Operation::Send, reference, accepted).encode(env)
        };
        let effects = self
            .drive(now, Some(accepted))
            .expect("TCP send without ingress cannot fail");

        Ok(self.finish_call(env, result, effects, Vec::new()))
    }

    pub fn tcp_recv<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        length: usize,
        pid: LocalPid,
        reference: Reference<'a>,
        now: Instant,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;
        let record = *self.tcp_record(identity)?;

        if self
            .socket_table
            .has_waiter(identity, SocketKind::Tcp, Direction::Read)?
        {
            return Err(SocketError::Busy);
        }

        self.ensure_connected(record)?;

        if record.read_shutdown {
            return Err(SocketError::Closed);
        }

        let requested = if length == 0 {
            self.limits.bytes_copied
        } else {
            length.min(self.limits.bytes_copied)
        };
        let needs_waiter = {
            let socket = self.sockets.get::<tcp::Socket<'static>>(record.handle);
            let read_length = requested.min(socket.recv_queue());
            let remaining_queue = socket.recv_queue() - read_length;
            let receive_open = matches!(
                socket.state(),
                tcp::State::Established | tcp::State::FinWait1 | tcp::State::FinWait2
            ) || remaining_queue > 0;
            let complete = if length == 0 {
                read_length > 0
            } else {
                read_length == length || (!receive_open && read_length > 0)
            };

            !complete && receive_open
        };

        if needs_waiter {
            self.socket_table.ensure_waiter_capacity()?;
        }

        let (data, receive_open, immediately_readable) = {
            let socket = self.sockets.get_mut::<tcp::Socket<'static>>(record.handle);
            let read_length = requested.min(socket.recv_queue());
            let mut data = vec![0; read_length];

            if read_length > 0 {
                let received = socket
                    .recv_slice(&mut data)
                    .map_err(|_| SocketError::Closed)?;
                data.truncate(received);
            }

            (data, socket.may_recv(), socket.can_recv())
        };
        let copied = data.len();
        let complete = if length == 0 {
            copied > 0
        } else {
            copied == length || (!receive_open && copied > 0)
        };

        let result = if complete {
            let binary: Term<'a> = NewBinary::from_iter(env, data.into_iter()).into();
            (crate::atoms::ok(), binary).encode(env)
        } else if !receive_open {
            return Err(SocketError::EndOfStream);
        } else {
            self.arm_tcp_waiter(
                env,
                identity,
                record.handle,
                Direction::Read,
                Operation::Recv,
                pid,
                reference,
                immediately_readable,
            )?;

            if data.is_empty() {
                (crate::atoms::select(), Operation::Recv, reference).encode(env)
            } else {
                let binary: Term<'a> = NewBinary::from_iter(env, data.into_iter()).into();
                (crate::atoms::select(), Operation::Recv, reference, binary).encode(env)
            }
        };
        let effects = self
            .drive(now, Some(copied))
            .expect("TCP receive without ingress cannot fail");

        Ok(self.finish_call(env, result, effects, Vec::new()))
    }

    pub fn tcp_shutdown(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        how: ShutdownHow,
        now: Instant,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;

        let record = *self.tcp_record(identity)?;
        self.ensure_connected(record)?;
        let shutdown_read = matches!(how, ShutdownHow::Read | ShutdownHow::ReadWrite);
        let shutdown_write = matches!(how, ShutdownHow::Write | ShutdownHow::ReadWrite);
        let mut aborts = Vec::with_capacity(2);

        for direction in [Direction::Read, Direction::Write] {
            let selected = match direction {
                Direction::Read => shutdown_read,
                Direction::Write => shutdown_write,
            };

            if selected
                && let Some(waiter) =
                    self.socket_table
                        .take_waiter(identity, SocketKind::Tcp, direction)?
            {
                aborts.push(PendingNotification {
                    identity,
                    direction,
                    waiter,
                });
            }
        }

        if shutdown_write && !record.write_shutdown {
            self.sockets
                .get_mut::<tcp::Socket<'static>>(record.handle)
                .close();
        }

        let record = self.tcp_record_mut(identity)?;
        record.read_shutdown |= shutdown_read;
        record.write_shutdown |= shutdown_write;

        let effects = self
            .drive(now, None)
            .expect("TCP shutdown without ingress cannot fail");
        Ok(self.finish_call(env, crate::atoms::ok(), effects, aborts))
    }

    pub fn tcp_close(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        now: Instant,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Tcp)?;

        if self
            .tcp_listeners
            .get(&identity.id)
            .is_some_and(|listener| listener.identity == identity)
        {
            return self.tcp_listener_close(env, identity, now);
        }

        let record = *self.tcp_record(identity)?;
        let waiters = self.socket_table.close(identity)?;
        let aborts = waiters
            .into_iter()
            .map(|(direction, waiter)| PendingNotification {
                identity,
                direction,
                waiter,
            })
            .collect();

        let graceful = record.phase == ConnectPhase::Connected;

        if graceful {
            self.sockets
                .get_mut::<tcp::Socket<'static>>(record.handle)
                .close();
            let record = self.tcp_record_mut(identity)?;
            let deadline = now + Duration::from_millis(tcp_support::CLOSE_TIMEOUT_MILLIS);
            record.close_deadline = Some(deadline);
            self.closing_tcp.insert(identity.id);
            self.closing_deadlines.insert((deadline, identity.id));
            self.closing_sweep_cursor = None;
            self.request_closing_cleanup();
            self.maintenance_cleanup_turn = false;
        } else {
            self.sockets
                .get_mut::<tcp::Socket<'static>>(record.handle)
                .abort();
        }

        let effects = self
            .drive(now, None)
            .expect("TCP close without ingress cannot fail");

        if !graceful {
            self.remove_tcp_socket(record);
        }

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
        for listener in std::mem::take(&mut self.tcp_listeners).into_values() {
            for handle in listener.pool {
                self.sockets.remove(handle);
            }
        }
        self.tcp_connections.clear();
        self.closing_tcp.clear();
        self.closing_deadlines.clear();
        self.closing_sweep_cursor = None;
        self.closing_cleanup_pending = false;
        self.closing_cleanup_resweep = false;
        self.listener_scan_cursor = None;
        self.listener_scan_pending = false;
        self.listener_scan_resweep = false;

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

    fn arm_listener_waiter<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        pid: LocalPid,
        reference: Reference<'a>,
    ) -> Result<(), SocketError> {
        let handles = self
            .tcp_listeners
            .get(&identity.id)
            .filter(|listener| listener.identity == identity)
            .map(|listener| listener.pool.iter().copied().collect::<Vec<_>>())
            .ok_or(SocketError::InvalidSocket)?;
        let immediately_ready = self
            .tcp_listeners
            .get(&identity.id)
            .is_some_and(|listener| !listener.accepted.is_empty())
            || handles.iter().any(|handle| {
                matches!(
                    self.sockets.get::<tcp::Socket<'static>>(*handle).state(),
                    tcp::State::Established | tcp::State::CloseWait
                )
            });
        let flag = self
            .socket_table
            .ready_flag(identity, SocketKind::Tcp, Direction::Read)?;

        self.socket_table.install_waiter(
            env,
            WaiterRegistration {
                identity,
                expected_kind: SocketKind::Tcp,
                direction: Direction::Read,
                pid,
                operation: Operation::Accept,
                reference,
            },
        )?;

        let waker = self.ready.waker(
            ReadyKey {
                identity,
                direction: Direction::Read,
            },
            flag,
        );

        for handle in handles {
            self.sockets
                .get_mut::<tcp::Socket<'static>>(handle)
                .register_recv_waker(&waker);
        }

        if immediately_ready {
            waker.wake_by_ref();
        }

        Ok(())
    }

    fn arm_connect<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        handle: smoltcp::iface::SocketHandle,
        pid: LocalPid,
        reference: Reference<'a>,
    ) -> Result<(), SocketError> {
        self.arm_tcp_waiter(
            env,
            identity,
            handle,
            Direction::Write,
            Operation::Connect,
            pid,
            reference,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn arm_tcp_waiter<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        handle: smoltcp::iface::SocketHandle,
        direction: Direction,
        operation: Operation,
        pid: LocalPid,
        reference: Reference<'a>,
        immediately_ready: bool,
    ) -> Result<(), SocketError> {
        let flag = self
            .socket_table
            .ready_flag(identity, SocketKind::Tcp, direction)?;
        self.socket_table.install_waiter(
            env,
            WaiterRegistration {
                identity,
                expected_kind: SocketKind::Tcp,
                direction,
                pid,
                operation,
                reference,
            },
        )?;

        let waker = self.ready.waker(
            ReadyKey {
                identity,
                direction,
            },
            flag,
        );
        let socket = self.sockets.get_mut::<tcp::Socket<'static>>(handle);

        match direction {
            Direction::Read => socket.register_recv_waker(&waker),
            Direction::Write => socket.register_send_waker(&waker),
        }

        if immediately_ready {
            waker.wake_by_ref();
        }

        Ok(())
    }

    fn ensure_connected(&self, record: TcpRecord) -> Result<(), SocketError> {
        match record.phase {
            ConnectPhase::Connected => Ok(()),
            ConnectPhase::Failed(failure) => Err(failure.socket_error()),
            ConnectPhase::Open | ConnectPhase::Bound | ConnectPhase::Connecting => {
                Err(SocketError::NotConnected)
            }
        }
    }

    fn tcp_record(&self, identity: SocketIdentity) -> Result<&TcpRecord, SocketError> {
        if self
            .tcp_listeners
            .get(&identity.id)
            .is_some_and(|listener| listener.identity == identity)
        {
            return Err(SocketError::InvalidState);
        }

        self.tcp_records
            .get(&identity.id)
            .filter(|record| record.identity == identity)
            .ok_or(SocketError::InvalidSocket)
    }

    fn tcp_record_mut(&mut self, identity: SocketIdentity) -> Result<&mut TcpRecord, SocketError> {
        if self
            .tcp_listeners
            .get(&identity.id)
            .is_some_and(|listener| listener.identity == identity)
        {
            return Err(SocketError::InvalidState);
        }

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

    fn remove_tcp_socket(&mut self, record: TcpRecord) {
        self.sockets.remove(record.handle);
        self.closing_tcp.remove(&record.identity.id);
        if let Some(deadline) = record.close_deadline {
            self.closing_deadlines
                .remove(&(deadline, record.identity.id));
        }
        self.remove_tcp_record(record);
        self.request_listener_scan();
    }

    fn tcp_listener_close(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        now: Instant,
    ) -> Result<Envelope<Atom>, SocketError> {
        let listener = self
            .tcp_listeners
            .remove(&identity.id)
            .filter(|listener| listener.identity == identity)
            .ok_or(SocketError::InvalidSocket)?;
        let waiters = self.socket_table.close(identity)?;
        let mut aborts = waiters
            .into_iter()
            .map(|(direction, waiter)| PendingNotification {
                identity,
                direction,
                waiter,
            })
            .collect::<Vec<_>>();

        for handle in listener.pool {
            self.sockets.remove(handle);
        }

        for child in listener.accepted {
            if let Ok(waiters) = self.socket_table.close(child) {
                aborts.extend(
                    waiters
                        .into_iter()
                        .map(|(direction, waiter)| PendingNotification {
                            identity: child,
                            direction,
                            waiter,
                        }),
                );
            }

            if let Ok(record) = self.tcp_record(child).copied() {
                self.sockets.remove(record.handle);
                self.remove_tcp_record(record);
            }
        }

        self.listener_scan_cursor = None;
        self.listener_scan_pending = !self.tcp_listeners.is_empty();
        self.listener_scan_resweep = false;
        let effects = self
            .drive(now, None)
            .expect("TCP listener close without ingress cannot fail");
        Ok(self.finish_call(env, crate::atoms::ok(), effects, aborts))
    }

    fn request_listener_scan(&mut self) {
        if self.tcp_listeners.is_empty() {
            return;
        }

        if self.listener_scan_pending {
            self.listener_scan_resweep = true;
        } else {
            self.listener_scan_pending = true;
            self.listener_scan_cursor = None;
        }
    }

    fn refresh_listeners(&mut self, limit: usize) -> (usize, bool) {
        if self.tcp_listeners.is_empty() {
            self.listener_scan_cursor = None;
            self.listener_scan_pending = false;
            self.listener_scan_resweep = false;
            return (0, false);
        }

        if limit == 0 || !self.listener_scan_pending {
            return (0, self.listener_scan_pending);
        }

        let cursor = self.listener_scan_cursor;
        let listener_start = cursor.map_or(Unbounded, |key| Included(key.listener_id));
        let mut keys = Vec::with_capacity(limit.saturating_add(1));

        'listeners: for (&listener_id, listener) in
            self.tcp_listeners.range((listener_start, Unbounded))
        {
            let member_start = cursor
                .filter(|key| key.listener_id == listener_id)
                .map_or(Unbounded, |key| Excluded(key.handle));

            for &handle in listener.pool.range((member_start, Unbounded)) {
                keys.push(ListenerMemberKey {
                    listener_id,
                    handle,
                });

                if keys.len() > limit {
                    break 'listeners;
                }
            }
        }

        let mut more = keys.len() > limit;
        keys.truncate(limit);
        let work = keys.len();

        for key in &keys {
            self.refresh_listener_member(*key);
        }

        if more {
            self.listener_scan_cursor = keys.last().copied();
        } else if self.listener_scan_resweep {
            self.listener_scan_cursor = None;
            self.listener_scan_resweep = false;
            more = true;
        } else {
            self.listener_scan_cursor = None;
            self.listener_scan_pending = false;
        }

        (work, more)
    }

    fn refresh_listener_member(&mut self, key: ListenerMemberKey) {
        let Some(mut listener) = self.tcp_listeners.remove(&key.listener_id) else {
            return;
        };

        if !listener.pool.contains(&key.handle) {
            self.tcp_listeners.insert(key.listener_id, listener);
            return;
        }

        let socket = self.sockets.get::<tcp::Socket<'static>>(key.handle);
        let state = socket.state();
        let endpoints = socket.local_endpoint().zip(socket.remote_endpoint());

        if matches!(state, tcp::State::Established | tcp::State::CloseWait)
            && let Some((local, remote)) = endpoints
        {
            let can_queue = listener.accepted.len() < listener.backlog;
            let child = can_queue
                .then(|| self.socket_table.insert(SocketKind::Tcp, 0))
                .transpose();

            match child {
                Ok(Some(child)) => {
                    self.sockets
                        .get_mut::<tcp::Socket<'static>>(key.handle)
                        .set_timeout(None);
                    let record = TcpRecord::accepted(
                        child,
                        key.handle,
                        local,
                        remote,
                        listener.endpoint.scope_id,
                    );
                    self.tcp_connections
                        .insert(ConnectionKey::new(local, remote), child);
                    self.tcp_records.insert(child.id, record);
                    listener.accepted.push_back(child);
                    listener.pool.remove(&key.handle);
                    self.add_listener_member(&mut listener);
                    self.counters.listener_promotions += 1;
                    self.mark_listener_ready(listener.identity);
                }
                Ok(None) | Err(SocketError::SystemLimit) => {
                    self.recycle_listener_member(&mut listener, key.handle);
                    self.counters.listener_overflow_drops += 1;
                }
                Err(_error) => unreachable!("TCP child identity allocation has one failure mode"),
            }
        } else if !matches!(state, tcp::State::Listen | tcp::State::SynReceived) {
            self.recycle_listener_member(&mut listener, key.handle);
        }

        self.tcp_listeners.insert(key.listener_id, listener);
    }

    fn recycle_listener_member(&mut self, listener: &mut ListenerRecord, handle: SocketHandle) {
        listener.pool.remove(&handle);
        self.sockets.remove(handle);
        self.add_listener_member(listener);
    }

    fn add_listener_member(&mut self, listener: &mut ListenerRecord) {
        let handle = self.sockets.add(tcp_support::socket());
        let socket = self.sockets.get_mut::<tcp::Socket<'static>>(handle);
        socket.set_timeout(Some(Duration::from_millis(
            tcp_support::CONNECT_TIMEOUT_MILLIS,
        )));
        socket
            .listen(listener.endpoint.listen_endpoint())
            .expect("validated listener endpoint remains listenable");
        listener.pool.insert(handle);
        self.counters.listener_refills += 1;
    }

    fn mark_listener_ready(&mut self, identity: SocketIdentity) {
        if let Ok(flag) = self
            .socket_table
            .ready_flag(identity, SocketKind::Tcp, Direction::Read)
        {
            self.ready
                .waker(
                    ReadyKey {
                        identity,
                        direction: Direction::Read,
                    },
                    flag,
                )
                .wake();
        }
    }

    fn reap_closing_tcp(&mut self, now: Instant, limit: usize) -> (usize, bool) {
        if self.closing_tcp.is_empty() {
            self.closing_sweep_cursor = None;
            self.closing_cleanup_pending = false;
            self.closing_cleanup_resweep = false;
            return (0, false);
        }

        if limit == 0 || !self.closing_cleanup_pending {
            return (0, self.closing_cleanup_pending);
        }

        let start = self.closing_sweep_cursor.map_or(Unbounded, Excluded);
        let mut ids = self
            .closing_tcp
            .range((start, Unbounded))
            .take(limit + 1)
            .copied()
            .collect::<Vec<_>>();
        let mut more = ids.len() > limit;
        ids.truncate(limit);
        let work = ids.len();

        for id in &ids {
            let Some(record) = self.tcp_records.get(id).copied() else {
                self.closing_tcp.remove(id);
                continue;
            };

            let state = self
                .sockets
                .get::<tcp::Socket<'static>>(record.handle)
                .state();
            let reset_dispatched = self
                .sockets
                .get::<tcp::Socket<'static>>(record.handle)
                .local_endpoint()
                .is_none();

            if state == tcp::State::Closed && reset_dispatched {
                self.remove_tcp_socket(record);
            } else if record
                .close_deadline
                .is_some_and(|deadline| now >= deadline)
            {
                if state != tcp::State::Closed {
                    self.sockets
                        .get_mut::<tcp::Socket<'static>>(record.handle)
                        .abort();
                }
                let record = self
                    .tcp_records
                    .get_mut(id)
                    .expect("closing TCP record still exists");
                let deadline = record
                    .close_deadline
                    .expect("expired closing TCP record has a deadline");
                self.closing_deadlines.remove(&(deadline, *id));
                record.close_deadline = None;
            }
        }

        if self.closing_tcp.is_empty() {
            self.closing_sweep_cursor = None;
            self.closing_cleanup_pending = false;
            self.closing_cleanup_resweep = false;
            return (work, false);
        }

        if more {
            self.closing_sweep_cursor = ids.last().copied();
        } else if self.closing_cleanup_resweep {
            self.closing_sweep_cursor = None;
            self.closing_cleanup_resweep = false;
            more = true;
        } else {
            self.closing_sweep_cursor = None;
            self.closing_cleanup_pending = false;
        }

        (work, more)
    }

    fn next_close_deadline(&self) -> Option<Instant> {
        self.closing_deadlines
            .first()
            .map(|(deadline, _id)| *deadline)
    }

    fn request_closing_cleanup(&mut self) {
        if self.closing_tcp.is_empty() {
            return;
        }

        if self.closing_cleanup_pending {
            self.closing_cleanup_resweep = true;
        } else {
            self.closing_cleanup_pending = true;
            self.closing_sweep_cursor = None;
        }
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
                && !record.accepted
                && record.local.is_some_and(|local| local.port == port)
        }) || self
            .tcp_listeners
            .values()
            .any(|listener| Some(listener.identity) != excluding && listener.local.port == port)
    }

    fn allocate_ephemeral_port(&mut self) -> Result<u16, SocketError> {
        let used = self
            .tcp_records
            .values()
            .filter_map(|record| record.local.map(|local| local.port))
            .chain(
                self.tcp_listeners
                    .values()
                    .map(|listener| listener.local.port),
            )
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

    fn drive(&mut self, now: Instant, copied_bytes: Option<usize>) -> Result<Effects, StackError> {
        self.device.begin_call(self.limits.output_packets);
        let input_bytes = copied_bytes.unwrap_or(0);
        let output_byte_limit = self.limits.bytes_copied.saturating_sub(input_bytes);
        let mut output = self
            .device
            .take_transmit(self.limits.output_packets, output_byte_limit);
        let mut output_bytes: usize = output.iter().map(Vec::len).sum();

        if self.device.has_receive() {
            let _ = self
                .interface
                .poll_ingress_single(now, &mut self.device, &mut self.sockets);
            self.request_closing_cleanup();
            self.request_listener_scan();
        }

        self.interface.poll_maintenance(now);

        if self
            .next_close_deadline()
            .is_some_and(|deadline| now >= deadline)
        {
            self.request_closing_cleanup();
        }

        let mut maintenance_work = 0usize;
        let mut egress_may_remain = false;
        let mut cleanup_more = false;
        let mut cleanup_ran = false;
        let mut listener_more = false;
        let mut listener_ran = false;
        let mut egress_attempted = false;

        if self.maintenance_cleanup_turn && self.closing_cleanup_pending {
            let cleanup_limit = self.limits.maintenance_work.min(1);
            let (cleanup_work, more) = self.reap_closing_tcp(now, cleanup_limit);
            maintenance_work += cleanup_work;
            cleanup_more = more;
            cleanup_ran = cleanup_work > 0;
        }

        let remaining_maintenance = self
            .limits
            .maintenance_work
            .saturating_sub(maintenance_work);

        if self.listener_scan_pending
            && remaining_maintenance > 0
            && (remaining_maintenance > 1 || self.listener_maintenance_turn)
        {
            let listener_limit = if remaining_maintenance == 1 {
                1
            } else {
                (remaining_maintenance / 2).max(1)
            };
            let (listener_work, more) = self.refresh_listeners(listener_limit);
            maintenance_work += listener_work;
            listener_more = more;
            listener_ran = listener_work > 0;
        }

        while maintenance_work < self.limits.maintenance_work
            && self.device.queued_packets().1 < self.limits.output_packets
        {
            maintenance_work += 1;
            egress_attempted = true;

            match self
                .interface
                .poll_egress(now, &mut self.device, &mut self.sockets)
            {
                PollResult::None => {
                    egress_may_remain = false;
                    break;
                }
                PollResult::SocketStateChanged => {
                    egress_may_remain = true;
                    self.request_closing_cleanup();
                    self.request_listener_scan();
                }
            }
        }

        if !egress_attempted && maintenance_work == self.limits.maintenance_work {
            egress_may_remain = true;
        }

        let remaining_packets = self.limits.output_packets.saturating_sub(output.len());
        let remaining_bytes = output_byte_limit.saturating_sub(output_bytes);
        let additional_output = self
            .device
            .take_transmit(remaining_packets, remaining_bytes);
        output_bytes += additional_output.iter().map(Vec::len).sum::<usize>();
        output.extend(additional_output);
        if !cleanup_ran && self.closing_cleanup_pending {
            let remaining_maintenance = self
                .limits
                .maintenance_work
                .saturating_sub(maintenance_work);
            let (cleanup_work, more) = self.reap_closing_tcp(now, remaining_maintenance);
            maintenance_work += cleanup_work;
            cleanup_more |= more;
            cleanup_ran = cleanup_work > 0;
        }

        if !listener_ran && self.listener_scan_pending {
            let remaining_maintenance = self
                .limits
                .maintenance_work
                .saturating_sub(maintenance_work);
            let (listener_work, more) = self.refresh_listeners(remaining_maintenance);
            maintenance_work += listener_work;
            listener_more |= more;
            listener_ran = listener_work > 0;
        }

        self.maintenance_cleanup_turn = !cleanup_ran && self.closing_cleanup_pending;
        self.listener_maintenance_turn = !listener_ran && self.listener_scan_pending;
        let output_packets = output.len();
        let more = self.device.has_receive()
            || self.device.has_transmit()
            || egress_may_remain
            || cleanup_more
            || self.closing_cleanup_pending
            || listener_more
            || self.listener_scan_pending;
        let poll_at = if more {
            Some(now.total_millis())
        } else {
            self.interface
                .poll_at(now, &self.sockets)
                .into_iter()
                .chain(self.next_close_deadline())
                .min()
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ListenerMemberKey {
    listener_id: u64,
    handle: SocketHandle,
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
    listener_promotions: usize,
    listener_refills: usize,
    listener_overflow_drops: usize,
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
    tcp_listener_count: usize,
    listener_pool_socket_count: usize,
    listener_pool_target_count: usize,
    accepted_queue_count: usize,
    listener_backlog_capacity: usize,
    closing_tcp_socket_count: usize,
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
