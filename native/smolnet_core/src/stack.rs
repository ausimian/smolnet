use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, TryLockError};

use rustler::{
    Atom, Decoder, Encoder, Env, LocalPid, NewBinary, NifMap, NifResult, Reference, Resource,
    ResourceArc, Term,
};
use smoltcp::iface::{Config, Interface, PollResult, Route, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, ConnectError, ListenError};
use smoltcp::socket::udp::{self, BindError as UdpBindError, SendError as UdpSendError};
use smoltcp::time::Duration;
use smoltcp::time::Instant;
use smoltcp::wire::{
    HardwareAddress, IpAddress, IpCidr, IpEndpoint, IpListenEndpoint, IpProtocol, Ipv4Address,
    Ipv4Cidr, Ipv4Packet, Ipv6Address, Ipv6Cidr, Ipv6ExtHeader, Ipv6Packet, TcpPacket,
};

use crate::budget::{CALL_TARGET, CHARGE_CHUNK, CallBudget, ENCODING_HEADROOM, WORK_BUDGET};
use crate::decode_bounded_list;
use crate::device::{BeamDevice, OutputPacket};
use crate::limits::{Limits, Work};
#[cfg(debug_assertions)]
use crate::socket_table::InstallResult;
use crate::socket_table::{
    CancelResult, ReadyResult, SocketError, SocketKind, SocketTable, WaiterRegistration,
};
use crate::tcp::{
    self as tcp_support, AddressFamily, ConnectFailure, ConnectPhase, EncodedEndpoint,
    ListenerRecord, ShutdownHow, TcpBufferSizes, TcpEndpoint, TcpRecord, ValidatedEndpoint,
};
use crate::udp::{self as udp_support, UdpRecord};
#[cfg(debug_assertions)]
use crate::waiter::ArmPoint;
use crate::waiter::{
    Direction, Operation, ReadinessCounters, ReadyKey, ReadyQueue, SocketIdentity, Waiter,
};

static NEXT_STACK_ID: AtomicU64 = AtomicU64::new(1);
static CREATED: AtomicUsize = AtomicUsize::new(0);
static DROPPED: AtomicUsize = AtomicUsize::new(0);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
pub const NATIVE_SOCKET_CAPACITY: usize = 64;

pub struct StackResource {
    inner: Mutex<Option<NativeStack>>,
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
            inner: Mutex::new(Some(stack)),
        });

        CREATED.fetch_add(1, Ordering::Relaxed);
        ACTIVE.fetch_add(1, Ordering::Relaxed);
        Ok(resource)
    }

    pub fn with_stack<T>(&self, operation: impl FnOnce(&mut NativeStack) -> T) -> Result<T, ()> {
        match self.inner.try_lock() {
            Ok(mut guard) => {
                let stack = guard.as_mut().ok_or(())?;
                stack.begin_call();
                let result = operation(stack);
                stack.observe_call();
                Ok(result)
            }
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => Err(()),
        }
    }

    pub fn with_stack_unobserved<T>(
        &self,
        operation: impl FnOnce(&mut NativeStack) -> T,
    ) -> Result<T, ()> {
        match self.inner.try_lock() {
            Ok(mut guard) => guard.as_mut().map(operation).ok_or(()),
            Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => Err(()),
        }
    }

    #[cfg(debug_assertions)]
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
        let inner = self
            .inner
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(inner);
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
    udp_records: BTreeMap<u64, UdpRecord>,
    closing_tcp: BTreeSet<u64>,
    closing_deadlines: BTreeSet<(Instant, u64)>,
    closing_sweep_cursor: Option<u64>,
    closing_cleanup_pending: bool,
    closing_cleanup_resweep: bool,
    maintenance_cleanup_turn: bool,
    scheduled_poll_at: Option<Instant>,
    next_ephemeral_port: u16,
    next_udp_ephemeral_port: u16,
    listener_scan_cursor: Option<ListenerMemberKey>,
    listener_scan_pending: bool,
    listener_scan_resweep: bool,
    listener_maintenance_turn: bool,
    ready: ReadyQueue,
    ready_sweep: bool,
    ready_sweep_cursor: Option<ReadyKey>,
    ready_sweep_pending: VecDeque<ReadyKey>,
    counters: Counters,
    lifecycle: Lifecycle,
    limits: Limits,
    mtu: usize,
    route_prefixes: Vec<IpCidr>,
    call_budget: CallBudget,
    next_forced_budget_checkpoints: Option<usize>,
    next_forced_slice_exhaustion: Option<usize>,
    pending_notifications: VecDeque<PendingNotification>,
}

impl NativeStack {
    fn new(limits: Limits, stack_config: StackConfig, now: Instant) -> Result<Self, StackError> {
        stack_config.validate(limits)?;
        let route_prefixes = stack_config
            .routes
            .iter()
            .map(RouteConfig::to_cidr)
            .collect();

        let mut device = BeamDevice::new(stack_config.mtu, limits.input_packets);
        let mut interface_config = Config::new(HardwareAddress::Ip);
        interface_config.random_seed = NEXT_STACK_ID.fetch_add(1, Ordering::Relaxed);
        let id = interface_config.random_seed;
        let mut interface = Interface::new(interface_config, &mut device, now);

        interface.update_ip_addrs(|addresses| {
            for address in &stack_config.addresses {
                addresses
                    .push(address.to_cidr())
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
            udp_records: BTreeMap::new(),
            closing_tcp: BTreeSet::new(),
            closing_deadlines: BTreeSet::new(),
            closing_sweep_cursor: None,
            closing_cleanup_pending: false,
            closing_cleanup_resweep: false,
            maintenance_cleanup_turn: false,
            scheduled_poll_at: None,
            next_ephemeral_port: tcp_support::EPHEMERAL_PORT_FIRST,
            next_udp_ephemeral_port: tcp_support::EPHEMERAL_PORT_FIRST,
            listener_scan_cursor: None,
            listener_scan_pending: false,
            listener_scan_resweep: false,
            listener_maintenance_turn: true,
            ready: ReadyQueue::new((limits.ready_events / 2).max(1)),
            ready_sweep: false,
            ready_sweep_cursor: None,
            ready_sweep_pending: VecDeque::new(),
            counters: Counters::default(),
            lifecycle: Lifecycle::Running,
            limits,
            mtu: stack_config.mtu,
            route_prefixes,
            call_budget: CallBudget::start(None),
            next_forced_budget_checkpoints: None,
            next_forced_slice_exhaustion: None,
            pending_notifications: VecDeque::new(),
        })
    }

    fn begin_call(&mut self) {
        self.call_budget = CallBudget::start(self.next_forced_budget_checkpoints.take());

        if let Some(charges) = self.next_forced_slice_exhaustion.take() {
            self.call_budget.force_slice_exhaustion_after(charges);
        }
    }

    fn observe_call(&mut self) {
        self.counters.native_calls += 1;
        self.counters.max_native_work_nanoseconds = self
            .counters
            .max_native_work_nanoseconds
            .max(self.call_budget.elapsed_nanoseconds());

        if self.call_budget.yielded() {
            self.counters.deadline_yields += 1;
        }

        // A call may now charge the caller several times. The counter keeps its
        // established meaning: the number of calls in which any charge reported
        // the caller's reduction slice as exhausted.
        if self.call_budget.slice_exhausted() {
            self.counters.timeslice_exhaustions += 1;
        }
    }

    /// Charges the caller once a whole chunk of work units has accumulated.
    ///
    /// `force` flushes whatever is outstanding, including the time spent in
    /// work that is not unit-counted, and is used for the final charge of a
    /// call.
    fn charge_chunk(&mut self, env: Env<'_>, pending: &mut usize, force: bool) {
        if !force && *pending < CHARGE_CHUNK {
            return;
        }

        *pending = 0;
        self.call_budget.charge(env);
    }

    fn within_budget(&mut self) -> bool {
        self.call_budget.checkpoint()
    }

    pub fn snapshot(&self) -> Envelope<Snapshot> {
        let (receive_packets, transmit_packets) = self.device.queued_packets();
        let (read_waiters, write_waiters) = self.socket_table.waiter_counts();
        let (read_sent_waiters, write_sent_waiters) = self.socket_table.sent_waiter_counts();
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
        let mut socket_buffer_bytes = self
            .tcp_records
            .values()
            .map(|record| TcpSocketBufferBytes {
                id: record.identity.id,
                generation: record.identity.generation,
                rcvbuf: record.rcvbuf,
                sndbuf: record.sndbuf,
            })
            .chain(
                self.tcp_listeners
                    .values()
                    .map(|listener| TcpSocketBufferBytes {
                        id: listener.identity.id,
                        generation: listener.identity.generation,
                        rcvbuf: listener.rcvbuf,
                        sndbuf: listener.sndbuf,
                    }),
            )
            .collect::<Vec<_>>();
        socket_buffer_bytes.sort_by_key(|buffer| buffer.id);

        Envelope {
            result: Snapshot {
                id: self.id,
                limits: self.limits,
                socket_count: self.socket_table.len(),
                native_socket_count: self.sockets.iter().count(),
                native_socket_capacity: NATIVE_SOCKET_CAPACITY,
                tcp_socket_count: self.tcp_records.len(),
                tcp_listener_count: self.tcp_listeners.len(),
                udp_socket_count: self.udp_records.len(),
                listener_pool_socket_count,
                listener_pool_target_count,
                accepted_queue_count,
                listener_backlog_capacity,
                closing_tcp_socket_count: self.closing_tcp.len(),
                tcp_buffer_bytes: TcpBufferBytes {
                    default_rcvbuf: tcp_support::DEFAULT_BUFFER_BYTES,
                    default_sndbuf: tcp_support::DEFAULT_BUFFER_BYTES,
                    sockets: socket_buffer_bytes,
                },
                udp_packet_capacity: udp_support::PACKET_CAPACITY,
                udp_payload_bytes: udp_support::PAYLOAD_BYTES,
                udp_max_datagram_bytes: udp_support::max_datagram_bytes(
                    self.mtu,
                    AddressFamily::Inet6,
                ),
                udp_ipv4_max_datagram_bytes: udp_support::max_datagram_bytes(
                    self.mtu,
                    AddressFamily::Inet,
                ),
                waiter_count: self.socket_table.waiter_count(),
                read_waiter_count: read_waiters,
                write_waiter_count: write_waiters,
                sent_waiter_count: read_sent_waiters + write_sent_waiters,
                read_sent_waiter_count: read_sent_waiters,
                write_sent_waiter_count: write_sent_waiters,
                ready_count: self.ready.len() + self.ready_sweep_pending.len(),
                ready_overflow_pending: self.ready_sweep
                    || !self.ready_sweep_pending.is_empty()
                    || self.ready.has_pending(),
                readiness: self.ready.counters(),
                receive_packets,
                transmit_packets,
                mtu: self.mtu,
                ip_address_count: self.interface.ip_addrs().len(),
                lifecycle: self.lifecycle.as_atom(),
                call_target_nanoseconds: CALL_TARGET.as_nanos() as u64,
                work_budget_nanoseconds: WORK_BUDGET.as_nanos() as u64,
                encoding_headroom_nanoseconds: ENCODING_HEADROOM.as_nanos() as u64,
                counters: self.counters,
            },
            output: Vec::new(),
            poll_at: None,
            more: false,
        }
    }

    #[cfg(debug_assertions)]
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

    #[cfg(debug_assertions)]
    pub fn test_set_budget_checkpoints(&mut self, checkpoints: usize) -> Envelope<Atom> {
        self.next_forced_budget_checkpoints = Some(checkpoints);
        Envelope::empty(crate::atoms::ok())
    }

    #[cfg(debug_assertions)]
    pub fn test_set_slice_exhaustion(&mut self, charges: usize) -> Envelope<Atom> {
        self.next_forced_slice_exhaustion = Some(charges);
        Envelope::empty(crate::atoms::ok())
    }

    #[cfg(debug_assertions)]
    pub fn test_maximum_work(&mut self) -> Envelope<Work> {
        let completed = Work {
            bytes_copied: self.limits.bytes_copied,
            input_packets: self.limits.input_packets,
            output_packets: self.limits.output_packets,
            ready_events: self.limits.ready_events,
            maintenance_work: self.limits.maintenance_work,
        };
        let packet_count = completed.output_packets;
        let base_size = completed.bytes_copied / packet_count;
        let larger_packets = completed.bytes_copied % packet_count;

        let output = (0..packet_count)
            .map(|index| OutputPacket::zeroed(base_size + usize::from(index < larger_packets)))
            .collect();

        self.counters.observe(completed);

        Envelope {
            result: completed,
            output,
            poll_at: None,
            more: false,
        }
    }

    #[cfg(debug_assertions)]
    pub fn test_combined_maximum_work(
        &mut self,
        env: Env<'_>,
        keys: Vec<ReadyKey>,
        now: Instant,
    ) -> Result<Envelope<Work>, SocketError> {
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

        let completed = Work {
            bytes_copied: self.limits.bytes_copied,
            input_packets: self.limits.input_packets,
            output_packets: self.limits.output_packets,
            ready_events: self.limits.ready_events,
            maintenance_work: self.limits.maintenance_work,
        };
        let packet_count = completed.output_packets;
        let base_size = completed.bytes_copied / packet_count;
        let larger_packets = completed.bytes_copied % packet_count;
        let mut effects = self.poll(now);
        effects.output = (0..packet_count)
            .map(|index| OutputPacket::zeroed(base_size + usize::from(index < larger_packets)))
            .collect();
        self.counters.observe(Work {
            bytes_copied: completed.bytes_copied,
            output_packets: completed.output_packets,
            ..Work::default()
        });

        Ok(self.finish_call(env, completed, effects, Vec::new()))
    }

    #[cfg(debug_assertions)]
    pub fn test_prepare_closing(&mut self, count: usize) -> Result<Envelope<Atom>, SocketError> {
        if count > self.limits.maintenance_work || count > self.limits.ready_events {
            return Err(SocketError::SystemLimit);
        }

        let now = Instant::ZERO;
        let deadline = now + Duration::from_millis(tcp_support::CLOSE_TIMEOUT_MILLIS);

        for _index in 0..count {
            self.ensure_logical_socket_capacity()?;
            self.ensure_native_socket_capacity(1, 0)?;
            let handle = self.sockets.add(tcp_support::default_socket());
            let identity = match self.socket_table.insert(SocketKind::Tcp, 0) {
                Ok(identity) => identity,
                Err(error) => {
                    self.sockets.remove(handle);
                    return Err(error);
                }
            };
            let mut record = TcpRecord::new(
                identity,
                handle,
                AddressFamily::Inet6,
                tcp_support::DEFAULT_BUFFER_BYTES,
                tcp_support::DEFAULT_BUFFER_BYTES,
            );
            record.phase = ConnectPhase::Connected;
            record.close_deadline = Some(deadline);
            self.tcp_records.insert(identity.id, record);
            let _waiters = self.socket_table.close(identity)?;
            self.sockets.get_mut::<tcp::Socket<'static>>(handle).close();
            self.closing_tcp.insert(identity.id);
            self.closing_deadlines.insert((deadline, identity.id));
        }

        self.closing_sweep_cursor = None;
        self.request_closing_cleanup();
        self.maintenance_cleanup_turn = false;
        Ok(Envelope::empty(crate::atoms::ok()))
    }

    #[cfg(debug_assertions)]
    pub fn test_prepare_maximum_drop(&mut self) -> Result<Envelope<Atom>, SocketError> {
        self.device
            .test_fill_transmit(self.limits.output_packets, self.mtu)
            .map_err(|()| SocketError::InvalidState)?;
        Ok(Envelope::empty(crate::atoms::ok()))
    }

    pub fn tcp_open(
        &mut self,
        env: Env<'_>,
        family: AddressFamily,
        rcvbuf: usize,
        sndbuf: usize,
    ) -> Result<Envelope<SocketIdentity>, SocketError> {
        self.ensure_running()?;
        self.ensure_logical_socket_capacity()?;
        self.ensure_native_socket_capacity(1, 0)?;

        let handle = self.sockets.add(tcp_support::socket(rcvbuf, sndbuf)?);
        let identity = match self.socket_table.insert(SocketKind::Tcp, 0) {
            Ok(identity) => identity,
            Err(error) => {
                self.sockets.remove(handle);
                return Err(error);
            }
        };

        self.tcp_records.insert(
            identity.id,
            TcpRecord::new(identity, handle, family, rcvbuf, sndbuf),
        );

        Ok(self.finish_call(env, identity, Effects::empty(), Vec::new()))
    }

    pub fn socket_validate(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
    ) -> Result<Envelope<SocketKind>, SocketError> {
        self.ensure_running()?;
        let kind = self.socket_table.validate_any(identity)?.kind;
        Ok(self.finish_call(env, kind, Effects::empty(), Vec::new()))
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

        if !record.family.matches(endpoint.address) {
            return Err(SocketError::InvalidAddress);
        }

        if !endpoint.address.is_unspecified() && !self.has_ip_address(endpoint.address) {
            return Err(SocketError::AddressNotAvailable);
        }

        let port = if endpoint.port == 0 {
            self.allocate_ephemeral_port(record.family)?
        } else {
            if self.port_in_use(endpoint.port, record.family, Some(identity)) {
                return Err(SocketError::AddressInUse);
            }

            endpoint.port
        };

        let local = IpEndpoint::new(endpoint.address, port);
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
        let endpoint = ValidatedEndpoint {
            address: local.addr,
            port: local.port,
            scope_id: record.local_scope_id,
        };
        let listen_endpoint = self.concrete_listener_endpoint(endpoint)?;
        let pool_target = backlog.min(tcp_support::LISTENER_POOL_MAX);
        self.ensure_native_socket_capacity(pool_target.saturating_sub(1), 0)?;
        let mut handles = vec![record.handle];

        for _ in 1..pool_target {
            handles.push(
                self.sockets
                    .add(tcp_support::socket(record.rcvbuf, record.sndbuf)?),
            );
        }

        for handle in &handles {
            let socket = self.sockets.get_mut::<tcp::Socket<'static>>(*handle);
            socket.set_timeout(Some(Duration::from_millis(
                tcp_support::CONNECT_TIMEOUT_MILLIS,
            )));

            match socket.listen(listen_endpoint) {
                Ok(()) => {}
                Err(ListenError::InvalidState) => return Err(SocketError::InvalidState),
                Err(ListenError::Unaddressable) => return Err(SocketError::InvalidAddress),
            }
        }

        self.remove_tcp_record(record);
        self.tcp_listeners.insert(
            identity.id,
            ListenerRecord::new(
                identity,
                endpoint,
                listen_endpoint,
                local,
                backlog,
                handles,
                TcpBufferSizes {
                    rcvbuf: record.rcvbuf,
                    sndbuf: record.sndbuf,
                },
            ),
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
            let family = self.tcp_record(child)?.family;
            (crate::atoms::ok(), child, family).encode(env)
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

        if !record.family.matches(remote.address) {
            return Err(SocketError::InvalidAddress);
        }

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
                record.family.unspecified(),
                self.allocate_ephemeral_port(record.family)?,
            ),
        };

        if self.port_in_use(local.port, record.family, Some(identity)) {
            return Err(SocketError::AddressInUse);
        }

        let local_listen = ValidatedEndpoint {
            address: local.addr,
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

    pub fn udp_open(
        &mut self,
        env: Env<'_>,
        family: AddressFamily,
    ) -> Result<Envelope<SocketIdentity>, SocketError> {
        self.ensure_running()?;
        self.ensure_logical_socket_capacity()?;
        self.ensure_native_socket_capacity(1, 0)?;

        let handle = self.sockets.add(udp_support::socket());
        let identity = match self.socket_table.insert(SocketKind::Udp, 0) {
            Ok(identity) => identity,
            Err(error) => {
                self.sockets.remove(handle);
                return Err(error);
            }
        };

        self.udp_records
            .insert(identity.id, UdpRecord::new(identity, handle, family));

        Ok(self.finish_call(env, identity, Effects::empty(), Vec::new()))
    }

    pub fn udp_bind(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        endpoint: TcpEndpoint,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Udp)?;
        let mut endpoint = endpoint.bind_endpoint()?;
        let record = self.udp_record(identity)?.clone();

        if record.local.is_some() {
            return Err(SocketError::InvalidState);
        }

        if !record.family.matches(endpoint.address) {
            return Err(SocketError::InvalidAddress);
        }

        if !endpoint.address.is_unspecified() && !self.has_ip_address(endpoint.address) {
            return Err(SocketError::AddressNotAvailable);
        }

        endpoint.port = if endpoint.port == 0 {
            self.allocate_udp_ephemeral_port(record.family)?
        } else {
            if self.udp_port_in_use(endpoint.port, record.family, Some(identity)) {
                return Err(SocketError::AddressInUse);
            }
            endpoint.port
        };

        let addresses = if endpoint.address.is_unspecified() {
            self.interface
                .ip_addrs()
                .iter()
                .map(IpCidr::address)
                .filter(|address| record.family.matches(*address) && !address.is_unspecified())
                .collect::<Vec<_>>()
        } else {
            vec![endpoint.address]
        };

        if addresses.is_empty() {
            return Err(SocketError::AddressNotAvailable);
        }

        debug_assert_eq!(record.handles.len(), 1);
        let primary_handle = record.primary_handle();
        let mut addresses = addresses.into_iter();
        let primary_address = addresses.next().expect("non-empty addresses checked");
        let additional_addresses = addresses.collect::<Vec<_>>();
        self.ensure_native_socket_capacity(additional_addresses.len(), 0)?;
        let mut additional_sockets = Vec::with_capacity(additional_addresses.len());

        for address in additional_addresses {
            let mut socket = udp_support::socket();
            let bind_result = socket.bind(IpListenEndpoint {
                addr: Some(address),
                port: endpoint.port,
            });

            match bind_result {
                Ok(()) => additional_sockets.push(socket),
                Err(UdpBindError::InvalidState) => return Err(SocketError::InvalidState),
                Err(UdpBindError::Unaddressable) => return Err(SocketError::InvalidAddress),
            }
        }

        self.sockets
            .get_mut::<udp::Socket<'static>>(primary_handle)
            .bind(IpListenEndpoint {
                addr: Some(primary_address),
                port: endpoint.port,
            })
            .map_err(|error| match error {
                UdpBindError::InvalidState => SocketError::InvalidState,
                UdpBindError::Unaddressable => SocketError::InvalidAddress,
            })?;

        let mut handles = vec![primary_handle];
        handles.extend(
            additional_sockets
                .into_iter()
                .map(|socket| self.sockets.add(socket)),
        );

        debug_assert!(self.sockets.iter().count() <= NATIVE_SOCKET_CAPACITY);

        let record = self.udp_record_mut(identity)?;
        record.handles = handles;
        record.local = Some(endpoint);
        record.receive_cursor = 0;
        Ok(self.finish_call(env, crate::atoms::ok(), Effects::empty(), Vec::new()))
    }

    pub fn udp_connect(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        endpoint: TcpEndpoint,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Udp)?;
        let remote = endpoint.remote_endpoint()?;
        let record = self.udp_record(identity)?.clone();

        if record.local.is_none() {
            return Err(SocketError::NotBound);
        }

        if !record.family.matches(remote.address) {
            return Err(SocketError::InvalidAddress);
        }

        if !self.reachable(remote.address) {
            return Err(SocketError::NetworkUnreachable);
        }

        self.udp_record_mut(identity)?.peer = Some(remote);
        Ok(self.finish_call(env, crate::atoms::ok(), Effects::empty(), Vec::new()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn udp_sendto<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        endpoint: TcpEndpoint,
        data: &[u8],
        pid: LocalPid,
        reference: Reference<'a>,
        now: Instant,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Udp)?;

        if self
            .socket_table
            .has_waiter(identity, SocketKind::Udp, Direction::Write)?
        {
            return Err(SocketError::Busy);
        }

        let remote = endpoint.remote_endpoint()?;
        let record = self.udp_record(identity)?.clone();

        if record.local.is_none() {
            return Err(SocketError::NotBound);
        }

        if !record.family.matches(remote.address) {
            return Err(SocketError::InvalidAddress);
        }

        if record.peer.is_some_and(|peer| peer != remote) {
            return Err(SocketError::InvalidState);
        }

        if !self.reachable(remote.address) {
            return Err(SocketError::NetworkUnreachable);
        }

        if data.len() > udp_support::max_datagram_bytes(self.mtu, record.family) {
            return Err(SocketError::MessageTooLarge);
        }

        let local = record
            .local
            .expect("a UDP record checked as bound has a local endpoint");
        let source_address = if local.address.is_unspecified() {
            self.interface
                .get_source_address(&remote.address)
                .filter(|address| record.family.matches(*address))
                .ok_or(SocketError::NetworkUnreachable)?
        } else {
            local.address
        };
        let metadata = udp::UdpMetadata {
            endpoint: remote.ip_endpoint(),
            local_address: Some(source_address),
            meta: Default::default(),
        };
        let send_result = self
            .sockets
            .get_mut::<udp::Socket<'static>>(record.primary_handle())
            .send_slice(data, metadata);

        match send_result {
            Ok(()) => {
                let effects = self
                    .drive(now, Some(data.len()))
                    .expect("UDP send without ingress cannot fail");
                Ok(self.finish_call(env, crate::atoms::ok().encode(env), effects, Vec::new()))
            }
            Err(UdpSendError::BufferFull) => {
                self.arm_udp_waiter(
                    env,
                    identity,
                    &[record.primary_handle()],
                    Direction::Write,
                    Operation::Sendto,
                    pid,
                    reference,
                    false,
                )?;
                let result = (crate::atoms::select(), Operation::Sendto, reference).encode(env);
                let effects = self
                    .drive(now, None)
                    .expect("UDP send retry without ingress cannot fail");
                Ok(self.finish_call(env, result, effects, Vec::new()))
            }
            Err(UdpSendError::Unaddressable) => Err(SocketError::InvalidAddress),
        }
    }

    pub fn udp_recvfrom<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        length: usize,
        pid: LocalPid,
        reference: Reference<'a>,
        now: Instant,
    ) -> Result<Envelope<Term<'a>>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Udp)?;

        if self
            .socket_table
            .has_waiter(identity, SocketKind::Udp, Direction::Read)?
        {
            return Err(SocketError::Busy);
        }

        let record = self.udp_record(identity)?.clone();
        let local = record.local.ok_or(SocketError::NotBound)?;
        let mut received = None;
        let mut inspected = 0;
        let handle_count = record.handles.len();
        let start = record.receive_cursor % handle_count;
        let mut next_receive_cursor = start;

        for offset in 0..handle_count {
            let handle_index = (start + offset) % handle_count;
            let handle = record.handles[handle_index];
            while inspected < udp_support::PACKET_CAPACITY {
                let next = {
                    let socket = self.sockets.get_mut::<udp::Socket<'static>>(handle);

                    match socket.recv() {
                        Ok((payload, metadata)) => {
                            inspected += 1;
                            let destination_address =
                                metadata.local_address.unwrap_or(local.address);
                            let matching_family = record.family.matches(metadata.endpoint.addr)
                                && record.family.matches(destination_address);
                            let matching_peer = record
                                .peer
                                .is_none_or(|peer| peer.ip_endpoint() == metadata.endpoint);

                            if matching_family && matching_peer {
                                let copied = if length == 0 {
                                    payload.len()
                                } else {
                                    length.min(payload.len())
                                };
                                let truncated = copied < payload.len();
                                let binary: Term<'a> =
                                    NewBinary::from_iter(env, payload.iter().copied().take(copied))
                                        .into();
                                let source =
                                    EncodedEndpoint::new(metadata.endpoint, local.scope_id);
                                let destination = EncodedEndpoint::new(
                                    IpEndpoint::new(destination_address, local.port),
                                    local.scope_id,
                                );

                                Some(Some((binary, source, destination, truncated, copied)))
                            } else {
                                Some(None)
                            }
                        }
                        Err(udp::RecvError::Exhausted) => None,
                        Err(udp::RecvError::Truncated) => {
                            unreachable!("recv without a slice cannot truncate")
                        }
                    }
                };

                let Some(candidate) = next else {
                    break;
                };

                if let Some(datagram) = candidate {
                    received = Some(datagram);
                    break;
                }
            }

            if received.is_some() || inspected == udp_support::PACKET_CAPACITY {
                next_receive_cursor = (handle_index + 1) % handle_count;
                break;
            }
        }

        self.udp_record_mut(identity)?.receive_cursor = next_receive_cursor;

        if let Some((binary, source, destination, truncated, copied)) = received {
            let result = (crate::atoms::ok(), source, destination, binary, truncated).encode(env);
            let effects = self
                .drive(now, Some(copied))
                .expect("UDP receive without ingress cannot fail");
            Ok(self.finish_call(env, result, effects, Vec::new()))
        } else {
            let immediately_ready = record
                .handles
                .iter()
                .any(|handle| self.sockets.get::<udp::Socket<'static>>(*handle).can_recv());
            self.arm_udp_waiter(
                env,
                identity,
                &record.handles,
                Direction::Read,
                Operation::Recvfrom,
                pid,
                reference,
                immediately_ready,
            )?;
            let result = (crate::atoms::select(), Operation::Recvfrom, reference).encode(env);
            let effects = self
                .drive(now, None)
                .expect("UDP receive retry without ingress cannot fail");
            Ok(self.finish_call(env, result, effects, Vec::new()))
        }
    }

    pub fn udp_sockname(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
    ) -> Result<Envelope<EncodedEndpoint>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Udp)?;
        let local = self
            .udp_record(identity)?
            .local
            .ok_or(SocketError::NotBound)?;

        Ok(self.finish_call(
            env,
            EncodedEndpoint::new(local.ip_endpoint(), local.scope_id),
            Effects::empty(),
            Vec::new(),
        ))
    }

    pub fn udp_peername(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
    ) -> Result<Envelope<EncodedEndpoint>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Udp)?;
        let peer = self
            .udp_record(identity)?
            .peer
            .ok_or(SocketError::NotConnected)?;

        Ok(self.finish_call(
            env,
            EncodedEndpoint::new(peer.ip_endpoint(), peer.scope_id),
            Effects::empty(),
            Vec::new(),
        ))
    }

    pub fn udp_close(
        &mut self,
        env: Env<'_>,
        identity: SocketIdentity,
        now: Instant,
    ) -> Result<Envelope<Atom>, SocketError> {
        self.ensure_running()?;
        self.socket_table.validate(identity, SocketKind::Udp)?;
        let record = self.udp_record(identity)?.clone();
        let waiters = self.socket_table.close(identity)?;
        let aborts = waiters
            .into_iter()
            .map(|(direction, waiter)| PendingNotification {
                identity,
                direction,
                waiter,
            })
            .collect();

        for handle in record.handles {
            self.sockets.remove(handle);
        }
        self.udp_records.remove(&identity.id);
        let effects = self
            .drive(now, None)
            .expect("UDP close without ingress cannot fail");
        Ok(self.finish_call(env, crate::atoms::ok(), effects, aborts))
    }

    pub fn ingress(&mut self, packet: &[u8], now: Instant) -> Result<Effects, StackError> {
        self.ingress_batch(&[packet], now)
    }

    pub fn ingress_batch(
        &mut self,
        packets: &[&[u8]],
        now: Instant,
    ) -> Result<Effects, StackError> {
        if !matches!(self.lifecycle, Lifecycle::Running) {
            return Err(StackError::Closed);
        }

        if packets.len() > self.limits.input_packets {
            return Err(StackError::BatchTooLarge);
        }

        let mut copied_bytes = 0usize;
        for packet in packets {
            self.validate_packet(packet)?;
            copied_bytes = copied_bytes
                .checked_add(packet.len())
                .filter(|bytes| *bytes <= self.limits.bytes_copied)
                .ok_or(StackError::BatchTooLarge)?;
        }

        let owned_packets = packets
            .iter()
            .map(|packet| packet.to_vec())
            .collect::<Vec<_>>();
        self.device
            .enqueue_receive_batch(owned_packets)
            .map_err(|_| StackError::OwnershipInvariantViolation)?;
        self.counters.ingress_packets += packets.len();
        self.drive(now, Some(copied_bytes))
    }

    pub fn poll(&mut self, now: Instant) -> Effects {
        self.counters.poll_calls += 1;
        self.drive(now, None)
            .expect("poll without ingress cannot fail")
    }

    pub fn poll_call(&mut self, env: Env<'_>, now: Instant) -> Envelope<Atom> {
        match self.lifecycle {
            Lifecycle::Running => {
                let effects = self.poll(now);
                self.finish_call(env, crate::atoms::ok(), effects, Vec::new())
            }
            Lifecycle::ShuttingDown => self.continue_shutdown(env),
            Lifecycle::Shutdown => Envelope::empty(crate::atoms::ok()),
        }
    }

    pub fn finish_call<T>(
        &mut self,
        env: Env<'_>,
        result: T,
        mut effects: Effects,
        aborts: Vec<PendingNotification>,
    ) -> Envelope<T> {
        let mut readiness_work = 0usize;
        // Work units completed since the last charge. The caller is charged at
        // chunk boundaries so that its remaining reduction slice, not just the
        // deadline, decides how much more of this call runs.
        let mut uncharged = 0usize;
        self.pending_notifications.extend(aborts);

        while readiness_work < self.limits.ready_events
            && !self.pending_notifications.is_empty()
            && self.within_budget()
        {
            let notification = self
                .pending_notifications
                .pop_front()
                .expect("pending notification exists");
            self.send_abort(env, &notification.waiter, notification.identity);
            readiness_work += 1;
            uncharged += 1;
            self.charge_chunk(env, &mut uncharged, false);
        }

        if self.ready.take_overflow() && matches!(self.lifecycle, Lifecycle::Running) {
            self.ready_sweep = true;
            self.ready_sweep_cursor = None;
        }

        let queued = {
            let ready = &self.ready;
            let call_budget = &mut self.call_budget;
            ready.drain_while(self.limits.ready_events - readiness_work, || {
                call_budget.checkpoint()
            })
        };

        for key in queued {
            // Keys drained from the ready queue are retained rather than
            // dropped when the budget runs out: `ready_sweep_pending` is the
            // same cursor the sweep below uses, and `more` already accounts
            // for it.
            if self.within_budget() {
                self.deliver_or_discard_ready(env, key);
                readiness_work += 1;
                uncharged += 1;
                self.charge_chunk(env, &mut uncharged, false);
            } else {
                self.ready_sweep_pending.push_back(key);
            }
        }

        while readiness_work < self.limits.ready_events
            && !self.ready_sweep_pending.is_empty()
            && self.within_budget()
        {
            let key = self
                .ready_sweep_pending
                .pop_front()
                .expect("pending sweep key exists");
            self.deliver_or_discard_ready(env, key);
            readiness_work += 1;
            uncharged += 1;
            self.charge_chunk(env, &mut uncharged, false);
        }

        if self.ready_sweep
            && self.ready_sweep_pending.is_empty()
            && readiness_work < self.limits.ready_events
        {
            let remaining = self.limits.ready_events - readiness_work;
            let scan = {
                let socket_table = &self.socket_table;
                let call_budget = &mut self.call_budget;
                socket_table.scan_ready(self.ready_sweep_cursor, remaining, remaining, || {
                    call_budget.checkpoint()
                })
            };
            let scan_cost = scan.entries_scanned.max(scan.keys.len());

            for key in scan.keys {
                if self.within_budget() {
                    self.deliver_or_discard_ready(env, key);
                    uncharged += 1;
                    self.charge_chunk(env, &mut uncharged, false);
                } else {
                    self.ready_sweep_pending.push_back(key);
                }
            }

            readiness_work += scan_cost;
            self.ready_sweep = !scan.complete;
            self.ready_sweep_cursor = self.ready_sweep.then_some(scan.cursor).flatten();
        }

        effects.more = effects.more
            || !self.pending_notifications.is_empty()
            || self.ready.has_pending()
            || !self.ready_sweep_pending.is_empty()
            || self.ready_sweep;
        self.counters.observe(Work {
            ready_events: readiness_work,
            maintenance_work: effects.maintenance_work,
            ..Work::default()
        });
        // Flush the tail: whatever has not yet been charged, including the
        // work this call did before reaching the readiness loops.
        self.charge_chunk(env, &mut uncharged, true);

        // An envelope that ends without a continuation and carries a poll_at
        // replaces the BEAM timer, whichever call produced it.
        if let Some(millis) = effects.poll_at.filter(|_| !effects.more) {
            self.scheduled_poll_at = Some(Instant::from_millis(millis));
        }

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

        // Linearize shutdown before draining the bounded state. Any socket
        // operation serialized after this point is rejected as closed while
        // repeated polls resume cleanup from the retained native structures.
        self.lifecycle = Lifecycle::ShuttingDown;
        self.ready_sweep = false;
        self.ready_sweep_cursor = None;
        self.ready_sweep_pending.clear();
        self.continue_shutdown(env)
    }

    fn continue_shutdown(&mut self, env: Env<'_>) -> Envelope<Atom> {
        // Once structural cleanup has produced aborts, spend the next slice
        // delivering them before removing more state. This keeps waiter
        // notification latency bounded without exceeding the readiness cap.
        let effects = if self.pending_notifications.is_empty() {
            self.shutdown_work(Some(env))
        } else {
            Effects::empty()
        };
        let mut envelope = self.finish_call(env, crate::atoms::ok(), effects, Vec::new());

        if self.shutdown_pending() {
            envelope.more = true;
            envelope.poll_at = None;
        } else {
            self.lifecycle = Lifecycle::Shutdown;
            self.closing_sweep_cursor = None;
            self.closing_cleanup_pending = false;
            self.closing_cleanup_resweep = false;
            self.listener_scan_cursor = None;
            self.listener_scan_pending = false;
            self.listener_scan_resweep = false;
            self.ready_sweep = false;
            self.ready_sweep_cursor = None;
            self.ready_sweep_pending.clear();
            envelope.more = false;
        }

        envelope
    }

    /// `env` is absent only for the fuzzing harness, which drives the stack
    /// without a BEAM environment to charge.
    fn shutdown_work(&mut self, env: Option<Env<'_>>) -> Effects {
        let mut maintenance_work = 0;
        let mut uncharged = 0usize;

        while maintenance_work < self.limits.maintenance_work && self.within_budget() {
            if let Some((identity, waiters)) = self.socket_table.close_next() {
                self.pending_notifications.extend(waiters.into_iter().map(
                    |(direction, waiter)| PendingNotification {
                        identity,
                        direction,
                        waiter,
                    },
                ));
            } else if let Some((&id, _)) = self.tcp_records.first_key_value() {
                let record = self
                    .tcp_records
                    .remove(&id)
                    .expect("selected TCP record exists");
                self.sockets.remove(record.handle);
                self.closing_tcp.remove(&id);
                if let Some(deadline) = record.close_deadline {
                    self.closing_deadlines.remove(&(deadline, id));
                }
                if let (Some(local), Some(remote)) = (record.local, record.remote) {
                    self.tcp_connections
                        .remove(&ConnectionKey::new(local, remote));
                }
            } else if let Some((&id, _)) = self.tcp_listeners.first_key_value() {
                let listener = self
                    .tcp_listeners
                    .remove(&id)
                    .expect("selected TCP listener exists");
                for handle in listener.pool {
                    self.sockets.remove(handle);
                }
            } else if let Some((&id, _)) = self.udp_records.first_key_value() {
                let record = self
                    .udp_records
                    .remove(&id)
                    .expect("selected UDP record exists");
                for handle in record.handles {
                    self.sockets.remove(handle);
                }
            } else if self.tcp_connections.pop_first().is_some()
                || self.closing_tcp.pop_first().is_some()
                || self.closing_deadlines.pop_first().is_some()
                || self.device.discard_one()
            {
                // One retained index or packet was discarded.
            } else {
                break;
            }

            maintenance_work += 1;

            if let Some(env) = env {
                uncharged += 1;
                self.charge_chunk(env, &mut uncharged, false);
            }
        }

        self.counters.observe(Work {
            maintenance_work,
            ..Work::default()
        });

        Effects {
            output: Vec::new(),
            poll_at: None,
            more: self.shutdown_structures_pending(),
            maintenance_work,
        }
    }

    fn shutdown_structures_pending(&self) -> bool {
        self.socket_table.len() > 0
            || !self.tcp_records.is_empty()
            || !self.tcp_listeners.is_empty()
            || !self.udp_records.is_empty()
            || !self.tcp_connections.is_empty()
            || !self.closing_tcp.is_empty()
            || !self.closing_deadlines.is_empty()
            || self.device.queued_packets() != (0, 0)
    }

    fn shutdown_pending(&self) -> bool {
        self.shutdown_structures_pending()
            || !self.pending_notifications.is_empty()
            || self.ready.has_pending()
            || !self.ready_sweep_pending.is_empty()
            || self.ready_sweep
    }

    #[cfg(debug_assertions)]
    pub fn test_socket_open(
        &mut self,
        env: Env<'_>,
        internal_handle: u64,
    ) -> Result<Envelope<SocketIdentity>, SocketError> {
        self.ensure_running()?;
        self.ensure_logical_socket_capacity()?;
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

        let expected_kind = self.socket_table.validate_any(identity)?.kind;
        let flag = self
            .socket_table
            .ready_flag(identity, expected_kind, direction)?;
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
                    expected_kind,
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
            let expected_kind = self.socket_table.validate_any(key.identity)?.kind;
            let flag = self
                .socket_table
                .ready_flag(key.identity, expected_kind, key.direction)?;
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

    #[allow(clippy::too_many_arguments)]
    fn arm_udp_waiter<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        handles: &[SocketHandle],
        direction: Direction,
        operation: Operation,
        pid: LocalPid,
        reference: Reference<'a>,
        immediately_ready: bool,
    ) -> Result<(), SocketError> {
        let flag = self
            .socket_table
            .ready_flag(identity, SocketKind::Udp, direction)?;
        self.socket_table.install_waiter(
            env,
            WaiterRegistration {
                identity,
                expected_kind: SocketKind::Udp,
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
        for handle in handles {
            let socket = self.sockets.get_mut::<udp::Socket<'static>>(*handle);

            match direction {
                Direction::Read => socket.register_recv_waker(&waker),
                Direction::Write => socket.register_send_waker(&waker),
            }
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

    fn udp_record(&self, identity: SocketIdentity) -> Result<&UdpRecord, SocketError> {
        self.udp_records
            .get(&identity.id)
            .filter(|record| record.identity == identity)
            .ok_or(SocketError::InvalidSocket)
    }

    fn udp_record_mut(&mut self, identity: SocketIdentity) -> Result<&mut UdpRecord, SocketError> {
        self.udp_records
            .get_mut(&identity.id)
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
        let mut work = 0;
        let mut last_processed = None;

        for key in &keys {
            if !self.within_budget() {
                more = true;
                break;
            }

            self.refresh_listener_member(*key);
            work += 1;
            last_processed = Some(*key);
        }

        if more {
            self.listener_scan_cursor = last_processed.or(cursor);
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
                .then(|| {
                    self.ensure_logical_socket_capacity()?;
                    self.ensure_native_socket_capacity(1, 0)?;
                    self.socket_table.insert(SocketKind::Tcp, 0)
                })
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
                        listener.rcvbuf,
                        listener.sndbuf,
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
        debug_assert!(self.ensure_native_socket_capacity(1, 0).is_ok());
        let handle = self.sockets.add(
            tcp_support::socket(listener.rcvbuf, listener.sndbuf)
                .expect("listener TCP buffer sizes remain valid"),
        );
        let socket = self.sockets.get_mut::<tcp::Socket<'static>>(handle);
        socket.set_timeout(Some(Duration::from_millis(
            tcp_support::CONNECT_TIMEOUT_MILLIS,
        )));
        socket
            .listen(listener.listen_endpoint)
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

    fn ensure_logical_socket_capacity(&self) -> Result<(), SocketError> {
        let logical_socket_count = self
            .socket_table
            .len()
            .checked_add(self.closing_tcp.len())
            .ok_or(SocketError::SystemLimit)?;

        if logical_socket_count >= self.limits.ready_events {
            Err(SocketError::SystemLimit)
        } else {
            Ok(())
        }
    }

    fn ensure_native_socket_capacity(
        &self,
        added: usize,
        removed: usize,
    ) -> Result<(), SocketError> {
        let native_socket_count = self.sockets.iter().count();
        let resulting_socket_count = native_socket_count
            .checked_sub(removed)
            .and_then(|count| count.checked_add(added))
            .ok_or(SocketError::SystemLimit)?;

        if resulting_socket_count > NATIVE_SOCKET_CAPACITY {
            Err(SocketError::SystemLimit)
        } else {
            Ok(())
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
        let mut work = 0;
        let mut last_processed = None;

        for id in &ids {
            if !self.within_budget() {
                more = true;
                break;
            }

            work += 1;
            last_processed = Some(*id);
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
            self.closing_sweep_cursor = last_processed.or(self.closing_sweep_cursor);
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

    fn has_ip_address(&self, address: IpAddress) -> bool {
        self.interface
            .ip_addrs()
            .iter()
            .any(|cidr| cidr.address() == address)
    }

    fn concrete_listener_endpoint(
        &self,
        endpoint: ValidatedEndpoint,
    ) -> Result<smoltcp::wire::IpListenEndpoint, SocketError> {
        if !endpoint.address.is_unspecified() {
            return Ok(endpoint.listen_endpoint());
        }

        let family = AddressFamily::of(endpoint.address);
        let address = self
            .interface
            .ip_addrs()
            .iter()
            .map(IpCidr::address)
            .find(|address| family.matches(*address) && !address.is_unspecified())
            .ok_or(SocketError::AddressNotAvailable)?;

        Ok(smoltcp::wire::IpListenEndpoint {
            addr: Some(address),
            port: endpoint.port,
        })
    }

    fn retarget_wildcard_listener(&mut self, packet: &[u8]) {
        let Some((destination, port)) = tcp_listener_target(packet) else {
            return;
        };
        let family = AddressFamily::of(destination);
        let handles = self
            .tcp_listeners
            .values()
            .filter(|listener| {
                listener.endpoint.address.is_unspecified()
                    && family.matches(listener.endpoint.address)
                    && listener.local.port == port
            })
            .flat_map(|listener| listener.pool.iter().copied())
            .collect::<Vec<_>>();
        let endpoint = smoltcp::wire::IpListenEndpoint {
            addr: Some(destination),
            port,
        };

        for handle in handles {
            let socket = self.sockets.get_mut::<tcp::Socket<'static>>(handle);

            if socket.state() == tcp::State::Listen && socket.listen_endpoint() != endpoint {
                socket.abort();
                socket.set_timeout(Some(Duration::from_millis(
                    tcp_support::CONNECT_TIMEOUT_MILLIS,
                )));
                socket
                    .listen(endpoint)
                    .expect("a validated wildcard target remains listenable");
            }
        }
    }

    fn reachable(&self, address: IpAddress) -> bool {
        self.interface
            .ip_addrs()
            .iter()
            .any(|cidr| cidr.contains_addr(&address))
            || self
                .route_prefixes
                .iter()
                .any(|cidr| cidr.contains_addr(&address))
    }

    fn port_in_use(
        &self,
        port: u16,
        family: AddressFamily,
        excluding: Option<SocketIdentity>,
    ) -> bool {
        self.tcp_records.values().any(|record| {
            Some(record.identity) != excluding
                && record.family == family
                && !record.accepted
                && record.local.is_some_and(|local| local.port == port)
        }) || self.tcp_listeners.values().any(|listener| {
            Some(listener.identity) != excluding
                && family.matches(listener.local.addr)
                && listener.local.port == port
        })
    }

    fn allocate_ephemeral_port(&mut self, family: AddressFamily) -> Result<u16, SocketError> {
        let used = self
            .tcp_records
            .values()
            .filter(|record| record.family == family)
            .filter_map(|record| record.local.map(|local| local.port))
            .chain(
                self.tcp_listeners
                    .values()
                    .filter(|listener| family.matches(listener.local.addr))
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

    fn udp_port_in_use(
        &self,
        port: u16,
        family: AddressFamily,
        excluding: Option<SocketIdentity>,
    ) -> bool {
        self.udp_records.values().any(|record| {
            Some(record.identity) != excluding
                && record.family == family
                && record.local.is_some_and(|local| local.port == port)
        })
    }

    fn allocate_udp_ephemeral_port(&mut self, family: AddressFamily) -> Result<u16, SocketError> {
        let used = self
            .udp_records
            .values()
            .filter(|record| record.family == family)
            .filter_map(|record| record.local.map(|local| local.port))
            .collect::<BTreeSet<_>>();
        let port = tcp_support::allocate_ephemeral(
            &used,
            self.next_udp_ephemeral_port,
            tcp_support::EPHEMERAL_PORT_FIRST,
            tcp_support::EPHEMERAL_PORT_LAST,
        )?;
        self.next_udp_ephemeral_port = if port == tcp_support::EPHEMERAL_PORT_LAST {
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
        let (source, destination, next_header, transport) = match packet.first()? >> 4 {
            4 => {
                let ipv4 = Ipv4Packet::new_checked(packet).ok()?;
                (
                    IpAddress::Ipv4(ipv4.src_addr()),
                    IpAddress::Ipv4(ipv4.dst_addr()),
                    ipv4.next_header(),
                    ipv4.payload(),
                )
            }
            6 => {
                let ipv6 = Ipv6Packet::new_checked(packet).ok()?;
                let mut next_header = ipv6.next_header();
                let mut transport = ipv6.payload();

                if next_header == IpProtocol::HopByHop {
                    let extension = Ipv6ExtHeader::new_checked(transport).ok()?;
                    next_header = extension.next_header();
                    let header_len = (usize::from(extension.header_len()) + 1) * 8;
                    transport = transport.get(header_len..)?;
                }

                (
                    IpAddress::Ipv6(ipv6.src_addr()),
                    IpAddress::Ipv6(ipv6.dst_addr()),
                    next_header,
                    transport,
                )
            }
            _ => return None,
        };

        if next_header != IpProtocol::Tcp {
            return None;
        }

        let tcp = TcpPacket::new_checked(transport).ok()?;

        if !tcp.rst() {
            return None;
        }

        let key = ConnectionKey::new(
            IpEndpoint::new(destination, tcp.dst_port()),
            IpEndpoint::new(source, tcp.src_port()),
        );
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
            Lifecycle::ShuttingDown | Lifecycle::Shutdown => Err(SocketError::Closed),
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

    fn deliver_or_discard_ready(&mut self, env: Env<'_>, key: ReadyKey) {
        if matches!(self.lifecycle, Lifecycle::Running) {
            self.deliver_ready(env, key);
        } else {
            self.counters.readiness_dropped += 1;
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
        let mut output = self.device.take_transmit(
            self.limits.output_packets,
            output_byte_limit,
            &mut self.call_budget,
        );
        let mut output_bytes: usize = output.iter().map(|packet| packet.len()).sum();

        let mut deadline_work_deferred = false;
        let mut ingress_work = 0usize;

        while ingress_work < self.limits.input_packets
            && self.device.has_receive()
            && self.device.can_receive()
        {
            if !self.within_budget() {
                deadline_work_deferred = true;
                break;
            }

            let packet = self
                .device
                .take_receive()
                .expect("receive queue was checked as non-empty");
            self.retarget_wildcard_listener(&packet);
            let reset = self.inbound_reset(&packet);
            self.device.return_receive(packet);
            let queued_before = self.device.queued_packets().0;
            let _ = self
                .interface
                .poll_ingress_single(now, &mut self.device, &mut self.sockets);

            if self.device.queued_packets().0 == queued_before {
                break;
            }

            ingress_work += 1;
            self.record_inbound_reset(reset);
            self.request_closing_cleanup();
            self.request_listener_scan();
        }

        if self.within_budget() {
            self.interface.poll_maintenance(now);
        } else {
            deadline_work_deferred = true;
        }

        if self
            .next_close_deadline()
            .is_some_and(|deadline| now >= deadline)
        {
            self.request_closing_cleanup();
        }

        // smoltcp closes a socket whose TIME-WAIT has elapsed without emitting
        // a segment, so egress reports no state change for it. That timer is
        // part of the deadline this stack last published, so once a drive
        // reaches that deadline, sweep closing sockets after egress; otherwise
        // the slot stays held until the much later close deadline.
        let scheduled_deadline_reached = !self.closing_tcp.is_empty()
            && self
                .scheduled_poll_at
                .is_some_and(|deadline| now >= deadline);

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
            if !self.within_budget() {
                deadline_work_deferred = true;
                break;
            }

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

        if scheduled_deadline_reached {
            self.request_closing_cleanup();

            // Only a complete egress pass is sure to have dispatched the
            // expired socket; until one runs, later drives sweep again.
            if egress_attempted && !egress_may_remain {
                self.scheduled_poll_at = None;
            }
        }

        let remaining_packets = self.limits.output_packets.saturating_sub(output.len());
        let remaining_bytes = output_byte_limit.saturating_sub(output_bytes);
        let additional_output =
            self.device
                .take_transmit(remaining_packets, remaining_bytes, &mut self.call_budget);
        output_bytes += additional_output
            .iter()
            .map(|packet| packet.len())
            .sum::<usize>();
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
            || self.listener_scan_pending
            || deadline_work_deferred;
        // A received packet retained past the deadline keeps the continuation
        // alive through has_receive(), even if no later work loop ran.
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
            input_packets: ingress_work,
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
        if packet.is_empty() {
            self.counters.rejected_packets += 1;
            return Err(StackError::InvalidPacket);
        }

        if let Err(error) = validate_raw_packet(packet, self.mtu, self.limits.bytes_copied) {
            self.counters.rejected_packets += 1;
            return Err(error);
        }

        Ok(())
    }
}

pub(crate) fn validate_raw_packet(
    packet: &[u8],
    mtu: usize,
    bytes_copied: usize,
) -> Result<(), StackError> {
    if packet.is_empty() {
        return Err(StackError::InvalidPacket);
    }

    let valid = match packet[0] >> 4 {
        4 => Ipv4Packet::new_checked(packet).is_ok_and(|ipv4| {
            usize::from(ipv4.total_len()) == packet.len()
                && ipv4.verify_checksum()
                && !ipv4.more_frags()
                && ipv4.frag_offset() == 0
        }),
        6 => Ipv6Packet::new_checked(packet).is_ok_and(|ipv6| {
            40usize
                .checked_add(usize::from(ipv6.payload_len()))
                .is_some_and(|declared| declared == packet.len())
        }),
        _ => false,
    };

    if !valid {
        return Err(StackError::InvalidPacket);
    }

    if packet.len() > mtu || packet.len() > bytes_copied {
        return Err(StackError::PacketTooLarge);
    }

    Ok(())
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_raw_engine(data: &[u8]) {
    let limits = fuzz_limits();
    let Some(mut stack) = NativeStack::new(limits, fuzz_stack_config(), Instant::ZERO).ok() else {
        return;
    };
    let packet = data.get(..data.len().min(4_096)).unwrap_or_default();
    stack.begin_call();
    let _arbitrary_result = stack.ingress(packet, Instant::ZERO);

    let payload = data.get(..data.len().min(2_048)).unwrap_or_default();
    let valid_packet = fuzz_ipv6_packet(payload);
    stack.begin_call();
    let _valid_result = stack.ingress(&valid_packet, Instant::from_millis(1));

    for tick in 2..=2 + data.first().map_or(0, |byte| i64::from(byte % 4)) {
        stack.begin_call();
        let _effects = stack.poll(Instant::from_millis(tick));
    }
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_config_and_endpoints(data: &[u8]) {
    let limits = fuzz_limits();
    let mtu = data
        .first()
        .map_or(1_280, |byte| 1_200 + usize::from(*byte) * 256);
    let address_length = data.get(1).map_or(0, |byte| usize::from(byte % 18));
    let address = data
        .get(2..)
        .unwrap_or_default()
        .iter()
        .copied()
        .take(address_length)
        .collect::<Vec<_>>();
    let prefix_length = data.get(20).copied().unwrap_or_default();
    let route_gateway = data
        .get(21..)
        .unwrap_or_default()
        .iter()
        .copied()
        .take(address_length)
        .collect::<Vec<_>>();
    let config = StackConfig {
        mtu,
        addresses: vec![AddressConfig {
            address: address.clone(),
            prefix_length,
        }],
        routes: vec![RouteConfig {
            destination: address.clone(),
            prefix_length,
            gateway: route_gateway,
        }],
    };
    let _stack_result = NativeStack::new(limits, config, Instant::ZERO);

    let endpoint = TcpEndpoint {
        address,
        port: fuzz_signed_value(data.get(39..47).unwrap_or_default()),
        scope_id: fuzz_signed_value(data.get(47..55).unwrap_or_default()),
    };
    let _bind_result = endpoint.bind_endpoint();
    let _remote_result = endpoint.remote_endpoint();
}

#[cfg(feature = "fuzzing")]
pub(crate) fn fuzz_socket_lifecycle(data: &[u8]) {
    let Some(mut stack) = NativeStack::new(fuzz_limits(), fuzz_stack_config(), Instant::ZERO).ok()
    else {
        return;
    };
    let mut identities = Vec::with_capacity(32);

    for (step, operation) in data.iter().copied().take(512).enumerate() {
        let now = Instant::from_millis(i64::try_from(step).unwrap_or(i64::MAX));
        stack.begin_call();

        match operation % 11 {
            0 if identities.len() < 32 => {
                if let Ok(identity) = fuzz_open_socket(&mut stack, SocketKind::Tcp, operation) {
                    identities.push((identity, SocketKind::Tcp));
                }
            }
            1 if identities.len() < 32 => {
                if let Ok(identity) = fuzz_open_socket(&mut stack, SocketKind::Udp, operation) {
                    identities.push((identity, SocketKind::Udp));
                }
            }
            2 if !identities.is_empty() => {
                let (identity, kind) = identities[usize::from(operation) % identities.len()];
                let _result = fuzz_bind_socket(&mut stack, identity, kind, operation);
            }
            3 if !identities.is_empty() => {
                let (identity, kind) = identities[usize::from(operation) % identities.len()];
                let _result = fuzz_advance_socket(&mut stack, identity, kind, operation, now);
            }
            4 if !identities.is_empty() => {
                let index = usize::from(operation) % identities.len();
                let (identity, kind) = identities.swap_remove(index);
                let _result = fuzz_close_socket(&mut stack, identity, kind, operation, now);
            }
            5 if !identities.is_empty() => {
                let (identity, kind) = identities[usize::from(operation) % identities.len()];
                fuzz_readiness(&mut stack, identity, kind, operation);
            }
            6 => {
                let _effects = stack.poll(now);
            }
            7 => {
                let packet = fuzz_ipv6_packet(data.get(step..).unwrap_or_default());
                let _result = stack.ingress(&packet, now);
            }
            8 => {
                let packet = data
                    .get(step..)
                    .unwrap_or_default()
                    .get(..data.len().saturating_sub(step).min(256))
                    .unwrap_or_default();
                let _result = stack.ingress(packet, now);
            }
            9 if !identities.is_empty() => {
                let (mut identity, kind) = identities[usize::from(operation) % identities.len()];
                identity.generation = identity.generation.wrapping_add(1);
                let _result = stack.socket_table.validate(identity, kind);
            }
            10 => {
                fuzz_shutdown(&mut stack);
                break;
            }
            _other => {}
        }
    }

    fuzz_shutdown(&mut stack);
}

#[cfg(feature = "fuzzing")]
fn fuzz_limits() -> Limits {
    Limits {
        bytes_copied: 65_575,
        input_packets: 32,
        output_packets: 32,
        ready_events: 64,
        maintenance_work: 64,
    }
}

#[cfg(feature = "fuzzing")]
fn fuzz_stack_config() -> StackConfig {
    StackConfig {
        mtu: 4_096,
        addresses: vec![
            AddressConfig {
                address: vec![192, 0, 2, 1],
                prefix_length: 24,
            },
            AddressConfig {
                address: vec![0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                prefix_length: 64,
            },
        ],
        routes: vec![
            RouteConfig {
                destination: vec![0, 0, 0, 0],
                prefix_length: 0,
                gateway: vec![192, 0, 2, 2],
            },
            RouteConfig {
                destination: vec![0; 16],
                prefix_length: 0,
                gateway: vec![0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
            },
        ],
    }
}

#[cfg(feature = "fuzzing")]
fn fuzz_ipv6_packet(payload: &[u8]) -> Vec<u8> {
    let payload = payload.get(..payload.len().min(2_048)).unwrap_or_default();
    let payload_length = u16::try_from(payload.len()).expect("bounded fuzz payload");
    let mut packet = Vec::with_capacity(40 + payload.len());
    packet.extend_from_slice(&[0x60, 0, 0, 0]);
    packet.extend_from_slice(&payload_length.to_be_bytes());
    packet.extend_from_slice(&[59, 64]);
    packet.extend_from_slice(&[0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
    packet.extend_from_slice(&[0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    packet.extend_from_slice(payload);
    packet
}

#[cfg(feature = "fuzzing")]
fn fuzz_signed_value(bytes: &[u8]) -> i64 {
    let mut value = [0_u8; 8];
    let length = bytes.len().min(value.len());
    value[..length].copy_from_slice(&bytes[..length]);
    i64::from_le_bytes(value)
}

#[cfg(feature = "fuzzing")]
fn fuzz_family(operation: u8) -> AddressFamily {
    if operation & 0x80 == 0 {
        AddressFamily::Inet
    } else {
        AddressFamily::Inet6
    }
}

#[cfg(feature = "fuzzing")]
fn fuzz_endpoint(family: AddressFamily, operation: u8, remote: bool) -> TcpEndpoint {
    let address = match (family, remote) {
        (AddressFamily::Inet, false) => vec![192, 0, 2, 1],
        (AddressFamily::Inet, true) => vec![192, 0, 2, 2],
        (AddressFamily::Inet6, false) => {
            vec![0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        }
        (AddressFamily::Inet6, true) => {
            vec![0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]
        }
    };

    TcpEndpoint {
        address,
        port: if remote {
            1 + i64::from(operation) * 251
        } else {
            0
        },
        scope_id: 0,
    }
}

#[cfg(feature = "fuzzing")]
fn fuzz_open_socket(
    stack: &mut NativeStack,
    kind: SocketKind,
    operation: u8,
) -> Result<SocketIdentity, SocketError> {
    stack.ensure_running()?;
    stack.ensure_logical_socket_capacity()?;
    stack.ensure_native_socket_capacity(1, 0)?;
    let family = fuzz_family(operation);

    match kind {
        SocketKind::Tcp => {
            let handle = stack.sockets.add(tcp_support::default_socket());
            let identity = match stack.socket_table.insert(kind, 0) {
                Ok(identity) => identity,
                Err(error) => {
                    stack.sockets.remove(handle);
                    return Err(error);
                }
            };
            stack.tcp_records.insert(
                identity.id,
                TcpRecord::new(
                    identity,
                    handle,
                    family,
                    tcp_support::DEFAULT_BUFFER_BYTES,
                    tcp_support::DEFAULT_BUFFER_BYTES,
                ),
            );
            Ok(identity)
        }
        SocketKind::Udp => {
            let handle = stack.sockets.add(udp_support::socket());
            let identity = match stack.socket_table.insert(kind, 0) {
                Ok(identity) => identity,
                Err(error) => {
                    stack.sockets.remove(handle);
                    return Err(error);
                }
            };
            stack
                .udp_records
                .insert(identity.id, UdpRecord::new(identity, handle, family));
            Ok(identity)
        }
        SocketKind::Synthetic => Err(SocketError::WrongKind),
    }
}

#[cfg(feature = "fuzzing")]
fn fuzz_bind_socket(
    stack: &mut NativeStack,
    identity: SocketIdentity,
    kind: SocketKind,
    operation: u8,
) -> Result<(), SocketError> {
    let family = match kind {
        SocketKind::Tcp => stack.tcp_record(identity)?.family,
        SocketKind::Udp => stack.udp_record(identity)?.family,
        SocketKind::Synthetic => return Err(SocketError::WrongKind),
    };
    let mut endpoint = fuzz_endpoint(family, operation, false).bind_endpoint()?;

    match kind {
        SocketKind::Tcp => {
            stack.socket_table.validate(identity, SocketKind::Tcp)?;
            let record = *stack.tcp_record(identity)?;
            if record.phase != ConnectPhase::Open {
                return Err(SocketError::InvalidState);
            }
            endpoint.port = stack.allocate_ephemeral_port(record.family)?;
            let record = stack.tcp_record_mut(identity)?;
            record.phase = ConnectPhase::Bound;
            record.local = Some(endpoint.ip_endpoint());
            record.local_scope_id = endpoint.scope_id;
        }
        SocketKind::Udp => {
            stack.socket_table.validate(identity, SocketKind::Udp)?;
            let record = stack.udp_record(identity)?.clone();
            if record.local.is_some() {
                return Err(SocketError::InvalidState);
            }
            endpoint.port = stack.allocate_udp_ephemeral_port(record.family)?;
            stack
                .sockets
                .get_mut::<udp::Socket<'static>>(record.primary_handle())
                .bind(endpoint.ip_endpoint())
                .map_err(|error| match error {
                    UdpBindError::InvalidState => SocketError::InvalidState,
                    UdpBindError::Unaddressable => SocketError::InvalidAddress,
                })?;
            let record = stack.udp_record_mut(identity)?;
            record.local = Some(endpoint);
        }
        SocketKind::Synthetic => return Err(SocketError::WrongKind),
    }

    Ok(())
}

#[cfg(feature = "fuzzing")]
fn fuzz_advance_socket(
    stack: &mut NativeStack,
    identity: SocketIdentity,
    kind: SocketKind,
    operation: u8,
    now: Instant,
) -> Result<(), SocketError> {
    match kind {
        SocketKind::Tcp => {
            let record = *stack.tcp_record(identity)?;
            if record.phase != ConnectPhase::Bound {
                return Err(SocketError::NotBound);
            }
            let local = record.local.ok_or(SocketError::NotBound)?;
            let endpoint = ValidatedEndpoint {
                address: local.addr,
                port: local.port,
                scope_id: record.local_scope_id,
            };
            let listen_endpoint = stack.concrete_listener_endpoint(endpoint)?;
            stack
                .sockets
                .get_mut::<tcp::Socket<'static>>(record.handle)
                .listen(listen_endpoint)
                .map_err(|error| match error {
                    ListenError::InvalidState => SocketError::InvalidState,
                    ListenError::Unaddressable => SocketError::InvalidAddress,
                })?;
            stack.remove_tcp_record(record);
            stack.tcp_listeners.insert(
                identity.id,
                ListenerRecord::new(
                    identity,
                    endpoint,
                    listen_endpoint,
                    local,
                    1 + usize::from(operation % 4),
                    [record.handle],
                    TcpBufferSizes {
                        rcvbuf: record.rcvbuf,
                        sndbuf: record.sndbuf,
                    },
                ),
            );
            stack.request_listener_scan();
        }
        SocketKind::Udp => {
            let record = stack.udp_record(identity)?.clone();
            if record.local.is_none() {
                return Err(SocketError::NotBound);
            }
            let remote = fuzz_endpoint(record.family, operation, true).remote_endpoint()?;
            if !stack.reachable(remote.address) {
                return Err(SocketError::NetworkUnreachable);
            }
            stack.udp_record_mut(identity)?.peer = Some(remote);
        }
        SocketKind::Synthetic => return Err(SocketError::WrongKind),
    }

    let _effects = stack.poll(now);
    Ok(())
}

#[cfg(feature = "fuzzing")]
fn fuzz_readiness(
    stack: &mut NativeStack,
    identity: SocketIdentity,
    kind: SocketKind,
    operation: u8,
) {
    let direction = if operation & 0x40 == 0 {
        Direction::Read
    } else {
        Direction::Write
    };
    let key = ReadyKey {
        identity,
        direction,
    };
    let Ok(flag) = stack.socket_table.ready_flag(identity, kind, direction) else {
        return;
    };
    let waker = stack.ready.waker(key, flag);
    waker.wake_by_ref();
    waker.wake_by_ref();

    for ready in stack.ready.drain(1 + usize::from(operation % 4)) {
        let _result = stack.socket_table.take_ready_waiter(ready);
    }
}

#[cfg(feature = "fuzzing")]
fn fuzz_close_socket(
    stack: &mut NativeStack,
    identity: SocketIdentity,
    kind: SocketKind,
    operation: u8,
    now: Instant,
) -> Result<(), SocketError> {
    stack.socket_table.validate(identity, kind)?;
    let _waiters = stack.socket_table.close(identity)?;

    match kind {
        SocketKind::Tcp => {
            if let Some(listener) = stack.tcp_listeners.remove(&identity.id) {
                for handle in listener.pool {
                    stack.sockets.remove(handle);
                }
                stack.listener_scan_cursor = None;
                stack.listener_scan_pending = !stack.tcp_listeners.is_empty();
            } else {
                let mut record = *stack.tcp_record(identity)?;
                if operation & 0x80 != 0 {
                    record.phase = ConnectPhase::Connected;
                    stack.tcp_record_mut(identity)?.phase = ConnectPhase::Connected;
                }

                if record.phase == ConnectPhase::Connected {
                    stack
                        .sockets
                        .get_mut::<tcp::Socket<'static>>(record.handle)
                        .close();
                    let deadline = now + Duration::from_millis(tcp_support::CLOSE_TIMEOUT_MILLIS);
                    stack.tcp_record_mut(identity)?.close_deadline = Some(deadline);
                    stack.closing_tcp.insert(identity.id);
                    stack.closing_deadlines.insert((deadline, identity.id));
                    stack.request_closing_cleanup();
                } else {
                    stack
                        .sockets
                        .get_mut::<tcp::Socket<'static>>(record.handle)
                        .abort();
                    stack.remove_tcp_socket(record);
                }
            }
        }
        SocketKind::Udp => {
            let record = stack.udp_record(identity)?.clone();
            for handle in record.handles {
                stack.sockets.remove(handle);
            }
            stack.udp_records.remove(&identity.id);
        }
        SocketKind::Synthetic => return Err(SocketError::WrongKind),
    }

    let _effects = stack.poll(now);
    Ok(())
}

#[cfg(feature = "fuzzing")]
fn fuzz_shutdown(stack: &mut NativeStack) {
    if matches!(stack.lifecycle, Lifecycle::Shutdown) {
        return;
    }

    stack.lifecycle = Lifecycle::ShuttingDown;
    stack.ready_sweep = false;
    stack.ready_sweep_cursor = None;
    stack.ready_sweep_pending.clear();

    for _call in 0..1_024 {
        stack.begin_call();
        let _effects = stack.shutdown_work(None);

        if !stack.shutdown_structures_pending() {
            break;
        }
    }

    assert!(!stack.shutdown_structures_pending());
    stack.pending_notifications.clear();
    stack.ready.clear();
    stack.ready_sweep_pending.clear();
    stack.lifecycle = Lifecycle::Shutdown;
}

#[derive(Clone, Debug)]
pub struct StackConfig {
    mtu: usize,
    addresses: Vec<AddressConfig>,
    routes: Vec<RouteConfig>,
}

impl<'a> Decoder<'a> for StackConfig {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        Ok(Self {
            mtu: term.map_get(crate::atoms::mtu())?.decode()?,
            addresses: decode_bounded_list(term.map_get(crate::atoms::addresses())?, 8)?,
            routes: decode_bounded_list(term.map_get(crate::atoms::routes())?, 4)?,
        })
    }
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

#[derive(Clone, Debug)]
struct AddressConfig {
    address: Vec<u8>,
    prefix_length: u8,
}

impl<'a> Decoder<'a> for AddressConfig {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        Ok(Self {
            address: decode_bounded_list(term.map_get(crate::atoms::address())?, 16)?,
            prefix_length: term.map_get(crate::atoms::prefix_length())?.decode()?,
        })
    }
}

impl AddressConfig {
    fn valid(&self) -> bool {
        ip_address(&self.address).is_some_and(|address| {
            self.prefix_length <= prefix_limit(address)
                && (address.is_unspecified() || address.is_unicast())
        })
    }

    fn to_cidr(&self) -> IpCidr {
        match ip_address(&self.address).expect("validated IP address") {
            IpAddress::Ipv4(address) => IpCidr::Ipv4(Ipv4Cidr::new(address, self.prefix_length)),
            IpAddress::Ipv6(address) => IpCidr::Ipv6(Ipv6Cidr::new(address, self.prefix_length)),
        }
    }
}

#[derive(Clone, Debug)]
struct RouteConfig {
    destination: Vec<u8>,
    prefix_length: u8,
    gateway: Vec<u8>,
}

impl<'a> Decoder<'a> for RouteConfig {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        Ok(Self {
            destination: decode_bounded_list(term.map_get(crate::atoms::destination())?, 16)?,
            prefix_length: term.map_get(crate::atoms::prefix_length())?.decode()?,
            gateway: decode_bounded_list(term.map_get(crate::atoms::gateway())?, 16)?,
        })
    }
}

impl RouteConfig {
    fn valid(&self) -> bool {
        let Some(destination) = ip_address(&self.destination) else {
            return false;
        };
        let Some(gateway) = ip_address(&self.gateway) else {
            return false;
        };

        same_family(destination, gateway)
            && self.prefix_length <= prefix_limit(destination)
            && !destination.is_multicast()
            && gateway.is_unicast()
    }

    fn to_route(&self) -> Route {
        Route {
            cidr: self.to_cidr(),
            via_router: ip_address(&self.gateway).expect("validated route gateway"),
            preferred_until: None,
            expires_at: None,
        }
    }

    fn to_cidr(&self) -> IpCidr {
        match ip_address(&self.destination).expect("validated route destination") {
            IpAddress::Ipv4(address) => IpCidr::Ipv4(Ipv4Cidr::new(address, self.prefix_length)),
            IpAddress::Ipv6(address) => IpCidr::Ipv6(Ipv6Cidr::new(address, self.prefix_length)),
        }
    }
}

fn ip_address(bytes: &[u8]) -> Option<IpAddress> {
    match bytes {
        octets if octets.len() == 4 => {
            let octets: [u8; 4] = octets.try_into().ok()?;
            Some(IpAddress::Ipv4(Ipv4Address::from_octets(octets)))
        }
        octets if octets.len() == 16 => {
            let octets: [u8; 16] = octets.try_into().ok()?;
            if octets[..10].iter().all(|byte| *byte == 0) && octets[10..12] == [0xff, 0xff] {
                None
            } else {
                Some(IpAddress::Ipv6(Ipv6Address::from_octets(octets)))
            }
        }
        _ => None,
    }
}

fn same_family(left: IpAddress, right: IpAddress) -> bool {
    matches!(
        (left, right),
        (IpAddress::Ipv4(_), IpAddress::Ipv4(_)) | (IpAddress::Ipv6(_), IpAddress::Ipv6(_))
    )
}

fn prefix_limit(address: IpAddress) -> u8 {
    match address {
        IpAddress::Ipv4(_) => 32,
        IpAddress::Ipv6(_) => 128,
    }
}

fn tcp_listener_target(packet: &[u8]) -> Option<(IpAddress, u16)> {
    let (destination, next_header, transport) = match packet.first()? >> 4 {
        4 => {
            let ipv4 = Ipv4Packet::new_checked(packet).ok()?;
            (
                IpAddress::Ipv4(ipv4.dst_addr()),
                ipv4.next_header(),
                ipv4.payload(),
            )
        }
        6 => {
            let ipv6 = Ipv6Packet::new_checked(packet).ok()?;
            let mut next_header = ipv6.next_header();
            let mut transport = ipv6.payload();

            if next_header == IpProtocol::HopByHop {
                let extension = Ipv6ExtHeader::new_checked(transport).ok()?;
                next_header = extension.next_header();
                let header_len = (usize::from(extension.header_len()) + 1) * 8;
                transport = transport.get(header_len..)?;
            }

            (IpAddress::Ipv6(ipv6.dst_addr()), next_header, transport)
        }
        _ => return None,
    };

    if next_header != IpProtocol::Tcp {
        return None;
    }

    let tcp = TcpPacket::new_checked(transport).ok()?;
    (tcp.syn() && !tcp.ack()).then_some((destination, tcp.dst_port()))
}

#[derive(Debug, PartialEq, Eq)]
pub enum StackError {
    Closed,
    InvalidLimits,
    InvalidStackConfig,
    InvalidPacket,
    PacketTooLarge,
    BatchTooLarge,
    OwnershipInvariantViolation,
}

pub struct Effects {
    pub output: Vec<OutputPacket>,
    pub poll_at: Option<i64>,
    pub more: bool,
    maintenance_work: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ConnectionKey {
    family: AddressFamily,
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
        debug_assert!(same_family(local.addr, remote.addr));
        let family = match local.addr {
            IpAddress::Ipv4(_) => AddressFamily::Inet,
            IpAddress::Ipv6(_) => AddressFamily::Inet6,
        };

        Self {
            family,
            local_address: address_key(local.addr),
            local_port: local.port,
            remote_address: address_key(remote.addr),
            remote_port: remote.port,
        }
    }
}

fn address_key(address: IpAddress) -> [u8; 16] {
    match address {
        IpAddress::Ipv4(address) => {
            let mut key = [0; 16];
            key[..4].copy_from_slice(&address.octets());
            key
        }
        IpAddress::Ipv6(address) => address.octets(),
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
    ShuttingDown,
    Shutdown,
}

impl Lifecycle {
    fn as_atom(self) -> Atom {
        match self {
            Self::Running => crate::atoms::running(),
            Self::ShuttingDown => crate::atoms::shutting_down(),
            Self::Shutdown => crate::atoms::shutdown(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, NifMap)]
pub struct Counters {
    max_bytes_copied: usize,
    max_input_packets: usize,
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
    native_calls: usize,
    deadline_yields: usize,
    timeslice_exhaustions: usize,
    max_native_work_nanoseconds: u64,
}

impl Counters {
    fn observe(&mut self, work: Work) {
        self.max_bytes_copied = self.max_bytes_copied.max(work.bytes_copied);
        self.max_input_packets = self.max_input_packets.max(work.input_packets);
        self.max_output_packets = self.max_output_packets.max(work.output_packets);
        self.max_ready_events = self.max_ready_events.max(work.ready_events);
        self.max_maintenance_work = self.max_maintenance_work.max(work.maintenance_work);
    }
}

pub struct Envelope<T> {
    pub result: T,
    pub output: Vec<OutputPacket>,
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
pub struct TcpSocketBufferBytes {
    id: u64,
    generation: u64,
    rcvbuf: usize,
    sndbuf: usize,
}

#[derive(NifMap)]
pub struct TcpBufferBytes {
    default_rcvbuf: usize,
    default_sndbuf: usize,
    sockets: Vec<TcpSocketBufferBytes>,
}

#[derive(NifMap)]
pub struct Snapshot {
    id: u64,
    limits: Limits,
    socket_count: usize,
    native_socket_count: usize,
    native_socket_capacity: usize,
    tcp_socket_count: usize,
    tcp_listener_count: usize,
    udp_socket_count: usize,
    listener_pool_socket_count: usize,
    listener_pool_target_count: usize,
    accepted_queue_count: usize,
    listener_backlog_capacity: usize,
    closing_tcp_socket_count: usize,
    tcp_buffer_bytes: TcpBufferBytes,
    udp_packet_capacity: usize,
    udp_payload_bytes: usize,
    udp_max_datagram_bytes: usize,
    udp_ipv4_max_datagram_bytes: usize,
    waiter_count: usize,
    read_waiter_count: usize,
    write_waiter_count: usize,
    sent_waiter_count: usize,
    read_sent_waiter_count: usize,
    write_sent_waiter_count: usize,
    ready_count: usize,
    ready_overflow_pending: bool,
    readiness: ReadinessCounters,
    receive_packets: usize,
    transmit_packets: usize,
    mtu: usize,
    ip_address_count: usize,
    lifecycle: Atom,
    call_target_nanoseconds: u64,
    work_budget_nanoseconds: u64,
    encoding_headroom_nanoseconds: u64,
    counters: Counters,
}

#[derive(NifMap)]
pub struct ResourceCounts {
    created: usize,
    dropped: usize,
    active: usize,
}

#[cfg(debug_assertions)]
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
        input_packets: 1,
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
            inner: Mutex::new(Some(
                NativeStack::new(LIMITS, config(), Instant::ZERO).unwrap(),
            )),
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
