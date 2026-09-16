use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, TryLockError};

use rustler::{Atom, NifMap, Resource, ResourceArc};
use smoltcp::iface::{Config, Interface, PollResult, Route, SocketSet};
use smoltcp::time::Instant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, Ipv6Address, Ipv6Cidr};

use crate::device::BeamDevice;
use crate::limits::{Limits, Work};
use crate::socket_table::SocketTable;
use crate::waiter::ReadyQueue;

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
    ready: ReadyQueue,
    counters: Counters,
    lifecycle: Lifecycle,
    limits: Limits,
    mtu: usize,
}

impl NativeStack {
    fn new(limits: Limits, stack_config: StackConfig, now: Instant) -> Result<Self, StackError> {
        stack_config.validate(limits)?;

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
            socket_table: SocketTable::default(),
            ready: ReadyQueue::default(),
            counters: Counters::default(),
            lifecycle: Lifecycle::Running,
            limits,
            mtu: stack_config.mtu,
        })
    }

    pub fn snapshot(&self) -> Envelope<Snapshot> {
        let (receive_packets, transmit_packets) = self.device.queued_packets();

        Envelope {
            result: Snapshot {
                id: self.id,
                limits: self.limits,
                socket_count: self.socket_table.len(),
                native_socket_count: self.sockets.iter().count(),
                ready_count: self.ready.len(),
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

    pub fn ingress(&mut self, packet: &[u8], now: Instant) -> Result<Effects, StackError> {
        self.validate_packet(packet)?;
        self.device
            .enqueue_receive(packet.to_vec())
            .map_err(|_| StackError::OwnershipInvariantViolation)?;
        self.counters.ingress_packets += 1;
        self.drive(now, Some(packet.len()))
    }

    pub fn poll(&mut self, now: Instant) -> Effects {
        self.counters.poll_calls += 1;
        self.drive(now, None)
            .expect("poll without ingress cannot fail")
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
            cidr: IpCidr::Ipv6(Ipv6Cidr::new(
                ipv6_address(&self.destination),
                self.prefix_length,
            )),
            via_router: IpAddress::Ipv6(ipv6_address(&self.gateway)),
            preferred_until: None,
            expires_at: None,
        }
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
}

#[derive(Clone, Copy)]
enum Lifecycle {
    Running,
}

impl Lifecycle {
    fn as_atom(self) -> Atom {
        crate::atoms::running()
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
    ready_count: usize,
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
