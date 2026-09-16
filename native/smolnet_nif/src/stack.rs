use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, TryLockError};

use rustler::{Atom, NifMap, Resource, ResourceArc};
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::time::Instant;
use smoltcp::wire::HardwareAddress;

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
    pub fn new(limits: Limits, now: Instant) -> Result<ResourceArc<Self>, ()> {
        if !limits.valid() {
            return Err(());
        }

        let stack = NativeStack::new(limits, now);
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
}

impl NativeStack {
    fn new(limits: Limits, now: Instant) -> Self {
        let mut device = BeamDevice::new(1_500);
        let mut config = Config::new(HardwareAddress::Ip);
        config.random_seed = NEXT_STACK_ID.fetch_add(1, Ordering::Relaxed);
        let id = config.random_seed;
        let interface = Interface::new(config, &mut device, now);

        Self {
            id,
            interface,
            sockets: SocketSet::new(Vec::new()),
            device,
            socket_table: SocketTable::default(),
            ready: ReadyQueue::default(),
            counters: Counters::default(),
            lifecycle: Lifecycle::Running,
            limits,
        }
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

    use super::{NativeStack, StackResource};
    use crate::limits::Limits;

    const LIMITS: Limits = Limits {
        bytes_copied: 1,
        output_packets: 1,
        ready_events: 1,
        maintenance_work: 1,
    };

    #[test]
    fn contention_never_waits_for_the_mutex() {
        let resource = StackResource {
            inner: Mutex::new(NativeStack::new(LIMITS, Instant::ZERO)),
        };
        assert_eq!(resource.test_contention(), Err(()));
    }

    #[test]
    fn separate_resources_have_separate_stack_ids() {
        let first = NativeStack::new(LIMITS, Instant::ZERO);
        let second = NativeStack::new(LIMITS, Instant::ZERO);

        assert_ne!(first.id, second.id);
    }
}
