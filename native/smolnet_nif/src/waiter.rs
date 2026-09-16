use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::task::{Wake, Waker};

use rustler::env::SavedTerm;
use rustler::{Env, LocalPid, NifMap, NifUnitEnum, OwnedEnv, Reference, Term};

#[derive(Clone, Copy, Debug, Eq, NifUnitEnum, Ord, PartialEq, PartialOrd)]
pub enum Direction {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, NifUnitEnum, PartialEq)]
pub enum Operation {
    Recv,
    Recvfrom,
    Accept,
    Send,
    Sendto,
    Connect,
}

impl Operation {
    pub fn direction(self) -> Direction {
        match self {
            Self::Recv | Self::Recvfrom | Self::Accept => Direction::Read,
            Self::Send | Self::Sendto | Self::Connect => Direction::Write,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, NifUnitEnum, PartialEq)]
pub enum ArmPoint {
    None,
    BeforeTry,
    BetweenTryAndArm,
    AfterArm,
}

#[derive(Clone, Copy, Debug, Eq, NifMap, Ord, PartialEq, PartialOrd)]
pub struct SocketIdentity {
    pub id: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, NifMap, Ord, PartialEq, PartialOrd)]
pub struct ReadyKey {
    pub identity: SocketIdentity,
    pub direction: Direction,
}

pub struct Waiter {
    pub pid: LocalPid,
    pub operation: Operation,
    reference_env: OwnedEnv,
    reference: SavedTerm,
}

impl Waiter {
    pub fn new(pid: LocalPid, operation: Operation, reference: Reference<'_>) -> Self {
        let reference_env = OwnedEnv::new();
        let saved_reference = reference_env.save(reference);

        Self {
            pid,
            operation,
            reference_env,
            reference: saved_reference,
        }
    }

    pub fn reference<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.reference_env
            .run(|saved_env| self.reference.load(saved_env).in_env(env))
    }

    pub fn matches<'a>(
        &self,
        env: Env<'a>,
        operation: Operation,
        reference: Reference<'a>,
    ) -> bool {
        self.operation == operation && self.reference(env) == *reference
    }
}

impl std::fmt::Debug for Waiter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Waiter")
            .field("pid", &"<pid>")
            .field("operation", &self.operation)
            .field("reference", &"<reference>")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, NifMap)]
pub struct ReadinessCounters {
    pub queued: u64,
    pub coalesced: u64,
    pub overflows: u64,
}

#[derive(Debug)]
struct ReadyState {
    entries: Mutex<BTreeSet<ReadyKey>>,
    capacity: usize,
    overflow: AtomicBool,
    queued: AtomicU64,
    coalesced: AtomicU64,
    overflows: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct ReadyQueue {
    state: Arc<ReadyState>,
}

impl ReadyQueue {
    pub fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0);

        Self {
            state: Arc::new(ReadyState {
                entries: Mutex::new(BTreeSet::new()),
                capacity,
                overflow: AtomicBool::new(false),
                queued: AtomicU64::new(0),
                coalesced: AtomicU64::new(0),
                overflows: AtomicU64::new(0),
            }),
        }
    }

    pub fn waker(&self, key: ReadyKey, flag: Arc<AtomicBool>) -> Waker {
        Waker::from(Arc::new(ReadyWaker {
            key,
            flag,
            state: Arc::clone(&self.state),
        }))
    }

    pub fn drain(&self, limit: usize) -> Vec<ReadyKey> {
        let Some(mut entries) = self.try_entries() else {
            self.state.overflow.store(true, Ordering::Release);
            return Vec::new();
        };
        let count = limit.min(entries.len());
        let keys = entries.iter().take(count).copied().collect::<Vec<_>>();

        for key in &keys {
            entries.remove(key);
        }

        keys
    }

    pub fn take_overflow(&self) -> bool {
        self.state.overflow.swap(false, Ordering::AcqRel)
    }

    pub fn len(&self) -> usize {
        self.try_entries().map_or(0, |entries| entries.len())
    }

    pub fn has_pending(&self) -> bool {
        self.state.overflow.load(Ordering::Acquire)
            || self.try_entries().is_none_or(|entries| !entries.is_empty())
    }

    pub fn clear(&self) {
        if let Some(mut entries) = self.try_entries() {
            entries.clear();
        }
        self.state.overflow.store(false, Ordering::Release);
    }

    pub fn counters(&self) -> ReadinessCounters {
        ReadinessCounters {
            queued: self.state.queued.load(Ordering::Relaxed),
            coalesced: self.state.coalesced.load(Ordering::Relaxed),
            overflows: self.state.overflows.load(Ordering::Relaxed),
        }
    }

    fn try_entries(&self) -> Option<std::sync::MutexGuard<'_, BTreeSet<ReadyKey>>> {
        match self.state.entries.try_lock() {
            Ok(guard) => Some(guard),
            Err(TryLockError::Poisoned(error)) => Some(error.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }
}

#[derive(Debug)]
struct ReadyWaker {
    key: ReadyKey,
    flag: Arc<AtomicBool>,
    state: Arc<ReadyState>,
}

impl ReadyWaker {
    fn mark_ready(&self) {
        if self.flag.swap(true, Ordering::AcqRel) {
            self.state.coalesced.fetch_add(1, Ordering::Relaxed);
            return;
        }

        match self.state.entries.try_lock() {
            Ok(mut entries) if entries.len() < self.state.capacity => {
                entries.insert(self.key);
                self.state.queued.fetch_add(1, Ordering::Relaxed);
            }
            Err(TryLockError::Poisoned(error)) => {
                let mut entries = error.into_inner();

                if entries.len() < self.state.capacity {
                    entries.insert(self.key);
                    self.state.queued.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.state.overflow.store(true, Ordering::Release);
                    self.state.overflows.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(_) | Err(TryLockError::WouldBlock) => {
                self.state.overflow.store(true, Ordering::Release);
                self.state.overflows.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl Wake for ReadyWaker {
    fn wake(self: Arc<Self>) {
        self.mark_ready();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.mark_ready();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::{Direction, ReadyKey, ReadyQueue, SocketIdentity};

    fn key(id: u64, direction: Direction) -> ReadyKey {
        ReadyKey {
            identity: SocketIdentity { id, generation: id },
            direction,
        }
    }

    #[test]
    fn repeated_wakes_coalesce() {
        let queue = ReadyQueue::new(2);
        let flag = Arc::new(AtomicBool::new(false));
        let waker = queue.waker(key(1, Direction::Read), Arc::clone(&flag));

        waker.wake_by_ref();
        waker.wake_by_ref();
        waker.wake_by_ref();

        assert!(flag.load(Ordering::Acquire));
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.counters().queued, 1);
        assert_eq!(queue.counters().coalesced, 2);
    }

    #[test]
    fn overflow_preserves_the_per_socket_ready_flag() {
        let queue = ReadyQueue::new(1);
        let first_flag = Arc::new(AtomicBool::new(false));
        let overflow_flag = Arc::new(AtomicBool::new(false));

        queue
            .waker(key(1, Direction::Read), Arc::clone(&first_flag))
            .wake();
        queue
            .waker(key(2, Direction::Read), Arc::clone(&overflow_flag))
            .wake();

        assert_eq!(queue.len(), 1);
        assert!(queue.take_overflow());
        assert!(overflow_flag.load(Ordering::Acquire));
        assert_eq!(queue.counters().overflows, 1);
    }

    #[test]
    fn waker_contention_never_blocks_and_requests_a_sweep() {
        let queue = ReadyQueue::new(1);
        let flag = Arc::new(AtomicBool::new(false));
        let waker = queue.waker(key(1, Direction::Read), Arc::clone(&flag));
        let _guard = queue.state.entries.lock().unwrap();

        waker.wake_by_ref();

        assert!(flag.load(Ordering::Acquire));
        assert!(queue.take_overflow());
        assert_eq!(queue.counters().overflows, 1);
    }
}
