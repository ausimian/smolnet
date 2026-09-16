use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use rustler::{Env, LocalPid, NifUnitEnum, Reference};

use crate::waiter::{Direction, Operation, ReadyKey, SocketIdentity, Waiter};

// A 64-bit BEAM small integer has a signed 60-bit payload. Keeping identities
// at or below 2^59 - 1 ensures encoding never allocates a bignum.
pub const MAX_SOCKET_ID: u64 = (1_u64 << 59) - 1;

static NEXT_SOCKET_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, NifUnitEnum, PartialEq)]
pub enum SocketKind {
    Tcp,
    Udp,
    Synthetic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)] // Closing is validated now and entered by graceful TCP close in Phase 5.
pub enum SocketLifecycle {
    Open,
    Closing,
}

#[derive(Debug)]
pub struct SocketEntry {
    pub kind: SocketKind,
    pub lifecycle: SocketLifecycle,
    _internal_handle: u64,
    read_waiter: Option<Waiter>,
    write_waiter: Option<Waiter>,
    read_ready: Arc<AtomicBool>,
    write_ready: Arc<AtomicBool>,
    read_sent: Option<Waiter>,
    write_sent: Option<Waiter>,
}

impl SocketEntry {
    fn new(kind: SocketKind, internal_handle: u64) -> Self {
        Self {
            kind,
            lifecycle: SocketLifecycle::Open,
            _internal_handle: internal_handle,
            read_waiter: None,
            write_waiter: None,
            read_ready: Arc::new(AtomicBool::new(false)),
            write_ready: Arc::new(AtomicBool::new(false)),
            read_sent: None,
            write_sent: None,
        }
    }

    fn waiter(&self, direction: Direction) -> &Option<Waiter> {
        match direction {
            Direction::Read => &self.read_waiter,
            Direction::Write => &self.write_waiter,
        }
    }

    fn waiter_mut(&mut self, direction: Direction) -> &mut Option<Waiter> {
        match direction {
            Direction::Read => &mut self.read_waiter,
            Direction::Write => &mut self.write_waiter,
        }
    }

    fn sent(&self, direction: Direction) -> &Option<Waiter> {
        match direction {
            Direction::Read => &self.read_sent,
            Direction::Write => &self.write_sent,
        }
    }

    fn sent_mut(&mut self, direction: Direction) -> &mut Option<Waiter> {
        match direction {
            Direction::Read => &mut self.read_sent,
            Direction::Write => &mut self.write_sent,
        }
    }

    fn ready_flag(&self, direction: Direction) -> &Arc<AtomicBool> {
        match direction {
            Direction::Read => &self.read_ready,
            Direction::Write => &self.write_ready,
        }
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        usize::from(self.read_waiter.is_some()) + usize::from(self.write_waiter.is_some())
    }
}

#[derive(Debug)]
pub struct SocketTable {
    entries: BTreeMap<u64, (u64, SocketEntry)>,
    max_entries: usize,
    waiter_count: usize,
    max_waiters: usize,
}

impl SocketTable {
    pub fn new(max_entries: usize, max_waiters: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            max_entries,
            waiter_count: 0,
            max_waiters,
        }
    }

    pub fn insert(
        &mut self,
        kind: SocketKind,
        internal_handle: u64,
    ) -> Result<SocketIdentity, SocketError> {
        if self.entries.len() >= self.max_entries {
            return Err(SocketError::SystemLimit);
        }

        let id = next_identity(&NEXT_SOCKET_ID)?;
        let generation = next_identity(&NEXT_GENERATION)?;
        let identity = SocketIdentity { id, generation };

        self.entries
            .insert(id, (generation, SocketEntry::new(kind, internal_handle)));

        Ok(identity)
    }

    pub fn validate(
        &self,
        identity: SocketIdentity,
        expected_kind: SocketKind,
    ) -> Result<&SocketEntry, SocketError> {
        let (generation, entry) = self
            .entries
            .get(&identity.id)
            .ok_or(SocketError::InvalidSocket)?;

        if *generation != identity.generation {
            return Err(SocketError::InvalidSocket);
        }

        if entry.kind != expected_kind {
            return Err(SocketError::WrongKind);
        }

        if entry.lifecycle != SocketLifecycle::Open {
            return Err(SocketError::InvalidState);
        }

        Ok(entry)
    }

    pub fn validate_any(&self, identity: SocketIdentity) -> Result<&SocketEntry, SocketError> {
        let (generation, entry) = self
            .entries
            .get(&identity.id)
            .ok_or(SocketError::InvalidSocket)?;

        if *generation != identity.generation {
            return Err(SocketError::InvalidSocket);
        }

        if entry.lifecycle != SocketLifecycle::Open {
            return Err(SocketError::InvalidState);
        }

        Ok(entry)
    }

    pub fn ready_flag(
        &self,
        identity: SocketIdentity,
        expected_kind: SocketKind,
        direction: Direction,
    ) -> Result<Arc<AtomicBool>, SocketError> {
        Ok(Arc::clone(
            self.validate(identity, expected_kind)?
                .ready_flag(direction),
        ))
    }

    pub fn has_waiter(
        &self,
        identity: SocketIdentity,
        expected_kind: SocketKind,
        direction: Direction,
    ) -> Result<bool, SocketError> {
        Ok(self
            .validate(identity, expected_kind)?
            .waiter(direction)
            .is_some())
    }

    pub fn ensure_waiter_capacity(&self) -> Result<(), SocketError> {
        if self.waiter_count >= self.max_waiters {
            Err(SocketError::SystemLimit)
        } else {
            Ok(())
        }
    }

    pub fn take_waiter(
        &mut self,
        identity: SocketIdentity,
        expected_kind: SocketKind,
        direction: Direction,
    ) -> Result<Option<Waiter>, SocketError> {
        self.validate(identity, expected_kind)?;
        let (_, entry) = self
            .entries
            .get_mut(&identity.id)
            .expect("validated socket entry exists");
        let waiter = entry.waiter_mut(direction).take();

        if waiter.is_some() {
            entry.ready_flag(direction).store(false, Ordering::Release);
            self.waiter_count -= 1;
        }

        Ok(waiter)
    }

    pub fn install_waiter<'a>(
        &mut self,
        env: Env<'a>,
        registration: WaiterRegistration<'a>,
    ) -> Result<InstallResult, SocketError> {
        let WaiterRegistration {
            identity,
            expected_kind,
            direction,
            pid,
            operation,
            reference,
        } = registration;

        if operation.direction() != direction {
            return Err(SocketError::InvalidOperation);
        }

        {
            let entry = self.validate(identity, expected_kind)?;

            if let Some(waiter) = entry.waiter(direction) {
                return if waiter.pid == pid && waiter.matches(env, operation, reference) {
                    Ok(InstallResult::AlreadyArmed)
                } else {
                    Err(SocketError::Busy)
                };
            }
        }

        if self.waiter_count >= self.max_waiters {
            return Err(SocketError::SystemLimit);
        }

        let (_, entry) = self
            .entries
            .get_mut(&identity.id)
            .expect("validated socket entry exists");
        *entry.sent_mut(direction) = None;
        *entry.waiter_mut(direction) = Some(Waiter::new(pid, operation, reference));
        self.waiter_count += 1;

        Ok(InstallResult::Armed)
    }

    pub fn cancel<'a>(
        &mut self,
        env: Env<'a>,
        identity: SocketIdentity,
        operation: Operation,
        reference: Reference<'a>,
    ) -> Result<CancelResult, SocketError> {
        let direction = operation.direction();
        let (_, entry) = self
            .entries
            .get_mut(&identity.id)
            .filter(|(generation, _entry)| *generation == identity.generation)
            .ok_or(SocketError::InvalidSocket)?;

        if entry.lifecycle != SocketLifecycle::Open {
            return Err(SocketError::InvalidState);
        }

        if entry
            .waiter(direction)
            .as_ref()
            .is_some_and(|waiter| waiter.matches(env, operation, reference))
        {
            *entry.waiter_mut(direction) = None;
            entry.ready_flag(direction).store(false, Ordering::Release);
            self.waiter_count -= 1;
            return Ok(CancelResult::Cancelled);
        }

        if entry
            .sent(direction)
            .as_ref()
            .is_some_and(|waiter| waiter.matches(env, operation, reference))
        {
            return Ok(CancelResult::AlreadySent);
        }

        Ok(CancelResult::NotFound)
    }

    pub fn take_ready_waiter(&mut self, key: ReadyKey) -> ReadyResult {
        let Some((generation, entry)) = self.entries.get_mut(&key.identity.id) else {
            return ReadyResult::Stale;
        };

        if *generation != key.identity.generation || entry.lifecycle != SocketLifecycle::Open {
            return ReadyResult::Stale;
        }

        if !entry
            .ready_flag(key.direction)
            .swap(false, Ordering::AcqRel)
        {
            return ReadyResult::Coalesced;
        }

        let Some(waiter) = entry.waiter_mut(key.direction).take() else {
            return ReadyResult::NoWaiter;
        };

        self.waiter_count -= 1;
        *entry.sent_mut(key.direction) = Some(waiter);

        ReadyResult::Notify(
            entry
                .sent_mut(key.direction)
                .take()
                .expect("sent waiter was just stored"),
        )
    }

    pub fn remember_sent(
        &mut self,
        identity: SocketIdentity,
        direction: Direction,
        waiter: Waiter,
    ) {
        if let Some((generation, entry)) = self.entries.get_mut(&identity.id)
            && *generation == identity.generation
        {
            *entry.sent_mut(direction) = Some(waiter);
        }
    }

    pub fn close(
        &mut self,
        identity: SocketIdentity,
    ) -> Result<Vec<(Direction, Waiter)>, SocketError> {
        let (generation, mut entry) = self
            .entries
            .remove(&identity.id)
            .ok_or(SocketError::InvalidSocket)?;

        if generation != identity.generation {
            self.entries.insert(identity.id, (generation, entry));
            return Err(SocketError::InvalidSocket);
        }

        let mut waiters = Vec::with_capacity(2);

        if let Some(waiter) = entry.read_waiter.take() {
            waiters.push((Direction::Read, waiter));
        }

        if let Some(waiter) = entry.write_waiter.take() {
            waiters.push((Direction::Write, waiter));
        }

        self.waiter_count -= waiters.len();
        Ok(waiters)
    }

    pub fn close_all(&mut self) -> Vec<(SocketIdentity, Direction, Waiter)> {
        let entries = std::mem::take(&mut self.entries);
        let mut waiters = Vec::with_capacity(self.waiter_count);

        for (id, (generation, mut entry)) in entries {
            let identity = SocketIdentity { id, generation };

            if let Some(waiter) = entry.read_waiter.take() {
                waiters.push((identity, Direction::Read, waiter));
            }

            if let Some(waiter) = entry.write_waiter.take() {
                waiters.push((identity, Direction::Write, waiter));
            }
        }

        self.waiter_count = 0;
        waiters
    }

    pub fn scan_ready(
        &self,
        cursor: Option<ReadyKey>,
        entry_limit: usize,
        key_limit: usize,
    ) -> ReadyScan {
        if entry_limit == 0 || key_limit == 0 {
            return ReadyScan {
                keys: Vec::new(),
                cursor,
                complete: false,
                entries_scanned: 0,
            };
        }

        let start_id = cursor.map_or(0, |key| key.identity.id);
        let start_bound = if cursor.is_some() {
            Included(start_id)
        } else {
            Unbounded
        };
        let mut keys = Vec::new();
        let mut next_cursor = cursor;
        let mut entries_scanned = 0;
        let mut complete = true;

        for (&id, &(generation, ref entry)) in self.entries.range((start_bound, Unbounded)) {
            let identity = SocketIdentity { id, generation };
            let skip_read =
                cursor.is_some_and(|key| key.identity.id == id && key.direction == Direction::Read);

            if cursor.is_some_and(|key| key.identity.id == id && key.direction == Direction::Write)
            {
                continue;
            }

            entries_scanned += 1;

            if !skip_read && entry.read_ready.load(Ordering::Acquire) {
                let key = ReadyKey {
                    identity,
                    direction: Direction::Read,
                };
                keys.push(key);
                next_cursor = Some(key);

                if keys.len() == key_limit {
                    complete = false;
                    break;
                }
            }

            let write_key = ReadyKey {
                identity,
                direction: Direction::Write,
            };

            if entry.write_ready.load(Ordering::Acquire) {
                keys.push(write_key);
            }

            next_cursor = Some(write_key);

            if keys.len() == key_limit || entries_scanned == entry_limit {
                complete = self
                    .entries
                    .range((Excluded(id), Unbounded))
                    .next()
                    .is_none();
                break;
            }
        }

        ReadyScan {
            keys,
            cursor: next_cursor,
            complete,
            entries_scanned,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn waiter_count(&self) -> usize {
        self.waiter_count
    }

    pub fn waiter_counts(&self) -> (usize, usize) {
        self.entries
            .values()
            .fold((0, 0), |(read, write), (_, entry)| {
                (
                    read + usize::from(entry.read_waiter.is_some()),
                    write + usize::from(entry.write_waiter.is_some()),
                )
            })
    }

    #[cfg(test)]
    fn entry_mut(&mut self, identity: SocketIdentity) -> &mut SocketEntry {
        &mut self.entries.get_mut(&identity.id).unwrap().1
    }
}

fn next_identity(counter: &AtomicU64) -> Result<u64, SocketError> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            (current <= MAX_SOCKET_ID).then(|| current + 1)
        })
        .map_err(|_| SocketError::SystemLimit)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallResult {
    Armed,
    AlreadyArmed,
}

pub struct WaiterRegistration<'a> {
    pub identity: SocketIdentity,
    pub expected_kind: SocketKind,
    pub direction: Direction,
    pub pid: LocalPid,
    pub operation: Operation,
    pub reference: Reference<'a>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelResult {
    Cancelled,
    AlreadySent,
    NotFound,
}

#[derive(Debug)]
pub enum ReadyResult {
    Notify(Waiter),
    NoWaiter,
    Coalesced,
    Stale,
}

#[derive(Debug)]
pub struct ReadyScan {
    pub keys: Vec<ReadyKey>,
    pub cursor: Option<ReadyKey>,
    pub complete: bool,
    pub entries_scanned: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SocketError {
    Closed,
    EndOfStream,
    InvalidSocket,
    WrongKind,
    InvalidState,
    InvalidOperation,
    Busy,
    SystemLimit,
    UnsupportedFamily,
    InvalidAddress,
    InvalidPort,
    InvalidBacklog,
    ScopeRequired,
    InvalidScope,
    AddressInUse,
    AddressNotAvailable,
    EphemeralPortsExhausted,
    NetworkUnreachable,
    AlreadyConnected,
    NotBound,
    NotConnected,
    ConnectionRefused,
    ConnectionReset,
    ConnectionTimeout,
    MessageTooLarge,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::{MAX_SOCKET_ID, SocketError, SocketKind, SocketLifecycle, SocketTable};

    #[test]
    fn identities_are_unique_when_internal_handles_are_reused() {
        let mut table = SocketTable::new(4, 4);
        let first = table.insert(SocketKind::Synthetic, 7).unwrap();
        table.close(first).unwrap();
        let second = table.insert(SocketKind::Synthetic, 7).unwrap();

        assert_ne!(first, second);
        assert_eq!(
            table.validate(first, SocketKind::Synthetic).unwrap_err(),
            SocketError::InvalidSocket
        );
        assert_eq!(
            table
                .validate(second, SocketKind::Synthetic)
                .unwrap()
                ._internal_handle,
            7
        );
    }

    #[test]
    fn validation_checks_generation_kind_and_lifecycle() {
        let mut table = SocketTable::new(4, 4);
        let identity = table.insert(SocketKind::Tcp, 3).unwrap();
        let mut stale = identity;
        stale.generation += 1;

        assert_eq!(
            table.validate(stale, SocketKind::Tcp).unwrap_err(),
            SocketError::InvalidSocket
        );
        assert_eq!(
            table.validate(identity, SocketKind::Udp).unwrap_err(),
            SocketError::WrongKind
        );

        table.entry_mut(identity).lifecycle = SocketLifecycle::Closing;
        assert_eq!(
            table.validate(identity, SocketKind::Tcp).unwrap_err(),
            SocketError::InvalidState
        );
    }

    #[test]
    fn identity_limit_stays_within_beam_small_integer_range() {
        assert_eq!(MAX_SOCKET_ID, (1_u64 << 59) - 1);
        assert!(MAX_SOCKET_ID < i64::MAX as u64);

        let counter = AtomicU64::new(MAX_SOCKET_ID);
        assert_eq!(super::next_identity(&counter), Ok(MAX_SOCKET_ID));
        assert_eq!(
            super::next_identity(&counter),
            Err(SocketError::SystemLimit)
        );
    }

    #[test]
    fn entry_waiter_count_never_exceeds_two() {
        let entry = super::SocketEntry::new(SocketKind::Synthetic, 1);
        assert_eq!(entry.waiter_count(), 0);
    }

    #[test]
    fn socket_entries_are_bounded_and_capacity_is_reusable() {
        let mut table = SocketTable::new(2, 2);
        let first = table.insert(SocketKind::Synthetic, 1).unwrap();
        table.insert(SocketKind::Synthetic, 2).unwrap();

        assert_eq!(
            table.insert(SocketKind::Synthetic, 3),
            Err(SocketError::SystemLimit)
        );

        table.close(first).unwrap();
        assert!(table.insert(SocketKind::Synthetic, 3).is_ok());
    }
}
