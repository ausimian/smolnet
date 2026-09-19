use std::collections::{BTreeSet, VecDeque};

use rustler::{NifMap, NifUnitEnum};
use smoltcp::iface::SocketHandle;
use smoltcp::socket::tcp;
use smoltcp::time::Instant;
use smoltcp::wire::{IpAddress, IpEndpoint, IpListenEndpoint, Ipv4Address, Ipv6Address};

use crate::socket_table::SocketError;
use crate::waiter::SocketIdentity;

pub const DEFAULT_BUFFER_BYTES: usize = 64 * 1024;
pub const MIN_BUFFER_BYTES: usize = 1024;
pub const MAX_BUFFER_BYTES: usize = 1024 * 1024;
pub const CONNECT_TIMEOUT_MILLIS: u64 = 30_000;
pub const CLOSE_TIMEOUT_MILLIS: u64 = 30_000;
pub const EPHEMERAL_PORT_FIRST: u16 = 49_152;
pub const EPHEMERAL_PORT_LAST: u16 = 50_175;
pub const LISTENER_POOL_MAX: usize = 4;
pub const LISTENER_BACKLOG_MAX: usize = 128;

#[derive(Clone, Copy, Debug, Eq, NifUnitEnum, Ord, PartialEq, PartialOrd)]
pub enum AddressFamily {
    Inet,
    Inet6,
}

impl AddressFamily {
    pub fn of(address: IpAddress) -> Self {
        match address {
            IpAddress::Ipv4(_) => Self::Inet,
            IpAddress::Ipv6(_) => Self::Inet6,
        }
    }

    pub fn unspecified(self) -> IpAddress {
        match self {
            Self::Inet => IpAddress::Ipv4(Ipv4Address::UNSPECIFIED),
            Self::Inet6 => IpAddress::Ipv6(Ipv6Address::UNSPECIFIED),
        }
    }

    pub fn matches(self, address: IpAddress) -> bool {
        matches!(
            (self, address),
            (Self::Inet, IpAddress::Ipv4(_)) | (Self::Inet6, IpAddress::Ipv6(_))
        )
    }
}

#[derive(Clone, Debug)]
pub struct TcpEndpoint {
    pub address: Vec<u8>,
    pub port: i64,
    pub scope_id: i64,
}

impl TcpEndpoint {
    pub fn bind_endpoint(&self) -> Result<ValidatedEndpoint, SocketError> {
        self.validate(true)
    }

    pub fn remote_endpoint(&self) -> Result<ValidatedEndpoint, SocketError> {
        self.validate(false)
    }

    fn validate(&self, bind: bool) -> Result<ValidatedEndpoint, SocketError> {
        let address = match self.address.as_slice() {
            octets if octets.len() == 4 => {
                let octets: [u8; 4] = octets.try_into().expect("checked IPv4 length");
                IpAddress::Ipv4(Ipv4Address::from_octets(octets))
            }
            octets if octets.len() == 16 => {
                let octets: [u8; 16] = octets.try_into().expect("checked IPv6 length");
                if ipv4_mapped(&octets) {
                    return Err(SocketError::InvalidAddress);
                }
                IpAddress::Ipv6(Ipv6Address::from_octets(octets))
            }
            _ => return Err(SocketError::InvalidAddress),
        };

        if address.is_multicast() || address.is_broadcast() || (!bind && address.is_unspecified()) {
            return Err(SocketError::InvalidAddress);
        }

        if self.port < 0 || self.port > i64::from(u16::MAX) || (!bind && self.port == 0) {
            return Err(SocketError::InvalidPort);
        }

        match address {
            IpAddress::Ipv6(address) if link_local(&address.octets()) => {
                if self.scope_id == 0 {
                    return Err(SocketError::ScopeRequired);
                }

                if self.scope_id < 0 || self.scope_id > i64::from(u32::MAX) {
                    return Err(SocketError::InvalidScope);
                }
            }
            _ if self.scope_id != 0 => return Err(SocketError::InvalidScope),
            _ => {}
        }

        Ok(ValidatedEndpoint {
            address,
            port: self.port as u16,
            scope_id: self.scope_id as u32,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedEndpoint {
    pub address: IpAddress,
    pub port: u16,
    pub scope_id: u32,
}

impl ValidatedEndpoint {
    pub fn ip_endpoint(self) -> IpEndpoint {
        IpEndpoint::new(self.address, self.port)
    }

    pub fn listen_endpoint(self) -> IpListenEndpoint {
        IpListenEndpoint {
            addr: (!self.address.is_unspecified()).then_some(self.address),
            port: self.port,
        }
    }
}

#[derive(Clone, Debug, NifMap)]
pub struct EncodedEndpoint {
    pub address: Vec<u8>,
    pub port: u16,
    pub scope_id: u32,
}

impl EncodedEndpoint {
    pub fn new(endpoint: IpEndpoint, scope_id: u32) -> Self {
        Self {
            address: match endpoint.addr {
                IpAddress::Ipv4(address) => address.octets().to_vec(),
                IpAddress::Ipv6(address) => address.octets().to_vec(),
            },
            port: endpoint.port,
            scope_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectPhase {
    Open,
    Bound,
    Connecting,
    Connected,
    Failed(ConnectFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectFailure {
    Refused,
    Reset,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, NifUnitEnum, PartialEq)]
pub enum ShutdownHow {
    Read,
    Write,
    ReadWrite,
}

impl ConnectFailure {
    pub fn socket_error(self) -> SocketError {
        match self {
            Self::Refused => SocketError::ConnectionRefused,
            Self::Reset => SocketError::ConnectionReset,
            Self::TimedOut => SocketError::ConnectionTimeout,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TcpBufferSizes {
    pub rcvbuf: usize,
    pub sndbuf: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct TcpRecord {
    pub identity: SocketIdentity,
    pub handle: SocketHandle,
    pub family: AddressFamily,
    pub phase: ConnectPhase,
    pub local: Option<IpEndpoint>,
    pub local_scope_id: u32,
    pub remote: Option<IpEndpoint>,
    pub remote_scope_id: u32,
    pub read_shutdown: bool,
    pub write_shutdown: bool,
    pub close_deadline: Option<Instant>,
    pub accepted: bool,
    pub rcvbuf: usize,
    pub sndbuf: usize,
}

#[derive(Debug)]
pub struct ListenerRecord {
    pub identity: SocketIdentity,
    pub endpoint: ValidatedEndpoint,
    pub listen_endpoint: IpListenEndpoint,
    pub local: IpEndpoint,
    pub backlog: usize,
    pub pool_target: usize,
    pub pool: BTreeSet<SocketHandle>,
    pub accepted: VecDeque<SocketIdentity>,
    pub rcvbuf: usize,
    pub sndbuf: usize,
}

impl ListenerRecord {
    pub fn new(
        identity: SocketIdentity,
        endpoint: ValidatedEndpoint,
        listen_endpoint: IpListenEndpoint,
        local: IpEndpoint,
        backlog: usize,
        handles: impl IntoIterator<Item = SocketHandle>,
        buffer_sizes: TcpBufferSizes,
    ) -> Self {
        Self {
            identity,
            endpoint,
            listen_endpoint,
            local,
            backlog,
            pool_target: backlog.min(LISTENER_POOL_MAX),
            pool: handles.into_iter().collect(),
            accepted: VecDeque::with_capacity(backlog),
            rcvbuf: buffer_sizes.rcvbuf,
            sndbuf: buffer_sizes.sndbuf,
        }
    }
}

impl TcpRecord {
    pub fn new(
        identity: SocketIdentity,
        handle: SocketHandle,
        family: AddressFamily,
        rcvbuf: usize,
        sndbuf: usize,
    ) -> Self {
        Self {
            identity,
            handle,
            family,
            phase: ConnectPhase::Open,
            local: None,
            local_scope_id: 0,
            remote: None,
            remote_scope_id: 0,
            read_shutdown: false,
            write_shutdown: false,
            close_deadline: None,
            accepted: false,
            rcvbuf,
            sndbuf,
        }
    }

    pub fn accepted(
        identity: SocketIdentity,
        handle: SocketHandle,
        local: IpEndpoint,
        remote: IpEndpoint,
        scope_id: u32,
        rcvbuf: usize,
        sndbuf: usize,
    ) -> Self {
        Self {
            identity,
            handle,
            family: AddressFamily::of(local.addr),
            phase: ConnectPhase::Connected,
            local: Some(local),
            local_scope_id: scope_id,
            remote: Some(remote),
            remote_scope_id: scope_id,
            read_shutdown: false,
            write_shutdown: false,
            close_deadline: None,
            accepted: true,
            rcvbuf,
            sndbuf,
        }
    }
}

fn ipv4_mapped(octets: &[u8; 16]) -> bool {
    octets[..10].iter().all(|byte| *byte == 0) && octets[10..12] == [0xff, 0xff]
}

pub fn valid_buffer_bytes(bytes: usize) -> bool {
    (MIN_BUFFER_BYTES..=MAX_BUFFER_BYTES).contains(&bytes)
}

pub fn socket(rcvbuf: usize, sndbuf: usize) -> Result<tcp::Socket<'static>, SocketError> {
    if !valid_buffer_bytes(rcvbuf) || !valid_buffer_bytes(sndbuf) {
        return Err(SocketError::InvalidOptions);
    }

    Ok(tcp::Socket::new(
        tcp::SocketBuffer::new(vec![0; rcvbuf]),
        tcp::SocketBuffer::new(vec![0; sndbuf]),
    ))
}

pub fn default_socket() -> tcp::Socket<'static> {
    socket(DEFAULT_BUFFER_BYTES, DEFAULT_BUFFER_BYTES)
        .expect("the TCP default buffer size is within the validated range")
}

pub fn allocate_ephemeral(
    used: &BTreeSet<u16>,
    start: u16,
    first: u16,
    last: u16,
) -> Result<u16, SocketError> {
    let count = usize::from(last - first) + 1;
    let start = start.clamp(first, last);

    for offset in 0..count {
        let candidate = first + ((usize::from(start - first) + offset) % count) as u16;

        if !used.contains(&candidate) {
            return Ok(candidate);
        }
    }

    Err(SocketError::EphemeralPortsExhausted)
}

pub fn link_local(address: &[u8; 16]) -> bool {
    address[0] == 0xfe && address[1] & 0xc0 == 0x80
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        DEFAULT_BUFFER_BYTES, EPHEMERAL_PORT_FIRST, EPHEMERAL_PORT_LAST, MAX_BUFFER_BYTES,
        MIN_BUFFER_BYTES, TcpEndpoint, allocate_ephemeral, socket,
    };
    use crate::socket_table::SocketError;

    #[test]
    fn tcp_buffers_have_configurable_bounded_capacity() {
        let configured = socket(32 * 1024, 96 * 1024).unwrap();
        assert_eq!(configured.recv_capacity(), 32 * 1024);
        assert_eq!(configured.send_capacity(), 96 * 1024);

        assert_eq!(
            socket(MIN_BUFFER_BYTES - 1, DEFAULT_BUFFER_BYTES).unwrap_err(),
            SocketError::InvalidOptions
        );
        assert_eq!(
            socket(DEFAULT_BUFFER_BYTES, MAX_BUFFER_BYTES + 1).unwrap_err(),
            SocketError::InvalidOptions
        );
    }

    #[test]
    fn ephemeral_allocation_wraps_and_exhausts_deterministically() {
        let mut used = BTreeSet::from([10, 11, 12]);
        assert_eq!(allocate_ephemeral(&used, 11, 10, 13), Ok(13));
        used.insert(13);
        assert_eq!(
            allocate_ephemeral(&used, 11, 10, 13),
            Err(SocketError::EphemeralPortsExhausted)
        );

        assert_eq!(EPHEMERAL_PORT_LAST - EPHEMERAL_PORT_FIRST + 1, 1_024);
    }

    #[test]
    fn endpoint_validation_rejects_bad_ports_addresses_and_scopes() {
        let global = vec![0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let link_local = vec![0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let broadcast = vec![255, 255, 255, 255];

        assert_eq!(
            TcpEndpoint {
                address: global.clone(),
                port: -1,
                scope_id: 0,
            }
            .bind_endpoint(),
            Err(SocketError::InvalidPort)
        );
        assert_eq!(
            TcpEndpoint {
                address: vec![0; 15],
                port: 80,
                scope_id: 0,
            }
            .remote_endpoint(),
            Err(SocketError::InvalidAddress)
        );
        assert_eq!(
            TcpEndpoint {
                address: link_local,
                port: 80,
                scope_id: 0,
            }
            .remote_endpoint(),
            Err(SocketError::ScopeRequired)
        );
        assert_eq!(
            TcpEndpoint {
                address: global,
                port: 80,
                scope_id: 1,
            }
            .remote_endpoint(),
            Err(SocketError::InvalidScope)
        );
        assert_eq!(
            TcpEndpoint {
                address: broadcast,
                port: 80,
                scope_id: 0,
            }
            .remote_endpoint(),
            Err(SocketError::InvalidAddress)
        );
    }
}
