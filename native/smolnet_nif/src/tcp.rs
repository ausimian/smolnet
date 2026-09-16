use std::collections::BTreeSet;

use rustler::NifMap;
use smoltcp::iface::SocketHandle;
use smoltcp::socket::tcp;
use smoltcp::wire::{IpAddress, IpEndpoint, IpListenEndpoint, Ipv6Address};

use crate::socket_table::SocketError;
use crate::waiter::SocketIdentity;

pub const BUFFER_BYTES: usize = 4 * 1024;
pub const CONNECT_TIMEOUT_MILLIS: u64 = 30_000;
pub const EPHEMERAL_PORT_FIRST: u16 = 49_152;
pub const EPHEMERAL_PORT_LAST: u16 = 50_175;

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
        let octets: [u8; 16] = self
            .address
            .as_slice()
            .try_into()
            .map_err(|_| SocketError::InvalidAddress)?;
        let address = Ipv6Address::from_octets(octets);

        if address.is_multicast() || (!bind && address.is_unspecified()) {
            return Err(SocketError::InvalidAddress);
        }

        if self.port < 0 || self.port > i64::from(u16::MAX) || (!bind && self.port == 0) {
            return Err(SocketError::InvalidPort);
        }

        if link_local(&octets) {
            if self.scope_id == 0 {
                return Err(SocketError::ScopeRequired);
            }

            if self.scope_id < 0 || self.scope_id > i64::from(u32::MAX) {
                return Err(SocketError::InvalidScope);
            }
        } else if self.scope_id != 0 {
            return Err(SocketError::InvalidScope);
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
    pub address: Ipv6Address,
    pub port: u16,
    pub scope_id: u32,
}

impl ValidatedEndpoint {
    pub fn ip_endpoint(self) -> IpEndpoint {
        IpEndpoint::new(IpAddress::Ipv6(self.address), self.port)
    }

    pub fn listen_endpoint(self) -> IpListenEndpoint {
        IpListenEndpoint {
            addr: (!self.address.is_unspecified()).then_some(IpAddress::Ipv6(self.address)),
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
        let IpAddress::Ipv6(address) = endpoint.addr;

        Self {
            address: address.octets().to_vec(),
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
pub struct TcpRecord {
    pub identity: SocketIdentity,
    pub handle: SocketHandle,
    pub phase: ConnectPhase,
    pub local: Option<IpEndpoint>,
    pub local_scope_id: u32,
    pub remote: Option<IpEndpoint>,
    pub remote_scope_id: u32,
}

impl TcpRecord {
    pub fn new(identity: SocketIdentity, handle: SocketHandle) -> Self {
        Self {
            identity,
            handle,
            phase: ConnectPhase::Open,
            local: None,
            local_scope_id: 0,
            remote: None,
            remote_scope_id: 0,
        }
    }
}

pub fn socket() -> tcp::Socket<'static> {
    tcp::Socket::new(
        tcp::SocketBuffer::new(vec![0; BUFFER_BYTES]),
        tcp::SocketBuffer::new(vec![0; BUFFER_BYTES]),
    )
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
        BUFFER_BYTES, EPHEMERAL_PORT_FIRST, EPHEMERAL_PORT_LAST, TcpEndpoint, allocate_ephemeral,
        socket,
    };
    use crate::socket_table::SocketError;

    #[test]
    fn tcp_buffers_have_fixed_bounded_capacity() {
        let socket = socket();
        assert_eq!(socket.recv_capacity(), BUFFER_BYTES);
        assert_eq!(socket.send_capacity(), BUFFER_BYTES);
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
    }
}
