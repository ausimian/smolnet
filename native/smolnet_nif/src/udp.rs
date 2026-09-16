use smoltcp::iface::SocketHandle;
use smoltcp::socket::udp;

use crate::tcp::{AddressFamily, ValidatedEndpoint};
use crate::waiter::SocketIdentity;

pub const PACKET_CAPACITY: usize = 16;
pub const PAYLOAD_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct UdpRecord {
    pub identity: SocketIdentity,
    pub handle: SocketHandle,
    pub family: AddressFamily,
    pub local: Option<ValidatedEndpoint>,
    pub peer: Option<ValidatedEndpoint>,
}

impl UdpRecord {
    pub fn new(identity: SocketIdentity, handle: SocketHandle, family: AddressFamily) -> Self {
        Self {
            identity,
            handle,
            family,
            local: None,
            peer: None,
        }
    }
}

pub fn socket() -> udp::Socket<'static> {
    let receive = udp::PacketBuffer::new(
        vec![udp::PacketMetadata::EMPTY; PACKET_CAPACITY],
        vec![0; PAYLOAD_BYTES],
    );
    let transmit = udp::PacketBuffer::new(
        vec![udp::PacketMetadata::EMPTY; PACKET_CAPACITY],
        vec![0; PAYLOAD_BYTES],
    );

    udp::Socket::new(receive, transmit)
}

pub fn max_datagram_bytes(mtu: usize) -> usize {
    mtu.saturating_sub(48).min(PAYLOAD_BYTES)
}

#[cfg(test)]
mod tests {
    use super::{PACKET_CAPACITY, PAYLOAD_BYTES, max_datagram_bytes, socket};
    use smoltcp::socket::udp::SendError;
    use smoltcp::wire::{IpAddress, IpEndpoint, Ipv6Address};

    #[test]
    fn udp_buffers_have_fixed_packet_and_payload_capacity() {
        let socket = socket();

        assert_eq!(socket.packet_recv_capacity(), PACKET_CAPACITY);
        assert_eq!(socket.packet_send_capacity(), PACKET_CAPACITY);
        assert_eq!(socket.payload_recv_capacity(), PAYLOAD_BYTES);
        assert_eq!(socket.payload_send_capacity(), PAYLOAD_BYTES);
        assert_eq!(max_datagram_bytes(1_500), 1_452);
        assert_eq!(max_datagram_bytes(65_575), PAYLOAD_BYTES);
    }

    #[test]
    fn full_packet_ring_rejects_a_whole_datagram_without_partial_progress() {
        let mut socket = socket();
        let endpoint = IpEndpoint::new(
            IpAddress::Ipv6(Ipv6Address::new(0, 0, 0, 0, 0, 0, 0, 1)),
            42_000,
        );
        socket.bind(42_001).expect("test socket binds");

        for sequence in 0..PACKET_CAPACITY {
            socket
                .send_slice(&[sequence as u8], endpoint)
                .expect("bounded packet slot remains available");
        }

        assert_eq!(
            socket.send_slice(b"rejected", endpoint),
            Err(SendError::BufferFull)
        );
        assert_eq!(socket.send_queue(), PACKET_CAPACITY);
    }
}
