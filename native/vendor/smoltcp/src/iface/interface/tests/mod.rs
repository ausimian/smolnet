#[cfg(feature = "proto-ipv4")]
mod ipv4;
#[cfg(feature = "proto-ipv6")]
mod ipv6;
#[cfg(feature = "proto-sixlowpan")]
mod sixlowpan;

#[allow(unused)]
use std::vec::Vec;

use crate::tests::setup;

use rstest::*;

use super::*;

use crate::iface::Interface;
use crate::phy::ChecksumCapabilities;
#[cfg(feature = "alloc")]
use crate::phy::Loopback;
use crate::time::Instant;

#[allow(unused)]
fn fill_slice(s: &mut [u8], val: u8) {
    for x in s.iter_mut() {
        *x = val
    }
}

#[allow(unused)]
fn recv_all(device: &mut crate::tests::TestingDevice, timestamp: Instant) -> Vec<Vec<u8>> {
    let mut pkts = Vec::new();
    while let Some(pkt) = device.tx_queue.pop_front() {
        pkts.push(pkt)
    }
    pkts
}

#[derive(Debug, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
struct MockTxToken;

impl TxToken for MockTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut junk = [0; 1536];
        f(&mut junk[..len])
    }
}

#[test]
#[should_panic(expected = "The hardware address does not match the medium of the interface.")]
#[cfg(all(feature = "medium-ip", feature = "medium-ethernet", feature = "alloc"))]
fn test_new_panic() {
    let mut device = Loopback::new(Medium::Ethernet);
    let config = Config::new(HardwareAddress::Ip);
    Interface::new(config, &mut device, Instant::ZERO);
}

#[cfg(feature = "socket-udp")]
#[rstest]
#[case::ip(Medium::Ip)]
#[cfg(feature = "medium-ip")]
#[case::ethernet(Medium::Ethernet)]
#[cfg(feature = "medium-ethernet")]
#[case::ieee802154(Medium::Ieee802154)]
#[cfg(feature = "medium-ieee802154")]
fn test_handle_udp_broadcast(#[case] medium: Medium) {
    use crate::socket::udp;
    use crate::wire::IpEndpoint;

    static UDP_PAYLOAD: [u8; 5] = [0x48, 0x65, 0x6c, 0x6c, 0x6f];

    let (mut iface, mut sockets, _device) = setup(medium);

    let rx_buffer = udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY], vec![0; 15]);
    let tx_buffer = udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY], vec![0; 15]);

    let udp_socket = udp::Socket::new(rx_buffer, tx_buffer);

    let mut udp_bytes = vec![0u8; 13];
    let mut packet = UdpPacket::new_unchecked(&mut udp_bytes);

    let socket_handle = sockets.add(udp_socket);

    #[cfg(feature = "proto-ipv6")]
    let src_ip = Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 1);
    #[cfg(all(not(feature = "proto-ipv6"), feature = "proto-ipv4"))]
    let src_ip = Ipv4Address::new(0x7f, 0x00, 0x00, 0x02);

    let udp_repr = UdpRepr {
        src_port: 67,
        dst_port: 68,
    };

    #[cfg(feature = "proto-ipv6")]
    let ip_repr = IpRepr::Ipv6(Ipv6Repr {
        src_addr: src_ip,
        dst_addr: IPV6_LINK_LOCAL_ALL_NODES,
        next_header: IpProtocol::Udp,
        payload_len: udp_repr.header_len() + UDP_PAYLOAD.len(),
        hop_limit: 0x40,
    });
    #[cfg(all(not(feature = "proto-ipv6"), feature = "proto-ipv4"))]
    let ip_repr = IpRepr::Ipv4(Ipv4Repr {
        src_addr: src_ip,
        dst_addr: Ipv4Address::BROADCAST,
        next_header: IpProtocol::Udp,
        payload_len: udp_repr.header_len() + UDP_PAYLOAD.len(),
        hop_limit: 0x40,
    });
    let dst_addr = ip_repr.dst_addr();

    // Bind the socket to port 68
    let socket = sockets.get_mut::<udp::Socket>(socket_handle);
    assert_eq!(socket.bind(68), Ok(()));
    assert!(!socket.can_recv());
    assert!(socket.can_send());

    udp_repr.emit(
        &mut packet,
        &ip_repr.src_addr(),
        &ip_repr.dst_addr(),
        UDP_PAYLOAD.len(),
        |buf| buf.copy_from_slice(&UDP_PAYLOAD),
        &ChecksumCapabilities::default(),
    );

    // Packet should be handled by bound UDP socket
    assert_eq!(
        iface.inner.process_udp(
            &mut sockets,
            PacketMeta::default(),
            false,
            ip_repr,
            packet.into_inner(),
        ),
        None
    );

    // Make sure the payload to the UDP packet processed by process_udp is
    // appended to the bound sockets rx_buffer
    let socket = sockets.get_mut::<udp::Socket>(socket_handle);
    assert!(socket.can_recv());
    assert_eq!(
        socket.recv(),
        Ok((
            &UDP_PAYLOAD[..],
            udp::UdpMetadata {
                local_address: Some(dst_addr),
                ..IpEndpoint::new(src_ip.into(), 67).into()
            }
        ))
    );
}

#[test]
#[cfg(all(feature = "medium-ip", feature = "socket-tcp", feature = "proto-ipv6"))]
pub fn tcp_not_accepted() {
    let (mut iface, mut sockets, _) = setup(Medium::Ip);
    let tcp = TcpRepr {
        src_port: 4242,
        dst_port: 4243,
        control: TcpControl::Syn,
        seq_number: TcpSeqNumber(-10001),
        ack_number: None,
        window_len: 256,
        window_scale: None,
        max_seg_size: None,
        sack_permitted: false,
        sack_ranges: [None, None, None],
        timestamp: None,
        payload: &[],
    };

    let mut tcp_bytes = vec![0u8; tcp.buffer_len()];

    tcp.emit(
        &mut TcpPacket::new_unchecked(&mut tcp_bytes),
        &Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 2).into(),
        &Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 1).into(),
        &ChecksumCapabilities::default(),
    );

    assert_eq!(
        iface.inner.process_tcp(
            &mut sockets,
            false,
            IpRepr::Ipv6(Ipv6Repr {
                src_addr: Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 2),
                dst_addr: Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
                next_header: IpProtocol::Tcp,
                payload_len: tcp.buffer_len(),
                hop_limit: 64,
            }),
            &tcp_bytes,
        ),
        Some(Packet::new_ipv6(
            Ipv6Repr {
                src_addr: Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
                dst_addr: Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 2),
                next_header: IpProtocol::Tcp,
                payload_len: tcp.buffer_len(),
                hop_limit: 64,
            },
            IpPayload::Tcp(TcpRepr {
                src_port: 4243,
                dst_port: 4242,
                control: TcpControl::Rst,
                seq_number: TcpSeqNumber(0),
                ack_number: Some(TcpSeqNumber(-10000)),
                window_len: 0,
                window_scale: None,
                max_seg_size: None,
                sack_permitted: false,
                sack_ranges: [None, None, None],
                timestamp: None,
                payload: &[],
            })
        ))
    );
    // Unspecified destination address.
    tcp.emit(
        &mut TcpPacket::new_unchecked(&mut tcp_bytes),
        &Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 2).into(),
        &Ipv6Address::UNSPECIFIED.into(),
        &ChecksumCapabilities::default(),
    );

    assert_eq!(
        iface.inner.process_tcp(
            &mut sockets,
            false,
            IpRepr::Ipv6(Ipv6Repr {
                src_addr: Ipv6Address::new(0xfe80, 0, 0, 0, 0, 0, 0, 2),
                dst_addr: Ipv6Address::UNSPECIFIED,
                next_header: IpProtocol::Tcp,
                payload_len: tcp.buffer_len(),
                hop_limit: 64,
            }),
            &tcp_bytes,
        ),
        None,
    );
}

#[test]
#[cfg(all(feature = "medium-ip", feature = "socket-tcp", feature = "proto-ipv4"))]
pub fn tcp_listen_drops_unspecified_src() {
    use crate::socket::tcp;

    let (mut iface, mut sockets, _) = setup(Medium::Ip);

    let tcp_socket = tcp::Socket::new(
        tcp::SocketBuffer::new(vec![0; 64]),
        tcp::SocketBuffer::new(vec![0; 64]),
    );
    let handle = sockets.add(tcp_socket);
    sockets.get_mut::<tcp::Socket>(handle).listen(1234).unwrap();

    let tcp = TcpRepr {
        src_port: 65000,
        dst_port: 1234,
        control: TcpControl::Syn,
        seq_number: TcpSeqNumber(0),
        ack_number: None,
        window_len: 1024,
        window_scale: None,
        max_seg_size: Some(1460),
        sack_permitted: false,
        sack_ranges: [None, None, None],
        timestamp: None,
        payload: &[],
    };

    let mut tcp_bytes = vec![0u8; tcp.buffer_len()];
    tcp.emit(
        &mut TcpPacket::new_unchecked(&mut tcp_bytes),
        &Ipv4Address::UNSPECIFIED.into(),
        &Ipv4Address::new(127, 0, 0, 1).into(),
        &ChecksumCapabilities::default(),
    );

    let reply = iface.inner.process_tcp(
        &mut sockets,
        false,
        IpRepr::Ipv4(Ipv4Repr {
            src_addr: Ipv4Address::UNSPECIFIED,
            dst_addr: Ipv4Address::new(127, 0, 0, 1),
            next_header: IpProtocol::Tcp,
            payload_len: tcp.buffer_len(),
            hop_limit: 64,
        }),
        &tcp_bytes,
    );

    assert_eq!(reply, None);
    assert!(sockets.get_mut::<tcp::Socket>(handle).is_listening());
}

/// For path MTU discovery: a TCP connection from `local` to `remote`, set
/// up by hand, whose data segments the remote never acknowledges.
#[cfg(all(feature = "medium-ip", feature = "socket-tcp"))]
struct PmtuConnection {
    iface: Interface,
    sockets: SocketSet<'static>,
    device: crate::tests::TestingDevice,
    local: IpAddress,
    remote: IpAddress,
    /// The sequence number of the first octet of data.
    first: TcpSeqNumber,
    /// The data segments sent, as IP packets.
    sent: Vec<Vec<u8>>,
    now: Instant,
}

#[cfg(all(feature = "medium-ip", feature = "socket-tcp"))]
impl PmtuConnection {
    const LOCAL_PORT: u16 = 49152;
    const REMOTE_PORT: u16 = 80;

    /// Connects, and sends `len` octets in segments of the interface's MTU.
    fn new(local: IpAddress, remote: IpAddress, len: usize) -> Self {
        use crate::socket::tcp;

        let (mut iface, mut sockets, device) = setup(Medium::Ip);
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 1024]),
            tcp::SocketBuffer::new(vec![0; len]),
        );
        let remote_end = (remote, Self::REMOTE_PORT);
        let local_end = (local, Self::LOCAL_PORT);
        socket
            .connect(iface.context(), remote_end, local_end)
            .unwrap();
        let handle = sockets.add(socket);

        let mut conn = PmtuConnection {
            iface,
            sockets,
            device,
            local,
            remote,
            first: TcpSeqNumber(0),
            sent: Vec::new(),
            now: Instant::ZERO,
        };
        let syn = conn.poll().pop().expect("a SYN");
        let isn = pmtu_segment(&syn).0;
        conn.first = isn + 1;

        let syn_ack = TcpRepr {
            src_port: Self::REMOTE_PORT,
            dst_port: Self::LOCAL_PORT,
            control: TcpControl::Syn,
            seq_number: TcpSeqNumber(-10_000),
            ack_number: Some(isn + 1),
            window_len: 65535,
            window_scale: None,
            max_seg_size: Some(1460),
            sack_permitted: false,
            sack_ranges: [None, None, None],
            timestamp: None,
            payload: &[],
        };
        let syn_ack = pmtu_tcp_packet(remote, local, &syn_ack);
        conn.deliver(syn_ack);

        conn.sockets
            .get_mut::<tcp::Socket>(handle)
            .send_slice(&vec![0x5a; len])
            .unwrap();
        conn.sent = conn.poll();
        conn
    }

    /// Polls the interface a millisecond on, and returns the data segments
    /// it sends, or a SYN.
    fn poll(&mut self) -> Vec<Vec<u8>> {
        self.now += crate::time::Duration::from_millis(1);
        self.iface
            .poll(self.now, &mut self.device, &mut self.sockets);
        self.device
            .tx_queue
            .drain(..)
            .filter(|packet| {
                let (_, len, syn) = pmtu_segment(packet);
                len > 0 || syn
            })
            .collect()
    }

    /// Delivers `packet` to the interface, and returns what [`Self::poll`]
    /// does.
    fn deliver(&mut self, packet: Vec<u8>) -> Vec<Vec<u8>> {
        self.device.rx_queue.push_back(packet);
        self.poll()
    }

    /// Asserts that `resent` holds all `len` octets of data again, from the
    /// first, in segments of `mss` octets.
    #[track_caller]
    fn assert_resent(&self, resent: &[Vec<u8>], mss: usize, len: usize) {
        let mut next = self.first;
        for packet in resent {
            let (seq, payload_len, _) = pmtu_segment(packet);
            assert_eq!(seq, next, "resends in order");
            assert_eq!(payload_len, mss.min(len - (next - self.first)));
            next += payload_len;
        }
        assert_eq!(next - self.first, len, "resends all the data");
    }
}

/// The payload of an IP packet.
#[cfg(all(feature = "medium-ip", feature = "socket-tcp"))]
fn pmtu_ip_payload(packet: &[u8]) -> &[u8] {
    match IpVersion::of_packet(packet).unwrap() {
        #[cfg(feature = "proto-ipv4")]
        IpVersion::Ipv4 => Ipv4Packet::new_checked(packet).unwrap().payload(),
        #[cfg(feature = "proto-ipv6")]
        IpVersion::Ipv6 => Ipv6Packet::new_checked(packet).unwrap().payload(),
    }
}

/// The sequence number, payload length and SYN flag of the TCP segment in
/// an IP packet.
#[cfg(all(feature = "medium-ip", feature = "socket-tcp"))]
fn pmtu_segment(packet: &[u8]) -> (TcpSeqNumber, usize, bool) {
    let segment = TcpPacket::new_checked(pmtu_ip_payload(packet)).unwrap();
    (segment.seq_number(), segment.payload().len(), segment.syn())
}

/// An IP packet from `src` to `dst` that carries `tcp`.
#[cfg(all(feature = "medium-ip", feature = "socket-tcp"))]
fn pmtu_tcp_packet(src: IpAddress, dst: IpAddress, tcp: &TcpRepr) -> Vec<u8> {
    let caps = ChecksumCapabilities::default();
    let ip_repr = IpRepr::new(src, dst, IpProtocol::Tcp, tcp.buffer_len(), 64);
    let mut bytes = vec![0; ip_repr.buffer_len()];
    ip_repr.emit(&mut bytes[..], &caps);
    let segment = &mut bytes[ip_repr.header_len()..];
    tcp.emit(&mut TcpPacket::new_unchecked(segment), &src, &dst, &caps);
    bytes
}
