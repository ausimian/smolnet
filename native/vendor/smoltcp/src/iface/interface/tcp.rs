use super::*;

use crate::socket::tcp::{PathMtuOutcome, Socket};

/// Counts of the ICMP errors that report a path MTU for a TCP segment:
/// IPv4 "Fragmentation Needed" (RFC 1191) and ICMPv6 "Packet Too Big"
/// (RFC 8201).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PathMtuStats {
    /// The errors received.
    pub received: u64,
    /// Those ignored as invalid: malformed, quoting a packet this interface
    /// did not send or no connection's segment, or a sequence number that
    /// is not in flight (RFC 5927 4.1).
    pub rejected: u64,
    /// Those that lowered the size of a connection's segments.
    pub reductions: u64,
}

impl InterfaceInner {
    /// Counts an ICMP error that reports a path MTU but is malformed.
    pub(crate) fn reject_path_mtu_error(&mut self) {
        self.path_mtu_stats.received += 1;
        self.path_mtu_stats.rejected += 1;
    }

    /// Applies an ICMP error that reports `mtu` as the path MTU of the
    /// packet it quotes, sent from `src_addr` to `dst_addr`, to the TCP
    /// connection that sent it. `quoted` is what follows the quoted IP
    /// header: the start of the TCP header, at least eight octets of it if
    /// the error is to be used (RFC 792).
    pub(crate) fn process_tcp_path_mtu(
        &mut self,
        sockets: &mut SocketSet,
        src_addr: IpAddress,
        dst_addr: IpAddress,
        quoted: &[u8],
        mtu: usize,
    ) {
        self.path_mtu_stats.received += 1;

        // The quoted packet must be one this interface sent (RFC 5927 4).
        if quoted.len() < 8 || !self.has_ip_addr(src_addr) {
            self.path_mtu_stats.rejected += 1;
            return;
        }
        let local = IpEndpoint::new(src_addr, u16::from_be_bytes([quoted[0], quoted[1]]));
        let remote = IpEndpoint::new(dst_addr, u16::from_be_bytes([quoted[2], quoted[3]]));
        let seq = TcpSeqNumber(i32::from_be_bytes([
            quoted[4], quoted[5], quoted[6], quoted[7],
        ]));

        let outcome = sockets
            .items_mut()
            .filter_map(|i| Socket::downcast_mut(&mut i.socket))
            .map(|socket| socket.process_path_mtu(self, local, remote, seq, mtu))
            .find(|outcome| *outcome != PathMtuOutcome::NotMatched)
            .unwrap_or(PathMtuOutcome::NotMatched);

        match outcome {
            PathMtuOutcome::NotMatched | PathMtuOutcome::Rejected => {
                self.path_mtu_stats.rejected += 1
            }
            PathMtuOutcome::Reduced => self.path_mtu_stats.reductions += 1,
            PathMtuOutcome::Unchanged => {}
        }
    }

    pub(crate) fn process_tcp<'frame>(
        &mut self,
        sockets: &mut SocketSet,
        handled_by_raw_socket: bool,
        ip_repr: IpRepr,
        ip_payload: &'frame [u8],
    ) -> Option<Packet<'frame>> {
        let (src_addr, dst_addr) = (ip_repr.src_addr(), ip_repr.dst_addr());

        // Per RFC 1122 §3.2.1.3, the unspecified address must never appear as a source
        // or destination in any IP datagram. Drop such TCP segments early to avoid
        // creating sockets with unspecified peers (which would later panic on egress).
        // This is not done at the iface level because it might be useful with
        // UDP or raw sockets, but it's definitely not useful for TCP.
        if src_addr.is_unspecified() || dst_addr.is_unspecified() {
            return None;
        }

        let tcp_packet = check!(TcpPacket::new_checked(ip_payload));
        let tcp_repr = check!(TcpRepr::parse(
            &tcp_packet,
            &src_addr,
            &dst_addr,
            &self.caps.checksum
        ));

        for tcp_socket in sockets
            .items_mut()
            .filter_map(|i| Socket::downcast_mut(&mut i.socket))
        {
            if tcp_socket.accepts(self, &ip_repr, &tcp_repr) {
                return tcp_socket
                    .process(self, &ip_repr, &tcp_repr)
                    .map(|(ip, tcp)| Packet::new(ip, IpPayload::Tcp(tcp)));
            }
        }

        if tcp_repr.control == TcpControl::Rst
            || ip_repr.dst_addr().is_unspecified()
            || ip_repr.src_addr().is_unspecified()
            || handled_by_raw_socket
        {
            // Never reply to a TCP RST packet with another TCP RST packet.
            // Never send a TCP RST packet with unspecified addresses.
            // Never send a TCP RST when packet has been handled by raw socket.
            None
        } else {
            // The packet wasn't handled by a socket, send a TCP RST packet.
            let (ip, tcp) = tcp::Socket::rst_reply(&ip_repr, &tcp_repr);
            Some(Packet::new(ip, IpPayload::Tcp(tcp)))
        }
    }
}
