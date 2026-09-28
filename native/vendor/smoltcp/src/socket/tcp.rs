// Heads up! Before working on this file you should read, at least, RFC 793 and
// the parts of RFC 1122 that discuss TCP, as well as RFC 7323 for some of the TCP options.
// Consult RFC 7414 when implementing a new feature.

use core::fmt::Display;
#[cfg(feature = "async")]
use core::task::Waker;
use core::{fmt, mem};

use crate::phy::PacketMeta;
#[cfg(feature = "async")]
use crate::socket::WakerRegistration;
use crate::socket::{Context, PollAt};
use crate::storage::{Assembler, RingBuffer};
use crate::time::{Duration, Instant};
use crate::wire::{
    IpAddress, IpEndpoint, IpListenEndpoint, IpProtocol, IpRepr, TCP_HEADER_LEN, TcpControl,
    TcpRepr, TcpSeqNumber, TcpTimestampGenerator, TcpTimestampRepr,
};

mod congestion;
mod rack;
mod scoreboard;

use rack::{Probe, Rack};
use scoreboard::Scoreboard;

macro_rules! tcp_trace {
    ($($arg:expr),*) => (net_log!(trace, $($arg),*));
}

/// Error returned by [`Socket::listen`]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ListenError {
    InvalidState,
    Unaddressable,
}

impl Display for ListenError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ListenError::InvalidState => write!(f, "invalid state"),
            ListenError::Unaddressable => write!(f, "unaddressable destination"),
        }
    }
}

impl core::error::Error for ListenError {}

/// Error returned by [`Socket::connect`]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ConnectError {
    InvalidState,
    Unaddressable,
}

impl Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConnectError::InvalidState => write!(f, "invalid state"),
            ConnectError::Unaddressable => write!(f, "unaddressable destination"),
        }
    }
}

impl core::error::Error for ConnectError {}

/// Error returned by [`Socket::send`]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum SendError {
    InvalidState,
}

impl Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SendError::InvalidState => write!(f, "invalid state"),
        }
    }
}

impl core::error::Error for SendError {}

/// Error returned by [`Socket::recv`]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum RecvError {
    InvalidState,
    Finished,
}

impl Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RecvError::InvalidState => write!(f, "invalid state"),
            RecvError::Finished => write!(f, "operation finished"),
        }
    }
}

impl core::error::Error for RecvError {}

/// A TCP socket ring buffer.
pub type SocketBuffer<'a> = RingBuffer<'a, u8>;

/// The state of a TCP socket, according to [RFC 793].
///
/// [RFC 793]: https://tools.ietf.org/html/rfc793
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum State {
    Closed,
    Listen,
    SynSent,
    SynReceived,
    Established,
    FinWait1,
    FinWait2,
    CloseWait,
    Closing,
    LastAck,
    TimeWait,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            State::Closed => write!(f, "CLOSED"),
            State::Listen => write!(f, "LISTEN"),
            State::SynSent => write!(f, "SYN-SENT"),
            State::SynReceived => write!(f, "SYN-RECEIVED"),
            State::Established => write!(f, "ESTABLISHED"),
            State::FinWait1 => write!(f, "FIN-WAIT-1"),
            State::FinWait2 => write!(f, "FIN-WAIT-2"),
            State::CloseWait => write!(f, "CLOSE-WAIT"),
            State::Closing => write!(f, "CLOSING"),
            State::LastAck => write!(f, "LAST-ACK"),
            State::TimeWait => write!(f, "TIME-WAIT"),
        }
    }
}

/// RFC 6298: (2.1) Until a round-trip time (RTT) measurement has been made for a
/// segment sent between the sender and receiver, the sender SHOULD
/// set RTO <- 1 second,
const RTTE_INITIAL_RTO: u32 = 1000;

// Minimum "safety margin" for the RTO that kicks in when the
// variance gets very low.
const RTTE_MIN_MARGIN: u32 = 5;

/// K, according to RFC 6298
const RTTE_K: u32 = 4;

// RFC 6298 (2.4): Whenever RTO is computed, if it is less than 1 second, then the
// RTO SHOULD be rounded up to 1 second.
//
// SmolNet departs from that SHOULD, as Linux does (TCP_RTO_MIN): a 1 s floor
// costs a whole second for every loss fast retransmit cannot repair, such as a
// lost retransmission or the tail of a transfer, on paths whose round trip is
// tens of milliseconds. The computed RTO, SRTT + 4 * RTTVAR, still applies
// above this floor, and the initial RTO before any sample is still 1 s.
const RTTE_MIN_RTO: u32 = 200;

// RFC 6298 (2.5) A maximum value MAY be placed on RTO provided it is at least 60
// seconds
const RTTE_MAX_RTO: u32 = 60_000;

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
struct RttEstimator {
    /// true if we have made at least one rtt measurement.
    have_measurement: bool,
    // Using u32 instead of Duration to save space (Duration is i64)
    /// Smoothed RTT
    srtt: u32,
    /// RTT variance.
    rttvar: u32,
    /// Retransmission Time-Out
    rto: u32,
    timestamp: Option<(Instant, TcpSeqNumber)>,
    max_seq_sent: Option<TcpSeqNumber>,
    rto_count: u8,
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self {
            have_measurement: false,
            srtt: 0,   // ignored, will be overwritten on first measurement.
            rttvar: 0, // ignored, will be overwritten on first measurement.
            rto: RTTE_INITIAL_RTO,
            timestamp: None,
            max_seq_sent: None,
            rto_count: 0,
        }
    }
}

impl RttEstimator {
    fn retransmission_timeout(&self) -> Duration {
        Duration::from_millis(self.rto as _)
    }

    /// The smoothed round trip, once one has been measured.
    fn srtt(&self) -> Option<Duration> {
        self.have_measurement
            .then(|| Duration::from_millis(self.srtt as _))
    }

    /// Whether a round trip is being timed.
    fn sampling(&self) -> bool {
        self.timestamp.is_some()
    }

    #[cfg(feature = "socket-tcp-cubic")]
    fn smoothed_rtt(&self) -> u32 {
        if self.have_measurement { self.srtt } else { 0 }
    }

    fn sample(&mut self, new_rtt: u32) {
        if self.have_measurement {
            // RFC 6298 (2.3) When a subsequent RTT measurement R' is made, a host MUST set (...)
            let diff = (self.srtt as i32 - new_rtt as i32).unsigned_abs();
            self.rttvar = (self.rttvar * 3 + diff).div_ceil(4);
            self.srtt = (self.srtt * 7 + new_rtt).div_ceil(8);
        } else {
            // RFC 6298 (2.2) When the first RTT measurement R is made, the host MUST set (...)
            self.have_measurement = true;
            self.srtt = new_rtt;
            self.rttvar = new_rtt / 2;
        }

        // RFC 6298 (2.2), (2.3)
        let margin = RTTE_MIN_MARGIN.max(self.rttvar * RTTE_K);
        self.rto = (self.srtt + margin).clamp(RTTE_MIN_RTO, RTTE_MAX_RTO);

        self.rto_count = 0;

        tcp_trace!(
            "rtte: sample={:?} srtt={:?} rttvar={:?} rto={:?}",
            new_rtt,
            self.srtt,
            self.rttvar,
            self.rto
        );
    }

    fn on_send(&mut self, timestamp: Instant, seq: TcpSeqNumber) {
        if self
            .max_seq_sent
            .map(|max_seq_sent| seq > max_seq_sent)
            .unwrap_or(true)
        {
            self.max_seq_sent = Some(seq);
            if self.timestamp.is_none() {
                self.timestamp = Some((timestamp, seq));
                tcp_trace!("rtte: sampling at seq={:?}", seq);
            }
        }
    }

    fn on_ack(&mut self, timestamp: Instant, seq: TcpSeqNumber) {
        if let Some((sent_timestamp, sent_seq)) = self.timestamp
            && seq >= sent_seq
        {
            self.sample((timestamp - sent_timestamp).total_millis() as u32);
            self.timestamp = None;
        }
    }

    fn on_rto(&mut self) {
        // RFC 6298 (5.5) The host MUST set RTO <- RTO * 2 ("back off the timer").  The
        // maximum value discussed in (2.5) above may be used to provide
        // an upper bound to this doubling operation.
        self.rto = (self.rto * 2).min(RTTE_MAX_RTO);
        tcp_trace!("rtte: doubling rto to {:?}", self.rto);

        // RFC 6298: a TCP implementation MAY clear SRTT and RTTVAR after
        // backing off the timer multiple times as it is likely that the current
        // SRTT and RTTVAR are bogus in this situation.  Once SRTT and RTTVAR
        // are cleared, they should be initialized with the next RTT sample
        // taken per (2.2) rather than using (2.3).
        self.rto_count += 1;
        if self.rto_count >= 3 {
            self.rto_count = 0;
            self.have_measurement = false;
            tcp_trace!("rtte: too many retransmissions, clearing srtt, rttvar.");
        }
    }

    fn on_retransmit(&mut self) {
        if self.timestamp.is_some() {
            tcp_trace!("rtte: abort sampling due to retransmit");
        }
        self.timestamp = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
enum Timer {
    Idle {
        keep_alive_at: Option<Instant>,
    },
    Retransmit {
        expires_at: Instant,
    },
    FastRetransmit,
    ZeroWindowProbe {
        expires_at: Instant,
        delay: Duration,
    },
    Close {
        expires_at: Instant,
    },
}

const ACK_DELAY_DEFAULT: Duration = Duration::from_millis(10);
const CLOSE_DELAY: Duration = Duration::from_millis(10_000);

/// The least a loss probe waits beyond twice the smoothed round trip, as
/// Linux's `TCP_TIMEOUT_MIN`.
const TLP_MIN_TIMEOUT: Duration = Duration::from_millis(2);

/// The least a loss probe waits at all, as the TLP draft's `max(2 * SRTT,
/// 10ms)` and early Linux did. Where the round trip is well under a
/// millisecond, an ACK held up for a few milliseconds by the host, as a
/// busy stack's is, would otherwise draw a needless probe.
const TLP_MIN_PTO: Duration = Duration::from_millis(10);

/// RFC 8985 `WCDelAckT`: the longest a receiver delays an ACK, which a loss
/// probe allows for when only one segment is in flight to be ACKed.
const TLP_DELAYED_ACK: Duration = Duration::from_millis(200);

impl Timer {
    fn new() -> Timer {
        Timer::Idle {
            keep_alive_at: None,
        }
    }

    fn should_keep_alive(&self, timestamp: Instant) -> bool {
        match *self {
            Timer::Idle {
                keep_alive_at: Some(keep_alive_at),
            } if timestamp >= keep_alive_at => true,
            _ => false,
        }
    }

    fn should_retransmit(&self, timestamp: Instant) -> bool {
        match *self {
            Timer::Retransmit { expires_at } if timestamp >= expires_at => true,
            Timer::FastRetransmit => true,
            _ => false,
        }
    }

    fn should_close(&self, timestamp: Instant) -> bool {
        match *self {
            Timer::Close { expires_at } if timestamp >= expires_at => true,
            _ => false,
        }
    }

    fn should_zero_window_probe(&self, timestamp: Instant) -> bool {
        match *self {
            Timer::ZeroWindowProbe { expires_at, .. } if timestamp >= expires_at => true,
            _ => false,
        }
    }

    fn poll_at(&self) -> PollAt {
        match *self {
            Timer::Idle {
                keep_alive_at: Some(keep_alive_at),
            } => PollAt::Time(keep_alive_at),
            Timer::Idle {
                keep_alive_at: None,
            } => PollAt::Ingress,
            Timer::ZeroWindowProbe { expires_at, .. } => PollAt::Time(expires_at),
            Timer::Retransmit { expires_at, .. } => PollAt::Time(expires_at),
            Timer::FastRetransmit => PollAt::Now,
            Timer::Close { expires_at } => PollAt::Time(expires_at),
        }
    }

    fn set_for_idle(&mut self, timestamp: Instant, keep_alive: Option<KeepAlive>) {
        *self = Timer::Idle {
            keep_alive_at: keep_alive.map(|keep_alive| timestamp + keep_alive.idle),
        }
    }

    fn set_keep_alive(&mut self) {
        if let Timer::Idle { keep_alive_at } = self
            && keep_alive_at.is_none()
        {
            *keep_alive_at = Some(Instant::from_millis(0))
        }
    }

    fn rewind_keep_alive(&mut self, timestamp: Instant, keep_alive: Option<KeepAlive>) {
        if let Timer::Idle { keep_alive_at } = self {
            *keep_alive_at = keep_alive.map(|keep_alive| timestamp + keep_alive.interval)
        }
    }

    fn arm_keep_alive(&mut self, at: Option<Instant>) {
        if let Timer::Idle { keep_alive_at } = self {
            *keep_alive_at = at
        }
    }

    fn set_for_retransmit(&mut self, timestamp: Instant, delay: Duration) {
        match *self {
            Timer::Idle { .. }
            | Timer::FastRetransmit
            | Timer::Retransmit { .. }
            | Timer::ZeroWindowProbe { .. } => {
                *self = Timer::Retransmit {
                    expires_at: timestamp + delay,
                }
            }
            Timer::Close { .. } => (),
        }
    }

    fn set_for_fast_retransmit(&mut self) {
        *self = Timer::FastRetransmit
    }

    fn set_for_close(&mut self, timestamp: Instant) {
        *self = Timer::Close {
            expires_at: timestamp + CLOSE_DELAY,
        }
    }

    fn set_for_zero_window_probe(&mut self, timestamp: Instant, delay: Duration) {
        *self = Timer::ZeroWindowProbe {
            expires_at: timestamp + delay,
            delay,
        }
    }

    fn rewind_zero_window_probe(&mut self, timestamp: Instant) {
        if let Timer::ZeroWindowProbe { mut delay, .. } = *self {
            delay = (delay * 2).min(Duration::from_millis(RTTE_MAX_RTO as _));
            *self = Timer::ZeroWindowProbe {
                expires_at: timestamp + delay,
                delay,
            }
        }
    }

    fn is_idle(&self) -> bool {
        matches!(self, Timer::Idle { .. })
    }

    fn is_zero_window_probe(&self) -> bool {
        matches!(self, Timer::ZeroWindowProbe { .. })
    }

    fn is_retransmit(&self) -> bool {
        matches!(self, Timer::Retransmit { .. } | Timer::FastRetransmit)
    }
}

/// Keep-alive timing, as Linux's `TCP_KEEPIDLE`, `TCP_KEEPINTVL` and
/// `TCP_KEEPCNT` set it.
///
/// See [set_keep_alive_config](Socket::set_keep_alive_config).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct KeepAlive {
    /// How long the connection receives nothing before the first probe.
    pub idle: Duration,
    /// How long a probe waits for an answer before the next one is sent.
    pub interval: Duration,
    /// How many unanswered probes abort the connection, when the next one
    /// falls due. `None` probes without end, and leaves the abort to the
    /// [timeout](Socket::set_timeout), as upstream smoltcp's keep-alive does.
    pub probes: Option<u8>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum AckDelayTimer {
    Idle,
    Waiting(Instant),
    Immediate,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
struct Tuple {
    local: IpEndpoint,
    remote: IpEndpoint,
}

impl Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.local, self.remote)
    }
}

/// A congestion control algorithm.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum CongestionControl {
    None,

    #[cfg(feature = "socket-tcp-reno")]
    Reno,

    #[cfg(feature = "socket-tcp-cubic")]
    Cubic,
}

/// A Transmission Control Protocol socket.
///
/// A TCP socket may passively listen for connections or actively connect to another endpoint.
/// Note that, for listening sockets, there is no "backlog"; to be able to simultaneously
/// accept several connections, as many sockets must be allocated, or any new connection
/// attempts will be reset.
#[derive(Debug)]
pub struct Socket<'a> {
    state: State,
    timer: Timer,
    rtte: RttEstimator,
    assembler: Assembler,
    rx_buffer: SocketBuffer<'a>,
    rx_fin_received: bool,
    tx_buffer: SocketBuffer<'a>,
    /// Interval after which, if no inbound packets are received while the connection
    /// waits on the remote, the connection is aborted.
    timeout: Option<Duration>,
    /// When keep-alive packets are sent, and how many may go unanswered.
    keep_alive: Option<KeepAlive>,
    /// The keep-alive packets sent since a packet was last received.
    keep_alive_probes: u8,
    /// Whether the connection was aborted because the remote stopped answering.
    timeout_abort: bool,
    /// The time-to-live (IPv4) or hop limit (IPv6) value used in outgoing packets.
    hop_limit: Option<u8>,
    /// Address passed to listen(). Listen address is set when listen() is called and
    /// used every time the socket is reset back to the LISTEN state.
    listen_endpoint: IpListenEndpoint,
    /// Current 4-tuple (local and remote endpoints).
    tuple: Option<Tuple>,
    /// The sequence number corresponding to the beginning of the transmit buffer.
    /// I.e. an ACK(local_seq_no+n) packet removes n bytes from the transmit buffer.
    local_seq_no: TcpSeqNumber,
    /// The sequence number corresponding to the beginning of the receive buffer.
    /// I.e. userspace reading n bytes adds n to remote_seq_no.
    remote_seq_no: TcpSeqNumber,
    /// The last sequence number sent.
    /// I.e. in an idle socket, local_seq_no+tx_buffer.len().
    remote_last_seq: TcpSeqNumber,
    /// The last acknowledgement number sent.
    /// I.e. in an idle socket, remote_seq_no+rx_buffer.len().
    remote_last_ack: Option<TcpSeqNumber>,
    /// The last window length sent.
    remote_last_win: u16,
    /// The sending window scaling factor advertised to remotes which support RFC 1323.
    /// It is zero if the window <= 64KiB and/or the remote does not support it.
    remote_win_shift: u8,
    /// The remote window size, relative to local_seq_no
    /// I.e. we're allowed to send octets until local_seq_no+remote_win_len
    remote_win_len: usize,
    /// The receive window scaling factor for remotes which support RFC 1323, None if unsupported.
    remote_win_scale: Option<u8>,
    /// Whether or not the remote supports selective ACK as described in RFC 2018.
    remote_has_sack: bool,
    /// The maximum number of data octets that the remote side may receive.
    remote_mss: usize,
    /// The path MTU an ICMP "Fragmentation Needed" (RFC 1191) or "Packet Too
    /// Big" (RFC 8201) error reported for this connection, if it was below the
    /// interface's MTU. It only ever falls, and is forgotten with the connection.
    path_mtu: Option<usize>,
    /// The timestamp of the last packet received.
    remote_last_ts: Option<Instant>,
    /// The sequence number of the last packet received, used for sACK
    local_rx_last_seq: Option<TcpSeqNumber>,
    /// The ACK number of the last packet received.
    local_rx_last_ack: Option<TcpSeqNumber>,
    /// The number of packets received directly after
    /// each other which have the same ACK number.
    local_rx_dup_acks: u8,
    /// If a fast retransmit needs to occur
    pending_fast_retransmit: bool,
    /// RFC 6582 `recover`: the highest sequence number sent when fast recovery
    /// began, or `None` outside fast recovery.
    recover: Option<TcpSeqNumber>,
    /// RFC 6675: the octets above the cumulative ACK that the remote has
    /// reported in SACK blocks.
    scoreboard: Scoreboard,
    /// RFC 6675 `HighRxt`, as the end of the highest range retransmitted
    /// during fast recovery, or `None` outside it.
    recovery_rxt_end: Option<TcpSeqNumber>,
    /// RFC 8985: when each octet in flight was last sent, RACK's loss
    /// detection, and tail loss probes.
    rack: Rack,

    /// Duration for Delayed ACK. If None no ACKs will be delayed.
    ack_delay: Option<Duration>,
    /// Delayed ack timer. If set, packets containing exclusively
    /// ACK or window updates (ie, no data) won't be sent until expiry.
    ack_delay_timer: AckDelayTimer,

    /// Used for rate-limiting: No more challenge ACKs will be sent until this instant.
    challenge_ack_timer: Instant,

    /// Nagle's Algorithm enabled.
    nagle: bool,
    /// The end of the last segment sent with less than a full MSS of data, until it
    /// is acknowledged. Nagle's algorithm holds back another such segment only while
    /// this one is in flight (Minshall's variant).
    nagle_small_segment_end: Option<TcpSeqNumber>,

    /// The congestion control algorithm.
    congestion_controller: congestion::AnyController,

    /// tsval generator - if some, tcp timestamp is enabled
    tsval_generator: Option<TcpTimestampGenerator>,

    /// 0 if not seen or timestamp not enabled
    last_remote_tsval: u32,

    #[cfg(feature = "async")]
    rx_waker: WakerRegistration,
    #[cfg(feature = "async")]
    tx_waker: WakerRegistration,

    /// If this is set, we will not send a SYN|ACK until this is unset.
    #[cfg(feature = "socket-tcp-pause-synack")]
    synack_paused: bool,
}

const DEFAULT_MSS: usize = 536;

/// Minimum MSS we accept from the remote, same value as Linux's `TCP_MIN_SND_MSS`.
/// Without it, a peer advertising a tiny MSS could force segments to carry little
/// or no payload once the TCP options length is subtracted from the effective MSS,
/// stalling the connection in an endless stream of empty segments.
///
/// Must exceed the maximum possible length of the TCP options (currently 12, for
/// timestamps) so that every segment carries some payload.
const MIN_REMOTE_MSS: usize = 48;

/// The least path MTU an ICMP error can lower an IPv4 connection to: 576, the
/// datagram every IPv4 host must accept (RFC 791, RFC 1122 3.3.2), whose MSS
/// is TCP's default of 536 (RFC 9293 3.7.1). RFC 1191 allows 68, but every
/// packet has Don't Fragment set, so a path narrower than this stays a black
/// hole whatever the floor, and a forged error cannot shrink a connection's
/// segments further. Linux's `min_pmtu` is 552, to the same end.
#[cfg(feature = "proto-ipv4")]
const IPV4_MIN_PATH_MTU: usize = 576;

/// The least path MTU an ICMPv6 "Packet Too Big" can lower an IPv6 connection
/// to: the IPv6 minimum link MTU (RFC 8200 5, RFC 8201 4).
#[cfg(feature = "proto-ipv6")]
const IPV6_MIN_PATH_MTU: usize = crate::wire::IPV6_MIN_MTU;

/// What [`Socket::process_path_mtu`] made of an ICMP error.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum PathMtuOutcome {
    /// The error quotes a segment of another connection.
    NotMatched,
    /// The error is for this connection, but fails validation: it quotes a
    /// sequence number that is not in flight (RFC 5927 4.1), or the
    /// connection is not sending data.
    Rejected,
    /// The error is valid, but reports no less than the connection sends.
    Unchanged,
    /// The connection now sends smaller segments, starting with a resend.
    Reduced,
}

impl<'a> Socket<'a> {
    #[allow(unused_comparisons)] // small usize platforms always pass rx_capacity check
    /// Create a socket using the given buffers.
    pub fn new<T>(rx_buffer: T, tx_buffer: T) -> Socket<'a>
    where
        T: Into<SocketBuffer<'a>>,
    {
        let (rx_buffer, tx_buffer) = (rx_buffer.into(), tx_buffer.into());
        let rx_capacity = rx_buffer.capacity();

        // From RFC 1323:
        // [...] the above constraints imply that 2 * the max window size must be less
        // than 2**31 [...] Thus, the shift count must be limited to 14 (which allows
        // windows of 2**30 = 1 Gbyte).
        #[cfg(not(target_pointer_width = "16"))] // Prevent overflow
        if rx_capacity > (1 << 30) {
            panic!("receiving buffer too large, cannot exceed 1 GiB")
        }
        let rx_cap_log2 = mem::size_of::<usize>() * 8 - rx_capacity.leading_zeros() as usize;

        Socket {
            state: State::Closed,
            timer: Timer::new(),
            rtte: RttEstimator::default(),
            assembler: Assembler::new(),
            tx_buffer,
            rx_buffer,
            rx_fin_received: false,
            timeout: None,
            keep_alive: None,
            keep_alive_probes: 0,
            timeout_abort: false,
            hop_limit: None,
            listen_endpoint: IpListenEndpoint::default(),
            tuple: None,
            local_seq_no: TcpSeqNumber::default(),
            remote_seq_no: TcpSeqNumber::default(),
            remote_last_seq: TcpSeqNumber::default(),
            remote_last_ack: None,
            remote_last_win: 0,
            remote_win_len: 0,
            remote_win_shift: rx_cap_log2.saturating_sub(16) as u8,
            remote_win_scale: None,
            remote_has_sack: false,
            remote_mss: DEFAULT_MSS,
            path_mtu: None,
            remote_last_ts: None,
            local_rx_last_ack: None,
            local_rx_last_seq: None,
            local_rx_dup_acks: 0,
            pending_fast_retransmit: false,
            recover: None,
            scoreboard: Scoreboard::new(),
            recovery_rxt_end: None,
            rack: Rack::new(),
            ack_delay: Some(ACK_DELAY_DEFAULT),
            ack_delay_timer: AckDelayTimer::Idle,
            challenge_ack_timer: Instant::from_secs(0),
            nagle: true,
            nagle_small_segment_end: None,
            tsval_generator: None,
            last_remote_tsval: 0,
            congestion_controller: congestion::AnyController::new(),

            #[cfg(feature = "async")]
            rx_waker: WakerRegistration::new(),
            #[cfg(feature = "async")]
            tx_waker: WakerRegistration::new(),

            #[cfg(feature = "socket-tcp-pause-synack")]
            synack_paused: false,
        }
    }

    /// Enable or disable TCP Timestamp.
    pub fn set_tsval_generator(&mut self, generator: Option<TcpTimestampGenerator>) {
        self.tsval_generator = generator;
    }

    /// Return whether TCP Timestamp is enabled.
    pub fn timestamp_enabled(&self) -> bool {
        self.tsval_generator.is_some()
    }

    /// Set an algorithm for congestion control.
    ///
    /// `CongestionControl::None` indicates that no congestion control is applied.
    /// Options `CongestionControl::Cubic` and `CongestionControl::Reno` are also available.
    /// To use Reno and Cubic, please enable the `socket-tcp-reno` and `socket-tcp-cubic` features
    /// in the `smoltcp` crate, respectively.
    ///
    /// `CongestionControl::Reno` is a classic congestion control algorithm valued for its simplicity.
    /// Despite having a lower algorithmic complexity than `Cubic`,
    /// it is less efficient in terms of bandwidth usage.
    ///
    /// `CongestionControl::Cubic` represents a modern congestion control algorithm designed to
    /// be more efficient and fair compared to `CongestionControl::Reno`.
    /// It is the default choice for Linux, Windows, and macOS.
    /// `CongestionControl::Cubic` relies on double precision (`f64`) floating point operations, which may cause issues in some contexts:
    /// * Small embedded processors (such as Cortex-M0, Cortex-M1, and Cortex-M3) do not have an FPU, and floating point operations consume significant amounts of CPU time and Flash space.
    /// * Interrupt handlers should almost always avoid floating-point operations.
    /// * Kernel-mode code on desktop processors usually avoids FPU operations to reduce the penalty of saving and restoring FPU registers.
    ///
    /// In all these cases, `CongestionControl::Reno` is a better choice of congestion control algorithm.
    pub fn set_congestion_control(&mut self, congestion_control: CongestionControl) {
        use congestion::*;

        self.congestion_controller = match congestion_control {
            CongestionControl::None => AnyController::None(no_control::NoControl),

            #[cfg(feature = "socket-tcp-reno")]
            CongestionControl::Reno => AnyController::Reno(reno::Reno::new()),

            #[cfg(feature = "socket-tcp-cubic")]
            CongestionControl::Cubic => AnyController::Cubic(cubic::Cubic::new()),
        }
    }

    /// Return the current congestion control algorithm.
    pub fn congestion_control(&self) -> CongestionControl {
        use congestion::*;

        match self.congestion_controller {
            AnyController::None(_) => CongestionControl::None,

            #[cfg(feature = "socket-tcp-reno")]
            AnyController::Reno(_) => CongestionControl::Reno,

            #[cfg(feature = "socket-tcp-cubic")]
            AnyController::Cubic(_) => CongestionControl::Cubic,
        }
    }

    /// Register a waker for receive operations.
    ///
    /// The waker is woken on state changes that might affect the return value
    /// of `recv` method calls, such as receiving data, or the socket closing.
    ///
    /// Notes:
    ///
    /// - Only one waker can be registered at a time. If another waker was previously registered,
    ///   it is overwritten and will no longer be woken.
    /// - The Waker is woken only once. Once woken, you must register it again to receive more wakes.
    /// - "Spurious wakes" are allowed: a wake doesn't guarantee the result of `recv` has
    ///   necessarily changed.
    #[cfg(feature = "async")]
    pub fn register_recv_waker(&mut self, waker: &Waker) {
        self.rx_waker.register(waker)
    }

    /// Register a waker for send operations.
    ///
    /// The waker is woken on state changes that might affect the return value
    /// of `send` method calls, such as space becoming available in the transmit
    /// buffer, or the socket closing.
    ///
    /// Notes:
    ///
    /// - Only one waker can be registered at a time. If another waker was previously registered,
    ///   it is overwritten and will no longer be woken.
    /// - The Waker is woken only once. Once woken, you must register it again to receive more wakes.
    /// - "Spurious wakes" are allowed: a wake doesn't guarantee the result of `send` has
    ///   necessarily changed.
    #[cfg(feature = "async")]
    pub fn register_send_waker(&mut self, waker: &Waker) {
        self.tx_waker.register(waker)
    }

    /// Return the timeout duration.
    ///
    /// See also the [set_timeout](#method.set_timeout) method.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Return the ACK delay duration.
    ///
    /// See also the [set_ack_delay](#method.set_ack_delay) method.
    pub fn ack_delay(&self) -> Option<Duration> {
        self.ack_delay
    }

    /// Return whether Nagle's Algorithm is enabled.
    ///
    /// See also the [set_nagle_enabled](#method.set_nagle_enabled) method.
    pub fn nagle_enabled(&self) -> bool {
        self.nagle
    }

    /// Pause sending of SYN|ACK packets.
    ///
    /// When this flag is set, the socket will get stuck in `SynReceived` state without sending
    /// any SYN|ACK packets back, until this flag is unset. This is useful for certain niche TCP
    /// proxy usecases.
    #[cfg(feature = "socket-tcp-pause-synack")]
    pub fn pause_synack(&mut self, pause: bool) {
        self.synack_paused = pause;
    }

    /// Return the current window field value, including scaling according to RFC 1323.
    ///
    /// Used in internal calculations as well as packet generation.
    #[inline]
    fn scaled_window(&self) -> u16 {
        u16::try_from(self.rx_buffer.window() >> self.remote_win_shift).unwrap_or(u16::MAX)
    }

    /// Return the last window field value, including scaling according to RFC 1323.
    ///
    /// Used in internal calculations as well as packet generation.
    ///
    /// Unlike `remote_last_win`, we take into account new packets received (but not acknowledged)
    /// since the last window update and adjust the window length accordingly. This ensures a fair
    /// comparison between the last window length and the new window length we're going to
    /// advertise.
    #[inline]
    fn last_scaled_window(&self) -> Option<u16> {
        let last_ack = self.remote_last_ack?;
        let next_ack = self.remote_seq_no + self.rx_buffer.len();

        let last_win = (self.remote_last_win as usize) << self.remote_win_shift;
        let last_win_adjusted = last_ack + last_win - next_ack;

        Some(u16::try_from(last_win_adjusted >> self.remote_win_shift).unwrap_or(u16::MAX))
    }

    /// Set the timeout duration.
    ///
    /// A socket with a timeout duration set will abort the connection if either of the following
    /// occurs:
    ///
    ///   * After a [connect](#method.connect) call, the remote endpoint does not respond within
    ///     the specified duration;
    ///   * After establishing a connection, there is data in the transmit buffer or an
    ///     unacknowledged FIN, and the remote endpoint exceeds the specified duration between
    ///     any two packets it sends, or since the first of them was sent;
    ///   * After enabling [keep-alive](#method.set_keep_alive) with no limit on its probes, the
    ///     remote endpoint exceeds the specified duration between any two packets it sends.
    ///
    /// An idle connection, with nothing to send and nothing unacknowledged, is not aborted
    /// however long the remote endpoint stays quiet: that is what keep-alive probes are for.
    /// Used this way the timeout is the user timeout of RFC 5482, which Linux sets with
    /// `TCP_USER_TIMEOUT`, and it bounds zero-window probing too, since an endpoint that
    /// answers the probes keeps the connection open.
    pub fn set_timeout(&mut self, duration: Option<Duration>) {
        self.timeout = duration
    }

    /// Set the ACK delay duration.
    ///
    /// By default, the ACK delay is set to 10ms.
    pub fn set_ack_delay(&mut self, duration: Option<Duration>) {
        self.ack_delay = duration
    }

    /// Enable or disable Nagle's Algorithm.
    ///
    /// Also known as "tinygram prevention". By default, it is enabled.
    /// Disabling it is equivalent to Linux's TCP_NODELAY flag.
    ///
    /// When enabled, Nagle's Algorithm prevents sending segments smaller than MSS if
    /// there is data in flight (sent but not acknowledged). In other words, it ensures
    /// at most only one segment smaller than MSS is in flight at a time.
    ///
    /// It ensures better network utilization by preventing sending many very small packets,
    /// at the cost of increased latency in some situations, particularly when the remote peer
    /// has ACK delay enabled.
    pub fn set_nagle_enabled(&mut self, enabled: bool) {
        self.nagle = enabled
    }

    /// Return the keep-alive interval.
    ///
    /// See also the [set_keep_alive](#method.set_keep_alive) method.
    pub fn keep_alive(&self) -> Option<Duration> {
        self.keep_alive.map(|keep_alive| keep_alive.interval)
    }

    /// Set the keep-alive interval.
    ///
    /// An idle socket with a keep-alive interval set will transmit a "keep-alive ACK" packet
    /// every time it receives no communication during that interval. As a result, three things
    /// may happen:
    ///
    ///   * The remote endpoint is fine and answers with an ACK packet.
    ///   * The remote endpoint has rebooted and answers with an RST packet.
    ///   * The remote endpoint has crashed and does not answer.
    ///
    /// The keep-alive functionality together with the timeout functionality allows to react
    /// to these error conditions.
    pub fn set_keep_alive(&mut self, interval: Option<Duration>) {
        self.keep_alive = interval.map(|interval| KeepAlive {
            idle: interval,
            interval,
            probes: None,
        });
        self.keep_alive_probes = 0;
        if self.keep_alive.is_some() {
            // If the connection is idle and we've just set the option, it would not take effect
            // until the next packet, unless we wind up the timer explicitly.
            self.timer.set_keep_alive();
        }
    }

    /// Return the keep-alive timing.
    ///
    /// See also the [set_keep_alive_config](#method.set_keep_alive_config) method.
    pub fn keep_alive_config(&self) -> Option<KeepAlive> {
        self.keep_alive
    }

    /// Set the keep-alive timing, with an idle time, a probe interval and a probe count, as
    /// Linux has them.
    ///
    /// A connection with keep-alive set that has received nothing for `idle`, and has nothing
    /// to send or unacknowledged, sends a "keep-alive ACK" packet, and another every `interval`
    /// while none is answered. Any packet received answers them, and the next probe is then
    /// `idle` after it. Once `probes` of them have gone unanswered, the connection is aborted
    /// when the next falls due, as [aborted_by_timeout](#method.aborted_by_timeout) reports.
    ///
    /// Unlike [set_keep_alive](#method.set_keep_alive), setting it on an idle connection sends
    /// no probe at once: the first is due `idle` after the last packet received.
    pub fn set_keep_alive_config(&mut self, keep_alive: Option<KeepAlive>) {
        self.keep_alive = keep_alive;
        self.keep_alive_probes = 0;
        let last = self.remote_last_ts.unwrap_or(Instant::ZERO);
        self.timer
            .arm_keep_alive(keep_alive.map(|keep_alive| last + keep_alive.idle));
    }

    /// Return whether the connection was aborted because the remote endpoint stopped
    /// answering: the [timeout](#method.set_timeout) expired, or the
    /// [keep-alive probes](#method.set_keep_alive_config) all went unanswered. It stays set
    /// once the socket is closed, until the socket is reused.
    pub fn aborted_by_timeout(&self) -> bool {
        self.timeout_abort
    }

    /// Return the time-to-live (IPv4) or hop limit (IPv6) value used in outgoing packets.
    ///
    /// See also the [set_hop_limit](#method.set_hop_limit) method
    pub fn hop_limit(&self) -> Option<u8> {
        self.hop_limit
    }

    /// Set the time-to-live (IPv4) or hop limit (IPv6) value used in outgoing packets.
    ///
    /// A socket without an explicitly set hop limit value uses the default [IANA recommended]
    /// value (64).
    ///
    /// # Panics
    ///
    /// This function panics if a hop limit value of 0 is given. See [RFC 1122 § 3.2.1.7].
    ///
    /// [IANA recommended]: https://www.iana.org/assignments/ip-parameters/ip-parameters.xhtml
    /// [RFC 1122 § 3.2.1.7]: https://tools.ietf.org/html/rfc1122#section-3.2.1.7
    pub fn set_hop_limit(&mut self, hop_limit: Option<u8>) {
        // A host MUST NOT send a datagram with a hop limit value of 0
        if let Some(0) = hop_limit {
            panic!("the time-to-live value of a packet must not be zero")
        }

        self.hop_limit = hop_limit
    }

    /// Return the listen endpoint
    #[inline]
    pub fn listen_endpoint(&self) -> IpListenEndpoint {
        self.listen_endpoint
    }

    /// Return the local endpoint, or None if not connected.
    #[inline]
    pub fn local_endpoint(&self) -> Option<IpEndpoint> {
        Some(self.tuple?.local)
    }

    /// Return the remote endpoint, or None if not connected.
    #[inline]
    pub fn remote_endpoint(&self) -> Option<IpEndpoint> {
        Some(self.tuple?.remote)
    }

    /// Return the connection state, in terms of the TCP state machine.
    #[inline]
    pub fn state(&self) -> State {
        self.state
    }

    fn reset(&mut self) {
        let rx_cap_log2 =
            mem::size_of::<usize>() * 8 - self.rx_buffer.capacity().leading_zeros() as usize;

        self.state = State::Closed;
        self.timer = Timer::new();
        self.rtte = RttEstimator::default();
        self.assembler = Assembler::new();
        self.tx_buffer.clear();
        self.rx_buffer.clear();
        self.rx_fin_received = false;
        self.listen_endpoint = IpListenEndpoint::default();
        self.tuple = None;
        self.local_seq_no = TcpSeqNumber::default();
        self.remote_seq_no = TcpSeqNumber::default();
        self.remote_last_seq = TcpSeqNumber::default();
        self.remote_last_ack = None;
        self.remote_last_win = 0;
        self.remote_win_len = 0;
        self.remote_win_scale = None;
        self.remote_win_shift = rx_cap_log2.saturating_sub(16) as u8;
        self.remote_mss = DEFAULT_MSS;
        self.path_mtu = None;
        self.remote_last_ts = None;
        self.keep_alive_probes = 0;
        self.timeout_abort = false;
        self.recover = None;
        self.scoreboard.clear();
        self.recovery_rxt_end = None;
        self.rack = Rack::new();
        self.nagle_small_segment_end = None;
        self.ack_delay_timer = AckDelayTimer::Idle;
        self.challenge_ack_timer = Instant::from_secs(0);

        #[cfg(feature = "async")]
        {
            self.rx_waker.wake();
            self.tx_waker.wake();
        }
    }

    /// Start listening on the given endpoint.
    ///
    /// This function returns `Err(Error::InvalidState)` if the socket was already open
    /// (see [is_open](#method.is_open)), and `Err(Error::Unaddressable)`
    /// if the port in the given endpoint is zero.
    pub fn listen<T>(&mut self, local_endpoint: T) -> Result<(), ListenError>
    where
        T: Into<IpListenEndpoint>,
    {
        let local_endpoint = local_endpoint.into();
        if local_endpoint.port == 0 {
            return Err(ListenError::Unaddressable);
        }

        if self.is_open() {
            // If we were already listening to same endpoint there is nothing to do; exit early.
            //
            // In the past listening on an socket that was already listening was an error,
            // however this makes writing an acceptor loop with multiple sockets impossible.
            // Without this early exit, if you tried to listen on a socket that's already listening you'll
            // immediately get an error. The only way around this is to abort the socket first
            // before listening again, but this means that incoming connections can actually
            // get aborted between the abort() and the next listen().
            if matches!(self.state, State::Listen) && self.listen_endpoint == local_endpoint {
                return Ok(());
            } else {
                return Err(ListenError::InvalidState);
            }
        }

        self.reset();
        self.listen_endpoint = local_endpoint;
        self.tuple = None;
        self.set_state(State::Listen);
        Ok(())
    }

    /// Connect to a given endpoint.
    ///
    /// The local port must be provided explicitly. Assuming `fn get_ephemeral_port() -> u16`
    /// allocates a port between 49152 and 65535, a connection may be established as follows:
    ///
    /// ```no_run
    /// # #[cfg(all(
    /// #     feature = "medium-ethernet",
    /// #     feature = "proto-ipv4",
    /// # ))]
    /// # {
    /// # use smoltcp::socket::tcp::{Socket, SocketBuffer};
    /// # use smoltcp::iface::Interface;
    /// # use smoltcp::wire::IpAddress;
    /// #
    /// # fn get_ephemeral_port() -> u16 {
    /// #     49152
    /// # }
    /// #
    /// # let mut socket = Socket::new(
    /// #     SocketBuffer::new(vec![0; 1200]),
    /// #     SocketBuffer::new(vec![0; 1200])
    /// # );
    /// #
    /// # let mut iface: Interface = todo!();
    /// #
    /// socket.connect(
    ///     iface.context(),
    ///     (IpAddress::v4(10, 0, 0, 1), 80),
    ///     get_ephemeral_port()
    /// ).unwrap();
    /// # }
    /// ```
    ///
    /// The local address may optionally be provided.
    ///
    /// This function returns an error if the socket was open; see [is_open](#method.is_open).
    /// It also returns an error if the local or remote port is zero, or if the remote address
    /// is unspecified.
    pub fn connect<T, U>(
        &mut self,
        cx: &mut Context,
        remote_endpoint: T,
        local_endpoint: U,
    ) -> Result<(), ConnectError>
    where
        T: Into<IpEndpoint>,
        U: Into<IpListenEndpoint>,
    {
        let remote_endpoint: IpEndpoint = remote_endpoint.into();
        let local_endpoint: IpListenEndpoint = local_endpoint.into();

        if self.is_open() {
            return Err(ConnectError::InvalidState);
        }
        if remote_endpoint.port == 0 || remote_endpoint.addr.is_unspecified() {
            return Err(ConnectError::Unaddressable);
        }
        if local_endpoint.port == 0 {
            return Err(ConnectError::Unaddressable);
        }

        // If local address is not provided, choose it automatically.
        let local_endpoint = IpEndpoint {
            addr: match local_endpoint.addr {
                Some(addr) => {
                    if addr.is_unspecified() {
                        return Err(ConnectError::Unaddressable);
                    }
                    addr
                }
                None => cx
                    .get_source_address(&remote_endpoint.addr)
                    .ok_or(ConnectError::Unaddressable)?,
            },
            port: local_endpoint.port,
        };

        if local_endpoint.addr.version() != remote_endpoint.addr.version() {
            return Err(ConnectError::Unaddressable);
        }

        self.reset();
        self.tuple = Some(Tuple {
            local: local_endpoint,
            remote: remote_endpoint,
        });
        self.set_state(State::SynSent);

        let seq = Self::random_seq_no(cx);
        self.local_seq_no = seq;
        self.remote_last_seq = seq;
        Ok(())
    }

    #[cfg(test)]
    fn random_seq_no(_cx: &mut Context) -> TcpSeqNumber {
        TcpSeqNumber(10000)
    }

    #[cfg(not(test))]
    fn random_seq_no(cx: &mut Context) -> TcpSeqNumber {
        TcpSeqNumber(cx.rand().rand_u32() as i32)
    }

    /// Close the transmit half of the full-duplex connection.
    ///
    /// Note that there is no corresponding function for the receive half of the full-duplex
    /// connection; only the remote end can close it. If you no longer wish to receive any
    /// data and would like to reuse the socket right away, use [abort](#method.abort).
    pub fn close(&mut self) {
        match self.state {
            // In the LISTEN state there is no established connection.
            State::Listen => self.set_state(State::Closed),
            // In the SYN-SENT state the remote endpoint is not yet synchronized and, upon
            // receiving an RST, will abort the connection.
            State::SynSent => self.set_state(State::Closed),
            // In the SYN-RECEIVED, ESTABLISHED and CLOSE-WAIT states the transmit half
            // of the connection is open, and needs to be explicitly closed with a FIN.
            State::SynReceived | State::Established => {
                self.restart_timeout_if_idle();
                self.set_state(State::FinWait1)
            }
            State::CloseWait => {
                self.restart_timeout_if_idle();
                self.set_state(State::LastAck)
            }
            // In the FIN-WAIT-1, FIN-WAIT-2, CLOSING, LAST-ACK, TIME-WAIT and CLOSED states,
            // the transmit half of the connection is already closed, and no further
            // action is needed.
            State::FinWait1
            | State::FinWait2
            | State::Closing
            | State::TimeWait
            | State::LastAck
            | State::Closed => (),
        }
    }

    // A FIN queued on an idle connection is outstanding from now on, and the timeout counts
    // from when it is sent, not from the last packet received, which may be long ago. Queued
    // data restarts the count in send_impl() the same way.
    fn restart_timeout_if_idle(&mut self) {
        if self.tx_buffer.is_empty() {
            self.remote_last_ts = None;
            self.keep_alive_probes = 0;
        }
    }

    /// Aborts the connection, if any.
    ///
    /// This function instantly closes the socket. One reset packet will be sent to the remote
    /// endpoint.
    ///
    /// In terms of the TCP state machine, the socket may be in any state and is moved to
    /// the `CLOSED` state.
    pub fn abort(&mut self) {
        self.set_state(State::Closed);
    }

    /// Return whether the socket is passively listening for incoming connections.
    ///
    /// In terms of the TCP state machine, the socket must be in the `LISTEN` state.
    #[inline]
    pub fn is_listening(&self) -> bool {
        match self.state {
            State::Listen => true,
            _ => false,
        }
    }

    /// Return whether the socket is open.
    ///
    /// This function returns true if the socket will process incoming or dispatch outgoing
    /// packets. Note that this does not mean that it is possible to send or receive data through
    /// the socket; for that, use [can_send](#method.can_send) or [can_recv](#method.can_recv).
    ///
    /// In terms of the TCP state machine, the socket must not be in the `CLOSED`
    /// or `TIME-WAIT` states.
    #[inline]
    pub fn is_open(&self) -> bool {
        match self.state {
            State::Closed => false,
            State::TimeWait => false,
            _ => true,
        }
    }

    /// Return whether a connection is active.
    ///
    /// This function returns true if the socket is actively exchanging packets with
    /// a remote endpoint. Note that this does not mean that it is possible to send or receive
    /// data through the socket; for that, use [can_send](#method.can_send) or
    /// [can_recv](#method.can_recv).
    ///
    /// If a connection is established, [abort](#method.close) will send a reset to
    /// the remote endpoint.
    ///
    /// In terms of the TCP state machine, the socket must not be in the `CLOSED`, `TIME-WAIT`,
    /// or `LISTEN` state.
    #[inline]
    pub fn is_active(&self) -> bool {
        match self.state {
            State::Closed => false,
            State::TimeWait => false,
            State::Listen => false,
            _ => true,
        }
    }

    /// Return whether the transmit half of the full-duplex connection is open.
    ///
    /// This function returns true if it's possible to send data and have it arrive
    /// to the remote endpoint. However, it does not make any guarantees about the state
    /// of the transmit buffer, and even if it returns true, [send](#method.send) may
    /// not be able to enqueue any octets.
    ///
    /// In terms of the TCP state machine, the socket must be in the `ESTABLISHED` or
    /// `CLOSE-WAIT` state.
    #[inline]
    pub fn may_send(&self) -> bool {
        match self.state {
            State::Established => true,
            // In CLOSE-WAIT, the remote endpoint has closed our receive half of the connection
            // but we still can transmit indefinitely.
            State::CloseWait => true,
            _ => false,
        }
    }

    /// Return whether the receive half of the full-duplex connection is open.
    ///
    /// This function returns true if it's possible to receive data from the remote endpoint.
    /// It will return true while there is data in the receive buffer, and if there isn't,
    /// as long as the remote endpoint has not closed the connection.
    ///
    /// In terms of the TCP state machine, the socket must be in the `ESTABLISHED`,
    /// `FIN-WAIT-1`, or `FIN-WAIT-2` state, or have data in the receive buffer instead.
    #[inline]
    pub fn may_recv(&self) -> bool {
        match self.state {
            State::Established => true,
            // In FIN-WAIT-1/2, we have closed our transmit half of the connection but
            // we still can receive indefinitely.
            State::FinWait1 | State::FinWait2 => true,
            // If we have something in the receive buffer, we can receive that.
            _ if self.can_recv() => true,
            _ => false,
        }
    }

    /// Check whether the transmit half of the full-duplex connection is open
    /// (see [may_send](#method.may_send)), and the transmit buffer is not full.
    #[inline]
    pub fn can_send(&self) -> bool {
        if !self.may_send() {
            return false;
        }

        !self.tx_buffer.is_full()
    }

    /// Return the maximum number of bytes inside the recv buffer.
    #[inline]
    pub fn recv_capacity(&self) -> usize {
        self.rx_buffer.capacity()
    }

    /// Return the maximum number of bytes inside the transmit buffer.
    #[inline]
    pub fn send_capacity(&self) -> usize {
        self.tx_buffer.capacity()
    }

    /// Check whether the receive buffer is not empty.
    #[inline]
    pub fn can_recv(&self) -> bool {
        !self.rx_buffer.is_empty()
    }

    fn send_impl<'b, F, R>(&'b mut self, f: F) -> Result<R, SendError>
    where
        F: FnOnce(&'b mut SocketBuffer<'a>) -> (usize, R),
    {
        if !self.may_send() {
            return Err(SendError::InvalidState);
        }

        let old_length = self.tx_buffer.len();
        let (size, result) = f(&mut self.tx_buffer);
        if size > 0 {
            // The connection might have been idle for a long time, and so remote_last_ts
            // would be far in the past. Unless we clear it here, we'll abort the connection
            // down over in dispatch() by erroneously detecting it as timed out.
            if old_length == 0 {
                self.remote_last_ts = None;
                self.keep_alive_probes = 0;
            }

            // if remote win is zero and we go from having no data to some data pending to
            // send, start the zero window probe timer.
            if self.remote_win_len == 0 && self.timer.is_idle() {
                let delay = self.rtte.retransmission_timeout();
                tcp_trace!("starting zero-window-probe timer for t+{}", delay);

                // We don't have access to the current time here, so use Instant::ZERO instead.
                // this will cause the first ZWP to be sent immediately, but that's okay.
                self.timer.set_for_zero_window_probe(Instant::ZERO, delay);
            }

            #[cfg(any(test, feature = "verbose"))]
            tcp_trace!(
                "tx buffer: enqueueing {} octets (now {})",
                size,
                old_length + size
            );
        }
        Ok(result)
    }

    /// Call `f` with the largest contiguous slice of octets in the transmit buffer,
    /// and enqueue the amount of elements returned by `f`.
    ///
    /// This function returns `Err(Error::Illegal)` if the transmit half of
    /// the connection is not open; see [may_send](#method.may_send).
    pub fn send<'b, F, R>(&'b mut self, f: F) -> Result<R, SendError>
    where
        F: FnOnce(&'b mut [u8]) -> (usize, R),
    {
        self.send_impl(|tx_buffer| tx_buffer.enqueue_many_with(f))
    }

    /// Enqueue a sequence of octets to be sent, and fill it from a slice.
    ///
    /// This function returns the amount of octets actually enqueued, which is limited
    /// by the amount of free space in the transmit buffer; down to zero.
    ///
    /// See also [send](#method.send).
    pub fn send_slice(&mut self, data: &[u8]) -> Result<usize, SendError> {
        self.send_impl(|tx_buffer| {
            let size = tx_buffer.enqueue_slice(data);
            (size, size)
        })
    }

    fn recv_error_check(&mut self) -> Result<(), RecvError> {
        // We may have received some data inside the initial SYN, but until the connection
        // is fully open we must not dequeue any data, as it may be overwritten by e.g.
        // another (stale) SYN. (We do not support TCP Fast Open.)
        if !self.may_recv() {
            if self.rx_fin_received {
                return Err(RecvError::Finished);
            }
            return Err(RecvError::InvalidState);
        }

        Ok(())
    }

    fn recv_impl<'b, F, R>(&'b mut self, f: F) -> Result<R, RecvError>
    where
        F: FnOnce(&'b mut SocketBuffer<'a>) -> (usize, R),
    {
        self.recv_error_check()?;

        let _old_length = self.rx_buffer.len();
        let (size, result) = f(&mut self.rx_buffer);
        self.remote_seq_no += size;
        if size > 0 {
            #[cfg(any(test, feature = "verbose"))]
            tcp_trace!(
                "rx buffer: dequeueing {} octets (now {})",
                size,
                _old_length - size
            );
        }
        Ok(result)
    }

    /// Call `f` with the largest contiguous slice of octets in the receive buffer,
    /// and dequeue the amount of elements returned by `f`.
    ///
    /// This function errors if the receive half of the connection is not open.
    ///
    /// If the receive half has been gracefully closed (with a FIN packet), `Err(Error::Finished)`
    /// is returned. In this case, the previously received data is guaranteed to be complete.
    ///
    /// In all other cases, `Err(Error::Illegal)` is returned and previously received data (if any)
    /// may be incomplete (truncated).
    pub fn recv<'b, F, R>(&'b mut self, f: F) -> Result<R, RecvError>
    where
        F: FnOnce(&'b mut [u8]) -> (usize, R),
    {
        self.recv_impl(|rx_buffer| rx_buffer.dequeue_many_with(f))
    }

    /// Dequeue a sequence of received octets, and fill a slice from it.
    ///
    /// This function returns the amount of octets actually dequeued, which is limited
    /// by the amount of occupied space in the receive buffer; down to zero.
    ///
    /// See also [recv](#method.recv).
    pub fn recv_slice(&mut self, data: &mut [u8]) -> Result<usize, RecvError> {
        self.recv_impl(|rx_buffer| {
            let size = rx_buffer.dequeue_slice(data);
            (size, size)
        })
    }

    /// Peek at a sequence of received octets without removing them from
    /// the receive buffer, and return a pointer to it.
    ///
    /// This function otherwise behaves identically to [recv](#method.recv).
    pub fn peek(&mut self, size: usize) -> Result<&[u8], RecvError> {
        self.recv_error_check()?;

        let buffer = self.rx_buffer.get_allocated(0, size);
        if !buffer.is_empty() {
            #[cfg(any(test, feature = "verbose"))]
            tcp_trace!("rx buffer: peeking at {} octets", buffer.len());
        }
        Ok(buffer)
    }

    /// Peek at a sequence of received octets without removing them from
    /// the receive buffer, and fill a slice from it.
    ///
    /// This function otherwise behaves identically to [recv_slice](#method.recv_slice).
    pub fn peek_slice(&mut self, data: &mut [u8]) -> Result<usize, RecvError> {
        Ok(self.rx_buffer.read_allocated(0, data))
    }

    /// Return the amount of octets queued in the transmit buffer.
    ///
    /// Note that the Berkeley sockets interface does not have an equivalent of this API.
    pub fn send_queue(&self) -> usize {
        self.tx_buffer.len()
    }

    /// Number of octets transmitted but not yet ACKed.
    fn flight_size(&self) -> usize {
        self.remote_last_seq - self.local_seq_no
    }

    /// The largest IP packet the connection sends: the interface's MTU, or
    /// the path MTU if an ICMP error lowered it.
    fn send_ip_mtu(&self, cx: &Context) -> usize {
        let ip_mtu = cx.ip_mtu();
        self.path_mtu
            .map_or(ip_mtu, |path_mtu| path_mtu.min(ip_mtu))
    }

    /// Applies the MTU that an ICMP "Fragmentation Needed" (RFC 1191) or
    /// "Packet Too Big" (RFC 8201) error reports, for the segment it quotes:
    /// one sent from `local` to `remote`, with sequence number `seq`.
    ///
    /// An MTU below the family's floor counts as the floor, and one that is
    /// not below what the connection sends changes nothing. A lower one must
    /// be about data in flight, `SND.UNA <= seq < SND.NXT` (RFC 5927 4.1),
    /// so that an error forged without seeing the connection cannot shrink
    /// it. It then takes effect at once: every segment in flight larger than
    /// the new MTU was dropped, so the unacknowledged data is sent again from
    /// `SND.UNA` in segments that fit. The drop was not congestion, so the
    /// congestion window is left as it was.
    pub(crate) fn process_path_mtu(
        &mut self,
        cx: &Context,
        local: IpEndpoint,
        remote: IpEndpoint,
        seq: TcpSeqNumber,
        mtu: usize,
    ) -> PathMtuOutcome {
        if self.tuple != Some(Tuple { local, remote }) {
            return PathMtuOutcome::NotMatched;
        }

        let (ip_header_len, min_path_mtu) = match local.addr {
            #[cfg(feature = "proto-ipv4")]
            IpAddress::Ipv4(_) => (crate::wire::IPV4_HEADER_LEN, IPV4_MIN_PATH_MTU),
            #[cfg(feature = "proto-ipv6")]
            IpAddress::Ipv6(_) => (crate::wire::IPV6_HEADER_LEN, IPV6_MIN_PATH_MTU),
        };
        let current = self.send_ip_mtu(cx);
        let mtu = mtu.max(min_path_mtu);
        if mtu >= current {
            return PathMtuOutcome::Unchanged;
        }

        let sending = matches!(
            self.state,
            State::Established
                | State::FinWait1
                | State::CloseWait
                | State::Closing
                | State::LastAck
        );
        if !sending || seq < self.local_seq_no || seq >= self.remote_last_seq {
            return PathMtuOutcome::Rejected;
        }

        let mss_at = |mtu: usize| (mtu - ip_header_len - TCP_HEADER_LEN).min(self.remote_mss);
        let (old_mss, mss) = (mss_at(current), mss_at(mtu));
        self.path_mtu = Some(mtu);
        if mss >= old_mss {
            // The remote's MSS keeps the segments within the path already.
            return PathMtuOutcome::Unchanged;
        }

        net_debug!("path MTU {} lowers the MSS to {}, resending", mtu, mss);
        self.congestion_controller.inner_mut().set_mss(mss);

        // Resend from the last ACK, as a retransmission timeout does. That
        // resends SACKed data too, and supersedes fast recovery.
        self.remote_last_seq = self.local_seq_no;
        self.pending_fast_retransmit = false;
        self.recover = None;
        self.recovery_rxt_end = None;
        self.scoreboard.clear();
        self.rack.on_rewind();
        // The small segment in flight, if any, was dropped or is resent now:
        // Nagle's algorithm must not hold back the tail of the resend for it.
        self.nagle_small_segment_end = None;
        // Karn's algorithm: no RTT sample from what is resent.
        self.rtte.on_retransmit();

        PathMtuOutcome::Reduced
    }

    /// The SACK blocks of an incoming ACK that report data above
    /// `ack_number`, trimmed to start above it, and how many there are. A
    /// block must end within what has been sent; D-SACK blocks, at or below
    /// the ACK, are left out.
    fn sack_blocks(
        &self,
        repr: &TcpRepr,
        ack_number: TcpSeqNumber,
    ) -> ([(TcpSeqNumber, TcpSeqNumber); 3], usize) {
        let mut blocks = [(ack_number, ack_number); 3];
        let mut len = 0;
        if !self.remote_has_sack {
            return (blocks, len);
        }
        for &(left, right) in repr.sack_ranges.iter().flatten() {
            let right = TcpSeqNumber(right as i32);
            if right <= ack_number || right > self.remote_last_seq {
                continue;
            }
            blocks[len] = (TcpSeqNumber(left as i32).max(ack_number), right);
            len += 1;
        }
        (blocks, len)
    }

    /// Records SACK blocks, as `sack_blocks` returns them, in the
    /// scoreboard, and returns whether any of them reports data that no
    /// earlier ACK reported.
    fn sacks_new_data(
        &mut self,
        blocks: &[(TcpSeqNumber, TcpSeqNumber)],
        ack_number: TcpSeqNumber,
    ) -> bool {
        self.scoreboard.advance(ack_number);
        let mut new_data = false;
        for &(left, right) in blocks {
            new_data |= self.scoreboard.add(left, right);
        }
        new_data
    }

    /// Whether an incoming ACK carries a D-SACK (RFC 2883): a first SACK
    /// block at or below the cumulative ACK, or within the second block.
    fn has_dsack(&self, repr: &TcpRepr, ack_number: TcpSeqNumber) -> bool {
        let seq = |n: u32| TcpSeqNumber(n as i32);
        match repr.sack_ranges {
            [Some((left, right)), second, _] if self.remote_has_sack => {
                seq(right) <= ack_number
                    || second.is_some_and(|(l, r)| seq(l) <= seq(left) && seq(right) <= seq(r))
            }
            _ => false,
        }
    }

    /// Takes in what an incoming ACK reports delivered: RACK's state and the
    /// round trip sample it allows, the scoreboard, and the losses they
    /// imply. Returns whether the ACK SACKs data that no earlier ACK
    /// reported, and whether RACK newly deems data lost.
    fn on_ack_delivery(
        &mut self,
        now: Instant,
        repr: &TcpRepr,
        ack_number: TcpSeqNumber,
    ) -> (bool, bool) {
        let (blocks, len) = self.sack_blocks(repr, ack_number);
        let blocks = &blocks[..len];
        let acked = (self.local_seq_no, ack_number);
        let sample = self.rack.on_ack(now, &self.scoreboard, acked, blocks);
        // Karn's algorithm allows a sample from a first transmission. The
        // timer's own sample waits for a cumulative ACK, which recovery holds
        // back, and a retransmission cancels it, which would leave a
        // backed-off RTO in place until new data is acknowledged.
        if let Some(rtt) = sample
            && (self.recover.is_some() || !self.rtte.sampling())
        {
            self.rtte.sample(rtt.total_millis() as u32);
        }
        let sacks_new_data = self.sacks_new_data(blocks, ack_number);
        self.rack.log.advance(ack_number);
        if self.has_dsack(repr, ack_number) {
            self.rack.on_dsack(ack_number, self.remote_last_seq);
            if let Some(probe) = &mut self.rack.probe {
                probe.dsacked = true;
            }
        }
        (sacks_new_data, self.detect_losses(now))
    }

    /// Marks lost in the log what RFC 6675's `IsLost` and RACK deem lost,
    /// and returns whether RACK newly deems data lost that is not SACKed.
    fn detect_losses(&mut self, now: Instant) -> bool {
        if !self.remote_has_sack {
            return false;
        }
        if let Some(boundary) = self.scoreboard.lost_below(self.remote_mss) {
            self.rack.log.mark_lost_below(boundary);
        }
        let srtt = self.rtte.srtt();
        self.rack.detect(&self.scoreboard, now, srtt)
    }

    /// Enters fast recovery, with a fast retransmission of the first
    /// unacknowledged segment. Any loss probe episode ends: recovery answers
    /// for the loss it probed for.
    fn start_recovery(&mut self) {
        self.recover = Some(self.remote_last_seq);
        self.timer.set_for_fast_retransmit();
        self.rack.cancel_probe();
        self.rack.probe = None;
    }

    /// Whether the socket is in RFC 6675 loss recovery: fast recovery with a
    /// remote that reports SACK blocks.
    fn in_sack_recovery(&self) -> bool {
        self.recover.is_some() && self.remote_has_sack
    }

    /// RFC 6675 `SetPipe`: the octets thought to be in the network. Each
    /// unacknowledged octet counts once unless it is SACKed, or its last
    /// transmission is deemed lost, by `IsLost` or by RACK; one resent while
    /// its earlier transmission was not deemed lost counts twice.
    fn pipe(&self) -> usize {
        self.rack
            .log
            .pipe(&self.scoreboard, self.local_seq_no, self.remote_last_seq)
    }

    fn cwnd_remaining(&self) -> usize {
        let in_flight = if self.in_sack_recovery() {
            self.pipe()
        } else {
            self.flight_size()
        };
        // RFC 3042 limited transmit: each of the first two duplicate ACKs
        // before recovery lets one more new segment out, so that a small
        // window still draws the duplicates that start recovery.
        let limited_transmit = if self.recover.is_none() {
            usize::from(self.local_rx_dup_acks.min(2)) * self.remote_mss
        } else {
            0
        };
        self.congestion_controller
            .inner()
            .window()
            .saturating_add(limited_transmit)
            .saturating_sub(in_flight)
    }

    /// RFC 6675 `NextSeg` rules (1) and (3): during SACK recovery, the next
    /// hole to retransmit, as a sequence number and length of at most `mss`,
    /// if the congestion window has room. Rule (1) takes the lowest octets
    /// not SACKed whose last transmission is deemed lost, by `IsLost` or by
    /// RACK, which may be a lost retransmission. A hole not yet deemed lost
    /// is returned only when `no_new_data`, that is when rule (2) cannot
    /// send.
    fn sack_retransmission(&self, mss: usize, no_new_data: bool) -> Option<(TcpSeqNumber, usize)> {
        if !self.in_sack_recovery() || self.cwnd_remaining() == 0 {
            return None;
        }
        let data_end = self.local_seq_no + self.tx_buffer.len();
        let segment = |(start, end): (TcpSeqNumber, TcpSeqNumber)| {
            let end = end.min(data_end);
            (start < end).then(|| (start, (end - start).min(mss)))
        };
        if let Some(hole) = self
            .rack
            .log
            .first_lost(&self.scoreboard, self.remote_last_seq)
        {
            return segment(hole);
        }
        if !no_new_data {
            return None;
        }
        let from = self
            .recovery_rxt_end
            .map_or(self.local_seq_no, |end| end.max(self.local_seq_no));
        segment(self.scoreboard.next_hole(from)?)
    }

    /// Whether RFC 8985 7.2 allows a loss probe: the remote SACKs, data is in
    /// flight, and no loss recovery or probe episode is under way, nor is
    /// anything SACKed, which recovery or RACK's reordering timer sees to.
    fn loss_probe_allowed(&self) -> bool {
        self.remote_has_sack
            && matches!(
                self.state,
                State::Established
                    | State::CloseWait
                    | State::FinWait1
                    | State::Closing
                    | State::LastAck
            )
            && self.remote_last_seq > self.local_seq_no
            && self.recover.is_none()
            && self.scoreboard.is_empty()
            && self.rack.probe.is_none()
            && !self.rack.log.recovering()
            && self.remote_win_len > 0
    }

    /// Starts or restarts the loss probe timer (RFC 8985 7.2). The probe
    /// timeout is twice SRTT, and at least 10 ms, plus a worst-case delayed
    /// ACK when only one segment is in flight, and the timer is armed only
    /// if it would expire before the retransmission timer.
    fn arm_loss_probe(&mut self, now: Instant) {
        self.rack.cancel_probe();
        let Some(srtt) = self.rtte.srtt() else {
            return;
        };
        if !self.loss_probe_allowed() {
            return;
        }
        let mut pto = (srtt * 2 + TLP_MIN_TIMEOUT).max(TLP_MIN_PTO);
        if self.flight_size() <= self.remote_mss {
            pto += TLP_DELAYED_ACK;
        }
        if let Timer::Retransmit { expires_at } = self.timer
            && now + pto < expires_at
        {
            self.rack.probe_at = Some(now + pto);
        }
    }

    /// Runs RACK's reordering timer and the loss probe timer, if either has
    /// expired (RFC 8985 6.3, 7.3).
    fn poll_rack_timers(&mut self, now: Instant) {
        if self.rack.reo_timeout.is_some_and(|at| now >= at) {
            self.rack.reo_timeout = None;
            if self.detect_losses(now) && self.recover.is_none() {
                net_debug!("RACK reordering timer deems data lost, starting fast retransmit");
                self.start_recovery();
            }
        }
        if self.rack.probe_at.is_some_and(|at| now >= at) {
            self.rack.probe_at = None;
            self.rack.probe_pending = self.loss_probe_allowed();
        }
    }

    /// Whether all the data the remote window allows has been sent, so that
    /// SACK recovery has no new data to send.
    fn no_new_data(&self) -> bool {
        let limit = self.tx_buffer.len().min(self.remote_win_len);
        self.remote_last_seq >= self.local_seq_no + limit
    }

    /// Return the amount of octets queued in the receive buffer. This value can be larger than
    /// the slice read by the next `recv` or `peek` call because it includes all queued octets,
    /// and not only the octets that may be returned as a contiguous slice.
    ///
    /// Note that the Berkeley sockets interface does not have an equivalent of this API.
    pub fn recv_queue(&self) -> usize {
        self.rx_buffer.len()
    }

    fn set_state(&mut self, state: State) {
        if self.state != state {
            tcp_trace!("state={}=>{}", self.state, state);
        }

        self.state = state;

        #[cfg(feature = "async")]
        {
            // Wake all tasks waiting. Even if we haven't received/sent data, this
            // is needed because return values of functions may change depending on the state.
            // For example, a pending read has to fail with an error if the socket is closed.
            self.rx_waker.wake();
            self.tx_waker.wake();
        }
    }

    pub(crate) fn reply(ip_repr: &IpRepr, repr: &TcpRepr) -> (IpRepr, TcpRepr<'static>) {
        let reply_repr = TcpRepr {
            src_port: repr.dst_port,
            dst_port: repr.src_port,
            control: TcpControl::None,
            seq_number: TcpSeqNumber(0),
            ack_number: None,
            window_len: 0,
            window_scale: None,
            max_seg_size: None,
            sack_permitted: false,
            sack_ranges: [None, None, None],
            timestamp: None,
            payload: &[],
        };
        let ip_reply_repr = IpRepr::new(
            ip_repr.dst_addr(),
            ip_repr.src_addr(),
            IpProtocol::Tcp,
            reply_repr.buffer_len(),
            64,
        );
        (ip_reply_repr, reply_repr)
    }

    pub(crate) fn rst_reply(ip_repr: &IpRepr, repr: &TcpRepr) -> (IpRepr, TcpRepr<'static>) {
        debug_assert!(repr.control != TcpControl::Rst);

        let (ip_reply_repr, mut reply_repr) = Self::reply(ip_repr, repr);

        // See https://www.snellman.net/blog/archive/2016-02-01-tcp-rst/ for explanation
        // of why we sometimes send an RST and sometimes an RST|ACK
        reply_repr.control = TcpControl::Rst;
        reply_repr.seq_number = repr.ack_number.unwrap_or_default();
        if repr.control == TcpControl::Syn && repr.ack_number.is_none() {
            reply_repr.ack_number = Some(repr.seq_number + repr.segment_len());
        }

        (ip_reply_repr, reply_repr)
    }

    fn ack_reply(&mut self, ip_repr: &IpRepr, repr: &TcpRepr) -> (IpRepr, TcpRepr<'static>) {
        let (mut ip_reply_repr, mut reply_repr) = Self::reply(ip_repr, repr);
        reply_repr.timestamp = repr
            .timestamp
            .and_then(|tcp_ts| tcp_ts.generate_reply(self.tsval_generator));

        // From RFC 793:
        // [...] an empty acknowledgment segment containing the current send-sequence number
        // and an acknowledgment indicating the next sequence number expected
        // to be received.
        reply_repr.seq_number = self.remote_last_seq;
        reply_repr.ack_number = Some(self.remote_seq_no + self.rx_buffer.len());
        self.remote_last_ack = reply_repr.ack_number;

        // From RFC 1323:
        // The window field [...] of every outgoing segment, with the exception of SYN
        // segments, is right-shifted by [advertised scale value] bits[...]
        reply_repr.window_len = self.scaled_window();
        self.remote_last_win = reply_repr.window_len;

        // If the remote supports selective acknowledgement, add the option to the outgoing
        // segment.
        if self.remote_has_sack {
            net_debug!("sending sACK option with current assembler ranges");

            // RFC 2018: The first SACK block (i.e., the one immediately following the kind and
            // length fields in the option) MUST specify the contiguous block of data containing
            // the segment which triggered this ACK, unless that segment advanced the
            // Acknowledgment Number field in the header.
            reply_repr.sack_ranges[0] = None;

            let ack = reply_repr.ack_number.unwrap_or(TcpSeqNumber(0));

            if let Some(last_seg_seq) = self.local_rx_last_seq {
                reply_repr.sack_ranges[0] = self
                    .assembler
                    .iter_data()
                    .map(|(left, right)| (ack + left, ack + right))
                    .find(|&(left, right)| left <= last_seg_seq && right >= last_seg_seq)
                    .map(|(left, right)| (left.0 as u32, right.0 as u32));
            }

            if reply_repr.sack_ranges[0].is_none() {
                // The matching segment was removed from the assembler, meaning the acknowledgement
                // number has advanced, or there was no previous sACK.
                //
                // While the RFC says we SHOULD keep a list of reported sACK ranges, and iterate
                // through those, that is currently infeasible. Instead, we offer the range with
                // the lowest sequence number (if one exists) to hint at what segments would
                // most quickly advance the acknowledgement number.
                reply_repr.sack_ranges[0] = self
                    .assembler
                    .iter_data()
                    .map(|(left, right)| (ack + left, ack + right))
                    .next()
                    .map(|(left, right)| (left.0 as u32, right.0 as u32));
            }
        }

        // Since the sACK option may have changed the length of the payload, update that.
        ip_reply_repr.set_payload_len(reply_repr.buffer_len());
        (ip_reply_repr, reply_repr)
    }

    fn challenge_ack_reply(
        &mut self,
        cx: &mut Context,
        ip_repr: &IpRepr,
        repr: &TcpRepr,
    ) -> Option<(IpRepr, TcpRepr<'static>)> {
        if cx.now() < self.challenge_ack_timer {
            return None;
        }

        // Rate-limit to 1 per second max.
        self.challenge_ack_timer = cx.now() + Duration::from_secs(1);

        Some(self.ack_reply(ip_repr, repr))
    }

    pub(crate) fn accepts(&self, _cx: &mut Context, ip_repr: &IpRepr, repr: &TcpRepr) -> bool {
        if self.state == State::Closed {
            return false;
        }

        // If we're still listening for SYNs and the packet has an ACK or a RST,
        // it cannot be destined to this socket, but another one may well listen
        // on the same local endpoint.
        if self.state == State::Listen
            && (repr.ack_number.is_some() || repr.control == TcpControl::Rst)
        {
            return false;
        }

        if let Some(tuple) = &self.tuple {
            // Reject packets not matching the 4-tuple
            ip_repr.dst_addr() == tuple.local.addr
                && repr.dst_port == tuple.local.port
                && ip_repr.src_addr() == tuple.remote.addr
                && repr.src_port == tuple.remote.port
        } else {
            // We're listening, reject packets not matching the listen endpoint.
            let addr_ok = match self.listen_endpoint.addr {
                Some(addr) => ip_repr.dst_addr() == addr,
                None => true,
            };
            addr_ok && repr.dst_port != 0 && repr.dst_port == self.listen_endpoint.port
        }
    }

    pub(crate) fn process(
        &mut self,
        cx: &mut Context,
        ip_repr: &IpRepr,
        repr: &TcpRepr,
    ) -> Option<(IpRepr, TcpRepr<'static>)> {
        debug_assert!(self.accepts(cx, ip_repr, repr));

        // Consider how much the sequence number space differs from the transmit buffer space.
        let (sent_syn, sent_fin) = match self.state {
            // In SYN-SENT or SYN-RECEIVED, we've just sent a SYN.
            State::SynSent | State::SynReceived => (true, false),
            // In FIN-WAIT-1, LAST-ACK, or CLOSING, we've just sent a FIN.
            State::FinWait1 | State::LastAck | State::Closing => (false, true),
            // In all other states we've already got acknowledgements for
            // all of the control flags we sent.
            _ => (false, false),
        };
        let control_len = (sent_syn as usize) + (sent_fin as usize);

        // Reject unacceptable acknowledgements.
        match (self.state, repr.control, repr.ack_number) {
            // An RST received in response to initial SYN is acceptable if it acknowledges
            // the initial SYN.
            (State::SynSent, TcpControl::Rst, None) => {
                net_debug!("unacceptable RST (expecting RST|ACK) in response to initial SYN");
                return None;
            }
            (State::SynSent, TcpControl::Rst, Some(ack_number)) => {
                if ack_number != self.local_seq_no + 1 {
                    net_debug!("unacceptable RST|ACK in response to initial SYN");
                    return None;
                }
            }
            // Any other RST need only have a valid sequence number.
            (_, TcpControl::Rst, _) => (),
            // The initial SYN cannot contain an acknowledgement.
            (State::Listen, _, None) => (),
            // This case is handled in `accepts()`.
            (State::Listen, _, Some(_)) => unreachable!(),
            // SYN|ACK in the SYN-SENT state must have the exact ACK number.
            (State::SynSent, TcpControl::Syn, Some(ack_number)) => {
                if ack_number != self.local_seq_no + 1 {
                    net_debug!("unacceptable SYN|ACK in response to initial SYN");
                    return Some(Self::rst_reply(ip_repr, repr));
                }
            }
            // TCP simultaneous open.
            // This is required by RFC 9293, which states "A TCP implementation MUST support
            // simultaneous open attempts (MUST-10)."
            (State::SynSent, TcpControl::Syn, None) => (),
            // ACKs in the SYN-SENT state are invalid.
            (State::SynSent, TcpControl::None, Some(ack_number)) => {
                // If the sequence number matches, ignore it instead of RSTing.
                // I'm not sure why, I think it may be a workaround for broken TCP
                // servers, or a defense against reordering. Either way, if Linux
                // does it, we do too.
                if ack_number == self.local_seq_no + 1 {
                    net_debug!(
                        "expecting a SYN|ACK, received an ACK with the right ack_number, ignoring."
                    );
                    return None;
                }

                net_debug!(
                    "expecting a SYN|ACK, received an ACK with the wrong ack_number, sending RST."
                );
                return Some(Self::rst_reply(ip_repr, repr));
            }
            // Anything else in the SYN-SENT state is invalid.
            (State::SynSent, _, _) => {
                net_debug!("expecting a SYN|ACK");
                return None;
            }
            // Every packet after the initial SYN must be an acknowledgement.
            (_, _, None) => {
                net_debug!("expecting an ACK");
                return None;
            }
            // ACK in the SYN-RECEIVED state must have the exact ACK number, or we RST it.
            (State::SynReceived, _, Some(ack_number)) => {
                if ack_number != self.local_seq_no + 1 {
                    net_debug!("unacceptable ACK in response to SYN|ACK");
                    return Some(Self::rst_reply(ip_repr, repr));
                }
            }
            // Every acknowledgement must be for transmitted but unacknowledged data.
            (_, _, Some(ack_number)) => {
                let unacknowledged = self.tx_buffer.len() + control_len;

                // Acceptable ACK range (both inclusive)
                let mut ack_min = self.local_seq_no;
                let ack_max = self.local_seq_no + unacknowledged;

                // If we have sent a SYN, it MUST be acknowledged.
                if sent_syn {
                    ack_min += 1;
                }

                if ack_number < ack_min {
                    net_debug!(
                        "duplicate ACK ({} not in {}...{})",
                        ack_number,
                        ack_min,
                        ack_max
                    );
                    return None;
                }

                if ack_number > ack_max {
                    net_debug!(
                        "unacceptable ACK ({} not in {}...{})",
                        ack_number,
                        ack_min,
                        ack_max
                    );
                    return self.challenge_ack_reply(cx, ip_repr, repr);
                }
            }
        }

        let window_start = self.remote_seq_no + self.rx_buffer.len();
        let window_end = if let Some(last_ack) = self.remote_last_ack {
            last_ack + ((self.remote_last_win as usize) << self.remote_win_shift)
        } else {
            window_start
        };
        let segment_start = repr.seq_number;
        let segment_end = repr.seq_number + repr.payload.len();

        let (payload, payload_offset) = match self.state {
            // In LISTEN and SYN-SENT states, we have not yet synchronized with the remote end.
            State::Listen | State::SynSent => (&[][..], 0),
            _ => {
                // https://www.rfc-editor.org/rfc/rfc9293.html#name-segment-acceptability-tests
                let segment_in_window = match (
                    segment_start == segment_end,
                    window_start == window_end,
                ) {
                    (true, _) if segment_end == window_start - 1 => {
                        net_debug!(
                            "received a keep-alive or window probe packet, will send an ACK"
                        );
                        false
                    }
                    (true, true) => {
                        if window_start == segment_start {
                            true
                        } else {
                            net_debug!(
                                "zero-length segment not inside zero-length window, will send an ACK."
                            );
                            false
                        }
                    }
                    (true, false) => {
                        if window_start <= segment_start && segment_start < window_end {
                            true
                        } else {
                            net_debug!("zero-length segment not inside window, will send an ACK.");
                            false
                        }
                    }
                    (false, true) => {
                        net_debug!(
                            "non-zero-length segment with zero receive window, will only send an ACK"
                        );
                        false
                    }
                    (false, false) => {
                        if (window_start <= segment_start && segment_start < window_end)
                            || (window_start < segment_end && segment_end <= window_end)
                        {
                            true
                        } else {
                            net_debug!(
                                "segment not in receive window ({}..{} not intersecting {}..{}), will send challenge ACK",
                                segment_start,
                                segment_end,
                                window_start,
                                window_end
                            );
                            false
                        }
                    }
                };

                if segment_in_window {
                    let overlap_start = window_start.max(segment_start);
                    let overlap_end = window_end.min(segment_end);

                    // the checks done above imply this.
                    debug_assert!(overlap_start <= overlap_end);

                    self.local_rx_last_seq = Some(repr.seq_number);

                    (
                        &repr.payload[overlap_start - segment_start..overlap_end - segment_start],
                        overlap_start - window_start,
                    )
                } else {
                    // Out-of-window RSTs are silently dropped, per RFC 9293
                    // (3.10.7.4) and RFC 5961 (3.2): no reply is sent, and the
                    // TIME-WAIT timer below is not refreshed. RST senders don't
                    // need a reply to make progress.
                    if repr.control == TcpControl::Rst {
                        net_debug!("dropping out-of-window RST");
                        return None;
                    }

                    // If we're in the TIME-WAIT state, restart the TIME-WAIT timeout, since
                    // the remote end may not have realized we've closed the connection.
                    if self.state == State::TimeWait {
                        self.timer.set_for_close(cx.now());
                    }

                    // Segments carrying data are exempt from challenge ACK rate
                    // limiting: an out-of-window data segment is a retransmission
                    // whose ACK was lost, or a window probe, and per RFC 9293
                    // (3.10.7.4, 3.8.6.1) it should elicit an ACK so the remote
                    // can make progress. Withholding these ACKs strands the
                    // remote in retransmission backoff or persist state. The
                    // rate limit exists to break ACK loops between desynced
                    // peers, and exempting data segments cannot sustain such a
                    // loop: the remote paces them with its retransmission and
                    // persist timers, and the data it may send in response to a
                    // duplicate ACK of ours (fast recovery) is bounded by its
                    // send window, which never advances during a desync.
                    //
                    // The exemption covers FIN (a retransmitted final segment is
                    // the same lost-ACK situation) but not SYN: a SYN in a
                    // synchronized state is a challenge ACK situation (RFC 5961
                    // 4.2), and challenge ACKs should be throttled (RFC 5961 7).
                    // One per second is ample for a restarted peer to complete
                    // the challenge exchange, since it retransmits its SYN on
                    // its own timer.
                    if !repr.payload.is_empty()
                        && matches!(
                            repr.control,
                            TcpControl::None | TcpControl::Psh | TcpControl::Fin
                        )
                    {
                        return Some(self.ack_reply(ip_repr, repr));
                    }

                    return self.challenge_ack_reply(cx, ip_repr, repr);
                }
            }
        };

        // Compute the amount of acknowledged octets, removing the SYN and FIN bits
        // from the sequence space.
        let mut ack_len = 0;
        let mut ack_of_fin = false;
        let mut ack_all = false;
        if repr.control != TcpControl::Rst
            && let Some(ack_number) = repr.ack_number
        {
            // Sequence number corresponding to the first byte in `tx_buffer`.
            // This normally equals `local_seq_no`, but is 1 higher if we have sent a SYN,
            // as the SYN occupies 1 sequence number "before" the data.
            let tx_buffer_start_seq = self.local_seq_no + (sent_syn as usize);

            if ack_number >= tx_buffer_start_seq {
                ack_len = ack_number - tx_buffer_start_seq;

                // We could've sent data before the FIN, so only remove FIN from the sequence
                // space if all of that data is acknowledged.
                if sent_fin && self.tx_buffer.len() + 1 == ack_len {
                    ack_len -= 1;
                    tcp_trace!("received ACK of FIN");
                    ack_of_fin = true;
                }

                ack_all = self.remote_last_seq <= ack_number;
            }
        }

        // Disregard control flags we don't care about or shouldn't act on yet.
        let mut control = repr.control;
        control = control.quash_psh();

        // If a FIN is received at the end of the current segment, but
        // we have a hole in the assembler before the current segment, disregard this FIN.
        if control == TcpControl::Fin && window_start < segment_start {
            tcp_trace!(
                "ignoring FIN because we don't have full data yet. window_start={} segment_start={}",
                window_start,
                segment_start
            );
            control = TcpControl::None;
        }

        // Validate and update the state.
        match (self.state, control) {
            // RSTs are not accepted in the LISTEN state.
            (State::Listen, TcpControl::Rst) => return None,

            // RSTs in SYN-RECEIVED flip the socket back to the LISTEN state.
            // Here we need to additionally check `listen_endpoint`, because we want to make sure
            // that SYN-RECEIVED was actually converted from the LISTEN state (another possible
            // reason is TCP simultaneous open).
            (State::SynReceived, TcpControl::Rst) if self.listen_endpoint.port != 0 => {
                tcp_trace!("received RST");
                self.tuple = None;
                self.set_state(State::Listen);
                return None;
            }

            // RSTs in any other state close the socket.
            (_, TcpControl::Rst) => {
                tcp_trace!("received RST");
                self.set_state(State::Closed);
                self.tuple = None;
                return None;
            }

            // SYN packets in the LISTEN state change it to SYN-RECEIVED.
            (State::Listen, TcpControl::Syn) => {
                tcp_trace!("received SYN");
                if let Some(max_seg_size) = repr.max_seg_size {
                    // Treat a zero MSS as if the option were absent, like Linux does.
                    if max_seg_size != 0 {
                        self.remote_mss = (max_seg_size as usize).max(MIN_REMOTE_MSS);
                        self.congestion_controller
                            .inner_mut()
                            .set_mss(self.remote_mss);
                    }
                }

                self.tuple = Some(Tuple {
                    local: IpEndpoint::new(ip_repr.dst_addr(), repr.dst_port),
                    remote: IpEndpoint::new(ip_repr.src_addr(), repr.src_port),
                });
                self.local_seq_no = Self::random_seq_no(cx);
                self.remote_seq_no = repr.seq_number + 1;
                self.remote_last_seq = self.local_seq_no;
                self.remote_has_sack = repr.sack_permitted;
                self.remote_win_scale = repr.window_scale;
                // Remote doesn't support window scaling, don't do it.
                if self.remote_win_scale.is_none() {
                    self.remote_win_shift = 0;
                }
                // Remote doesn't support timestamping, don't do it.
                if repr.timestamp.is_none() {
                    self.tsval_generator = None;
                }
                self.set_state(State::SynReceived);
                self.timer.set_for_idle(cx.now(), self.keep_alive);
            }

            // ACK packets in the SYN-RECEIVED state change it to ESTABLISHED.
            (State::SynReceived, TcpControl::None) => {
                self.set_state(State::Established);
            }

            // FIN packets in the SYN-RECEIVED state change it to CLOSE-WAIT.
            // It's not obvious from RFC 793 that this is permitted, but
            // 7th and 8th steps in the "SEGMENT ARRIVES" event describe this behavior.
            (State::SynReceived, TcpControl::Fin) => {
                self.remote_seq_no += 1;
                self.rx_fin_received = true;
                self.set_state(State::CloseWait);
            }

            // SYN|ACK packets in the SYN-SENT state change it to ESTABLISHED.
            // SYN packets in the SYN-SENT state change it to SYN-RECEIVED.
            (State::SynSent, TcpControl::Syn) => {
                if repr.ack_number.is_some() {
                    tcp_trace!("received SYN|ACK");
                } else {
                    tcp_trace!("received SYN");
                }
                if let Some(max_seg_size) = repr.max_seg_size {
                    // Treat a zero MSS as if the option were absent, like Linux does.
                    if max_seg_size != 0 {
                        self.remote_mss = (max_seg_size as usize).max(MIN_REMOTE_MSS);
                        self.congestion_controller
                            .inner_mut()
                            .set_mss(self.remote_mss);
                    }
                }

                self.remote_seq_no = repr.seq_number + 1;
                self.remote_last_seq = self.local_seq_no + 1;
                self.remote_last_ack = Some(repr.seq_number);
                self.remote_has_sack = repr.sack_permitted;
                self.remote_win_scale = repr.window_scale;
                // Remote doesn't support window scaling, don't do it.
                if self.remote_win_scale.is_none() {
                    self.remote_win_shift = 0;
                }
                // Remote doesn't support timestamping, don't do it.
                if repr.timestamp.is_none() {
                    self.tsval_generator = None;
                }

                if repr.ack_number.is_some() {
                    self.set_state(State::Established);
                } else {
                    self.set_state(State::SynReceived);
                }
            }

            (State::Established, TcpControl::None) => {}

            // FIN packets in ESTABLISHED state indicate the remote side has closed.
            (State::Established, TcpControl::Fin) => {
                self.remote_seq_no += 1;
                self.rx_fin_received = true;
                self.set_state(State::CloseWait);
            }

            // ACK packets in FIN-WAIT-1 state change it to FIN-WAIT-2, if we've already
            // sent everything in the transmit buffer. If not, they reset the retransmit timer.
            (State::FinWait1, TcpControl::None) => {
                if ack_of_fin {
                    self.set_state(State::FinWait2);
                }
            }

            // FIN packets in FIN-WAIT-1 state change it to CLOSING, or to TIME-WAIT
            // if they also acknowledge our FIN.
            (State::FinWait1, TcpControl::Fin) => {
                self.remote_seq_no += 1;
                self.rx_fin_received = true;
                if ack_of_fin {
                    self.set_state(State::TimeWait);
                    self.timer.set_for_close(cx.now());
                } else {
                    self.set_state(State::Closing);
                }
            }

            (State::FinWait2, TcpControl::None) => {}

            // FIN packets in FIN-WAIT-2 state change it to TIME-WAIT.
            (State::FinWait2, TcpControl::Fin) => {
                self.remote_seq_no += 1;
                self.rx_fin_received = true;
                self.set_state(State::TimeWait);
                self.timer.set_for_close(cx.now());
            }

            // ACK packets in CLOSING state change it to TIME-WAIT.
            (State::Closing, TcpControl::None) => {
                if ack_of_fin {
                    self.set_state(State::TimeWait);
                    self.timer.set_for_close(cx.now());
                }
            }

            (State::CloseWait, TcpControl::None) => {}

            // ACK packets in LAST-ACK state change it to CLOSED.
            (State::LastAck, TcpControl::None) => {
                if ack_of_fin {
                    // Clear the remote endpoint, or we'll send an RST there.
                    self.set_state(State::Closed);
                    self.tuple = None;
                } else if ack_len == 0 {
                    // Duplicate ACK; our FIN has not been acknowledged.
                    // Per RFC 9293 (3.10.7.4), send a challenge ACK.
                    return self.challenge_ack_reply(cx, ip_repr, repr);
                }
                // Partial ACK: fall through to advance SND.UNA normally.
            }

            _ => {
                net_debug!("unexpected packet {}", repr);
                return None;
            }
        }

        // Update remote state. Any packet answers the keep-alive probes.
        self.remote_last_ts = Some(cx.now());
        self.keep_alive_probes = 0;

        // RFC 1323: The window field (SEG.WND) in the header of every incoming segment, with the
        // exception of SYN segments, is left-shifted by Snd.Wind.Scale bits before updating SND.WND.
        let scale = match repr.control {
            TcpControl::Syn => 0,
            _ => self.remote_win_scale.unwrap_or(0),
        };
        let new_remote_win_len = (repr.window_len as usize) << (scale as usize);
        let is_window_update = new_remote_win_len != self.remote_win_len;
        self.remote_win_len = new_remote_win_len;

        self.congestion_controller
            .inner_mut()
            .set_remote_window(new_remote_win_len);

        if ack_len > 0 {
            // Dequeue acknowledged octets.
            debug_assert!(self.tx_buffer.len() >= ack_len);
            tcp_trace!(
                "tx buffer: dequeueing {} octets (now {})",
                ack_len,
                self.tx_buffer.len() - ack_len
            );
            self.tx_buffer.dequeue_allocated(ack_len);

            // There's new room available in tx_buffer, wake the waiting task if any.
            #[cfg(feature = "async")]
            self.tx_waker.wake();
        }

        let mut rack_lost = false;
        if let Some(ack_number) = repr.ack_number {
            // TODO: When flow control is implemented,
            // refractor the following block within that implementation

            // RFC 6675 (2): an ACK whose SACK blocks report data above the
            // cumulative ACK that no earlier ACK reported is a duplicate ACK,
            // whatever window it carries. Linux grows its advertised window on
            // nearly every ACK while its receive buffer autotunes, so without
            // this none of its duplicate ACKs would count, and every loss would
            // wait for the retransmission timer.
            let sacks_new_data;
            (sacks_new_data, rack_lost) = self.on_ack_delivery(cx.now(), repr, ack_number);

            // RFC 8985 7.4: a loss probe's episode ends once what was in
            // flight when it was sent is acknowledged. A probe that resent
            // data, with no D-SACK to show the resend needless, repaired a
            // loss, and the congestion window answers for it as for any
            // other: the controller enters recovery here, and the ACK below
            // ends it.
            if let Some(probe) = self.rack.probe
                && ack_number >= probe.end
            {
                self.rack.probe = None;
                if probe.retransmitted && !probe.dsacked {
                    net_debug!("loss probe repaired a loss");
                    let in_flight = self.flight_size();
                    self.congestion_controller
                        .inner_mut()
                        .on_loss(cx.now(), in_flight);
                }
            }

            match self.local_rx_last_ack {
                // Duplicate ACK if payload empty and ACK doesn't move send window ->
                // Increment duplicate ACK count, notify congestion controller and
                // set for retransmit if we just received the third duplicate ACK
                Some(last_rx_ack)
                    if repr.payload.is_empty()
                        && last_rx_ack == ack_number
                        && ack_number < self.remote_last_seq
                        && (!is_window_update || sacks_new_data) =>
                {
                    // Increment duplicate ACK count
                    self.local_rx_dup_acks = self.local_rx_dup_acks.saturating_add(1);

                    net_debug!(
                        "received duplicate ACK for seq {} (duplicate nr {}{})",
                        ack_number,
                        self.local_rx_dup_acks,
                        if self.local_rx_dup_acks == u8::MAX {
                            "+"
                        } else {
                            ""
                        }
                    );

                    // RFC 6582: during fast recovery, partial ACKs drive retransmission,
                    // so further duplicates must not start it again. RFC 6675 also
                    // starts it once the SACK blocks show the first unacknowledged
                    // segment lost, however few duplicates have counted.
                    let head_lost = self
                        .scoreboard
                        .lost_below(self.remote_mss)
                        .is_some_and(|lost| ack_number < lost);
                    if (self.local_rx_dup_acks == 3 || head_lost) && self.recover.is_none() {
                        self.start_recovery();
                        net_debug!("started fast retransmit");
                    }

                    // Notify of duplicate ACK. SACK recovery measures the data in
                    // flight with `pipe`, which already leaves out what each
                    // duplicate SACKs, so it does not inflate the window for it too.
                    if !self.in_sack_recovery() {
                        let in_flight = self.flight_size();
                        self.congestion_controller.inner_mut().on_dup_ack(
                            cx.now(),
                            self.remote_mss,
                            in_flight,
                        );
                    }
                }

                // No duplicate ACK means we reset the duplicate ACK count
                // and notify the congestion controller of the fresh ACK
                _ => {
                    if self.local_rx_dup_acks > 0 {
                        self.local_rx_dup_acks = 0;
                        net_debug!("reset duplicate ACK count");
                    }
                    self.local_rx_last_ack = Some(ack_number);

                    // Notify of fresh ACK
                    self.rtte.on_ack(cx.now(), ack_number);
                    let new_flight_size = self.flight_size().saturating_sub(ack_len);
                    self.congestion_controller.inner_mut().on_ack(
                        cx.now(),
                        ack_len,
                        new_flight_size,
                        &self.rtte,
                    );

                    // RFC 6582 (NewReno): an ACK during fast recovery that stops short of
                    // `recover` is a partial ACK, so the segment it now points at was lost
                    // too. Retransmit it at once: once the window has drained, no further
                    // duplicate ACKs can arrive, and only the retransmit timer would fire.
                    if let Some(recover) = self.recover
                        && ack_len > 0
                    {
                        if ack_number >= recover {
                            net_debug!("fast recovery complete");
                            self.recover = None;
                            self.recovery_rxt_end = None;
                            self.rack.on_recovery_end();
                            // A partial ACK earlier in the same poll may have queued a
                            // retransmission that this ACK has made unnecessary.
                            self.pending_fast_retransmit = false;
                        } else if !self.remote_has_sack
                            || self.recovery_rxt_end.is_none_or(|end| end <= ack_number)
                        {
                            // With SACK, the scoreboard usually has this segment
                            // resent already; if it has not, resend it now as
                            // NewReno would.
                            net_debug!("partial ACK during fast recovery, retransmitting");
                            self.pending_fast_retransmit = true;
                            self.rtte.on_retransmit();
                        }
                    }
                }
            };

            // We've processed everything in the incoming segment, so advance the local
            // sequence number past it.
            self.local_seq_no = ack_number;

            if self
                .nagle_small_segment_end
                .is_some_and(|end| end <= ack_number)
            {
                self.nagle_small_segment_end = None;
            }

            // During retransmission, if an earlier segment got lost but later was
            // successfully received, self.local_seq_no can move past self.remote_last_seq.
            // Do not attempt to retransmit the latter segments; not only this is pointless
            // in theory but also impossible in practice, since they have been already
            // deallocated from the buffer.
            if self.remote_last_seq < self.local_seq_no {
                self.remote_last_seq = self.local_seq_no
            }
        }

        // update last remote tsval
        if let Some(timestamp) = repr.timestamp {
            self.last_remote_tsval = timestamp.tsval;
        }

        // update timers.
        match self.timer {
            Timer::Retransmit { .. } | Timer::FastRetransmit => {
                if ack_all {
                    // RFC 6298: (5.2) ACK of all outstanding data turn off the retransmit timer.
                    self.timer.set_for_idle(cx.now(), self.keep_alive);
                } else if ack_len > 0 {
                    // (5.3) ACK of new data in ESTABLISHED state restart the retransmit timer.
                    let rto = self.rtte.retransmission_timeout();
                    self.timer.set_for_retransmit(cx.now(), rto);
                }
            }
            Timer::Idle { .. } => {
                // any packet on idle refresh the keepalive timer.
                self.timer.set_for_idle(cx.now(), self.keep_alive);
            }
            _ => {}
        }

        // start/stop the Zero Window Probe timer.
        if self.remote_win_len == 0
            && !self.tx_buffer.is_empty()
            && (self.timer.is_idle() || ack_len > 0)
        {
            let delay = self.rtte.retransmission_timeout();
            tcp_trace!("starting zero-window-probe timer for t+{}", delay);
            self.timer.set_for_zero_window_probe(cx.now(), delay);
        }
        if self.remote_win_len != 0 && self.timer.is_zero_window_probe() {
            tcp_trace!("stopping zero-window-probe timer");
            self.timer.set_for_idle(cx.now(), self.keep_alive);
        }

        // RFC 8985 6.2: data RACK deems lost starts recovery, as the third
        // duplicate ACK does. It may be a partial ACK, so this comes after the
        // retransmission timer has been restarted for it. As a fast
        // retransmission does, it goes ahead whatever the peer's window: the
        // loss is marked once, and nothing else would resend it.
        if rack_lost && self.recover.is_none() {
            net_debug!("RACK deems data lost, starting fast retransmit");
            self.start_recovery();
        }

        // RFC 8985 7.2: an ACK of new data restarts the loss probe timer.
        if ack_len > 0 {
            self.arm_loss_probe(cx.now());
        } else if !self.loss_probe_allowed() {
            self.rack.cancel_probe();
        }

        let payload_len = payload.len();
        if payload_len == 0 {
            return None;
        }

        let assembler_was_empty = self.assembler.is_empty();

        // Try adding payload octets to the assembler.
        let Ok(contig_len) = self
            .assembler
            .add_then_remove_front(payload_offset, payload_len)
        else {
            net_debug!(
                "assembler: too many holes to add {} octets at offset {}",
                payload_len,
                payload_offset
            );
            return None;
        };

        // Place payload octets into the buffer.
        tcp_trace!(
            "rx buffer: receiving {} octets at offset {}",
            payload_len,
            payload_offset
        );
        let len_written = self.rx_buffer.write_unallocated(payload_offset, payload);
        debug_assert!(len_written == payload_len);

        if contig_len != 0 {
            // Enqueue the contiguous data octets in front of the buffer.
            tcp_trace!(
                "rx buffer: enqueueing {} octets (now {})",
                contig_len,
                self.rx_buffer.len() + contig_len
            );
            self.rx_buffer.enqueue_unallocated(contig_len);

            // There's new data in rx_buffer, notify waiting task if any.
            #[cfg(feature = "async")]
            self.rx_waker.wake();
        }

        if !self.assembler.is_empty() {
            // Print the ranges recorded in the assembler.
            tcp_trace!("assembler: {}", self.assembler);
        }

        // Handle delayed acks
        if let Some(ack_delay) = self.ack_delay
            && self.ack_to_transmit()
        {
            self.ack_delay_timer = match self.ack_delay_timer {
                AckDelayTimer::Idle => {
                    tcp_trace!("starting delayed ack timer");
                    AckDelayTimer::Waiting(cx.now() + ack_delay)
                }
                AckDelayTimer::Waiting(_) if self.immediate_ack_to_transmit() => {
                    tcp_trace!("delayed ack timer already started, forcing expiry");
                    AckDelayTimer::Immediate
                }
                timer @ AckDelayTimer::Waiting(_) => {
                    tcp_trace!("waiting until delayed ack timer expires");
                    timer
                }
                AckDelayTimer::Immediate => {
                    tcp_trace!("delayed ack timer already force-expired");
                    AckDelayTimer::Immediate
                }
            };
        }

        // Per RFC 5681, we should send an immediate ACK when either:
        //  1) an out-of-order segment is received, or
        //  2) a segment arrives that fills in all or part of a gap in sequence space.
        if !self.assembler.is_empty() || !assembler_was_empty {
            // Note that we change the transmitter state here.
            // This is fine because smoltcp assumes that it can always transmit zero or one
            // packets for every packet it receives.
            tcp_trace!("ACKing incoming segment");
            Some(self.ack_reply(ip_repr, repr))
        } else {
            None
        }
    }

    fn timed_out(&self, timestamp: Instant) -> bool {
        match (self.remote_last_ts, self.timeout) {
            (Some(remote_last_ts), Some(timeout)) => {
                self.awaits_remote() && timestamp >= remote_last_ts + timeout
            }
            (_, _) => false,
        }
    }

    // Whether the connection is waiting on the remote endpoint, which is when the timeout
    // applies: during the handshake, while it has data or a FIN that is unacknowledged or
    // not yet sent, including data a zero window holds back, and while it sends keep-alive
    // probes that have no limit of their own. An idle connection is not.
    fn awaits_remote(&self) -> bool {
        match self.state {
            State::Closed | State::Listen | State::TimeWait => false,
            State::SynSent
            | State::SynReceived
            | State::FinWait1
            | State::Closing
            | State::LastAck => true,
            State::Established | State::FinWait2 | State::CloseWait => {
                !self.tx_buffer.is_empty()
                    || matches!(self.keep_alive, Some(KeepAlive { probes: None, .. }))
            }
        }
    }

    // Whether every keep-alive probe allowed has gone unanswered, and the next is due.
    fn keep_alive_exhausted(&self, timestamp: Instant) -> bool {
        matches!(
            self.keep_alive,
            Some(KeepAlive { probes: Some(probes), .. }) if self.keep_alive_probes >= probes
        ) && self.timer.should_keep_alive(timestamp)
    }

    fn seq_to_transmit(&self, cx: &mut Context) -> bool {
        // Fast retransmits should always send, even if later congestion checks would disallow
        if self.pending_fast_retransmit && !self.tx_buffer.is_empty() {
            return true;
        }

        // So should a loss probe.
        if self.rack.probe_pending && self.loss_probe_allowed() {
            return true;
        }

        let ip_header_len = match self.tuple.unwrap().local.addr {
            #[cfg(feature = "proto-ipv4")]
            IpAddress::Ipv4(_) => crate::wire::IPV4_HEADER_LEN,
            #[cfg(feature = "proto-ipv6")]
            IpAddress::Ipv6(_) => crate::wire::IPV6_HEADER_LEN,
        };

        // The effective max segment size, taking into account the options and the local and remote limits.
        let options_len = if self.tsval_generator.is_some() {
            12
        } else {
            0
        };

        let local_mss = self.send_ip_mtu(cx) - ip_header_len - TCP_HEADER_LEN;
        let effective_mss = local_mss.min(self.remote_mss).saturating_sub(options_len);

        // Have we sent data that hasn't been ACKed yet?
        let data_in_flight = self.remote_last_seq != self.local_seq_no;

        // If we want to send a SYN and we haven't done so, do it!
        if matches!(self.state, State::SynSent | State::SynReceived) && !data_in_flight {
            return true;
        }

        // During SACK recovery, is there a hole to resend?
        if self
            .sack_retransmission(effective_mss, self.no_new_data())
            .is_some()
        {
            return true;
        }

        // max sequence number we can send.
        let max_send_seq =
            self.local_seq_no + core::cmp::min(self.remote_win_len, self.tx_buffer.len());

        // Max amount of octets we can send.
        let capped_send_seq = if max_send_seq >= self.remote_last_seq {
            max_send_seq - self.remote_last_seq
        } else {
            0
        };

        // compare max bytes allowed by cwnd with max bytes allowed by remote
        let max_send = capped_send_seq.min(self.cwnd_remaining());

        // Can we send at least 1 octet?
        let mut can_send = max_send != 0;
        // Can we send at least 1 full segment?
        let can_send_full = max_send >= effective_mss;

        // Do we have to send a FIN?
        let want_fin = match self.state {
            State::FinWait1 => true,
            State::Closing => true,
            State::LastAck => true,
            _ => false,
        };

        // If we're applying the Nagle algorithm, in Minshall's variant as Linux
        // does, we don't want to send a segment shorter than a full MSS until one
        // of:
        // * There's no such segment in flight
        // * We can send a full packet
        // * We have all the data we'll ever send (we're closing send)
        //
        // Holding back every partial segment while any data at all is in flight,
        // as RFC 896 does, would hold the tail of every write longer than an MSS
        // for a round trip, plus the peer's delayed ACK.
        let small_segment_in_flight = self
            .nagle_small_segment_end
            .is_some_and(|end| end > self.local_seq_no);
        if self.nagle && data_in_flight && small_segment_in_flight && !can_send_full && !want_fin {
            can_send = false;
        }

        // Can we actually send the FIN? We can send it if:
        // 1. We have unsent data that fits in the remote window.
        // 2. We have no unsent data.
        // This condition matches only if #2, because #1 is already covered by can_data and we're ORing them.
        let can_fin = want_fin && self.remote_last_seq == self.local_seq_no + self.tx_buffer.len();

        can_send || can_fin
    }

    fn delayed_ack_expired(&self, timestamp: Instant) -> bool {
        match self.ack_delay_timer {
            AckDelayTimer::Idle => true,
            AckDelayTimer::Waiting(t) => t <= timestamp,
            AckDelayTimer::Immediate => true,
        }
    }

    fn ack_to_transmit(&self) -> bool {
        if let Some(remote_last_ack) = self.remote_last_ack {
            remote_last_ack < self.remote_seq_no + self.rx_buffer.len()
        } else {
            false
        }
    }

    /// Return whether to send ACK immediately due to the amount of unacknowledged data.
    ///
    /// RFC 9293 states "An ACK SHOULD be generated for at least every second full-sized segment or
    /// 2*RMSS bytes of new data (where RMSS is the MSS specified by the TCP endpoint receiving the
    /// segments to be acknowledged, or the default value if not specified) (SHLD-19)."
    ///
    /// Note that the RFC above only says "at least 2*RMSS bytes", which is not a hard requirement.
    /// In practice, we follow the Linux kernel's empirical value of sending an ACK for every RMSS
    /// byte of new data. For details, see
    /// <https://elixir.bootlin.com/linux/v6.11.4/source/net/ipv4/tcp_input.c#L5747>.
    fn immediate_ack_to_transmit(&self) -> bool {
        if let Some(remote_last_ack) = self.remote_last_ack {
            remote_last_ack + self.remote_mss < self.remote_seq_no + self.rx_buffer.len()
        } else {
            false
        }
    }

    /// Return whether we should send ACK immediately due to significant window updates.
    ///
    /// ACKs with significant window updates should be sent immediately to let the sender know that
    /// more data can be sent. According to the Linux kernel implementation, "significant" means
    /// doubling the receive window. The Linux kernel implementation can be found at
    /// <https://elixir.bootlin.com/linux/v6.9.9/source/net/ipv4/tcp.c#L1472>.
    fn window_to_update(&self) -> bool {
        match self.state {
            State::SynSent
            | State::SynReceived
            | State::Established
            | State::FinWait1
            | State::FinWait2 => {
                let new_win = self.scaled_window();
                if let Some(last_win) = self.last_scaled_window() {
                    new_win > 0 && new_win / 2 >= last_win
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub(crate) fn dispatch<F, E>(&mut self, cx: &mut Context, emit: F) -> Result<(), E>
    where
        F: FnOnce(&mut Context, PacketMeta, (IpRepr, TcpRepr)) -> Result<(), E>,
    {
        if self.tuple.is_none() {
            return Ok(());
        }

        // NOTE(unwrap): we check tuple is not None above.
        let tuple = self.tuple.unwrap();

        // Check if the interface still has our source IP address.
        // If not (e.g. the interface's IP changed), reset the socket.
        // We use reset() instead of set_state(Closed) to avoid sending
        // an RST packet with the now-invalid source IP.
        if !cx.has_ip_addr(tuple.local.addr) {
            net_debug!("source IP address no longer available, closing socket");
            self.reset();
            return Ok(());
        }

        if self.remote_last_ts.is_none() {
            // We get here in exactly two cases:
            //  1) This socket just transitioned into SYN-SENT.
            //  2) This socket had an empty transmit buffer and some data was added there.
            // Both are similar in that the socket has been quiet for an indefinite
            // period of time, it isn't anymore, and the local endpoint is talking.
            // So, we start counting the timeout not from the last received packet
            // but from the first transmitted one.
            self.remote_last_ts = Some(cx.now());
        }

        self.congestion_controller
            .inner_mut()
            .pre_transmit(cx.now());

        // RACK's reordering timer and the loss probe timer, unless the
        // retransmission timer has expired, which supersedes both.
        if !self.timer.should_retransmit(cx.now()) {
            self.poll_rack_timers(cx.now());
        }

        // Check if any state needs to be changed because of a timer.
        if self.timed_out(cx.now()) || self.keep_alive_exhausted(cx.now()) {
            // If a timeout expires, or no keep-alive probe was answered, we should abort the
            // connection.
            net_debug!("timeout exceeded");
            self.timeout_abort = true;
            self.set_state(State::Closed);
        } else if self.timer.should_retransmit(cx.now()) {
            if let Timer::Retransmit { .. } = self.timer {
                // If a retransmit timer expired, we should resend data starting at the last ACK.
                net_debug!("retransmitting after rto");

                // Inform the congestion controller that we're retransmitting and should enter the slow start state
                let in_flight = self.flight_size();
                self.congestion_controller
                    .inner_mut()
                    .on_rto(cx.now(), in_flight);

                // Rewind "last sequence number sent", as if we never
                // had sent them. This will cause all data in the queue
                // to be sent again.
                self.remote_last_seq = self.local_seq_no;

                // Resending from the last ACK supersedes fast recovery. It resends
                // SACKed data too, so the scoreboard starts afresh (RFC 6675 5.1).
                self.recover = None;
                self.recovery_rxt_end = None;
                self.scoreboard.clear();
                self.rack.on_rewind();

                // Inform RTTE, so that it can can handle RTO backoff
                self.rtte.on_rto();
            } else {
                // If a fast rentrasmit timer expired, we should resend only the earliest unAcked segment
                net_debug!("retransmitting for fast-retransmit");

                // Inform the congestion controller that we're doing a fast retransmit and should enter the fast recovery state
                let in_flight = self.flight_size();
                self.congestion_controller
                    .inner_mut()
                    .on_loss(cx.now(), in_flight);

                self.pending_fast_retransmit = true;
            }

            // Clear the `should_retransmit` state. If we can't retransmit right
            // now for whatever reason (like zero window), this avoids an
            // infinite polling loop where `poll_at` returns `Now` but `dispatch`
            // can't actually do anything.
            self.timer.set_for_idle(cx.now(), self.keep_alive);

            // Inform RTTE, so that it can avoid bogus measurements.
            self.rtte.on_retransmit();
        }

        #[cfg(feature = "socket-tcp-pause-synack")]
        if matches!(self.state, State::SynReceived) && self.synack_paused {
            return Ok(());
        }

        // Decide whether we're sending a packet.
        if self.seq_to_transmit(cx) {
            // If we have data to transmit and it fits into partner's window, do it.
            tcp_trace!("outgoing segment will send data or flags");
        } else if self.ack_to_transmit() && self.delayed_ack_expired(cx.now()) {
            // If we have data to acknowledge, do it.
            tcp_trace!("outgoing segment will acknowledge");
        } else if self.window_to_update() {
            // If we have window length increase to advertise, do it.
            tcp_trace!("outgoing segment will update window");
        } else if self.state == State::Closed {
            // If we need to abort the connection, do it.
            tcp_trace!("outgoing segment will abort connection");
        } else if self.timer.should_keep_alive(cx.now()) {
            // If we need to transmit a keep-alive packet, do it.
            tcp_trace!("keep-alive timer expired");
        } else if self.timer.should_zero_window_probe(cx.now()) {
            tcp_trace!("sending zero-window probe");
        } else if self.timer.should_close(cx.now()) {
            // If we have spent enough time in the TIME-WAIT state, close the socket.
            tcp_trace!("TIME-WAIT timer expired");
            self.reset();
            return Ok(());
        } else {
            return Ok(());
        }

        // Construct the lowered IP representation.
        // We might need this to calculate the MSS, so do it early.
        let mut ip_repr = IpRepr::new(
            tuple.local.addr,
            tuple.remote.addr,
            IpProtocol::Tcp,
            0,
            self.hop_limit.unwrap_or(64),
        );

        // Construct the basic TCP representation, an empty ACK packet.
        // We'll adjust this to be more specific as needed.
        let mut repr = TcpRepr {
            src_port: tuple.local.port,
            dst_port: tuple.remote.port,
            control: TcpControl::None,
            seq_number: self.remote_last_seq,
            ack_number: Some(self.remote_seq_no + self.rx_buffer.len()),
            window_len: self.scaled_window(),
            window_scale: None,
            max_seg_size: None,
            sack_permitted: false,
            sack_ranges: [None, None, None],
            timestamp: TcpTimestampRepr::generate_reply_with_tsval(
                self.tsval_generator,
                self.last_remote_tsval,
            ),
            payload: &[],
        };

        let mut is_zero_window_probe = false;
        let mut is_fast_retransmit = false;
        let mut is_sack_retransmission = false;
        let mut is_loss_probe = false;
        let mut is_probe_retransmission = false;
        let mut is_small_segment = false;

        #[cfg_attr(
            not(feature = "segmentation-offload"),
            expect(
                unused_mut,
                reason = "The default is not mutated if the segmentation offload feature is not enabled."
            )
        )]
        let mut packet_meta = PacketMeta::default();

        match self.state {
            // We transmit an RST in the CLOSED state. If we ended up in the CLOSED state
            // with a specified endpoint, it means that the socket was aborted.
            State::Closed => {
                repr.control = TcpControl::Rst;
            }

            // We never transmit anything in the LISTEN state.
            State::Listen => return Ok(()),

            // We transmit a SYN in the SYN-SENT state.
            // We transmit a SYN|ACK in the SYN-RECEIVED state.
            State::SynSent | State::SynReceived => {
                repr.control = TcpControl::Syn;
                repr.seq_number = self.local_seq_no;
                // window len must NOT be scaled in SYNs.
                repr.window_len = u16::try_from(self.rx_buffer.window()).unwrap_or(u16::MAX);
                if self.state == State::SynSent {
                    repr.ack_number = None;
                    repr.window_scale = Some(self.remote_win_shift);
                    repr.sack_permitted = true;
                } else {
                    repr.sack_permitted = self.remote_has_sack;
                    repr.window_scale = self.remote_win_scale.map(|_| self.remote_win_shift);
                }
            }

            // We transmit data in all states where we may have data in the buffer,
            // or the transmit half of the connection is still open.
            State::Established
            | State::FinWait1
            | State::Closing
            | State::CloseWait
            | State::LastAck => {
                // Extract as much data as the remote side can receive in this packet
                // from the transmit buffer.

                // Maximum size we're allowed to send. This can be limited by 4 factors:
                // 1. remote window
                // 2. MSS the remote is willing to accept, probably determined by their MTU
                // 3. MSS we can send, determined by our MTU.
                // 4. Our congestion window
                let options_len = repr.header_len() - TCP_HEADER_LEN;
                let local_mss = self.send_ip_mtu(cx) - ip_repr.header_len() - TCP_HEADER_LEN;
                let effective_mss = local_mss.min(self.remote_mss).saturating_sub(options_len);

                let sack_retransmission = if self.pending_fast_retransmit {
                    None
                } else {
                    self.sack_retransmission(effective_mss, self.no_new_data())
                };

                let offset = if self.pending_fast_retransmit {
                    // Stop short of any SACKed data that follows the segment.
                    let hole = self
                        .scoreboard
                        .next_hole(self.local_seq_no)
                        .filter(|&(start, _)| start == self.local_seq_no)
                        .map_or(usize::MAX, |(start, end)| end - start);
                    let size = effective_mss.min(self.tx_buffer.len()).min(hole);
                    repr.seq_number = self.local_seq_no;
                    repr.payload = self.tx_buffer.get_allocated(0, size);

                    // The resend stays pending until `emit` accepts it, so a device
                    // that refuses it now is asked again on the next dispatch.
                    is_fast_retransmit = true;

                    0
                } else if self.rack.probe_pending {
                    // RFC 8985 7.3: a loss probe is one new segment, if the
                    // remote window allows one, and otherwise a resend of the
                    // last segment sent. The congestion window does not hold
                    // it back.
                    let sent = self.flight_size().min(self.tx_buffer.len());
                    let unsent = self
                        .tx_buffer
                        .len()
                        .min(self.remote_win_len)
                        .saturating_sub(self.flight_size());
                    let (offset, size) = if unsent > 0 {
                        (sent, unsent.min(effective_mss))
                    } else {
                        is_probe_retransmission = true;
                        let size = effective_mss.min(sent);
                        (sent - size, size)
                    };
                    repr.seq_number = self.local_seq_no + offset;
                    repr.payload = self.tx_buffer.get_allocated(offset, size);
                    is_loss_probe = true;

                    offset
                } else if let Some((seq, size)) = sack_retransmission {
                    let offset = seq - self.local_seq_no;
                    repr.seq_number = seq;
                    repr.payload = self.tx_buffer.get_allocated(offset, size);
                    is_sack_retransmission = true;

                    offset
                } else {
                    // Right edge of window, ie the max sequence number we're allowed to send.
                    let win_right_edge = self.local_seq_no + self.remote_win_len;

                    // Max amount of octets we're allowed to send according to the remote window.
                    let mut win_limit = if win_right_edge >= self.remote_last_seq {
                        win_right_edge - self.remote_last_seq
                    } else {
                        // This can happen if we've sent some data and later the remote side
                        // has shrunk its window so that data is no longer inside the window.
                        // This should be very rare and is strongly discouraged by the RFCs,
                        // but it does happen in practice.
                        // http://www.tcpipguide.com/free/t_TCPWindowManagementIssues.htm
                        0
                    };

                    // To send a zero-window-probe, force the window limit to at least 1 byte.
                    if win_limit == 0 && self.timer.should_zero_window_probe(cx.now()) {
                        win_limit = 1;
                        is_zero_window_probe = true;
                    }

                    // Maximum size we're allowed to send. This can be limited by 4 factors:
                    // 1. remote window
                    // 2. congestion window
                    // 3. MSS the remote is willing to accept, probably determined by their MTU
                    // 4. MSS we can send, determined by our MTU.
                    //
                    // If the device supports its offload, segmentation that is needed
                    // to comply with the latter two will be handled by the device based on the
                    // metadata we provide.

                    #[cfg(not(feature = "segmentation-offload"))]
                    let device_limit = effective_mss;

                    #[cfg(feature = "segmentation-offload")]
                    let device_limit = {
                        let segmentation_caps = cx.segmentation_caps();
                        match ip_repr.version() {
                            #[cfg(feature = "proto-ipv4")]
                            crate::wire::IpVersion::Ipv4 => segmentation_caps.tcpv4,
                            #[cfg(feature = "proto-ipv6")]
                            crate::wire::IpVersion::Ipv6 => segmentation_caps.tcpv6,
                        }
                        .map(|buf_size| {
                            let pre_ip_header_len = cx.max_transmission_unit() - cx.ip_mtu();
                            buf_size.get()
                                - pre_ip_header_len
                                - ip_repr.header_len()
                                - TCP_HEADER_LEN
                                - options_len
                        })
                        .unwrap_or(effective_mss)
                    };

                    let size = if is_zero_window_probe {
                        // Zero-window probes are exempt from the congestion window: they
                        // are sent precisely when normal transmission is impossible, and
                        // an empty segment elicits no reply, so capping the probe to a
                        // zero length would stall the connection if a window update from
                        // the remote got lost.
                        win_limit.min(device_limit)
                    } else {
                        win_limit.min(device_limit).min(self.cwnd_remaining())
                    };

                    let offset = self.flight_size();
                    repr.payload = self.tx_buffer.get_allocated(offset, size);

                    #[cfg(feature = "segmentation-offload")]
                    if repr.payload.len() > effective_mss {
                        packet_meta.segmentation_offload_size =
                            core::num::NonZeroU16::try_from(u16::try_from(effective_mss).unwrap())
                                .unwrap()
                                .into();
                    }

                    offset
                };

                is_small_segment = !repr.payload.is_empty() && repr.payload.len() < effective_mss;

                // If we've sent everything we had in the buffer, follow it with the PSH or FIN
                // flags, depending on whether the transmit half of the connection is open.
                if offset + repr.payload.len() == self.tx_buffer.len() {
                    match self.state {
                        State::FinWait1 | State::LastAck | State::Closing => {
                            repr.control = TcpControl::Fin
                        }
                        State::Established | State::CloseWait if !repr.payload.is_empty() => {
                            repr.control = TcpControl::Psh
                        }
                        _ => (),
                    }
                }
            }

            // In FIN-WAIT-2 and TIME-WAIT states we may only transmit ACKs for incoming data or FIN
            State::FinWait2 | State::TimeWait => {}
        }

        // There might be more than one reason to send a packet. E.g. the keep-alive timer
        // has expired, and we also have data in transmit buffer. Since any packet that occupies
        // sequence space will elicit an ACK, we only need to send an explicit packet if we
        // couldn't fill the sequence space with anything.
        let is_keep_alive;
        if self.timer.should_keep_alive(cx.now()) && repr.is_empty() {
            repr.seq_number = repr.seq_number - 1;
            repr.payload = b"\x00"; // RFC 1122 says we should do this
            is_keep_alive = true;
        } else {
            is_keep_alive = false;
        }

        // Trace a summary of what will be sent.
        if is_keep_alive {
            tcp_trace!("sending a keep-alive");
        } else if !repr.payload.is_empty() {
            tcp_trace!(
                "tx buffer: sending {} octets at offset {}",
                repr.payload.len(),
                self.flight_size()
            );
        }
        if repr.control != TcpControl::None || repr.payload.is_empty() {
            let flags = match (repr.control, repr.ack_number) {
                (TcpControl::Syn, None) => "SYN",
                (TcpControl::Syn, Some(_)) => "SYN|ACK",
                (TcpControl::Fin, Some(_)) => "FIN|ACK",
                (TcpControl::Rst, Some(_)) => "RST|ACK",
                (TcpControl::Psh, Some(_)) => "PSH|ACK",
                (TcpControl::None, Some(_)) => "ACK",
                _ => "<unreachable>",
            };
            tcp_trace!("sending {}", flags);
        }

        if repr.control == TcpControl::Syn {
            // Fill the MSS option. See RFC 6691 for an explanation of this calculation.
            let max_segment_size = cx.ip_mtu() - ip_repr.header_len() - TCP_HEADER_LEN;
            repr.max_seg_size = Some(max_segment_size as u16);
        }

        // Actually send the packet. If this succeeds, it means the packet is in
        // the device buffer, and its transmission is imminent. If not, we might have
        // a number of problems, e.g. we need neighbor discovery.
        //
        // Bailing out if the packet isn't placed in the device buffer allows us
        // to not waste time waiting for the retransmit timer on packets that we know
        // for sure will not be successfully transmitted.
        ip_repr.set_payload_len(repr.buffer_len());
        emit(cx, packet_meta, (ip_repr, repr))?;

        if is_fast_retransmit {
            self.pending_fast_retransmit = false;
        }
        if (is_fast_retransmit || is_sack_retransmission) && self.recover.is_some() {
            let end = repr.seq_number + repr.payload.len();
            self.recovery_rxt_end = Some(self.recovery_rxt_end.map_or(end, |last| last.max(end)));
        }
        if is_sack_retransmission || is_probe_retransmission {
            self.rtte.on_retransmit();
        }

        // A keep-alive packet waits `interval` for an answer before the next. Other packets
        // leave the timer alone: it counts from the last packet received, as on Linux, and
        // the next probe is `idle` after that.
        if is_keep_alive {
            self.keep_alive_probes = self.keep_alive_probes.saturating_add(1);
            self.timer.rewind_keep_alive(cx.now(), self.keep_alive);
        }

        // Reset delayed-ack timer
        match self.ack_delay_timer {
            AckDelayTimer::Idle => {}
            AckDelayTimer::Waiting(_) => {
                tcp_trace!("stop delayed ack timer")
            }
            AckDelayTimer::Immediate => {
                tcp_trace!("stop delayed ack timer (was force-expired)")
            }
        }
        self.ack_delay_timer = AckDelayTimer::Idle;

        // Leave the rest of the state intact if sending a zero-window probe.
        if is_zero_window_probe {
            self.timer.rewind_zero_window_probe(cx.now());
            return Ok(());
        }

        // Leave the rest of the state intact if sending a keep-alive packet, since those
        // carry a fake segment.
        if is_keep_alive {
            return Ok(());
        }

        // We've sent a packet successfully, so we can update the internal state now.
        // Use max() so a fast-retransmit segment (whose seq_number is local_seq_no, well
        // behind the current frontier) doesn't rewind the tracked "highest sent" sequence.
        self.remote_last_seq = self
            .remote_last_seq
            .max(repr.seq_number + repr.segment_len());
        if repr.segment_len() > 0 && repr.control != TcpControl::Syn {
            let end = repr.seq_number + repr.segment_len();
            self.rack
                .log
                .record(self.local_seq_no, repr.seq_number, end, cx.now());
        }

        if is_small_segment {
            let end = repr.seq_number + repr.segment_len();
            self.nagle_small_segment_end = Some(
                self.nagle_small_segment_end
                    .map_or(end, |last| last.max(end)),
            );
        }
        self.remote_last_ack = repr.ack_number;
        self.remote_last_win = repr.window_len;

        if repr.segment_len() > 0 {
            self.rtte
                .on_send(cx.now(), repr.seq_number + repr.segment_len());
            self.congestion_controller
                .inner_mut()
                .post_transmit(cx.now(), repr.segment_len());
        }

        if repr.segment_len() > 0 && !self.timer.is_retransmit() {
            // RFC 6298 (5.1) Every time a packet containing data is sent (including a
            // retransmission), if the timer is not running, start it running
            // so that it will expire after RTO seconds.
            let rto = self.rtte.retransmission_timeout();
            self.timer.set_for_retransmit(cx.now(), rto);
        }

        if is_loss_probe {
            // RFC 8985 7.3: the probe's episode lasts until what is in flight
            // now is acknowledged, and the retransmission timer restarts to
            // guard it.
            self.rack.probe_pending = false;
            self.rack.probe = Some(Probe {
                end: self.remote_last_seq,
                retransmitted: is_probe_retransmission,
                dsacked: false,
            });
            let rto = self.rtte.retransmission_timeout();
            self.timer.set_for_retransmit(cx.now(), rto);
        } else if repr.segment_len() > 0 && !is_fast_retransmit && !is_sack_retransmission {
            // RFC 8985 7.2: sending new data restarts the loss probe timer.
            self.arm_loss_probe(cx.now());
        }

        if self.state == State::Closed {
            // When aborting a connection, forget about it after sending a single RST packet.
            self.tuple = None;
            #[cfg(feature = "async")]
            {
                // Wake tx now so that async users can wait for the RST to be sent
                self.tx_waker.wake();
            }
        }

        Ok(())
    }

    #[allow(clippy::if_same_then_else)]
    pub(crate) fn poll_at(&self, cx: &mut Context) -> PollAt {
        // The logic here mirrors the beginning of dispatch() closely.
        if self.tuple.is_none() {
            // No one to talk to, nothing to transmit.
            PollAt::Ingress
        } else if self.remote_last_ts.is_none() {
            // Socket stopped being quiet recently, we need to acquire a timestamp.
            PollAt::Now
        } else if self.state == State::Closed {
            // Socket was aborted, we have an RST packet to transmit.
            PollAt::Now
        } else if self.seq_to_transmit(cx) {
            // We have a data or flag packet to transmit.
            PollAt::Now
        } else if self.window_to_update() {
            // The receive window has been raised significantly.
            PollAt::Now
        } else {
            let want_ack = self.ack_to_transmit();

            let delayed_ack_poll_at = match (want_ack, self.ack_delay_timer) {
                (false, _) => PollAt::Ingress,
                (true, AckDelayTimer::Idle) => PollAt::Now,
                (true, AckDelayTimer::Waiting(t)) => PollAt::Time(t),
                (true, AckDelayTimer::Immediate) => PollAt::Now,
            };

            let timeout_poll_at = match (self.remote_last_ts, self.timeout) {
                // If we're transmitting or retransmitting data, we need to poll at the moment
                // when the timeout would expire.
                (Some(remote_last_ts), Some(timeout)) if self.awaits_remote() => {
                    PollAt::Time(remote_last_ts + timeout)
                }
                // Otherwise we have no timeout.
                (_, _) => PollAt::Ingress,
            };

            let rack_poll_at = [self.rack.reo_timeout, self.rack.probe_at]
                .into_iter()
                .flatten()
                .min()
                .map_or(PollAt::Ingress, PollAt::Time);

            // We wait for the earliest of our timers to fire.
            *[
                self.timer.poll_at(),
                timeout_poll_at,
                delayed_ack_poll_at,
                rack_poll_at,
            ]
            .iter()
            .min()
            .unwrap_or(&PollAt::Ingress)
        }
    }
}

impl<'a> fmt::Write for Socket<'a> {
    fn write_str(&mut self, slice: &str) -> fmt::Result {
        let slice = slice.as_bytes();
        if self.send_slice(slice) == Ok(slice.len()) {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

// TODO: TCP should work for all features. For now, we only test with the IP feature. We could do
// it for other features as well with rstest, however, this means we have to modify a lot of the
// tests in here, which I didn't had the time for at the moment.
#[cfg(all(test, feature = "medium-ip"))]
mod test {
    use super::*;
    use crate::config::IFACE_MAX_ADDR_COUNT;
    use crate::wire::{IpCidr, IpRepr};
    use std::ops::{Deref, DerefMut};
    use std::vec::Vec;

    // =========================================================================================//
    // Constants
    // =========================================================================================//

    const LOCAL_PORT: u16 = 80;
    const REMOTE_PORT: u16 = 49500;
    const LISTEN_END: IpListenEndpoint = IpListenEndpoint {
        addr: None,
        port: LOCAL_PORT,
    };
    const TUPLE: Tuple = Tuple {
        local: LOCAL_END,
        remote: REMOTE_END,
    };
    const LOCAL_SEQ: TcpSeqNumber = TcpSeqNumber(10000);
    const REMOTE_SEQ: TcpSeqNumber = TcpSeqNumber(-10001);

    cfg_if::cfg_if! {
        if #[cfg(feature = "proto-ipv4")] {
            use crate::wire::Ipv4Address as IpvXAddress;
            use crate::wire::Ipv4Repr as IpvXRepr;
            use IpRepr::Ipv4 as IpReprIpvX;

            const LOCAL_ADDR: IpvXAddress = IpvXAddress::new(192, 168, 1, 1);
            const REMOTE_ADDR: IpvXAddress = IpvXAddress::new(192, 168, 1, 2);
            const OTHER_ADDR: IpvXAddress = IpvXAddress::new(192, 168, 1, 3);

            const BASE_MSS: u16 = 1460;

            const LOCAL_END: IpEndpoint = IpEndpoint {
                addr: IpAddress::Ipv4(LOCAL_ADDR),
                port: LOCAL_PORT,
            };
            const REMOTE_END: IpEndpoint = IpEndpoint {
                addr: IpAddress::Ipv4(REMOTE_ADDR),
                port: REMOTE_PORT,
            };
        } else {
            use crate::wire::Ipv6Address as IpvXAddress;
            use crate::wire::Ipv6Repr as IpvXRepr;
            use IpRepr::Ipv6 as IpReprIpvX;

            const LOCAL_ADDR: IpvXAddress = IpvXAddress::new(0xfe80, 0, 0, 0, 0, 0, 0, 1);
            const REMOTE_ADDR: IpvXAddress = IpvXAddress::new(0xfe80, 0, 0, 0, 0, 0, 0, 2);
            const OTHER_ADDR: IpvXAddress = IpvXAddress::new(0xfe80, 0, 0, 0, 0, 0, 0, 3);

            const BASE_MSS: u16 = 1440;

            const LOCAL_END: IpEndpoint = IpEndpoint {
                addr: IpAddress::Ipv6(LOCAL_ADDR),
                port: LOCAL_PORT,
            };
            const REMOTE_END: IpEndpoint = IpEndpoint {
                addr: IpAddress::Ipv6(REMOTE_ADDR),
                port: REMOTE_PORT,
            };
        }
    }

    const SEND_IP_TEMPL: IpRepr = IpReprIpvX(IpvXRepr {
        src_addr: LOCAL_ADDR,
        dst_addr: REMOTE_ADDR,
        next_header: IpProtocol::Tcp,
        payload_len: 20,
        hop_limit: 64,
    });
    const SEND_TEMPL: TcpRepr<'static> = TcpRepr {
        src_port: REMOTE_PORT,
        dst_port: LOCAL_PORT,
        control: TcpControl::None,
        seq_number: TcpSeqNumber(0),
        ack_number: Some(TcpSeqNumber(0)),
        window_len: 256,
        window_scale: None,
        max_seg_size: None,
        sack_permitted: false,
        sack_ranges: [None, None, None],
        timestamp: None,
        payload: &[],
    };
    const _RECV_IP_TEMPL: IpRepr = IpReprIpvX(IpvXRepr {
        src_addr: LOCAL_ADDR,
        dst_addr: REMOTE_ADDR,
        next_header: IpProtocol::Tcp,
        payload_len: 20,
        hop_limit: 64,
    });
    const RECV_TEMPL: TcpRepr<'static> = TcpRepr {
        src_port: LOCAL_PORT,
        dst_port: REMOTE_PORT,
        control: TcpControl::None,
        seq_number: TcpSeqNumber(0),
        ack_number: Some(TcpSeqNumber(0)),
        window_len: 64,
        window_scale: None,
        max_seg_size: None,
        sack_permitted: false,
        sack_ranges: [None, None, None],
        timestamp: None,
        payload: &[],
    };

    // =========================================================================================//
    // Helper functions
    // =========================================================================================//

    struct TestSocket {
        socket: Socket<'static>,
        cx: Context,
    }

    impl Deref for TestSocket {
        type Target = Socket<'static>;
        fn deref(&self) -> &Self::Target {
            &self.socket
        }
    }

    impl DerefMut for TestSocket {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.socket
        }
    }

    #[track_caller]
    fn send(
        socket: &mut TestSocket,
        timestamp: Instant,
        repr: &TcpRepr,
    ) -> Option<TcpRepr<'static>> {
        socket.cx.set_now(timestamp);

        let ip_repr = IpReprIpvX(IpvXRepr {
            src_addr: REMOTE_ADDR,
            dst_addr: LOCAL_ADDR,
            next_header: IpProtocol::Tcp,
            payload_len: repr.buffer_len(),
            hop_limit: 64,
        });
        net_trace!("send: {}", repr);

        assert!(socket.socket.accepts(&mut socket.cx, &ip_repr, repr));

        match socket.socket.process(&mut socket.cx, &ip_repr, repr) {
            Some((_ip_repr, repr)) => {
                net_trace!("recv: {}", repr);
                Some(repr)
            }
            None => None,
        }
    }

    #[track_caller]
    fn recv<F>(socket: &mut TestSocket, timestamp: Instant, mut f: F)
    where
        F: FnMut(Result<TcpRepr, ()>),
    {
        socket.cx.set_now(timestamp);

        let mut sent = 0;
        let result = socket
            .socket
            .dispatch(&mut socket.cx, |_, _, (ip_repr, tcp_repr)| {
                assert_eq!(ip_repr.next_header(), IpProtocol::Tcp);
                assert_eq!(ip_repr.src_addr(), LOCAL_ADDR.into());
                assert_eq!(ip_repr.dst_addr(), REMOTE_ADDR.into());
                assert_eq!(ip_repr.payload_len(), tcp_repr.buffer_len());

                net_trace!("recv: {}", tcp_repr);
                sent += 1;
                Ok(f(Ok(tcp_repr)))
            });
        match result {
            Ok(()) => assert_eq!(sent, 1, "Exactly one packet should be sent"),
            Err(e) => f(Err(e)),
        }
    }

    #[track_caller]
    fn recv_nothing(socket: &mut TestSocket, timestamp: Instant) {
        socket.cx.set_now(timestamp);

        let mut fail = false;
        let result: Result<(), ()> = socket.socket.dispatch(&mut socket.cx, |_, _, _| {
            fail = true;
            Ok(())
        });
        if fail {
            panic!("Should not send a packet")
        }

        assert_eq!(result, Ok(()))
    }

    #[collapse_debuginfo(yes)]
    macro_rules! send {
        ($socket:ident, $repr:expr) =>
            (send!($socket, time 0, $repr));
        ($socket:ident, $repr:expr, $result:expr) =>
            (send!($socket, time 0, $repr, $result));
        ($socket:ident, time $time:expr, $repr:expr) =>
            (send!($socket, time $time, $repr, None));
        ($socket:ident, time $time:expr, $repr:expr, $result:expr) =>
            (assert_eq!(send(&mut $socket, Instant::from_millis($time), &$repr), $result));
    }

    #[collapse_debuginfo(yes)]
    macro_rules! recv {
        ($socket:ident, [$( $repr:expr ),*]) => ({
            $( recv!($socket, Ok($repr)); )*
            recv_nothing!($socket)
        });
        ($socket:ident, time $time:expr, [$( $repr:expr ),*]) => ({
            $( recv!($socket, time $time, Ok($repr)); )*
            recv_nothing!($socket, time $time)
        });
        ($socket:ident, $result:expr) =>
            (recv!($socket, time 0, $result));
        ($socket:ident, time $time:expr, $result:expr) =>
            (recv(&mut $socket, Instant::from_millis($time), |result| {
                // Most of the time we don't care about the PSH flag.
                let result = result.map(|mut repr| {
                    repr.control = repr.control.quash_psh();
                    repr
                });
                assert_eq!(result, $result)
            }));
        ($socket:ident, time $time:expr, $result:expr, exact) =>
            (recv(&mut $socket, Instant::from_millis($time), |repr| assert_eq!(repr, $result)));
    }

    #[collapse_debuginfo(yes)]
    macro_rules! recv_nothing {
        ($socket:ident) => (recv_nothing!($socket, time 0));
        ($socket:ident, time $time:expr) => (recv_nothing(&mut $socket, Instant::from_millis($time)));
    }

    #[collapse_debuginfo(yes)]
    macro_rules! sanity {
        ($socket1:expr, $socket2:expr) => {{
            let (s1, s2) = ($socket1, $socket2);
            assert_eq!(s1.state, s2.state, "state");
            assert_eq!(s1.tuple, s2.tuple, "tuple");
            assert_eq!(s1.local_seq_no, s2.local_seq_no, "local_seq_no");
            assert_eq!(s1.remote_seq_no, s2.remote_seq_no, "remote_seq_no");
            assert_eq!(s1.remote_last_seq, s2.remote_last_seq, "remote_last_seq");
            assert_eq!(s1.remote_last_ack, s2.remote_last_ack, "remote_last_ack");
            assert_eq!(s1.remote_last_win, s2.remote_last_win, "remote_last_win");
            assert_eq!(s1.remote_win_len, s2.remote_win_len, "remote_win_len");
            assert_eq!(s1.timer, s2.timer, "timer");
        }};
    }

    fn socket() -> TestSocket {
        socket_with_buffer_sizes(64, 64)
    }

    fn socket_with_buffer_sizes(tx_len: usize, rx_len: usize) -> TestSocket {
        let (iface, _, _) = crate::tests::setup(crate::phy::Medium::Ip);

        let rx_buffer = SocketBuffer::new(vec![0; rx_len]);
        let tx_buffer = SocketBuffer::new(vec![0; tx_len]);
        let mut socket = Socket::new(rx_buffer, tx_buffer);
        socket.set_ack_delay(None);
        TestSocket {
            socket,
            cx: iface.inner,
        }
    }

    fn socket_syn_received_with_buffer_sizes(tx_len: usize, rx_len: usize) -> TestSocket {
        let mut s = socket_with_buffer_sizes(tx_len, rx_len);
        s.state = State::SynReceived;
        s.tuple = Some(TUPLE);
        s.local_seq_no = LOCAL_SEQ;
        s.remote_seq_no = REMOTE_SEQ + 1;
        s.remote_last_seq = LOCAL_SEQ;
        s.remote_win_len = 256;
        s
    }

    fn socket_syn_received() -> TestSocket {
        socket_syn_received_with_buffer_sizes(64, 64)
    }

    fn socket_syn_sent_with_buffer_sizes(tx_len: usize, rx_len: usize) -> TestSocket {
        let mut s = socket_with_buffer_sizes(tx_len, rx_len);
        s.state = State::SynSent;
        s.tuple = Some(TUPLE);
        s.local_seq_no = LOCAL_SEQ;
        s.remote_last_seq = LOCAL_SEQ;
        s
    }

    fn socket_syn_sent() -> TestSocket {
        socket_syn_sent_with_buffer_sizes(64, 64)
    }

    fn socket_established_with_buffer_sizes(tx_len: usize, rx_len: usize) -> TestSocket {
        let mut s = socket_syn_received_with_buffer_sizes(tx_len, rx_len);
        s.state = State::Established;
        s.local_seq_no = LOCAL_SEQ + 1;
        s.remote_last_seq = LOCAL_SEQ + 1;
        s.remote_last_ack = Some(REMOTE_SEQ + 1);
        s.remote_last_win = s.scaled_window();
        s
    }

    fn socket_established() -> TestSocket {
        socket_established_with_buffer_sizes(64, 64)
    }

    fn socket_fin_wait_1() -> TestSocket {
        let mut s = socket_established();
        s.state = State::FinWait1;
        s
    }

    fn socket_fin_wait_2() -> TestSocket {
        let mut s = socket_fin_wait_1();
        s.state = State::FinWait2;
        s.local_seq_no = LOCAL_SEQ + 1 + 1;
        s.remote_last_seq = LOCAL_SEQ + 1 + 1;
        s
    }

    fn socket_closing() -> TestSocket {
        let mut s = socket_fin_wait_1();
        s.state = State::Closing;
        s.remote_last_seq = LOCAL_SEQ + 1 + 1;
        s.remote_seq_no = REMOTE_SEQ + 1 + 1;
        s.timer = Timer::Retransmit {
            expires_at: Instant::from_millis_const(1000),
        };
        s
    }

    fn socket_time_wait(from_closing: bool) -> TestSocket {
        let mut s = socket_fin_wait_2();
        s.state = State::TimeWait;
        s.remote_seq_no = REMOTE_SEQ + 1 + 1;
        if from_closing {
            s.remote_last_ack = Some(REMOTE_SEQ + 1 + 1);
        }
        s.timer = Timer::Close {
            expires_at: Instant::from_secs(1) + CLOSE_DELAY,
        };
        s
    }

    fn socket_close_wait() -> TestSocket {
        let mut s = socket_established();
        s.state = State::CloseWait;
        s.remote_seq_no = REMOTE_SEQ + 1 + 1;
        s.remote_last_ack = Some(REMOTE_SEQ + 1 + 1);
        s
    }

    fn socket_last_ack() -> TestSocket {
        let mut s = socket_close_wait();
        s.state = State::LastAck;
        s
    }

    fn socket_recved() -> TestSocket {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 58,
                ..RECV_TEMPL
            }]
        );
        s
    }

    // =========================================================================================//
    // Tests for the CLOSED state.
    // =========================================================================================//
    #[test]
    fn test_closed_reject() {
        let mut s = socket();
        assert_eq!(s.state, State::Closed);

        let tcp_repr = TcpRepr {
            control: TcpControl::Syn,
            ..SEND_TEMPL
        };
        assert!(!s.socket.accepts(&mut s.cx, &SEND_IP_TEMPL, &tcp_repr));
    }

    #[test]
    fn test_closed_reject_after_listen() {
        let mut s = socket();
        s.listen(LOCAL_END).unwrap();
        s.close();

        let tcp_repr = TcpRepr {
            control: TcpControl::Syn,
            ..SEND_TEMPL
        };
        assert!(!s.socket.accepts(&mut s.cx, &SEND_IP_TEMPL, &tcp_repr));
    }

    #[test]
    fn test_closed_close() {
        let mut s = socket();
        s.close();
        assert_eq!(s.state, State::Closed);
    }

    // =========================================================================================//
    // Tests for the LISTEN state.
    // =========================================================================================//
    fn socket_listen() -> TestSocket {
        let mut s = socket();
        s.state = State::Listen;
        s.listen_endpoint = LISTEN_END;
        s
    }

    #[test]
    fn test_listen_sack_option() {
        let mut s = socket_listen();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                sack_permitted: false,
                ..SEND_TEMPL
            }
        );
        assert!(!s.remote_has_sack);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );

        let mut s = socket_listen();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                sack_permitted: true,
                ..SEND_TEMPL
            }
        );
        assert!(s.remote_has_sack);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_listen_syn_win_scale_buffers() {
        for (buffer_size, shift_amt) in &[
            (64, 0),
            (128, 0),
            (1024, 0),
            (65535, 0),
            (65536, 1),
            (65537, 1),
            (131071, 1),
            (131072, 2),
            (524287, 3),
            (524288, 4),
            (655350, 4),
            (1048576, 5),
        ] {
            let mut s = socket_with_buffer_sizes(64, *buffer_size);
            s.state = State::Listen;
            s.listen_endpoint = LISTEN_END;
            assert_eq!(s.remote_win_shift, *shift_amt);
            send!(
                s,
                TcpRepr {
                    control: TcpControl::Syn,
                    seq_number: REMOTE_SEQ,
                    ack_number: None,
                    window_scale: Some(0),
                    ..SEND_TEMPL
                }
            );
            assert_eq!(s.remote_win_shift, *shift_amt);
            recv!(
                s,
                [TcpRepr {
                    control: TcpControl::Syn,
                    seq_number: LOCAL_SEQ,
                    ack_number: Some(REMOTE_SEQ + 1),
                    max_seg_size: Some(BASE_MSS),
                    window_scale: Some(*shift_amt),
                    window_len: u16::try_from(*buffer_size).unwrap_or(u16::MAX),
                    ..RECV_TEMPL
                }]
            );
        }
    }

    #[test]
    fn test_listen_syn_tiny_mss_is_clamped() {
        let mut s = socket_listen();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                max_seg_size: Some(10),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::SynReceived);
        assert_eq!(s.remote_mss, MIN_REMOTE_MSS);
    }

    #[test]
    fn test_listen_syn_zero_mss_is_ignored() {
        let mut s = socket_listen();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                max_seg_size: Some(0),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::SynReceived);
        assert_eq!(s.remote_mss, DEFAULT_MSS);
    }

    #[test]
    fn test_listen_sanity() {
        let mut s = socket();
        s.listen(LOCAL_PORT).unwrap();
        sanity!(s, socket_listen());
    }

    #[test]
    fn test_listen_validation() {
        let mut s = socket();
        assert_eq!(s.listen(0), Err(ListenError::Unaddressable));
    }

    #[test]
    fn test_listen_twice() {
        let mut s = socket();
        assert_eq!(s.listen(80), Ok(()));
        // multiple calls to listen are okay if its the same local endpoint and the state is still in listening
        assert_eq!(s.listen(80), Ok(()));
        s.set_state(State::SynReceived); // state change, simulate incoming connection
        assert_eq!(s.listen(80), Err(ListenError::InvalidState));
    }

    #[test]
    fn test_listen_syn() {
        let mut s = socket_listen();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                ..SEND_TEMPL
            }
        );
        sanity!(s, socket_syn_received());
    }

    #[test]
    fn test_listen_syn_reject_ack() {
        let mut s = socket_listen();

        let tcp_repr = TcpRepr {
            control: TcpControl::Syn,
            seq_number: REMOTE_SEQ,
            ack_number: Some(LOCAL_SEQ),
            ..SEND_TEMPL
        };
        assert!(!s.socket.accepts(&mut s.cx, &SEND_IP_TEMPL, &tcp_repr));

        assert_eq!(s.state, State::Listen);
    }

    #[test]
    fn test_listen_rst() {
        let mut s = socket_listen();
        let tcp_repr = TcpRepr {
            control: TcpControl::Rst,
            seq_number: REMOTE_SEQ,
            ack_number: None,
            ..SEND_TEMPL
        };
        assert!(!s.socket.accepts(&mut s.cx, &SEND_IP_TEMPL, &tcp_repr));
        assert_eq!(s.state, State::Listen);
    }

    #[test]
    fn test_listen_close() {
        let mut s = socket_listen();
        s.close();
        assert_eq!(s.state, State::Closed);
    }

    // =========================================================================================//
    // Tests for the SYN-RECEIVED state.
    // =========================================================================================//

    #[test]
    fn test_syn_received_ack() {
        let mut s = socket_syn_received();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
        sanity!(s, socket_established());
    }

    #[cfg(feature = "socket-tcp-pause-synack")]
    #[test]
    fn test_syn_paused_ack() {
        let mut s = socket_syn_received();

        s.pause_synack(true);
        recv_nothing!(s);
        assert_eq!(s.state, State::SynReceived);

        s.pause_synack(false);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
    }

    #[test]
    fn test_syn_received_ack_too_low() {
        let mut s = socket_syn_received();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ), // wrong
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                control: TcpControl::Rst,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                window_len: 0,
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.state, State::SynReceived);
    }

    #[test]
    fn test_syn_received_ack_too_high() {
        let mut s = socket_syn_received();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 2), // wrong
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                control: TcpControl::Rst,
                seq_number: LOCAL_SEQ + 2,
                ack_number: None,
                window_len: 0,
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.state, State::SynReceived);
    }

    #[test]
    fn test_syn_received_fin() {
        let mut s = socket_syn_received();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6 + 1),
                window_len: 58,
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::CloseWait);

        let mut s2 = socket_close_wait();
        s2.remote_last_ack = Some(REMOTE_SEQ + 1 + 6 + 1);
        s2.remote_last_win = 58;
        sanity!(s, s2);
    }

    #[test]
    fn test_syn_received_rst() {
        let mut s = socket_syn_received();
        s.listen_endpoint = LISTEN_END;
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Listen);
        assert_eq!(s.listen_endpoint, LISTEN_END);
        assert_eq!(s.tuple, None);
    }

    #[test]
    fn test_syn_received_no_window_scaling() {
        let mut s = socket_listen();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::SynReceived);
        assert_eq!(s.tuple, Some(TUPLE));
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                window_scale: None,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_scale: None,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.remote_win_shift, 0);
        assert_eq!(s.remote_win_scale, None);
    }

    #[test]
    fn test_syn_received_window_scaling() {
        for scale in 0..14 {
            let mut s = socket_listen();
            send!(
                s,
                TcpRepr {
                    control: TcpControl::Syn,
                    seq_number: REMOTE_SEQ,
                    ack_number: None,
                    window_scale: Some(scale),
                    ..SEND_TEMPL
                }
            );
            assert_eq!(s.state(), State::SynReceived);
            assert_eq!(s.tuple, Some(TUPLE));
            recv!(
                s,
                [TcpRepr {
                    control: TcpControl::Syn,
                    seq_number: LOCAL_SEQ,
                    ack_number: Some(REMOTE_SEQ + 1),
                    max_seg_size: Some(BASE_MSS),
                    window_scale: Some(0),
                    ..RECV_TEMPL
                }]
            );
            send!(
                s,
                TcpRepr {
                    seq_number: REMOTE_SEQ + 1,
                    ack_number: Some(LOCAL_SEQ + 1),
                    window_scale: None,
                    ..SEND_TEMPL
                }
            );
            assert_eq!(s.remote_win_scale, Some(scale));
        }
    }

    #[test]
    fn test_syn_received_close() {
        let mut s = socket_syn_received();
        s.close();
        assert_eq!(s.state, State::FinWait1);
    }

    // =========================================================================================//
    // Tests for the SYN-SENT state.
    // =========================================================================================//

    #[test]
    fn test_connect_validation() {
        let mut s = socket();
        assert_eq!(
            s.socket
                .connect(&mut s.cx, REMOTE_END, (IpvXAddress::UNSPECIFIED, 0)),
            Err(ConnectError::Unaddressable)
        );
        assert_eq!(
            s.socket
                .connect(&mut s.cx, REMOTE_END, (IpvXAddress::UNSPECIFIED, 1024)),
            Err(ConnectError::Unaddressable)
        );
        assert_eq!(
            s.socket
                .connect(&mut s.cx, (IpvXAddress::UNSPECIFIED, 0), LOCAL_END),
            Err(ConnectError::Unaddressable)
        );
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END)
            .expect("Connect failed with valid parameters");
        assert_eq!(s.tuple, Some(TUPLE));
    }

    #[test]
    fn test_connect() {
        let mut s = socket();
        s.local_seq_no = LOCAL_SEQ;
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END.port)
            .unwrap();
        assert_eq!(s.tuple, Some(TUPLE));
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.tuple, Some(TUPLE));
    }

    #[test]
    fn test_connect_synack_tiny_mss_is_clamped() {
        let mut s = socket();
        s.local_seq_no = LOCAL_SEQ;
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END.port)
            .unwrap();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(10),
                window_scale: Some(0),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
        assert_eq!(s.remote_mss, MIN_REMOTE_MSS);
    }

    #[test]
    fn test_connect_synack_zero_mss_is_ignored() {
        let mut s = socket();
        s.local_seq_no = LOCAL_SEQ;
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END.port)
            .unwrap();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(0),
                window_scale: Some(0),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
        assert_eq!(s.remote_mss, DEFAULT_MSS);
    }

    #[test]
    fn test_connect_unspecified_local() {
        let mut s = socket();
        assert_eq!(s.socket.connect(&mut s.cx, REMOTE_END, 80), Ok(()));
    }

    #[test]
    fn test_connect_specified_local() {
        let mut s = socket();
        assert_eq!(
            s.socket.connect(&mut s.cx, REMOTE_END, (REMOTE_ADDR, 80)),
            Ok(())
        );
    }

    #[test]
    fn test_connect_twice() {
        let mut s = socket();
        assert_eq!(s.socket.connect(&mut s.cx, REMOTE_END, 80), Ok(()));
        assert_eq!(
            s.socket.connect(&mut s.cx, REMOTE_END, 80),
            Err(ConnectError::InvalidState)
        );
    }

    #[test]
    fn test_syn_sent_sanity() {
        let mut s = socket();
        s.local_seq_no = LOCAL_SEQ;
        s.socket.connect(&mut s.cx, REMOTE_END, LOCAL_END).unwrap();
        sanity!(s, socket_syn_sent());
    }

    #[test]
    fn test_syn_sent_syn_ack() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
        recv_nothing!(s, time 1000);
        assert_eq!(s.state, State::Established);
        sanity!(s, socket_established());
    }

    #[test]
    fn test_syn_sent_syn_received_ack() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );

        // A SYN packet changes the SYN-SENT state to SYN-RECEIVED.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::SynReceived);

        // The socket will then send a SYN|ACK packet.
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                ..RECV_TEMPL
            }]
        );
        recv_nothing!(s);

        // The socket may retransmit the SYN|ACK packet.
        recv!(
            s,
            time 1001,
            Ok(TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                ..RECV_TEMPL
            })
        );

        // An ACK packet changes the SYN-RECEIVED state to ESTABLISHED.
        send!(
            s,
            TcpRepr {
                control: TcpControl::None,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
        sanity!(s, socket_established());
    }

    #[test]
    fn test_syn_sent_syn_ack_not_incremented() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ), // WRONG
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                control: TcpControl::Rst,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                window_len: 0,
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.state, State::SynSent);
    }

    #[test]
    fn test_syn_sent_syn_received_rst() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );

        // A SYN packet changes the SYN-SENT state to SYN-RECEIVED.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::SynReceived);

        // A RST packet changes the SYN-RECEIVED state to CLOSED.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_syn_sent_rst() {
        let mut s = socket_syn_sent();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_syn_sent_rst_no_ack() {
        let mut s = socket_syn_sent();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::SynSent);
    }

    #[test]
    fn test_syn_sent_rst_bad_ack() {
        let mut s = socket_syn_sent();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ,
                ack_number: Some(TcpSeqNumber(1234)),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::SynSent);
    }

    #[test]
    fn test_syn_sent_bad_ack() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::None, // Unexpected
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1), // Correct
                ..SEND_TEMPL
            }
        );

        // It should trigger no response and change no state
        recv!(s, []);
        assert_eq!(s.state, State::SynSent);
    }

    #[test]
    fn test_syn_sent_bad_ack_seq_1() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::None,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ), // WRONG
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                control: TcpControl::Rst,
                seq_number: LOCAL_SEQ, // matching the ack_number of the unexpected ack
                ack_number: None,
                window_len: 0,
                ..RECV_TEMPL
            })
        );

        // It should trigger a RST, and change no state
        assert_eq!(s.state, State::SynSent);
    }

    #[test]
    fn test_syn_sent_bad_ack_seq_2() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::None,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 123456), // WRONG
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                control: TcpControl::Rst,
                seq_number: LOCAL_SEQ + 123456, // matching the ack_number of the unexpected ack
                ack_number: None,
                window_len: 0,
                ..RECV_TEMPL
            })
        );

        // It should trigger a RST, and change no state
        assert_eq!(s.state, State::SynSent);
    }

    #[test]
    fn test_syn_sent_close() {
        let mut s = socket();
        s.close();
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_syn_sent_sack_option() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                sack_permitted: true,
                ..SEND_TEMPL
            }
        );
        assert!(s.remote_has_sack);

        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                sack_permitted: false,
                ..SEND_TEMPL
            }
        );
        assert!(!s.remote_has_sack);
    }

    #[test]
    fn test_syn_sent_win_scale_buffers() {
        for (buffer_size, shift_amt) in &[
            (64, 0),
            (128, 0),
            (1024, 0),
            (65535, 0),
            (65536, 1),
            (65537, 1),
            (131071, 1),
            (131072, 2),
            (524287, 3),
            (524288, 4),
            (655350, 4),
            (1048576, 5),
        ] {
            let mut s = socket_with_buffer_sizes(64, *buffer_size);
            s.local_seq_no = LOCAL_SEQ;
            assert_eq!(s.remote_win_shift, *shift_amt);
            s.socket.connect(&mut s.cx, REMOTE_END, LOCAL_END).unwrap();
            recv!(
                s,
                [TcpRepr {
                    control: TcpControl::Syn,
                    seq_number: LOCAL_SEQ,
                    ack_number: None,
                    max_seg_size: Some(BASE_MSS),
                    window_scale: Some(*shift_amt),
                    window_len: u16::try_from(*buffer_size).unwrap_or(u16::MAX),
                    sack_permitted: true,
                    ..RECV_TEMPL
                }]
            );
        }
    }

    #[test]
    fn test_syn_sent_syn_ack_no_window_scaling() {
        let mut s = socket_syn_sent_with_buffer_sizes(1048576, 1048576);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                // scaling does NOT apply to the window value in SYN packets
                window_len: 65535,
                window_scale: Some(5),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.remote_win_shift, 5);
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: None,
                window_len: 42,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
        assert_eq!(s.remote_win_shift, 0);
        assert_eq!(s.remote_win_scale, None);
        assert_eq!(s.remote_win_len, 42);
    }

    #[test]
    fn test_syn_sent_syn_ack_window_scaling() {
        let mut s = socket_syn_sent();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(7),
                window_len: 42,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
        assert_eq!(s.remote_win_scale, Some(7));
        // scaling does NOT apply to the window value in SYN packets
        assert_eq!(s.remote_win_len, 42);
    }

    // =========================================================================================//
    // Tests for the ESTABLISHED state.
    // =========================================================================================//

    #[test]
    fn test_established_recv() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 58,
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.rx_buffer.dequeue_many(6), &b"abcdef"[..]);
    }

    #[test]
    fn test_peek_slice() {
        const BUF_SIZE: usize = 10;

        let send_buf = b"0123456";

        let mut s = socket_established_with_buffer_sizes(BUF_SIZE, BUF_SIZE);

        // Populate the recv buffer
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &send_buf[..],
                ..SEND_TEMPL
            }
        );

        // Peek into the recv buffer
        let mut peeked_buf = [0u8; BUF_SIZE];
        let actually_peeked = s.peek_slice(&mut peeked_buf[..]).unwrap();
        let mut recv_buf = [0u8; BUF_SIZE];
        let actually_recvd = s.recv_slice(&mut recv_buf[..]).unwrap();
        assert_eq!(
            &mut peeked_buf[..actually_peeked],
            &mut recv_buf[..actually_recvd]
        );
    }

    #[test]
    fn test_peek_slice_buffer_wrap() {
        const BUF_SIZE: usize = 10;

        let send_buf = b"0123456789";

        let mut s = socket_established_with_buffer_sizes(BUF_SIZE, BUF_SIZE);

        let _ = s.rx_buffer.enqueue_slice(&send_buf[..8]);
        let _ = s.rx_buffer.dequeue_many(6);
        let _ = s.rx_buffer.enqueue_slice(&send_buf[..5]);

        let mut peeked_buf = [0u8; BUF_SIZE];
        let actually_peeked = s.peek_slice(&mut peeked_buf[..]).unwrap();
        let mut recv_buf = [0u8; BUF_SIZE];
        let actually_recvd = s.recv_slice(&mut recv_buf[..]).unwrap();
        assert_eq!(
            &mut peeked_buf[..actually_peeked],
            &mut recv_buf[..actually_recvd]
        );
    }

    fn setup_rfc2018_cases() -> (TestSocket, Vec<u8>) {
        // This is a utility function used by the tests for RFC 2018 cases. It configures a socket
        // in a particular way suitable for those cases.
        //
        // RFC 2018: Assume the left window edge is 5000 and that the data transmitter sends [...]
        // segments, each containing 500 data bytes.
        let mut s = socket_established_with_buffer_sizes(4000, 4000);
        s.remote_has_sack = true;

        // create a segment that is 500 bytes long
        let mut segment: Vec<u8> = Vec::with_capacity(500);

        // move the last ack to 5000 by sending ten of them
        for _ in 0..50 {
            segment.extend_from_slice(b"abcdefghij")
        }
        for offset in (0..5000).step_by(500) {
            send!(
                s,
                TcpRepr {
                    seq_number: REMOTE_SEQ + 1 + offset,
                    ack_number: Some(LOCAL_SEQ + 1),
                    payload: &segment,
                    ..SEND_TEMPL
                }
            );
            recv!(
                s,
                [TcpRepr {
                    seq_number: LOCAL_SEQ + 1,
                    ack_number: Some(REMOTE_SEQ + 1 + offset + 500),
                    window_len: 3500,
                    ..RECV_TEMPL
                }]
            );
            s.recv(|data| {
                assert_eq!(data.len(), 500);
                assert_eq!(data, segment.as_slice());
                (500, ())
            })
            .unwrap();
        }
        assert_eq!(s.remote_last_win, 3500);
        (s, segment)
    }

    #[test]
    fn test_established_rfc2018_cases() {
        // This test case verifies the exact scenarios described on pages 8-9 of RFC 2018. Please
        // ensure its behavior does not deviate from those scenarios.

        let (mut s, segment) = setup_rfc2018_cases();
        // RFC 2018:
        //
        // Case 2: The first segment is dropped but the remaining 7 are received.
        //
        // Upon receiving each of the last seven packets, the data receiver will return a TCP ACK
        // segment that acknowledges sequence number 5000 and contains a SACK option specifying one
        // block of queued data:
        //
        //   Triggering   ACK      Left Edge  Right Edge
        //   Segment
        //
        //   5000         (lost)
        //   5500         5000     5500       6000
        //   6000         5000     5500       6500
        //   6500         5000     5500       7000
        //   7000         5000     5500       7500
        //   7500         5000     5500       8000
        //   8000         5000     5500       8500
        //   8500         5000     5500       9000
        //
        for offset in (500..3500).step_by(500) {
            send!(
                s,
                TcpRepr {
                    seq_number: REMOTE_SEQ + 1 + offset + 5000,
                    ack_number: Some(LOCAL_SEQ + 1),
                    payload: &segment,
                    ..SEND_TEMPL
                },
                Some(TcpRepr {
                    seq_number: LOCAL_SEQ + 1,
                    ack_number: Some(REMOTE_SEQ + 1 + 5000),
                    window_len: 4000,
                    sack_ranges: [
                        Some((
                            REMOTE_SEQ.0 as u32 + 1 + 5500,
                            REMOTE_SEQ.0 as u32 + 1 + 5500 + offset as u32
                        )),
                        None,
                        None
                    ],
                    ..RECV_TEMPL
                })
            );
        }
    }

    #[test]
    fn test_established_sack_no_overflow_on_near_max_seqnumber() {
        let mut s = socket_established();
        s.remote_has_sack = true;
        s.remote_seq_no = TcpSeqNumber(-4);
        s.remote_last_ack = Some(TcpSeqNumber(-4));

        // Send an out-of-order segment 10 bytes past the expected sequence,
        // creating a 10-byte hole at the front of the assembler.
        send!(
            s,
            TcpRepr {
                seq_number: TcpSeqNumber(-4 + 10),
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"AAAAAAAAAA"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(TcpSeqNumber(-4)),
                window_len: 64,
                sack_ranges: [
                    Some(((-4_i32 + 10) as u32, (-4_i32 + 20) as u32,)),
                    None,
                    None,
                ],
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_established_sliding_window_recv() {
        let mut s = socket_established();
        // Update our scaling parameters for a TCP with a scaled buffer.
        assert_eq!(s.rx_buffer.len(), 0);
        s.rx_buffer = SocketBuffer::new(vec![0; 262143]);
        s.assembler = Assembler::new();
        s.remote_win_scale = Some(0);
        s.remote_last_win = 65535;
        s.remote_win_shift = 2;

        // Create a TCP segment that will mostly fill an IP frame.
        let mut segment: Vec<u8> = Vec::with_capacity(1400);
        for _ in 0..100 {
            segment.extend_from_slice(b"abcdefghijklmn")
        }
        assert_eq!(segment.len(), 1400);

        // Send the frame
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &segment,
                ..SEND_TEMPL
            }
        );

        // Ensure that the received window size is shifted right by 2.
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1400),
                window_len: 65185,
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_established_send() {
        let mut s = socket_established();
        // First roundtrip after establishing.
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.tx_buffer.len(), 6);
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.tx_buffer.len(), 0);
        // Second roundtrip.
        s.send_slice(b"foobar").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"foobar"[..],
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 6),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.tx_buffer.len(), 0);
    }

    #[test]
    fn test_established_send_no_ack_send() {
        let mut s = socket_established();
        s.set_nagle_enabled(false);
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
        s.send_slice(b"foobar").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"foobar"[..],
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_established_send_buf_gt_win() {
        let mut data = [0; 32];
        for (i, elem) in data.iter_mut().enumerate() {
            *elem = i as u8
        }

        let mut s = socket_established();
        s.remote_win_len = 16;
        s.send_slice(&data[..]).unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &data[0..16],
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_established_send_window_shrink() {
        let mut s = socket_established();

        // 6 octets fit on the remote side's window, so we send them.
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.tx_buffer.len(), 6);

        println!(
            "local_seq_no={} remote_win_len={} remote_last_seq={}",
            s.local_seq_no, s.remote_win_len, s.remote_last_seq
        );

        // - Peer doesn't ack them yet
        // - Sends data so we need to reply with an ACK
        // - ...AND and sends a window announcement that SHRINKS the window, so data we've
        //   previously sent is now outside the window. Yes, this is allowed by TCP.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 3,
                payload: &b"xyzxyz"[..],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.tx_buffer.len(), 6);

        println!(
            "local_seq_no={} remote_win_len={} remote_last_seq={}",
            s.local_seq_no, s.remote_win_len, s.remote_last_seq
        );

        // More data should not get sent since it doesn't fit in the window
        s.send_slice(b"foobar").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 64 - 6,
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_established_receive_partially_outside_window() {
        let mut s = socket_established();

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );

        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();

        // Peer decides to retransmit (perhaps because the ACK was lost)
        // and also pushed data.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );

        s.recv(|data| {
            assert_eq!(data, b"def");
            (3, ())
        })
        .unwrap();
    }

    #[test]
    fn test_established_receive_partially_outside_window_fin() {
        let mut s = socket_established();

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );

        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();

        // Peer decides to retransmit (perhaps because the ACK was lost)
        // and also pushed data, and sent a FIN.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                control: TcpControl::Fin,
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );

        s.recv(|data| {
            assert_eq!(data, b"def");
            (3, ())
        })
        .unwrap();

        // We should accept the FIN, because even though the last packet was partially
        // outside the receive window, there is no hole after adding its data to the assembler.
        assert_eq!(s.state, State::CloseWait);
    }

    #[test]
    fn test_established_send_wrap() {
        let mut s = socket_established();
        let local_seq_start = TcpSeqNumber(i32::MAX - 1);
        s.local_seq_no = local_seq_start + 1;
        s.remote_last_seq = local_seq_start + 1;
        s.send_slice(b"abc").unwrap();
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: local_seq_start + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abc"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_established_no_ack() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: None,
                ..SEND_TEMPL
            }
        );
    }

    #[test]
    fn test_established_bad_ack() {
        let mut s = socket_established();
        // Already acknowledged data.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(TcpSeqNumber(LOCAL_SEQ.0 - 1)),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.local_seq_no, LOCAL_SEQ + 1);
        // Data not yet transmitted.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 10),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.local_seq_no, LOCAL_SEQ + 1);
    }

    #[test]
    fn test_established_bad_seq() {
        let mut s = socket_established();
        // Data outside of receive window.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 256,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.remote_seq_no, REMOTE_SEQ + 1);

        // Challenge ACKs are rate-limited, we don't get a second one immediately.
        send!(
            s,
            time 100,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 256,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );

        // If we wait a bit, we do get a new one.
        send!(
            s,
            time 2000,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 256,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.remote_seq_no, REMOTE_SEQ + 1);
    }

    #[test]
    fn test_old_data_ack_not_rate_limited() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 58,
                ..RECV_TEMPL
            }]
        );
        s.recv(|data| {
            assert_eq!(data, b"abcdef");
            (6, ())
        })
        .unwrap();
        // The remote retransmits data we already acknowledged, e.g. because
        // the ACK above was lost. Each retransmission must elicit a duplicate
        // ACK, even within the challenge ACK rate limit window: withholding it
        // strands the remote in retransmission backoff.
        send!(
            s,
            time 100,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                ..RECV_TEMPL
            })
        );
        send!(
            s,
            time 200,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_bad_seq_rst_dropped_silently() {
        let mut s = socket_established();
        // Out-of-window RSTs are silently dropped, per RFC 9293 (3.10.7.4)
        // and RFC 5961 (3.2): no challenge ACK, no state change.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1 + 256,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);

        // A payload doesn't make it eligible for the data segment exemption
        // from challenge ACK rate limiting either: still no reply.
        send!(
            s,
            time 100,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1 + 256,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
    }

    #[test]
    fn test_bad_seq_syn_with_data_rate_limited() {
        let mut s = socket_established();
        // An out-of-window SYN carrying data must not be exempt from challenge
        // ACK rate limiting: RFC 5961 (4.2) says challenge ACKs sent in
        // response to SYNs should be throttled.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ + 1 + 256,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );

        // The second one within the rate limit window gets no reply.
        send!(
            s,
            time 100,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ + 1 + 256,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
    }

    #[test]
    fn test_established_options_reduce_payload_when_local_mss_limited() {
        const EFFECTIVE_MSS: usize = 64;

        // construct socket where remote MSS is less than local MSS
        let mut s = socket_established();
        s.set_tsval_generator(Some(|| 1));
        s.remote_mss = EFFECTIVE_MSS;

        // Payload should contain 12 bytes less due to timestamp, and those 12 bytes
        // follow in a segment of their own
        s.send_slice(&[0; EFFECTIVE_MSS]).unwrap();
        recv!(
            s,
            [
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &[0; EFFECTIVE_MSS - 12],
                    timestamp: Some(TcpTimestampRepr::new(1, 0)),
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + (EFFECTIVE_MSS - 12),
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &[0; 12],
                    timestamp: Some(TcpTimestampRepr::new(1, 0)),
                    ..RECV_TEMPL
                }
            ]
        );
    }

    #[test]
    fn test_established_options_reduce_payload_when_remote_mss_limited() {
        const EFFECTIVE_MSS: usize = BASE_MSS as usize;

        // construct socket where remote MSS is more than local MSS
        let mut s = socket_established_with_buffer_sizes(EFFECTIVE_MSS, 64);
        s.set_tsval_generator(Some(|| 1));
        s.remote_mss = 9999;
        s.remote_win_len = 9999;

        // Payload should contain 12 bytes less due to timestamp, and those 12 bytes
        // follow in a segment of their own
        s.send_slice(&[0; EFFECTIVE_MSS]).unwrap();
        recv!(
            s,
            [
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &[0; EFFECTIVE_MSS - 12],
                    timestamp: Some(TcpTimestampRepr::new(1, 0)),
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + (EFFECTIVE_MSS - 12),
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &[0; 12],
                    timestamp: Some(TcpTimestampRepr::new(1, 0)),
                    ..RECV_TEMPL
                }
            ]
        );
    }

    #[test]
    fn test_established_tiny_mss_with_options_makes_progress() {
        // Connect with timestamps enabled to a remote advertising an absurdly
        // small MSS. Without the MIN_SND_MSS clamp, an MSS smaller than the
        // options length would result in an effective MSS of zero, sending
        // empty segments in a loop without ever making progress.
        let mut s = socket();
        s.set_tsval_generator(Some(|| 1));
        s.local_seq_no = LOCAL_SEQ;
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END.port)
            .unwrap();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                timestamp: Some(TcpTimestampRepr::new(1, 0)),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(10),
                window_scale: Some(0),
                timestamp: Some(TcpTimestampRepr::new(500, 1)),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Established);
        assert_eq!(s.remote_mss, MIN_REMOTE_MSS);

        s.send_slice(&[0; 64]).unwrap();
        recv!(
            s,
            [
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &[0; MIN_REMOTE_MSS - 12],
                    timestamp: Some(TcpTimestampRepr::new(1, 500)),
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + (MIN_REMOTE_MSS - 12),
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &[0; 64 - (MIN_REMOTE_MSS - 12)],
                    timestamp: Some(TcpTimestampRepr::new(1, 500)),
                    ..RECV_TEMPL
                }
            ]
        );
    }

    #[test]
    fn test_established_fin() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::CloseWait);
        sanity!(s, socket_close_wait());
    }

    #[test]
    fn test_established_fin_after_missing() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"123456"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.state, State::Established);
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6 + 6),
                window_len: 52,
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.state, State::Established);
    }

    #[test]
    fn test_established_send_fin() {
        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::CloseWait);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_established_rst() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_established_rst_no_ack() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1,
                ack_number: None,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_established_close() {
        let mut s = socket_established();
        s.close();
        assert_eq!(s.state, State::FinWait1);
        sanity!(s, socket_fin_wait_1());
    }

    #[test]
    fn test_established_abort() {
        let mut s = socket_established();
        s.abort();
        assert_eq!(s.state, State::Closed);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Rst,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_established_rst_bad_seq() {
        let mut s = socket_established();
        // Out-of-window RSTs are dropped silently, per RFC 9293 (3.10.7.4)
        // and RFC 5961 (3.2).
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ, // Wrong seq
                ack_number: None,
                ..SEND_TEMPL
            }
        );

        assert_eq!(s.state, State::Established);

        // An in-window RST still resets the connection.
        send!(
            s,
            time 2000,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1, // Correct seq
                ack_number: None,
                ..SEND_TEMPL
            }
        );

        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_established_bad_seq_challenge_ack_updated() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ, // Wrong seq
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );

        assert_eq!(s.state, State::Established);

        // Send something to advance seq by 1
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1, // correct seq
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"a"[..],
                ..SEND_TEMPL
            }
        );

        // Send the wrong seq again, check that the challenge ack is correctly updated
        // The ack number must be updated even if we don't call dispatch on the socket
        // See https://github.com/smoltcp-rs/smoltcp/issues/338
        send!(
            s,
            time 2000,
            TcpRepr {
                seq_number: REMOTE_SEQ, // Wrong seq
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 2), // this has changed
                window_len: 63,
                ..RECV_TEMPL
            })
        );
    }

    // =========================================================================================//
    // Tests for the FIN-WAIT-1 state.
    // =========================================================================================//

    #[test]
    fn test_fin_wait_1_fin_ack() {
        let mut s = socket_fin_wait_1();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::FinWait2);
        sanity!(s, socket_fin_wait_2());
    }

    #[test]
    fn test_fin_wait_1_fin_fin() {
        let mut s = socket_fin_wait_1();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closing);
        sanity!(s, socket_closing());
    }

    #[test]
    fn test_fin_wait_1_fin_with_data_queued() {
        let mut s = socket_established();
        s.remote_win_len = 6;
        s.send_slice(b"abcdef123456").unwrap();
        s.close();
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            })
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::FinWait1);
    }

    #[test]
    fn test_fin_wait_1_recv() {
        let mut s = socket_fin_wait_1();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::FinWait1);
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
    }

    #[test]
    fn test_fin_wait_1_close() {
        let mut s = socket_fin_wait_1();
        s.close();
        assert_eq!(s.state, State::FinWait1);
    }

    // =========================================================================================//
    // Tests for the FIN-WAIT-2 state.
    // =========================================================================================//

    #[test]
    fn test_fin_wait_2_fin() {
        let mut s = socket_fin_wait_2();
        send!(s, time 1_000, TcpRepr {
            control: TcpControl::Fin,
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 1),
            ..SEND_TEMPL
        });
        assert_eq!(s.state, State::TimeWait);
        sanity!(s, socket_time_wait(false));
    }

    #[test]
    fn test_fin_wait_2_recv() {
        let mut s = socket_fin_wait_2();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::FinWait2);
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 3),
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_fin_wait_2_close() {
        let mut s = socket_fin_wait_2();
        s.close();
        assert_eq!(s.state, State::FinWait2);
    }

    // =========================================================================================//
    // Tests for the CLOSING state.
    // =========================================================================================//

    #[test]
    fn test_closing_ack_fin() {
        let mut s = socket_closing();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        send!(s, time 1_000, TcpRepr {
            seq_number: REMOTE_SEQ + 1 + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 1),
            ..SEND_TEMPL
        });
        assert_eq!(s.state, State::TimeWait);
        sanity!(s, socket_time_wait(true));
    }

    #[test]
    fn test_closing_close() {
        let mut s = socket_closing();
        s.close();
        assert_eq!(s.state, State::Closing);
    }

    // =========================================================================================//
    // Tests for the TIME-WAIT state.
    // =========================================================================================//

    #[test]
    fn test_time_wait_from_fin_wait_2_ack() {
        let mut s = socket_time_wait(false);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_time_wait_from_closing_no_ack() {
        let mut s = socket_time_wait(true);
        recv!(s, []);
    }

    #[test]
    fn test_time_wait_close() {
        let mut s = socket_time_wait(false);
        s.close();
        assert_eq!(s.state, State::TimeWait);
    }

    #[test]
    fn test_time_wait_retransmit() {
        let mut s = socket_time_wait(false);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        send!(s, time 5_000, TcpRepr {
            control: TcpControl::Fin,
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 1),
            ..SEND_TEMPL
        }, Some(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 1),
            ..RECV_TEMPL
        }));
        assert_eq!(
            s.timer,
            Timer::Close {
                expires_at: Instant::from_secs(5) + CLOSE_DELAY
            }
        );
    }

    #[test]
    fn test_time_wait_timeout() {
        let mut s = socket_time_wait(false);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::TimeWait);
        recv_nothing!(s, time 60_000);
        assert_eq!(s.state, State::Closed);
    }

    // =========================================================================================//
    // Tests for the CLOSE-WAIT state.
    // =========================================================================================//

    #[test]
    fn test_close_wait_ack() {
        let mut s = socket_close_wait();
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6),
                ..SEND_TEMPL
            }
        );
    }

    #[test]
    fn test_close_wait_close() {
        let mut s = socket_close_wait();
        s.close();
        assert_eq!(s.state, State::LastAck);
        sanity!(s, socket_last_ack());
    }

    // =========================================================================================//
    // Tests for the LAST-ACK state.
    // =========================================================================================//
    #[test]
    fn test_last_ack_fin_ack() {
        let mut s = socket_last_ack();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::LastAck);
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_last_ack_ack_not_of_fin() {
        let mut s = socket_last_ack();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::LastAck);

        // A duplicate ACK (ack_number == SND.UNA, not the FIN ACK) must elicit a
        // challenge ACK per RFC 9293 §3.10.7.4 and must keep the state in LAST-ACK.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.state, State::LastAck);

        // ACK received of fin: socket should change to Closed.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    // RFC 9293 §3.10.7.4: duplicate ACK in LAST-ACK must elicit a challenge ACK,
    // not be silently dropped.
    #[test]
    fn test_last_ack_duplicate_ack_challenge_ack() {
        let mut s = socket_last_ack();
        // Trigger dispatch so our FIN is sent and remote_last_seq advances.
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::LastAck);

        // Remote re-sends an ACK for SND.UNA (not the FIN).  RFC 9293 requires a
        // challenge ACK in response so the remote can learn the current state.
        let challenge = send(
            &mut s,
            Instant::from_millis(0),
            &TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            },
        );
        assert_eq!(
            challenge,
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }),
            "expected challenge ACK in response to duplicate ACK in LAST-ACK"
        );
        // State must remain LAST-ACK: we have not received the FIN ACK.
        assert_eq!(s.state, State::LastAck);

        // A second duplicate in the same second is rate-limited; the FIN ACK
        // must still be correctly accepted regardless.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    // A partial ACK in LAST-ACK (ack_len > 0 but not FIN ACK) advances SND.UNA
    // without a challenge ACK; the FIN will be retransmitted by the timer.
    #[test]
    fn test_last_ack_partial_ack_no_challenge_ack() {
        // Build a LAST-ACK socket that has one byte of data still unacknowledged
        // before the FIN.  We manually wire the state so we can send a partial ACK.
        let mut s = socket_last_ack();
        // Push one byte into the tx buffer to simulate data that preceded the FIN.
        let _ = s.tx_buffer.enqueue_slice(b"x");
        // Mark it as already sent (remote_last_seq is past the data byte and the FIN).
        s.remote_last_seq = LOCAL_SEQ + 1 + 1 + 1; // data(1) + FIN(1)

        // Remote ACKs just the data byte, not the FIN (partial ACK).
        // ack_number = local_seq_no + 1  =>  ack_len = 1, ack_of_fin = false.
        // Per RFC 9293, a valid partial ACK should advance SND.UNA normally;
        // no challenge ACK should be emitted.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1), // acks the data byte, not FIN
                ..SEND_TEMPL
            }
        );
        // State remains LAST-ACK; FIN retransmission is handled by the timer.
        assert_eq!(s.state, State::LastAck);
        // SND.UNA has advanced to the partial ACK number.
        assert_eq!(s.local_seq_no, LOCAL_SEQ + 1 + 1);
    }

    #[test]
    fn test_last_ack_close() {
        let mut s = socket_last_ack();
        s.close();
        assert_eq!(s.state, State::LastAck);
    }

    // =========================================================================================//
    // Tests for transitioning through multiple states.
    // =========================================================================================//

    #[test]
    fn test_listen() {
        let mut s = socket();
        s.listen(LISTEN_END).unwrap();
        assert_eq!(s.state, State::Listen);
    }

    #[test]
    fn test_three_way_handshake() {
        let mut s = socket_listen();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::SynReceived);
        assert_eq!(s.tuple, Some(TUPLE));
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::Established);
        assert_eq!(s.local_seq_no, LOCAL_SEQ + 1);
        assert_eq!(s.remote_seq_no, REMOTE_SEQ + 1);
    }

    #[test]
    fn test_remote_close() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::CloseWait);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        s.close();
        assert_eq!(s.state, State::LastAck);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_local_close() {
        let mut s = socket_established();
        s.close();
        assert_eq!(s.state, State::FinWait1);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::FinWait2);
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::TimeWait);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_simultaneous_close() {
        let mut s = socket_established();
        s.close();
        assert_eq!(s.state, State::FinWait1);
        recv!(
            s,
            [TcpRepr {
                // due to reordering, this is logically located...
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closing);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        // ... at this point
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::TimeWait);
        recv!(s, []);
    }

    #[test]
    fn test_simultaneous_close_combined_fin_ack() {
        let mut s = socket_established();
        s.close();
        assert_eq!(s.state, State::FinWait1);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::TimeWait);
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_simultaneous_close_raced() {
        let mut s = socket_established();
        s.close();
        assert_eq!(s.state, State::FinWait1);

        // Socket receives FIN before it has a chance to send its own FIN
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closing);

        // FIN + ack-of-FIN
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::Closing);

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::TimeWait);
        recv!(s, []);
    }

    #[test]
    fn test_simultaneous_close_raced_with_data() {
        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        s.close();
        assert_eq!(s.state, State::FinWait1);

        // Socket receives FIN before it has a chance to send its own data+FIN
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closing);

        // data + FIN + ack-of-FIN
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::Closing);

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::TimeWait);
        recv!(s, []);
    }

    #[test]
    fn test_fin_with_data() {
        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        s.close();
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        )
    }

    #[test]
    fn test_mutual_close_with_data_1() {
        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        s.close();
        assert_eq!(s.state, State::FinWait1);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 1),
                ..SEND_TEMPL
            }
        );
    }

    #[test]
    fn test_mutual_close_with_data_2() {
        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        s.close();
        assert_eq!(s.state, State::FinWait1);
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::FinWait2);
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 1),
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6 + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 1),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.state, State::TimeWait);
    }

    // =========================================================================================//
    // Tests for retransmission on packet loss.
    // =========================================================================================//

    #[test]
    fn test_duplicate_seq_ack() {
        let mut s = socket_recved();
        // remote retransmission
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 58,
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_data_retransmit() {
        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 1050);
        recv!(s, time 2000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    #[cfg(feature = "socket-tcp-reno")]
    fn test_congestion_window_limits_data_in_flight() {
        let mut s = socket_established_with_buffer_sizes(8192, 64);
        s.set_congestion_control(CongestionControl::Reno);
        s.remote_win_len = 65535;
        s.remote_mss = 1024;

        let data = [b'x'; 8192];
        s.send_slice(&data[..]).unwrap();

        // Reno's initial congestion window is 2048 bytes: only two
        // 1024-byte segments may be in flight, the rest must wait for ACKs.
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 1024,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 0);

        // ACKing one segment frees congestion window space and grows
        // cwnd (slow start), allowing further segments out.
        send!(s, time 10, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 1024),
            window_len: 65535,
            ..SEND_TEMPL
        });
        recv!(s, time 10, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 2048,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));
    }

    #[test]
    #[cfg(feature = "socket-tcp-reno")]
    fn test_congestion_window_doesnt_limit_fast_retransmit() {
        let mut s = socket_established_with_buffer_sizes(8192, 64);
        s.set_congestion_control(CongestionControl::Reno);
        s.remote_win_len = 65535;
        s.remote_mss = 1024;

        // Normal ACK of previously received segment
        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            window_len: 65535,
            ..SEND_TEMPL
        });

        let data = [b'x'; 8192];
        s.send_slice(&data[..]).unwrap();

        // Reno's initial congestion window is 2048 bytes, allowing 2 segments
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));

        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 1024,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 0);

        // Send three duplicate ACKS, treating the first segment as lost
        send!(s, time 10, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            window_len: 65535,
            ..SEND_TEMPL
        });
        send!(s, time 10, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            window_len: 65535,
            ..SEND_TEMPL
        });
        send!(s, time 10, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            window_len: 65535,
            ..SEND_TEMPL
        });

        // A fast retrnasmit should be sent and not be blocked by congestion control
        recv!(s, time 20, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_data_retransmit_bursts() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef012345").unwrap();

        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::None,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }), exact);
        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::Psh,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);
        recv_nothing!(s, time 0);

        recv_nothing!(s, time 50);

        recv!(s, time 1000, Ok(TcpRepr {
            control:    TcpControl::None,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }), exact);
        recv!(s, time 1500, Ok(TcpRepr {
            control:    TcpControl::Psh,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);
        recv_nothing!(s, time 1550);
    }

    #[test]
    fn test_path_mtu_needs_a_connection_with_smaller_segments_to_send() {
        // Not while connecting: there are no segments to size.
        let mut s = socket_syn_sent();
        let outcome = s
            .socket
            .process_path_mtu(&s.cx, LOCAL_END, REMOTE_END, LOCAL_SEQ, 1280);
        assert_eq!(outcome, PathMtuOutcome::Rejected);

        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );

        // Nor for another connection.
        let other = IpEndpoint::new(OTHER_ADDR.into(), REMOTE_PORT);
        let outcome = s
            .socket
            .process_path_mtu(&s.cx, LOCAL_END, other, LOCAL_SEQ + 1, 1280);
        assert_eq!(outcome, PathMtuOutcome::NotMatched);

        // The remote's MSS of 536 already keeps the segments within the path.
        let outcome = s
            .socket
            .process_path_mtu(&s.cx, LOCAL_END, REMOTE_END, LOCAL_SEQ + 1, 1280);
        assert_eq!(outcome, PathMtuOutcome::Unchanged);
        assert_eq!(s.path_mtu, Some(1280));
        recv_nothing!(s);
    }

    #[test]
    fn test_data_retransmit_bursts_half_ack() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef012345").unwrap();

        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::None,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }), exact);
        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::Psh,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);
        // Acknowledge the first packet
        send!(s, time 5, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        // The second packet should be re-sent.
        recv!(s, time 1500, Ok(TcpRepr {
            control:    TcpControl::Psh,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);

        recv_nothing!(s, time 1550);
    }

    #[test]
    fn test_retransmit_timer_restart_on_partial_ack() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef012345").unwrap();

        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::None,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }), exact);
        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::Psh,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);
        // Acknowledge the first packet
        send!(s, time 600, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        // The ACK of the first packet should restart the retransmit timer and delay a retransmission.
        recv_nothing!(s, time 2399);
        // The second packet should be re-sent.
        recv!(s, time 2400, Ok(TcpRepr {
            control:    TcpControl::Psh,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);
    }

    #[test]
    fn test_data_retransmit_bursts_half_ack_close() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef012345").unwrap();
        s.close();

        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::None,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }), exact);
        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);
        // Acknowledge the first packet
        send!(s, time 5, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        // The second packet should be re-sent.
        recv!(s, time 1500, Ok(TcpRepr {
            control:    TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"012345"[..],
            ..RECV_TEMPL
        }), exact);

        recv_nothing!(s, time 1550);
    }

    #[test]
    fn test_send_data_after_syn_ack_retransmit() {
        let mut s = socket_syn_received();
        recv!(s, time 50, Ok(TcpRepr {
            control:    TcpControl::Syn,
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            max_seg_size: Some(BASE_MSS),
            ..RECV_TEMPL
        }));
        recv!(s, time 1050, Ok(TcpRepr { // retransmit
            control:    TcpControl::Syn,
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            max_seg_size: Some(BASE_MSS),
            ..RECV_TEMPL
        }));
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::Established);
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        )
    }

    #[test]
    fn test_established_retransmit_for_dup_ack() {
        let mut s = socket_established();
        // Duplicate ACKs do not replace the retransmission timer
        s.send_slice(b"abc").unwrap();
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abc"[..],
            ..RECV_TEMPL
        }));
        // Retransmit timer is on because all data was sent
        assert_eq!(s.tx_buffer.len(), 3);
        // ACK nothing new
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        // Retransmit
        recv!(s, time 4000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abc"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_established_retransmit_reset_after_ack() {
        let mut s = socket_established();
        s.remote_win_len = 6;
        s.send_slice(b"abcdef").unwrap();
        s.send_slice(b"123456").unwrap();
        s.send_slice(b"ABCDEF").unwrap();
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
        send!(s, time 1005, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        recv!(s, time 1010, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"123456"[..],
            ..RECV_TEMPL
        }));
        send!(s, time 1015, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        recv!(s, time 1020, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"ABCDEF"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_established_queue_during_retransmission() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef123456ABCDEF").unwrap();
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        })); // this one is dropped
        recv!(s, time 1005, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"123456"[..],
            ..RECV_TEMPL
        })); // this one is received
        recv!(s, time 1010, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"ABCDEF"[..],
            ..RECV_TEMPL
        })); // also dropped
        recv!(s, time 3000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        })); // retransmission
        send!(s, time 3005, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6 + 6),
            ..SEND_TEMPL
        }); // acknowledgement of both segments
        recv!(s, time 3010, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"ABCDEF"[..],
            ..RECV_TEMPL
        })); // retransmission of only unacknowledged data
    }

    #[test]
    fn test_close_wait_retransmit_reset_after_ack() {
        let mut s = socket_close_wait();
        s.remote_win_len = 6;
        s.send_slice(b"abcdef").unwrap();
        s.send_slice(b"123456").unwrap();
        s.send_slice(b"ABCDEF").unwrap();
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
        send!(s, time 1005, TcpRepr {
            seq_number: REMOTE_SEQ + 1 + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        recv!(s, time 1010, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1 + 1),
            payload:    &b"123456"[..],
            ..RECV_TEMPL
        }));
        send!(s, time 1015, TcpRepr {
            seq_number: REMOTE_SEQ + 1 + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        recv!(s, time 1020, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6 + 6,
            ack_number: Some(REMOTE_SEQ + 1 + 1),
            payload:    &b"ABCDEF"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_fin_wait_1_retransmit_reset_after_ack() {
        let mut s = socket_established();
        s.remote_win_len = 6;
        s.send_slice(b"abcdef").unwrap();
        s.send_slice(b"123456").unwrap();
        s.send_slice(b"ABCDEF").unwrap();
        s.close();
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
        send!(s, time 1005, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        recv!(s, time 1010, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"123456"[..],
            ..RECV_TEMPL
        }));
        send!(s, time 1015, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + 6 + 6),
            window_len: 6,
            ..SEND_TEMPL
        });
        recv!(s, time 1020, Ok(TcpRepr {
            control:    TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1 + 6 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"ABCDEF"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_fast_retransmit_after_triple_duplicate_ack() {
        let mut s = socket_established();
        s.remote_mss = 3;

        // Normal ACK of previously received segment
        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });

        // Send a long string of text divided into several packets
        // because of previously received "window_len"
        s.send_slice(b"aaaBBBcccDDDeeeFFF").unwrap();

        // This packet is lost
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"aaa"[..],
            ..RECV_TEMPL
        }));

        // These packets arrive
        recv!(s, time 1005, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 3,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"BBB"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 1010, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + (3 * 2),
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"ccc"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 1015, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + (3 * 3),
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"DDD"[..],
            ..RECV_TEMPL
        }));

        // Duplicate ACKs trigger fast rentramsit after 3rd successive one
        send!(s, time 1050, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        send!(s, time 1055, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        send!(s, time 1060, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });

        // Fast retransmit should have triggered
        recv!(s, time 1100, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"aaa"[..],
            ..RECV_TEMPL
        }));

        // Transmission should continue as normal after re-transitting the first segment
        recv!(s, time 1105, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + (3 * 4),
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"eee"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 1110, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + (3 * 5),
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"FFF"[..],
            ..RECV_TEMPL
        }));

        // ACK all received segments
        send!(s, time 1120, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + (3 * 5)),
            ..SEND_TEMPL
        });
    }

    fn ack_repr(ack: usize) -> TcpRepr<'static> {
        TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + ack),
            ..SEND_TEMPL
        }
    }

    fn data_repr(offset: usize, payload: &[u8]) -> TcpRepr<'_> {
        TcpRepr {
            seq_number: LOCAL_SEQ + 1 + offset,
            ack_number: Some(REMOTE_SEQ + 1),
            payload,
            ..RECV_TEMPL
        }
    }

    // Sends five 3-octet segments of which the first two are lost, then fast
    // retransmits the first after three duplicate ACKs. The socket is left in
    // fast recovery with the second hole still outstanding.
    fn socket_fast_recovery_two_holes() -> TestSocket {
        let mut s = socket_established();
        s.remote_mss = 3;
        send!(s, time 0, ack_repr(0));

        s.send_slice(b"aaaBBBcccDDDeee").unwrap();
        for (i, payload) in [b"aaa", b"BBB", b"ccc", b"DDD", b"eee"].iter().enumerate() {
            recv!(s, time 1000, Ok(data_repr(3 * i, &payload[..])));
        }

        // "ccc", "DDD" and "eee" arrive, each producing a duplicate ACK.
        for _ in 0..3 {
            send!(s, time 1050, ack_repr(0));
        }
        recv!(s, time 1050, Ok(data_repr(0, b"aaa")));
        assert_eq!(s.recover, Some(LOCAL_SEQ + 1 + 15));
        s
    }

    #[test]
    fn test_fast_recovery_partial_ack_retransmits_next_hole() {
        let mut s = socket_fast_recovery_two_holes();

        // The retransmitted "aaa" arrives; the ACK advances to the second hole.
        send!(s, time 1060, ack_repr(3));
        recv!(s, time 1060, Ok(data_repr(3, b"BBB")));

        // Filling the second hole acknowledges everything and ends recovery.
        send!(s, time 1070, ack_repr(15));
        assert_eq!(s.recover, None);
        recv_nothing!(s, time 1070);
    }

    #[test]
    fn test_fast_recovery_ignores_duplicate_acks_below_recover() {
        let mut s = socket_fast_recovery_two_holes();

        send!(s, time 1060, ack_repr(3));
        recv!(s, time 1060, Ok(data_repr(3, b"BBB")));

        // Duplicates of the partial ACK must not retransmit "BBB" a second time.
        for _ in 0..3 {
            send!(s, time 1065, ack_repr(3));
        }
        recv_nothing!(s, time 1065);
        assert_eq!(s.recover, Some(LOCAL_SEQ + 1 + 15));
    }

    #[test]
    fn test_fast_recovery_full_ack_cancels_queued_retransmit() {
        let mut s = socket_fast_recovery_two_holes();

        // New data sent during recovery is in flight beyond `recover`.
        s.send_slice(b"fff").unwrap();
        recv!(s, time 1055, Ok(data_repr(15, b"fff")));

        // A partial ACK and the full ACK arrive before the socket next transmits.
        send!(s, time 1060, ack_repr(3));
        send!(s, time 1060, ack_repr(15));
        assert_eq!(s.recover, None);

        // "fff" was never lost, so nothing is resent.
        recv_nothing!(s, time 1060);
    }

    #[test]
    fn test_fast_recovery_abandoned_on_rto() {
        let mut s = socket_fast_recovery_two_holes();

        // The partial ACK's retransmission of "BBB" is lost as well.
        send!(s, time 1060, ack_repr(3));
        recv!(s, time 1060, Ok(data_repr(3, b"BBB")));

        // The retransmit timer resends from the last ACK and ends fast recovery.
        recv!(s, time 2060, Ok(data_repr(3, b"BBB")));
        assert_eq!(s.recover, None);
    }

    #[test]
    fn test_fast_retransmit_after_fast_recovery_completes() {
        let mut s = socket_fast_recovery_two_holes();

        send!(s, time 1060, ack_repr(3));
        recv!(s, time 1060, Ok(data_repr(3, b"BBB")));
        send!(s, time 1070, ack_repr(15));
        assert_eq!(s.recover, None);

        // A later loss enters fast recovery afresh.
        s.send_slice(b"fffGGGhhhIII").unwrap();
        for (i, payload) in [b"fff", b"GGG", b"hhh", b"III"].iter().enumerate() {
            recv!(s, time 1100, Ok(data_repr(15 + 3 * i, &payload[..])));
        }
        for _ in 0..3 {
            send!(s, time 1150, ack_repr(15));
        }
        recv!(s, time 1150, Ok(data_repr(15, b"fff")));
        assert_eq!(s.recover, Some(LOCAL_SEQ + 1 + 27));
    }

    // Dispatches to a device with no room, as when a link's egress credit runs
    // out part-way through a poll.
    #[track_caller]
    fn dispatch_refused(s: &mut TestSocket, time: i64) {
        s.cx.set_now(Instant::from_millis(time));
        let result: Result<(), ()> = s.socket.dispatch(&mut s.cx, |_, _, _| Err(()));
        assert_eq!(result, Err(()));
    }

    #[test]
    fn test_fast_retransmit_survives_refused_emit() {
        let mut s = socket_established();
        s.remote_mss = 3;
        send!(s, time 0, ack_repr(0));

        s.send_slice(b"aaaBBBcccDDD").unwrap();
        for (i, payload) in [b"aaa", b"BBB", b"ccc", b"DDD"].iter().enumerate() {
            recv!(s, time 1000, Ok(data_repr(3 * i, &payload[..])));
        }
        recv_nothing!(s, time 1000);
        for _ in 0..3 {
            send!(s, time 1050, ack_repr(0));
        }

        // The retransmission is refused, but stays due.
        dispatch_refused(&mut s, 1050);
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Now);

        // Once the device has room, the lost segment is resent and the
        // retransmit timer guards it.
        recv!(s, time 1060, Ok(data_repr(0, b"aaa")));
        assert!(s.timer.is_retransmit());
        recv_nothing!(s, time 1060);
    }

    #[test]
    fn test_fast_recovery_partial_ack_survives_refused_emit() {
        let mut s = socket_fast_recovery_two_holes();

        send!(s, time 1060, ack_repr(3));
        dispatch_refused(&mut s, 1060);

        recv!(s, time 1070, Ok(data_repr(3, b"BBB")));
        recv_nothing!(s, time 1070);
    }

    #[test]
    fn test_fast_retransmit_duplicate_detection_with_data() {
        let mut s = socket_established();

        s.send_slice(b"abc").unwrap(); // This is lost
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abc"[..],
            ..RECV_TEMPL
        }));

        // Normal ACK of previously received segment
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        // First duplicate
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        // Second duplicate
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );

        assert_eq!(s.local_rx_dup_acks, 2, "duplicate ACK counter is not set");

        // This packet has content, hence should not be detected
        // as a duplicate ACK and should reset the duplicate ACK count
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"xxxxxx"[..],
                ..SEND_TEMPL
            }
        );

        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 3,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 58,
                ..RECV_TEMPL
            }]
        );

        assert_eq!(
            s.local_rx_dup_acks, 0,
            "duplicate ACK counter is not reset when receiving data"
        );
    }

    #[test]
    fn test_fast_retransmit_duplicate_detection_with_window_update() {
        let mut s = socket_established();

        s.send_slice(b"abc").unwrap(); // This is lost
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abc"[..],
            ..RECV_TEMPL
        }));

        // Normal ACK of previously received segment
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        // First duplicate
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        // Second duplicate
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );

        assert_eq!(s.local_rx_dup_acks, 2, "duplicate ACK counter is not set");

        // This packet has a window update, hence should not be detected
        // as a duplicate ACK and should reset the duplicate ACK count
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 400,
                ..SEND_TEMPL
            }
        );

        assert_eq!(
            s.local_rx_dup_acks, 0,
            "duplicate ACK counter is not reset when receiving a window update"
        );
    }

    // An ACK carrying SACK blocks, as `ack_repr` does, with the given window
    // and blocks given as offsets from the first octet sent.
    fn sack_repr(ack: usize, window_len: u16, blocks: &[(usize, usize)]) -> TcpRepr<'static> {
        let mut sack_ranges = [None; 3];
        for (range, &(left, right)) in sack_ranges.iter_mut().zip(blocks) {
            *range = Some((
                (LOCAL_SEQ + 1 + left).0 as u32,
                (LOCAL_SEQ + 1 + right).0 as u32,
            ));
        }
        TcpRepr {
            window_len,
            sack_ranges,
            ..ack_repr(ack)
        }
    }

    // Sends five 3-octet segments to a SACK-capable peer. The first is lost.
    fn socket_sack_first_segment_lost() -> TestSocket {
        let mut s = socket_established();
        s.remote_has_sack = true;
        s.remote_mss = 3;
        send!(s, time 0, ack_repr(0));

        s.send_slice(b"aaaBBBcccDDDeee").unwrap();
        for (i, payload) in [b"aaa", b"BBB", b"ccc", b"DDD", b"eee"].iter().enumerate() {
            recv!(s, time 1000, Ok(data_repr(3 * i, &payload[..])));
        }
        s
    }

    #[test]
    fn test_fast_retransmit_sack_duplicates_with_growing_window() {
        let mut s = socket_sack_first_segment_lost();

        // A Linux receiver grows its window with each segment, so each of its
        // duplicate ACKs is also a window update. Their SACK blocks report new
        // data each time, which makes them duplicates all the same.
        send!(s, time 1050, sack_repr(0, 300, &[(3, 6)]));
        send!(s, time 1051, sack_repr(0, 310, &[(3, 9)]));
        send!(s, time 1052, sack_repr(0, 320, &[(3, 12)]));
        assert_eq!(s.local_rx_dup_acks, 3);
        recv!(s, time 1053, Ok(data_repr(0, b"aaa")));
    }

    #[test]
    fn test_fast_retransmit_sack_window_update_without_new_data() {
        let mut s = socket_sack_first_segment_lost();

        send!(s, time 1050, sack_repr(0, 300, &[(3, 6)]));
        assert_eq!(s.local_rx_dup_acks, 1);

        // The same SACK block with a new window reports nothing new, so it is a
        // window update, as it would be without SACK.
        send!(s, time 1051, sack_repr(0, 310, &[(3, 6)]));
        assert_eq!(s.local_rx_dup_acks, 0);

        // A D-SACK block, below the ACK, reports nothing above it either.
        send!(s, time 1052, sack_repr(0, 320, &[(0, 0)]));
        assert_eq!(s.local_rx_dup_acks, 0);
        recv_nothing!(s, time 1053);
    }

    #[test]
    fn test_fast_retransmit_sack_below_an_earlier_block_counts() {
        let mut s = socket_sack_first_segment_lost();

        // #115: "DDD" is SACKed first, then "BBB" below it, each ACK with a
        // new window. The second reports new data all the same.
        send!(s, time 1050, sack_repr(0, 300, &[(9, 12)]));
        send!(s, time 1051, sack_repr(0, 310, &[(3, 6), (9, 12)]));
        assert_eq!(s.local_rx_dup_acks, 2);
        send!(s, time 1052, sack_repr(0, 320, &[(3, 6), (9, 15)]));
        assert_eq!(s.local_rx_dup_acks, 3);
        recv!(s, time 1053, Ok(data_repr(0, b"aaa")));
    }

    // Sends eight 3-octet segments to a SACK-capable peer, of which the first,
    // third and fifth are lost, and has the peer SACK the other five.
    fn socket_sack_three_holes() -> TestSocket {
        let mut s = socket_established();
        s.remote_has_sack = true;
        s.remote_mss = 3;
        send!(s, time 0, ack_repr(0));

        let data = b"aaaBBBcccDDDeeeFFFgggHHH";
        s.send_slice(data).unwrap();
        for offset in (0..data.len()).step_by(3) {
            recv!(s, time 1000, Ok(data_repr(offset, &data[offset..offset + 3])));
        }

        send!(s, time 1050, sack_repr(0, 256, &[(3, 6)]));
        send!(s, time 1050, sack_repr(0, 256, &[(9, 12), (3, 6)]));
        send!(s, time 1050, sack_repr(0, 256, &[(15, 18), (9, 12), (3, 6)]));
        send!(s, time 1050, sack_repr(0, 256, &[(15, 21), (9, 12), (3, 6)]));
        send!(s, time 1050, sack_repr(0, 256, &[(15, 24), (9, 12), (3, 6)]));
        assert_eq!(s.recover, Some(LOCAL_SEQ + 1 + 24));
        s
    }

    #[test]
    fn test_sack_recovery_resends_every_lost_hole_at_once() {
        let mut s = socket_sack_three_holes();

        // All three holes are resent at once, and nothing SACKed is.
        recv!(s, time 1050, Ok(data_repr(0, b"aaa")));
        recv!(s, time 1050, Ok(data_repr(6, b"ccc")));
        recv!(s, time 1050, Ok(data_repr(12, b"eee")));
        recv_nothing!(s, time 1050);
        assert_eq!(s.pipe(), 9);

        // Their arrival acknowledges everything and ends recovery, well
        // before the retransmission timer would have fired.
        send!(s, time 1060, ack_repr(24));
        assert_eq!(s.recover, None);
        assert!(s.scoreboard.is_empty());
        recv_nothing!(s, time 1060);
    }

    #[test]
    fn test_sack_recovery_partial_ack_does_not_resend_a_resent_hole() {
        let mut s = socket_sack_three_holes();
        recv!(s, time 1050, Ok(data_repr(0, b"aaa")));
        recv!(s, time 1050, Ok(data_repr(6, b"ccc")));
        recv!(s, time 1050, Ok(data_repr(12, b"eee")));

        // "aaa" arrives, and the ACK moves to "ccc", which is on its way.
        send!(s, time 1060, sack_repr(6, 256, &[(9, 12), (15, 24)]));
        recv_nothing!(s, time 1060);
        assert_eq!(s.recover, Some(LOCAL_SEQ + 1 + 24));
    }

    #[test]
    fn test_sack_recovery_resends_an_unsacked_hole_when_nothing_is_new() {
        let mut s = socket_sack_first_segment_lost();

        // "aaa" and "DDD" are lost. Too little is SACKed above "DDD" to deem
        // it lost, but with no new data left to send it is resent anyway.
        send!(s, time 1050, sack_repr(0, 256, &[(3, 6)]));
        send!(s, time 1050, sack_repr(0, 256, &[(3, 9)]));
        send!(s, time 1050, sack_repr(0, 256, &[(12, 15), (3, 9)]));
        recv!(s, time 1050, Ok(data_repr(0, b"aaa")));
        recv!(s, time 1050, Ok(data_repr(9, b"DDD")));
        recv_nothing!(s, time 1050);
    }

    #[test]
    fn test_sack_fast_retransmit_stops_short_of_sacked_data() {
        let mut s = socket_sack_first_segment_lost();
        send!(s, time 1050, sack_repr(0, 256, &[(3, 6)]));
        send!(s, time 1050, sack_repr(0, 256, &[(3, 9)]));
        send!(s, time 1050, sack_repr(0, 256, &[(3, 12)]));

        // A segment may now be longer than "aaa", but "BBB" after it is SACKed.
        s.remote_mss = 6;
        recv!(s, time 1050, Ok(data_repr(0, b"aaa")));
    }

    #[test]
    #[cfg(feature = "socket-tcp-cubic")]
    fn test_sack_recovery_sends_while_pipe_is_below_the_window() {
        let mut s = socket_established_with_buffer_sizes(4096, 64);
        s.set_congestion_control(CongestionControl::Cubic);
        s.remote_has_sack = true;
        s.remote_mss = 128;
        s.congestion_controller.inner_mut().set_mss(128);
        send!(s, time 0, sack_repr(0, 4000, &[]));

        // Ten segments go out, and the first two are lost.
        let data: Vec<u8> = (0..2560u32).map(|i| (i / 128) as u8).collect();
        s.send_slice(&data[..1280]).unwrap();
        for offset in (0..1280).step_by(128) {
            recv!(s, time 1000, Ok(data_repr(offset, &data[offset..offset + 128])));
        }
        recv_nothing!(s, time 1000);
        for end in (384..=1280).step_by(128) {
            send!(s, time 1050, sack_repr(0, 4000, &[(256, end)]));
        }
        s.send_slice(&data[1280..]).unwrap();

        // CUBIC cuts the window to 0.7 * 1280 + 3 * 128 = 1280 octets. Both
        // holes are lost, and with them resent, eight new segments bring
        // `pipe` up to the window.
        recv!(s, time 1050, Ok(data_repr(0, &data[0..128])));
        recv!(s, time 1050, Ok(data_repr(128, &data[128..256])));
        for offset in (1280..2304).step_by(128) {
            recv!(s, time 1050, Ok(data_repr(offset, &data[offset..offset + 128])));
        }
        recv_nothing!(s, time 1050);
        assert_eq!(s.pipe(), 1280);
    }

    #[test]
    fn test_sack_scoreboard_reset_on_rto() {
        let mut s = socket_sack_three_holes();
        recv!(s, time 1050, Ok(data_repr(0, b"aaa")));
        recv!(s, time 1050, Ok(data_repr(6, b"ccc")));
        recv!(s, time 1050, Ok(data_repr(12, b"eee")));
        assert!(!s.scoreboard.is_empty());

        // The resends are lost as well. The timeout resends everything from
        // the last ACK, SACKed data included, so it forgets the scoreboard.
        recv!(s, time 3050, Ok(data_repr(0, b"aaa")));
        assert!(s.scoreboard.is_empty());
        assert_eq!(s.recovery_rxt_end, None);
        assert_eq!(s.recover, None);
        recv!(s, time 3050, Ok(data_repr(3, b"BBB")));
    }

    // Sends "xxx" and has the SACK-capable peer ACK it 50 ms later, which
    // makes SRTT and RACK's minimum round trip 50 ms, and the RTO 200 ms.
    fn socket_sack_rtt_50() -> TestSocket {
        let mut s = socket_established();
        s.remote_has_sack = true;
        s.remote_mss = 3;
        send!(s, time 0, ack_repr(0));
        s.send_slice(b"xxx").unwrap();
        recv!(s, time 0, Ok(data_repr(0, b"xxx")));
        send!(s, time 50, ack_repr(3));
        assert_eq!(s.rtte.retransmission_timeout(), Duration::from_millis(200));
        s
    }

    #[test]
    fn test_tlp_recovers_a_tail_loss_without_a_timeout() {
        let mut s = socket_sack_rtt_50();

        // Four segments go out at 100, and only the first arrives.
        s.send_slice(b"aaaBBBcccDDD").unwrap();
        for (i, payload) in [b"aaa", b"BBB", b"ccc", b"DDD"].iter().enumerate() {
            recv!(s, time 100, Ok(data_repr(3 + 3 * i, &payload[..])));
        }
        send!(s, time 150, ack_repr(6));

        // Too few segments are left to draw three duplicate ACKs, so before
        // RFC 8985 only the timeout, at 350, would resend them. Twice SRTT,
        // plus 2 ms, after the ACK, a probe resends the last segment.
        recv_nothing!(s, time 251);
        recv!(s, time 252, Ok(data_repr(12, b"DDD")));
        recv_nothing!(s, time 252);

        // Its SACK shows that the two segments sent before it were lost.
        send!(s, time 302, sack_repr(6, 256, &[(12, 15)]));
        recv!(s, time 302, Ok(data_repr(6, b"BBB")));
        recv!(s, time 302, Ok(data_repr(9, b"ccc")));
        recv_nothing!(s, time 302);
        send!(s, time 352, ack_repr(15));
        assert_eq!(s.recover, None);
        recv_nothing!(s, time 1000);
    }

    #[test]
    fn test_tlp_sends_new_data_when_the_window_allows() {
        let mut s = socket_sack_rtt_50();
        s.send_slice(b"aaaBBB").unwrap();
        recv!(s, time 100, Ok(data_repr(3, b"aaa")));
        recv!(s, time 100, Ok(data_repr(6, b"BBB")));

        // Data queued since, which the congestion window would let out
        // anyway, goes first as the probe.
        s.send_slice(b"ccc").unwrap();
        s.rack.probe_at = Some(Instant::from_millis(101));
        recv!(s, time 101, Ok(data_repr(9, b"ccc")));
        assert!(s.rack.probe.is_some_and(|probe| !probe.retransmitted));
        recv_nothing!(s, time 101);
    }

    #[test]
    fn test_rack_resends_a_lost_retransmission_before_a_timeout() {
        let mut s = socket_sack_three_holes();
        recv!(s, time 1050, Ok(data_repr(0, b"aaa")));
        recv!(s, time 1050, Ok(data_repr(6, b"ccc")));
        recv!(s, time 1050, Ok(data_repr(12, b"eee")));
        s.send_slice(b"iiiJJJ").unwrap();
        recv!(s, time 1050, Ok(data_repr(24, b"iii")));
        recv!(s, time 1050, Ok(data_repr(27, b"JJJ")));
        recv_nothing!(s, time 1050);

        // "aaa" and "eee" arrive, and so does the new data sent after them,
        // but the resent "ccc" is lost again. RFC 6675 alone resends a hole
        // once, and waits for the timeout; RACK sees that "ccc" was sent
        // before data now delivered, a round trip ago.
        // The wait is SRTT, 50 ms, plus a quarter of it.
        send!(s, time 1100, sack_repr(6, 256, &[(9, 30)]));
        recv_nothing!(s, time 1112);
        recv!(s, time 1113, Ok(data_repr(6, b"ccc")));
        recv_nothing!(s, time 1113);
        send!(s, time 1150, ack_repr(30));
        assert_eq!(s.recover, None);
    }

    #[test]
    fn test_rack_rtt_sample_in_recovery_resets_the_rto_backoff() {
        let mut s = socket_sack_rtt_50();

        // A lone segment is lost. The timeout resends it, with the RTO
        // doubled, and then data queued since.
        s.send_slice(b"aaa").unwrap();
        recv!(s, time 100, Ok(data_repr(3, b"aaa")));
        recv_nothing!(s, time 299);
        s.send_slice(b"BBBccc").unwrap();
        recv!(s, time 300, Ok(data_repr(3, b"aaa")));
        recv!(s, time 300, Ok(data_repr(6, b"BBB")));
        recv!(s, time 300, Ok(data_repr(9, b"ccc")));
        assert_eq!(s.rtte.retransmission_timeout(), Duration::from_millis(400));

        // The resent "aaa" is lost again, and RACK starts recovery for it.
        // The timer's own round trip sample waits for it to be acknowledged,
        // and a sample from "aaa" itself would break Karn's algorithm. "ccc",
        // sent once, is SACKed during recovery, and its sample ends the
        // backoff.
        send!(s, time 350, sack_repr(3, 256, &[(6, 9)]));
        recv!(s, time 363, Ok(data_repr(3, b"aaa")));
        assert_eq!(s.rtte.retransmission_timeout(), Duration::from_millis(400));
        send!(s, time 364, sack_repr(3, 256, &[(6, 12)]));
        assert_eq!(s.rtte.retransmission_timeout(), Duration::from_millis(200));
    }

    // Sends "aaa", "BBB" and "ccc" at 100 after a 40 ms round trip, so that
    // RACK's reordering window is 10 ms.
    fn socket_sack_reordering_window_10() -> TestSocket {
        let mut s = socket_established();
        s.remote_has_sack = true;
        s.remote_mss = 3;
        send!(s, time 0, ack_repr(0));
        s.send_slice(b"xxx").unwrap();
        recv!(s, time 0, Ok(data_repr(0, b"xxx")));
        send!(s, time 40, ack_repr(3));
        s.send_slice(b"aaaBBBccc").unwrap();
        for (i, payload) in [b"aaa", b"BBB", b"ccc"].iter().enumerate() {
            recv!(s, time 100, Ok(data_repr(3 + 3 * i, &payload[..])));
        }

        // "aaa" is overtaken.
        send!(s, time 140, sack_repr(3, 256, &[(6, 9)]));
        send!(s, time 140, sack_repr(3, 256, &[(6, 12)]));
        s
    }

    #[test]
    fn test_rack_tolerates_reordering_within_its_window() {
        let mut s = socket_sack_reordering_window_10();
        recv_nothing!(s, time 140);

        // "aaa" arrives within the window: nothing was lost.
        send!(s, time 149, ack_repr(12));
        recv_nothing!(s, time 160);
        assert_eq!(s.recover, None);
        assert!(s.rack.reo_timeout.is_none());
    }

    #[test]
    fn test_rack_reordering_timer_resends_what_stays_missing() {
        let mut s = socket_sack_reordering_window_10();
        recv_nothing!(s, time 149);

        // "aaa" is still missing a round trip plus the window after it was
        // sent: it is lost, too few segments were SACKed for RFC 6675 to
        // tell, and the timeout is at 300.
        recv!(s, time 150, Ok(data_repr(3, b"aaa")));
        assert!(s.recover.is_some());
        recv_nothing!(s, time 150);
    }

    #[test]
    fn test_rack_starts_recovery_under_a_zero_window() {
        let mut s = socket_sack_rtt_50();
        s.send_slice(b"aaaBBBcccDDD").unwrap();
        for (i, payload) in [b"aaa", b"BBB", b"ccc", b"DDD"].iter().enumerate() {
            recv!(s, time 100, Ok(data_repr(3 + 3 * i, &payload[..])));
        }

        // "BBB" is lost, and the ACK of the rest closes the window.
        send!(s, time 150, sack_repr(6, 0, &[(9, 15)]));
        assert!(s.timer.is_zero_window_probe());

        // RACK's timer finds "BBB" lost, and recovery resends it as a fast
        // retransmission would, window or no window: nothing else would.
        recv_nothing!(s, time 162);
        recv!(s, time 163, Ok(data_repr(6, b"BBB")));
        assert!(s.recover.is_some());
    }

    #[test]
    #[cfg(feature = "socket-tcp-cubic")]
    fn test_limited_transmit_sends_new_data_on_the_first_two_duplicates() {
        let mut s = socket_established_with_buffer_sizes(4096, 64);
        s.set_congestion_control(CongestionControl::Cubic);
        s.remote_mss = 128;
        s.congestion_controller.inner_mut().set_mss(128);
        send!(s, time 0, sack_repr(0, 4000, &[]));

        // Sixteen segments fill the congestion window, of 2,048 octets, and
        // the first is lost.
        let data: Vec<u8> = (0..2432u32).map(|i| (i / 128) as u8).collect();
        s.send_slice(&data).unwrap();
        for offset in (0..2048).step_by(128) {
            recv!(s, time 1000, Ok(data_repr(offset, &data[offset..offset + 128])));
        }
        recv_nothing!(s, time 1000);

        // RFC 3042: each of the first two duplicate ACKs lets out a new
        // segment. The third starts recovery.
        send!(s, time 1050, sack_repr(0, 4000, &[]));
        recv!(s, time 1050, Ok(data_repr(2048, &data[2048..2176])));
        recv_nothing!(s, time 1050);
        send!(s, time 1051, sack_repr(0, 4000, &[]));
        recv!(s, time 1051, Ok(data_repr(2176, &data[2176..2304])));
        recv_nothing!(s, time 1051);
        send!(s, time 1052, sack_repr(0, 4000, &[]));
        recv!(s, time 1052, Ok(data_repr(0, &data[0..128])));
    }

    #[test]
    #[cfg(feature = "socket-tcp-cubic")]
    fn test_tlp_that_repairs_a_loss_reduces_the_window() {
        for dsack in [false, true] {
            let mut s = socket_sack_rtt_50();
            s.send_slice(b"aaaBBB").unwrap();
            recv!(s, time 100, Ok(data_repr(3, b"aaa")));
            recv!(s, time 100, Ok(data_repr(6, b"BBB")));
            recv!(s, time 202, Ok(data_repr(6, b"BBB")));
            let window = s.congestion_controller.inner().window();

            // Only the probe's ACK arrives. With a D-SACK of "BBB", the
            // probe was needless: the first "BBB" arrived, and its ACK was
            // lost. Without one, the probe repaired a loss.
            let blocks: &[(usize, usize)] = if dsack { &[(6, 9)] } else { &[] };
            send!(s, time 252, sack_repr(9, 256, blocks));
            assert_eq!(s.rack.probe, None);
            let reduced = s.congestion_controller.inner().window() < window;
            assert_eq!(reduced, !dsack);
        }
    }

    #[test]
    fn test_path_mtu_reduction_stops_rack_timers() {
        let mut s = socket_established_with_buffer_sizes(4096, 64);
        s.remote_has_sack = true;
        s.remote_mss = 1400;
        s.congestion_controller.inner_mut().set_mss(1400);
        send!(s, time 0, sack_repr(0, 4000, &[]));
        s.send_slice(&[0; 10]).unwrap();
        recv!(s, time 0, Ok(data_repr(0, &[0; 10])));
        send!(s, time 50, sack_repr(10, 4000, &[]));

        // Two full segments are in flight, with a loss probe armed.
        s.send_slice(&[1; 2800]).unwrap();
        recv!(s, time 100, Ok(data_repr(10, &[1; 1400])));
        recv!(s, time 100, Ok(data_repr(1410, &[1; 1400])));
        assert!(s.rack.probe_at.is_some());

        // They are too big for the path, and are resent at the new size
        // from the last ACK. Resending is loss recovery of a kind, and no
        // probe is due during it.
        let outcome = s
            .socket
            .process_path_mtu(&s.cx, LOCAL_END, REMOTE_END, LOCAL_SEQ + 11, 1280);
        assert_eq!(outcome, PathMtuOutcome::Reduced);
        assert_eq!(s.rack.probe_at, None);
        recv(&mut s, Instant::from_millis(110), |repr| {
            assert_eq!(repr.unwrap().seq_number, LOCAL_SEQ + 11)
        });
        assert!(!s.loss_probe_allowed());
    }

    #[test]
    fn test_fast_retransmit_duplicate_detection() {
        let mut s = socket_established();
        s.remote_mss = 6;

        // Normal ACK of previously received segment
        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });

        // First duplicate, should not be counted as there is nothing to resend
        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });

        assert_eq!(
            s.local_rx_dup_acks, 0,
            "duplicate ACK counter is set but wound not transmit data"
        );

        // Send a long string of text divided into several packets
        // because of small remote_mss
        s.send_slice(b"xxxxxxyyyyyywwwwwwzzzzzz").unwrap();

        // This packet is reordered in network
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"xxxxxx"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 1005, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"yyyyyy"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 1010, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + (6 * 2),
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"wwwwww"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 1015, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + (6 * 3),
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"zzzzzz"[..],
            ..RECV_TEMPL
        }));

        // First duplicate ACK
        send!(s, time 1050, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        // Second duplicate ACK
        send!(s, time 1055, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        // Reordered packet arrives which should reset duplicate ACK count
        send!(s, time 1060, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + (6 * 3)),
            ..SEND_TEMPL
        });

        assert_eq!(
            s.local_rx_dup_acks, 0,
            "duplicate ACK counter is not reset when receiving ACK which updates send window"
        );

        // ACK all received segments
        send!(s, time 1120, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1 + (6 * 4)),
            ..SEND_TEMPL
        });
    }

    #[test]
    fn test_fast_retransmit_dup_acks_counter() {
        let mut s = socket_established();

        s.send_slice(b"abc").unwrap(); // This is lost
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abc"[..],
            ..RECV_TEMPL
        }));

        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });

        // A lot of retransmits happen here
        s.local_rx_dup_acks = u8::MAX - 1;

        // Send 3 more ACKs, which could overflow local_rx_dup_acks,
        // but intended behaviour is that we saturate the bounds
        // of local_rx_dup_acks
        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        send!(s, time 0, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        assert_eq!(
            s.local_rx_dup_acks,
            u8::MAX,
            "duplicate ACK count should not overflow but saturate"
        );
    }

    #[test]
    fn test_fast_retransmit_zero_window() {
        let mut s = socket_established();

        send!(s, time 1000, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });

        s.send_slice(b"abc").unwrap();

        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abc"[..],
            ..RECV_TEMPL
        }));

        // 3 dup acks
        send!(s, time 1050, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        send!(s, time 1050, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        send!(s, time 1050, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            window_len: 0, // boom
            ..SEND_TEMPL
        });

        // even though we're in "fast retransmit", we shouldn't
        // force-send anything because the remote's window is full.
        recv_nothing!(s);
    }

    #[test]
    fn test_retransmit_exponential_backoff() {
        let mut s = socket_established();
        s.send_slice(b"abcdef").unwrap();
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));

        let expected_retransmission_instant = s.rtte.retransmission_timeout().total_millis() as i64;
        recv_nothing!(s, time expected_retransmission_instant - 1);
        recv!(s, time expected_retransmission_instant, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));

        // "current time" is expected_retransmission_instant, and we want to wait 2 * retransmission timeout
        let expected_retransmission_instant = 3 * expected_retransmission_instant;

        recv_nothing!(s, time expected_retransmission_instant - 1);
        recv!(s, time expected_retransmission_instant, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_data_retransmit_ack_more_than_expected() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"aaaaaabbbbbbcccccc").unwrap();

        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"aaaaaa"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"bbbbbb"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 12,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"cccccc"[..],
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 0);

        recv_nothing!(s, time 50);

        // retransmit timer expires, we want to retransmit all 3 packets
        // but we only manage to retransmit 2 (due to e.g. lack of device buffer space)
        assert!(s.timer.is_retransmit());
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"aaaaaa"[..],
            ..RECV_TEMPL
        }));
        recv!(s, time 1000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"bbbbbb"[..],
            ..RECV_TEMPL
        }));

        // ack first packet.
        send!(
            s,
            time 3000,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6),
                ..SEND_TEMPL
            }
        );

        // this should keep retransmit timer on, because there's
        // still unacked data.
        assert!(s.timer.is_retransmit());

        // ack all three packets.
        // This might confuse the TCP stack because after the retransmit
        // it "thinks" the 3rd packet hasn't been transmitted yet, but it is getting acked.
        send!(
            s,
            time 3000,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 18),
                ..SEND_TEMPL
            }
        );

        // this should exit retransmit mode.
        assert!(!s.timer.is_retransmit());
        // and consider all data ACKed.
        assert!(s.tx_buffer.is_empty());
        recv_nothing!(s, time 5000);
    }

    #[test]
    fn test_retransmit_fin() {
        let mut s = socket_established();
        s.close();
        recv!(s, time 0, Ok(TcpRepr {
            control: TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));

        recv_nothing!(s, time 999);
        recv!(s, time 1000, Ok(TcpRepr {
            control: TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_retransmit_fin_wait() {
        let mut s = socket_fin_wait_1();
        // we send FIN
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            }]
        );
        // remote also sends FIN, does NOT ack ours.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        // we ack it
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::None,
                seq_number: LOCAL_SEQ + 2,
                ack_number: Some(REMOTE_SEQ + 2),
                ..RECV_TEMPL
            }]
        );

        // we haven't got an ACK for our FIN, we should retransmit.
        recv_nothing!(s, time 999);
        recv!(
            s,
            time 1000,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 2),
                ..RECV_TEMPL
            }]
        );
        recv_nothing!(s, time 2999);
        recv!(
            s,
            time 3000,
            [TcpRepr {
                control: TcpControl::Fin,
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 2),
                ..RECV_TEMPL
            }]
        );
    }

    // =========================================================================================//
    // Tests for window management.
    // =========================================================================================//

    #[test]
    fn test_maximum_segment_size() {
        let mut s = socket_listen();
        s.tx_buffer = SocketBuffer::new(vec![0; 32767]);
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                max_seg_size: Some(1000),
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 32767,
                ..SEND_TEMPL
            }
        );
        s.send_slice(&[0; 1200][..]).unwrap();
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &[0; 1000][..],
                ..RECV_TEMPL
            })
        );
    }

    #[cfg(feature = "segmentation-offload")]
    #[test]
    fn test_segmentation_offload() {
        use crate::tests::segmentation_offload::MAX_SEGMENTABLE_SIZE;
        use crate::wire;

        let (interface, _, _) =
            crate::tests::segmentation_offload::setup_segmenting(crate::phy::Medium::Ip);
        let mut s = TestSocket {
            cx: interface.inner,
            ..socket_listen()
        };
        s.tx_buffer = SocketBuffer::new(vec![0; 2 * MAX_SEGMENTABLE_SIZE]);

        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                window_scale: Some(2),
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: u16::MAX,
                ..SEND_TEMPL
            }
        );

        s.send_slice(&[0; 2 * MAX_SEGMENTABLE_SIZE][..]).unwrap();

        // We want to ensure that the size of the unsegmented packets exceed the
        // maximum allowed by the length field in the IP headers to check if the
        // relevant code incorrectly assumes that the length fits into the
        // field.
        let ip_header_len = match s.local_endpoint().unwrap().addr {
            #[cfg(feature = "proto-ipv4")]
            IpAddress::Ipv4(_) => {
                assert!(MAX_SEGMENTABLE_SIZE > usize::from(u16::MAX));
                wire::IPV4_HEADER_LEN
            }
            #[cfg(feature = "proto-ipv6")]
            IpAddress::Ipv6(_) => {
                assert!(MAX_SEGMENTABLE_SIZE - wire::IPV6_HEADER_LEN > usize::from(u16::MAX));
                wire::IPV6_HEADER_LEN
            }
        };
        let payload = vec![0; MAX_SEGMENTABLE_SIZE - ip_header_len - TCP_HEADER_LEN];
        assert!(
            payload.len() > usize::from(BASE_MSS),
            "the payload is not large enough to require segmentation!"
        );

        recv!(
            s,
            [
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: payload.as_slice(),
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + payload.len() + 1,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: payload.as_slice(),
                    ..RECV_TEMPL
                }
            ]
        );
    }

    #[test]
    fn test_recv_out_of_recv_win() {
        let mut s = socket_established();
        s.set_ack_delay(Some(ACK_DELAY_DEFAULT));
        s.remote_mss = 32;

        // No ACKs are sent due to the ACK delay.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Psh,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &[0; 32],
                ..SEND_TEMPL
            }
        );
        recv_nothing!(s);

        // RMSS+1 bytes of data has been received, so ACK is sent without delay.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Psh,
                seq_number: REMOTE_SEQ + 33,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &[0; 1],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 34),
                window_len: 31,
                ..RECV_TEMPL
            })
        );

        // This frees up a byte in the receive buffer. However, the remote shouldn't be aware of
        // this since no ACKs are sent.
        s.recv_slice(&mut [0; 1]).unwrap();
        recv_nothing!(s);

        // Now, if the remote wants to send one byte outside of the receive window that we
        // previously advertised, it should not succeed.
        send!(
            s,
            TcpRepr {
                control: TcpControl::Psh,
                seq_number: REMOTE_SEQ + 34,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &[0; 32],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 65),
                window_len: 1, // The last byte isn't accepted.
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_close_wait_no_window_update() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &[1, 2, 3, 4],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::CloseWait);

        // we ack the FIN, with the reduced window size.
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 6),
                window_len: 60,
                ..RECV_TEMPL
            })
        );

        let rx_buf = &mut [0; 32];
        assert_eq!(s.recv_slice(rx_buf), Ok(4));

        // check that we do NOT send a window update even if it has changed.
        recv_nothing!(s);
    }

    #[test]
    fn test_time_wait_no_window_update() {
        let mut s = socket_fin_wait_2();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 2),
                payload: &[1, 2, 3, 4],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::TimeWait);

        // we ack the FIN, with the reduced window size.
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 2,
                ack_number: Some(REMOTE_SEQ + 6),
                window_len: 60,
                ..RECV_TEMPL
            })
        );

        let rx_buf = &mut [0; 32];
        assert_eq!(s.recv_slice(rx_buf), Ok(4));

        // check that we do NOT send a window update even if it has changed.
        recv_nothing!(s);
    }

    // =========================================================================================//
    // Tests for flow control.
    // =========================================================================================//

    #[test]
    fn test_psh_transmit() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef").unwrap();
        s.send_slice(b"123456").unwrap();
        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::None,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }), exact);
        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::Psh,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"123456"[..],
            ..RECV_TEMPL
        }), exact);
    }

    #[test]
    fn test_psh_receive() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Psh,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 58,
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_zero_window_ack() {
        let mut s = socket_established();
        s.rx_buffer = SocketBuffer::new(vec![0; 6]);
        s.assembler = Assembler::new();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 0,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"123456"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 0,
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_zero_window_ack_not_rate_limited() {
        let mut s = socket_established();
        s.rx_buffer = SocketBuffer::new(vec![0; 6]);
        s.assembler = Assembler::new();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 0,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"123456"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 0,
                ..RECV_TEMPL
            })
        );
        // The remote retransmits into the zero window again within a second,
        // e.g. because the ACK above was lost. The ACK must not be withheld by
        // challenge ACK rate limiting: it is the remote's only way to learn
        // the window state, and a data segment cannot cause an ACK loop.
        send!(
            s,
            time 100,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"123456"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 0,
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_zero_window_fin() {
        let mut s = socket_established();
        s.rx_buffer = SocketBuffer::new(vec![0; 6]);
        s.assembler = Assembler::new();
        s.ack_delay = None;

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 0,
                ..RECV_TEMPL
            }]
        );

        // Even though the sequence space for the FIN itself is outside the window,
        // it is not data, so FIN must be accepted when window full.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &[],
                control: TcpControl::Fin,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::CloseWait);

        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 7),
                window_len: 0,
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_zero_window_ack_on_window_growth() {
        let mut s = socket_established();
        s.rx_buffer = SocketBuffer::new(vec![0; 6]);
        s.assembler = Assembler::new();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 0,
                ..RECV_TEMPL
            }]
        );
        recv_nothing!(s, time 0);
        s.recv(|buffer| {
            assert_eq!(&buffer[..3], b"abc");
            (3, ())
        })
        .unwrap();
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 6),
            window_len: 3,
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 0);
        s.recv(|buffer| {
            assert_eq!(buffer, b"def");
            (buffer.len(), ())
        })
        .unwrap();
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 6),
            window_len: 6,
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_window_update_with_delay_ack() {
        let mut s = socket_established_with_buffer_sizes(6, 6);
        s.ack_delay = Some(Duration::from_millis(10));

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            }
        );

        recv_nothing!(s, time 5);

        s.recv(|buffer| {
            assert_eq!(&buffer[..2], b"ab");
            (2, ())
        })
        .unwrap();
        recv!(
            s,
            time 5,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 2,
                ..RECV_TEMPL
            })
        );

        s.recv(|buffer| {
            assert_eq!(&buffer[..1], b"c");
            (1, ())
        })
        .unwrap();
        recv_nothing!(s, time 5);

        s.recv(|buffer| {
            assert_eq!(&buffer[..1], b"d");
            (1, ())
        })
        .unwrap();
        recv!(
            s,
            time 5,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 4,
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_fill_peer_window() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef123456!@#$%^").unwrap();
        recv!(
            s,
            [
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &b"abcdef"[..],
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + 6,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &b"123456"[..],
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + 6 + 6,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &b"!@#$%^"[..],
                    ..RECV_TEMPL
                }
            ]
        );
    }

    #[test]
    fn test_announce_window_after_read() {
        let mut s = socket_established();
        s.rx_buffer = SocketBuffer::new(vec![0; 6]);
        s.assembler = Assembler::new();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 3),
                window_len: 3,
                ..RECV_TEMPL
            }]
        );
        // Test that `dispatch` updates `remote_last_win`
        assert_eq!(s.remote_last_win, s.rx_buffer.window() as u16);
        s.recv(|buffer| (buffer.len(), ())).unwrap();
        assert!(s.window_to_update());
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 3),
                window_len: 6,
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.remote_last_win, s.rx_buffer.window() as u16);
        // Provoke immediate ACK to test that `process` updates `remote_last_win`
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"def"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 3),
                window_len: 6,
                ..RECV_TEMPL
            })
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 3,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 9),
                window_len: 0,
                ..RECV_TEMPL
            })
        );
        assert_eq!(s.remote_last_win, s.rx_buffer.window() as u16);
        s.recv(|buffer| (buffer.len(), ())).unwrap();
        assert!(s.window_to_update());
    }

    // =========================================================================================//
    // Tests for zero-window probes.
    // =========================================================================================//

    #[test]
    fn test_zero_window_probe_enter_on_win_update() {
        let mut s = socket_established();

        assert!(!s.timer.is_zero_window_probe());

        s.send_slice(b"abcdef123456!@#$%^").unwrap();

        assert!(!s.timer.is_zero_window_probe());

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        assert!(s.timer.is_zero_window_probe());
    }

    #[test]
    fn test_zero_window_probe_enter_on_send() {
        let mut s = socket_established();

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        assert!(!s.timer.is_zero_window_probe());

        s.send_slice(b"abcdef123456!@#$%^").unwrap();

        assert!(s.timer.is_zero_window_probe());
    }

    #[test]
    fn test_zero_window_probe_exit() {
        let mut s = socket_established();

        s.send_slice(b"abcdef123456!@#$%^").unwrap();

        assert!(!s.timer.is_zero_window_probe());

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        assert!(s.timer.is_zero_window_probe());

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 6,
                ..SEND_TEMPL
            }
        );

        assert!(!s.timer.is_zero_window_probe());
    }

    #[test]
    fn test_zero_window_probe_exit_ack() {
        let mut s = socket_established();

        s.send_slice(b"abcdef123456!@#$%^").unwrap();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        recv!(
            s,
            time 1000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );

        send!(
            s,
            time 1010,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 2),
                window_len: 6,
                ..SEND_TEMPL
            }
        );

        recv!(
            s,
            time 1010,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 2,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"bcdef1"[..],
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    #[cfg(feature = "socket-tcp-reno")]
    fn test_zero_window_probe_not_capped_by_cwnd() {
        let mut s = socket_established_with_buffer_sizes(8192, 64);
        s.set_congestion_control(CongestionControl::Reno);
        s.remote_win_len = 65535;
        s.remote_mss = 1024;

        let data = [b'x'; 4096];
        s.send_slice(&data[..]).unwrap();

        // Reno's initial cwnd is 2048: two segments fill the congestion window
        // exactly, leaving cwnd_remaining() == 0.
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 1024,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1024],
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 0);

        // The remote closes its window without acknowledging anything new, so
        // no congestion window space is freed either.
        send!(s, time 10, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            window_len: 0,
            ..SEND_TEMPL
        });

        // Arm the probe timer. (Set directly because the ACK above carries no
        // new data; in real traffic this state is reached e.g. when the
        // controller shrinks cwnd below the flight size while probing.)
        s.timer
            .set_for_zero_window_probe(Instant::from_millis(10), Duration::from_millis(100));

        // The probe must carry 1 byte of data past the window edge even though
        // the congestion window is exhausted: an empty probe occupies no
        // sequence space and elicits no reply, so the connection would stall
        // if the remote's window update got lost.
        recv!(s, time 110, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1 + 2048,
            ack_number: Some(REMOTE_SEQ + 1),
            payload: &data[..1],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_zero_window_probe_backoff_nack_reply() {
        let mut s = socket_established();
        s.send_slice(b"abcdef123456!@#$%^").unwrap();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        recv_nothing!(s, time 999);
        recv!(
            s,
            time 1000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            time 1100,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        recv_nothing!(s, time 2999);
        recv!(
            s,
            time 3000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            time 3100,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        recv_nothing!(s, time 6999);
        recv!(
            s,
            time 7000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_zero_window_probe_backoff_no_reply() {
        let mut s = socket_established();
        s.send_slice(b"abcdef123456!@#$%^").unwrap();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        recv_nothing!(s, time 999);
        recv!(
            s,
            time 1000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );

        recv_nothing!(s, time 2999);
        recv!(
            s,
            time 3000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_zero_window_probe_shift() {
        let mut s = socket_established();

        s.send_slice(b"abcdef123456!@#$%^").unwrap();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        recv_nothing!(s, time 999);
        recv!(
            s,
            time 1000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );

        recv_nothing!(s, time 2999);
        recv!(
            s,
            time 3000,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"a"[..],
                ..RECV_TEMPL
            }]
        );

        // ack the ZWP byte, but still advertise zero window.
        // this should restart the ZWP timer.
        send!(
            s,
            time 3100,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 2),
                window_len: 0,
                ..SEND_TEMPL
            }
        );

        // ZWP should be sent at 3100+1000 = 4100
        recv_nothing!(s, time 4099);
        recv!(
            s,
            time 4100,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 2,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"b"[..],
                ..RECV_TEMPL
            }]
        );
    }

    // =========================================================================================//
    // Tests for timeouts.
    // =========================================================================================//

    #[test]
    fn test_listen_timeout() {
        let mut s = socket_listen();
        s.set_timeout(Some(Duration::from_millis(100)));
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Ingress);
    }

    #[test]
    fn test_connect_timeout() {
        let mut s = socket();
        s.local_seq_no = LOCAL_SEQ;
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END.port)
            .unwrap();
        s.set_timeout(Some(Duration::from_millis(100)));
        recv!(s, time 150, Ok(TcpRepr {
            control:    TcpControl::Syn,
            seq_number: LOCAL_SEQ,
            ack_number: None,
            max_seg_size: Some(BASE_MSS),
            window_scale: Some(0),
            sack_permitted: true,
            ..RECV_TEMPL
        }));
        assert_eq!(s.state, State::SynSent);
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(250))
        );
        recv!(s, time 250, Ok(TcpRepr {
            control:    TcpControl::Rst,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(TcpSeqNumber(0)),
            window_scale: None,
            ..RECV_TEMPL
        }));
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_established_timeout() {
        let mut s = socket_established();
        s.set_timeout(Some(Duration::from_millis(2000)));
        recv_nothing!(s, time 250);
        // Idle, the connection is not waiting on the remote, so it has no timeout.
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Ingress);
        s.send_slice(b"abcdef").unwrap();
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Now);
        recv!(s, time 255, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(1255))
        );
        recv!(s, time 1255, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(2255))
        );
        recv!(s, time 2255, Ok(TcpRepr {
            control:    TcpControl::Rst,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_established_keep_alive_timeout() {
        let mut s = socket_established();
        s.set_keep_alive(Some(Duration::from_millis(50)));
        s.set_timeout(Some(Duration::from_millis(100)));
        recv!(s, time 100, Ok(TcpRepr {
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &[0],
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 100);
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(150))
        );
        send!(s, time 105, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(155))
        );
        recv!(s, time 155, Ok(TcpRepr {
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &[0],
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 155);
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(205))
        );
        recv_nothing!(s, time 200);
        recv!(s, time 205, Ok(TcpRepr {
            control:    TcpControl::Rst,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));
        recv_nothing!(s, time 205);
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_fin_wait_1_timeout() {
        let mut s = socket_fin_wait_1();
        s.set_timeout(Some(Duration::from_millis(1000)));
        recv!(s, time 100, Ok(TcpRepr {
            control:    TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));
        recv!(s, time 1100, Ok(TcpRepr {
            control:    TcpControl::Rst,
            seq_number: LOCAL_SEQ + 1 + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_last_ack_timeout() {
        let mut s = socket_last_ack();
        s.set_timeout(Some(Duration::from_millis(1000)));
        recv!(s, time 100, Ok(TcpRepr {
            control:    TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 1),
            ..RECV_TEMPL
        }));
        recv!(s, time 1100, Ok(TcpRepr {
            control:    TcpControl::Rst,
            seq_number: LOCAL_SEQ + 1 + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 1),
            ..RECV_TEMPL
        }));
        assert_eq!(s.state, State::Closed);
    }

    #[test]
    fn test_user_timeout_aborts_unacked_data_to_silent_remote() {
        let mut s = socket_established();
        s.set_timeout(Some(USER_TIMEOUT));
        send!(s, time 1_000, REMOTE_ACK);
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Ingress);

        // After a long idle, the timeout counts from the first transmission.
        s.cx.set_now(Instant::from_secs(5_000));
        s.send_slice(b"abcdef").unwrap();
        let sent = poll_silently(&mut s, 100_000_000);
        let (last, retransmissions) = sent.split_last().unwrap();

        assert_eq!(*last, (5_900_000, TcpControl::Rst, 0));
        assert!(retransmissions.len() > 10);
        assert!(
            retransmissions
                .iter()
                .all(|&(at, control, len)| at < 5_900_000
                    && control == TcpControl::None
                    && len == 6)
        );
        assert_eq!(s.state, State::Closed);
        assert!(s.aborted_by_timeout());
    }

    #[test]
    fn test_user_timeout_spares_idle_connection() {
        let mut s = socket_established();
        s.set_timeout(Some(USER_TIMEOUT));
        send!(s, time 1_000, REMOTE_ACK);
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Ingress);

        // A day later it has sent nothing and is still open, and sends as usual.
        recv_nothing!(s, time 86_400_000);
        assert_eq!(s.state, State::Established);
        assert!(!s.aborted_by_timeout());
        s.send_slice(b"abcdef").unwrap();
        recv!(s, time 86_400_000, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_user_timeout_counts_fin_from_when_sent() {
        let mut s = socket_established();
        s.set_timeout(Some(USER_TIMEOUT));
        send!(s, time 1_000, REMOTE_ACK);

        // Closing long after the last packet received sends a FIN, not a RST.
        s.close();
        recv!(s, time 5_000_000, Ok(TcpRepr {
            control:    TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));
        let sent = poll_silently(&mut s, 100_000_000);
        assert_eq!(sent.last(), Some(&(5_900_000, TcpControl::Rst, 0)));
        assert!(s.aborted_by_timeout());
    }

    #[test]
    fn test_user_timeout_bounds_zero_window_probing() {
        let mut s = socket_established();
        s.set_timeout(Some(USER_TIMEOUT));
        let closed = TcpRepr {
            window_len: 0,
            ..REMOTE_ACK
        };
        send!(s, time 1_000, closed);
        s.cx.set_now(Instant::from_millis(2_000));
        s.send_slice(b"abcdef").unwrap();

        // A remote that answers the probes keeps the connection open.
        let mut answered_at = 0;
        let mut probes = 0;
        loop {
            let at = match s.socket.poll_at(&mut s.cx) {
                PollAt::Now => s.cx.now(),
                PollAt::Time(at) => at,
                PollAt::Ingress => panic!("the zero-window probes stopped"),
            };
            if at.total_millis() > 2_000_000 {
                break;
            }
            s.cx.set_now(at);
            let mut probed = false;
            let _: Result<(), ()> = s.socket.dispatch(&mut s.cx, |_, _, (_, repr)| {
                probed = repr.payload.len() == 1;
                Ok(())
            });
            if probed {
                probes += 1;
                answered_at = at.total_millis();
                send(&mut s, at, &closed);
            }
        }
        assert!(probes > 30);
        assert_eq!(s.state, State::Established);

        // One that stops answering is given up on a timeout after its last answer.
        let sent = poll_silently(&mut s, 100_000_000);
        let (last, probes) = sent.split_last().unwrap();
        assert_eq!(*last, (answered_at + 900_000, TcpControl::Rst, 0));
        assert!(probes.iter().all(|&(_, _, len)| len == 1));
        assert!(s.aborted_by_timeout());
    }

    #[test]
    fn test_closed_timeout() {
        let mut s = socket_established();
        s.set_timeout(Some(Duration::from_millis(200)));
        s.remote_last_ts = Some(Instant::from_millis(100));
        s.abort();
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Now);
        recv!(s, time 100, Ok(TcpRepr {
            control:    TcpControl::Rst,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            ..RECV_TEMPL
        }));
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Ingress);
    }

    // =========================================================================================//
    // Tests for keep-alive.
    // =========================================================================================//

    // Polls the socket whenever it asks to be, until `until` ms or until it has nothing
    // left to send, with the remote answering nothing. Returns when each packet went out,
    // its control and its payload length.
    fn poll_silently(s: &mut TestSocket, until: i64) -> Vec<(i64, TcpControl, usize)> {
        let mut sent = Vec::new();

        for _ in 0..10_000 {
            let at = match s.socket.poll_at(&mut s.cx) {
                PollAt::Now => s.cx.now(),
                PollAt::Time(at) => at,
                PollAt::Ingress => return sent,
            };
            if at.total_millis() > until {
                return sent;
            }

            s.cx.set_now(at);
            let now = at.total_millis();
            let _: Result<(), ()> = s.socket.dispatch(&mut s.cx, |_, _, (_, repr)| {
                sent.push((now, repr.control.quash_psh(), repr.payload.len()));
                Ok(())
            });
        }

        panic!("the socket kept asking to be polled")
    }

    // SmolNet's keep-alive timing, which is Linux's default.
    const LINUX_KEEP_ALIVE: KeepAlive = KeepAlive {
        idle: Duration::from_secs(7_200),
        interval: Duration::from_secs(75),
        probes: Some(9),
    };

    const USER_TIMEOUT: Duration = Duration::from_secs(900);

    const REMOTE_ACK: TcpRepr<'static> = TcpRepr {
        seq_number: TcpSeqNumber(REMOTE_SEQ.0 + 1),
        ack_number: Some(TcpSeqNumber(LOCAL_SEQ.0 + 1)),
        ..SEND_TEMPL
    };

    #[test]
    fn test_keep_alive_probes_then_aborts() {
        let mut s = socket_established();
        s.set_timeout(Some(USER_TIMEOUT));
        send!(s, time 1_000, REMOTE_ACK);

        // The first probe is due `idle` after the last packet received, not at once.
        s.set_keep_alive_config(Some(LINUX_KEEP_ALIVE));
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(7_201_000))
        );

        // Nine go unanswered, `interval` apart, and the abort is due when a tenth would
        // be. The timeout, much shorter, does not end the idle connection first.
        let sent = poll_silently(&mut s, 100_000_000);
        let (last, probes) = sent.split_last().unwrap();
        let expected: Vec<_> = (0..9)
            .map(|n| (7_201_000 + n * 75_000, TcpControl::None, 1))
            .collect();
        assert_eq!(probes, &expected[..]);
        assert_eq!(*last, (7_876_000, TcpControl::Rst, 0));
        assert!(s.aborted_by_timeout());
    }

    #[test]
    fn test_keep_alive_answer_resets_probes() {
        let mut s = socket_established();
        send!(s, time 1_000, REMOTE_ACK);
        s.set_keep_alive_config(Some(LINUX_KEEP_ALIVE));

        let sent = poll_silently(&mut s, 7_501_000);
        assert_eq!(sent.len(), 5);

        // An answer to the fifth: the next probe is `idle` later, with nine to go.
        send!(s, time 7_501_500, REMOTE_ACK);
        let sent = poll_silently(&mut s, 100_000_000);
        assert_eq!(sent.len(), 10);
        assert_eq!(sent[0], (14_701_500, TcpControl::None, 1));
        assert_eq!(sent[8], (14_701_500 + 8 * 75_000, TcpControl::None, 1));
        assert_eq!(sent[9], (14_701_500 + 9 * 75_000, TcpControl::Rst, 0));
    }

    #[test]
    fn test_keep_alive_leaves_unacked_data_to_timeout() {
        let mut s = socket_established();
        s.set_timeout(Some(USER_TIMEOUT));
        s.set_keep_alive_config(Some(LINUX_KEEP_ALIVE));
        send!(s, time 1_000, REMOTE_ACK);

        s.cx.set_now(Instant::from_secs(5_000));
        s.send_slice(b"abcdef").unwrap();
        let sent = poll_silently(&mut s, 100_000_000);
        let (last, retransmissions) = sent.split_last().unwrap();
        assert_eq!(*last, (5_900_000, TcpControl::Rst, 0));
        assert!(retransmissions.iter().all(|&(_, _, len)| len == 6));
    }

    #[test]
    fn test_keep_alive_off_after_on_sends_no_probe() {
        let mut s = socket_established();
        send!(s, time 1_000, REMOTE_ACK);
        s.set_keep_alive_config(Some(LINUX_KEEP_ALIVE));
        s.set_keep_alive_config(None);
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Ingress);
        recv_nothing!(s, time 100_000_000);
    }

    #[test]
    fn test_responds_to_keep_alive() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_sends_keep_alive() {
        let mut s = socket_established();
        s.set_keep_alive(Some(Duration::from_millis(100)));

        // drain the forced keep-alive packet
        assert_eq!(s.socket.poll_at(&mut s.cx), PollAt::Now);
        recv!(s, time 0, Ok(TcpRepr {
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &[0],
            ..RECV_TEMPL
        }));

        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(100))
        );
        recv_nothing!(s, time 95);
        recv!(s, time 100, Ok(TcpRepr {
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &[0],
            ..RECV_TEMPL
        }));

        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(200))
        );
        recv_nothing!(s, time 195);
        recv!(s, time 200, Ok(TcpRepr {
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &[0],
            ..RECV_TEMPL
        }));

        send!(s, time 250, TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            ..SEND_TEMPL
        });
        assert_eq!(
            s.socket.poll_at(&mut s.cx),
            PollAt::Time(Instant::from_millis(350))
        );
        recv_nothing!(s, time 345);
        recv!(s, time 350, Ok(TcpRepr {
            seq_number: LOCAL_SEQ,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"\x00"[..],
            ..RECV_TEMPL
        }));
    }

    // =========================================================================================//
    // Tests for time-to-live configuration.
    // =========================================================================================//

    #[test]
    fn test_set_hop_limit() {
        let mut s = socket_syn_received();

        s.set_hop_limit(Some(0x2a));
        assert_eq!(
            s.socket.dispatch(&mut s.cx, |_, _, (ip_repr, _)| {
                assert_eq!(ip_repr.hop_limit(), 0x2a);
                Ok::<_, ()>(())
            }),
            Ok(())
        );

        // assert that user-configurable settings are kept,
        // see https://github.com/smoltcp-rs/smoltcp/issues/601.
        s.reset();
        assert_eq!(s.hop_limit(), Some(0x2a));
    }

    #[test]
    #[should_panic(expected = "the time-to-live value of a packet must not be zero")]
    fn test_set_hop_limit_zero() {
        let mut s = socket_syn_received();
        s.set_hop_limit(Some(0));
    }

    // =========================================================================================//
    // Tests for reassembly.
    // =========================================================================================//

    #[test]
    fn test_out_of_order() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 3,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"def"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                ..RECV_TEMPL
            })
        );
        s.recv(|buffer| {
            assert_eq!(buffer, b"");
            (buffer.len(), ())
        })
        .unwrap();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abcdef"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 6),
                window_len: 58,
                ..RECV_TEMPL
            })
        );
        s.recv(|buffer| {
            assert_eq!(buffer, b"abcdef");
            (buffer.len(), ())
        })
        .unwrap();
    }

    #[test]
    fn test_buffer_wraparound_rx() {
        let mut s = socket_established();
        s.rx_buffer = SocketBuffer::new(vec![0; 6]);
        s.assembler = Assembler::new();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        s.recv(|buffer| {
            assert_eq!(buffer, b"abc");
            (buffer.len(), ())
        })
        .unwrap();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 3,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"defghi"[..],
                ..SEND_TEMPL
            }
        );
        let mut data = [0; 6];
        assert_eq!(s.recv_slice(&mut data[..]), Ok(6));
        assert_eq!(data, &b"defghi"[..]);
    }

    #[test]
    fn test_buffer_wraparound_tx() {
        let mut s = socket_established();
        s.set_nagle_enabled(false);

        s.tx_buffer = SocketBuffer::new(vec![b'.'; 9]);
        assert_eq!(s.send_slice(b"xxxyyy"), Ok(6));
        assert_eq!(s.tx_buffer.dequeue_many(3), &b"xxx"[..]);
        assert_eq!(s.tx_buffer.len(), 3);

        // "abcdef" not contiguous in tx buffer
        assert_eq!(s.send_slice(b"abcdef"), Ok(6));
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"yyyabc"[..],
                ..RECV_TEMPL
            })
        );
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"def"[..],
                ..RECV_TEMPL
            })
        );
    }

    // =========================================================================================//
    // Tests for graceful vs ungraceful rx close
    // =========================================================================================//

    #[test]
    fn test_rx_close_fin() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
        assert_eq!(s.recv(|_| (0, ())), Err(RecvError::Finished));
    }

    #[test]
    fn test_rx_close_fin_in_fin_wait_1() {
        let mut s = socket_fin_wait_1();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::Closing);
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
        assert_eq!(s.recv(|_| (0, ())), Err(RecvError::Finished));
    }

    #[test]
    fn test_rx_close_fin_in_fin_wait_2() {
        let mut s = socket_fin_wait_2();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state, State::TimeWait);
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
        assert_eq!(s.recv(|_| (0, ())), Err(RecvError::Finished));
    }

    #[test]
    fn test_rx_close_fin_with_hole() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Fin,
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"ghi"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 3),
                window_len: 61,
                ..RECV_TEMPL
            })
        );
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
        s.recv(|data| {
            assert_eq!(data, b"");
            (0, ())
        })
        .unwrap();
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1 + 9,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        // Error must be `Illegal` even if we've received a FIN,
        // because we are missing data.
        assert_eq!(s.recv(|_| (0, ())), Err(RecvError::InvalidState));
    }

    #[test]
    fn test_rx_close_rst() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1 + 3,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
        assert_eq!(s.recv(|_| (0, ())), Err(RecvError::InvalidState));
    }

    #[test]
    fn test_rx_close_rst_with_hole() {
        let mut s = socket_established();
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + 6,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"ghi"[..],
                ..SEND_TEMPL
            },
            Some(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 3),
                window_len: 61,
                ..RECV_TEMPL
            })
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Rst,
                seq_number: REMOTE_SEQ + 1 + 9,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();
        assert_eq!(s.recv(|_| (0, ())), Err(RecvError::InvalidState));
    }

    // =========================================================================================//
    // Tests for delayed ACK
    // =========================================================================================//

    #[test]
    fn test_delayed_ack() {
        let mut s = socket_established();
        s.set_ack_delay(Some(ACK_DELAY_DEFAULT));
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );

        // No ACK is immediately sent.
        recv_nothing!(s);

        // After 10ms, it is sent.
        recv!(s, time 11, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 3),
            window_len: 61,
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_delayed_ack_win() {
        let mut s = socket_established();
        s.set_ack_delay(Some(ACK_DELAY_DEFAULT));
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );

        // Reading the data off the buffer should cause a window update.
        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();

        // However, no ACK or window update is immediately sent.
        recv_nothing!(s);

        // After 10ms, it is sent.
        recv!(s, time 11, Ok(TcpRepr {
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1 + 3),
            ..RECV_TEMPL
        }));
    }

    #[test]
    fn test_delayed_ack_reply() {
        let mut s = socket_established();
        s.set_ack_delay(Some(ACK_DELAY_DEFAULT));
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"abc"[..],
                ..SEND_TEMPL
            }
        );

        s.recv(|data| {
            assert_eq!(data, b"abc");
            (3, ())
        })
        .unwrap();

        s.send_slice(&b"xyz"[..]).unwrap();

        // Writing data to the socket causes ACK to not be delayed,
        // because it is immediately sent with the data.
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + 3),
                payload: &b"xyz"[..],
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_delayed_ack_every_rmss() {
        let mut s = socket_established_with_buffer_sizes(DEFAULT_MSS * 2, DEFAULT_MSS * 2);
        s.set_ack_delay(Some(ACK_DELAY_DEFAULT));
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &[0; DEFAULT_MSS - 1],
                ..SEND_TEMPL
            }
        );

        // No ACK is immediately sent.
        recv_nothing!(s);

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + (DEFAULT_MSS - 1),
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"a"[..],
                ..SEND_TEMPL
            }
        );

        // No ACK is immediately sent.
        recv_nothing!(s);

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + DEFAULT_MSS,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"a"[..],
                ..SEND_TEMPL
            }
        );

        // RMSS+1 bytes of data has been received, so ACK is sent without delay.
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + (DEFAULT_MSS + 1)),
                window_len: (DEFAULT_MSS - 1) as u16,
                ..RECV_TEMPL
            })
        );
    }

    #[test]
    fn test_delayed_ack_every_rmss_or_more() {
        let mut s = socket_established_with_buffer_sizes(DEFAULT_MSS * 2, DEFAULT_MSS * 2);
        s.set_ack_delay(Some(ACK_DELAY_DEFAULT));
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &[0; DEFAULT_MSS],
                ..SEND_TEMPL
            }
        );

        // No ACK is immediately sent.
        recv_nothing!(s);

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + DEFAULT_MSS,
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"a"[..],
                ..SEND_TEMPL
            }
        );

        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1 + (DEFAULT_MSS + 1),
                ack_number: Some(LOCAL_SEQ + 1),
                payload: &b"b"[..],
                ..SEND_TEMPL
            }
        );

        // RMSS+2 bytes of data has been received, so ACK is sent without delay.
        recv!(
            s,
            Ok(TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1 + (DEFAULT_MSS + 2)),
                window_len: (DEFAULT_MSS - 2) as u16,
                ..RECV_TEMPL
            })
        );
    }

    // =========================================================================================//
    // Tests for Nagle's Algorithm
    // =========================================================================================//

    #[test]
    fn test_nagle() {
        let mut s = socket_established();
        s.remote_mss = 6;

        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                ..RECV_TEMPL
            }]
        );

        // If there's data in flight, full segments get sent.
        s.send_slice(b"foobar").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"foobar"[..],
                ..RECV_TEMPL
            }]
        );

        s.send_slice(b"aaabbbccc").unwrap();
        // If there's data in flight but no not-full segment, the not-full segment at
        // the end of a write gets sent at once.
        recv!(
            s,
            [
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + 6 + 6,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &b"aaabbb"[..],
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + 6 + 6 + 6,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &b"ccc"[..],
                    ..RECV_TEMPL
                }
            ]
        );

        // If a not-full segment is in flight, another one doesn't get sent.
        s.send_slice(b"dd").unwrap();
        recv_nothing!(s);

        // The data before the not-full segment gets ACKd, but that segment is still
        // in flight.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 6 + 6),
                ..SEND_TEMPL
            }
        );
        recv_nothing!(s);

        // The not-full segment gets ACKd, so the next one gets sent.
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 6 + 6 + 3),
                ..SEND_TEMPL
            }
        );
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6 + 6 + 6 + 3,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"dd"[..],
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_nagle_sends_the_tail_of_a_write_longer_than_mss() {
        let mut s = socket_established();
        s.remote_mss = 6;

        // A small segment is in flight and gets ACKd.
        s.send_slice(b"abc").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abc"[..],
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 3),
                ..SEND_TEMPL
            }
        );

        // A write longer than the MSS goes out whole, with no ACK in between: its
        // full segment is in flight, but no not-full one is.
        s.send_slice(b"foobarbaz").unwrap();
        recv!(
            s,
            [
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + 3,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &b"foobar"[..],
                    ..RECV_TEMPL
                },
                TcpRepr {
                    seq_number: LOCAL_SEQ + 1 + 3 + 6,
                    ack_number: Some(REMOTE_SEQ + 1),
                    payload: &b"baz"[..],
                    ..RECV_TEMPL
                }
            ]
        );
    }

    #[test]
    fn test_nagle_works_with_reduced_payload_from_options() {
        const EFFECTIVE_MSS: usize = 64;

        let mut s = socket_established_with_buffer_sizes(256, 64);
        s.set_nagle_enabled(true);
        s.set_tsval_generator(Some(|| 1));
        s.remote_mss = EFFECTIVE_MSS;

        // Send small segment to "arm" Nagle's
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                timestamp: Some(TcpTimestampRepr::new(1, 0)),
                ..RECV_TEMPL
            }]
        );

        // A full segment (once options are accounted for) should not be delayed and contain 12 bytes less due to timestamp
        s.send_slice(&[0; EFFECTIVE_MSS - 12]).unwrap();
        recv!(
            s,
            time 0,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &[0; EFFECTIVE_MSS - 12],
                timestamp: Some(TcpTimestampRepr::new(1, 0)),
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_final_packet_in_stream_doesnt_wait_for_nagle() {
        let mut s = socket_established();
        s.remote_mss = 6;
        s.send_slice(b"abcdef0").unwrap();
        s.socket.close();

        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::None,
            seq_number: LOCAL_SEQ + 1,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"abcdef"[..],
            ..RECV_TEMPL
        }), exact);
        recv!(s, time 0, Ok(TcpRepr {
            control:    TcpControl::Fin,
            seq_number: LOCAL_SEQ + 1 + 6,
            ack_number: Some(REMOTE_SEQ + 1),
            payload:    &b"0"[..],
            ..RECV_TEMPL
        }), exact);
    }

    // =========================================================================================//
    // Tests for packet filtering.
    // =========================================================================================//

    #[test]
    fn test_doesnt_accept_wrong_port() {
        let mut s = socket_established();
        s.rx_buffer = SocketBuffer::new(vec![0; 6]);
        s.assembler = Assembler::new();

        let tcp_repr = TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            dst_port: LOCAL_PORT + 1,
            ..SEND_TEMPL
        };
        assert!(!s.socket.accepts(&mut s.cx, &SEND_IP_TEMPL, &tcp_repr));

        let tcp_repr = TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            src_port: REMOTE_PORT + 1,
            ..SEND_TEMPL
        };
        assert!(!s.socket.accepts(&mut s.cx, &SEND_IP_TEMPL, &tcp_repr));
    }

    #[test]
    fn test_doesnt_accept_wrong_ip() {
        let mut s = socket_established();

        let tcp_repr = TcpRepr {
            seq_number: REMOTE_SEQ + 1,
            ack_number: Some(LOCAL_SEQ + 1),
            payload: &b"abcdef"[..],
            ..SEND_TEMPL
        };

        let ip_repr = IpReprIpvX(IpvXRepr {
            src_addr: REMOTE_ADDR,
            dst_addr: LOCAL_ADDR,
            next_header: IpProtocol::Tcp,
            payload_len: tcp_repr.buffer_len(),
            hop_limit: 64,
        });
        assert!(s.socket.accepts(&mut s.cx, &ip_repr, &tcp_repr));

        let ip_repr_wrong_src = IpReprIpvX(IpvXRepr {
            src_addr: OTHER_ADDR,
            dst_addr: LOCAL_ADDR,
            next_header: IpProtocol::Tcp,
            payload_len: tcp_repr.buffer_len(),
            hop_limit: 64,
        });
        assert!(!s.socket.accepts(&mut s.cx, &ip_repr_wrong_src, &tcp_repr));

        let ip_repr_wrong_dst = IpReprIpvX(IpvXRepr {
            src_addr: REMOTE_ADDR,
            dst_addr: OTHER_ADDR,
            next_header: IpProtocol::Tcp,
            payload_len: tcp_repr.buffer_len(),
            hop_limit: 64,
        });
        assert!(!s.socket.accepts(&mut s.cx, &ip_repr_wrong_dst, &tcp_repr));
    }

    // =========================================================================================//
    // Timer tests
    // =========================================================================================//

    #[test]
    fn test_timer_retransmit() {
        const RTO: Duration = Duration::from_millis(100);
        let mut r = Timer::new();
        assert!(!r.should_retransmit(Instant::from_secs(1)));
        r.set_for_retransmit(Instant::from_millis(1000), RTO);
        assert!(!r.should_retransmit(Instant::from_millis(1000)));
        assert!(!r.should_retransmit(Instant::from_millis(1050)));
        assert!(r.should_retransmit(Instant::from_millis(1101)));
        r.set_for_retransmit(Instant::from_millis(1101), RTO);
        assert!(!r.should_retransmit(Instant::from_millis(1101)));
        assert!(!r.should_retransmit(Instant::from_millis(1150)));
        assert!(!r.should_retransmit(Instant::from_millis(1200)));
        assert!(r.should_retransmit(Instant::from_millis(1301)));
        r.set_for_idle(Instant::from_millis(1301), None);
        assert!(!r.should_retransmit(Instant::from_millis(1350)));
    }

    #[test]
    fn test_rtt_estimator() {
        let mut r = RttEstimator::default();

        let rtos = &[
            6000, 5000, 4252, 3692, 3272, 2956, 2720, 2540, 2408, 2308, 2232, 2176, 2132, 2100,
            2076, 2060, 2048, 2036, 2028, 2024, 2020, 2016, 2012, 2012,
        ];

        for &rto in rtos {
            r.sample(2000);
            assert_eq!(r.retransmission_timeout(), Duration::from_millis(rto));
        }
    }

    #[test]
    fn test_rtt_estimator_min_rto() {
        let mut r = RttEstimator::default();
        assert_eq!(r.retransmission_timeout(), Duration::from_millis(1000));

        // Steady 10 ms samples converge on SRTT + 4 * RTTVAR well below 200 ms,
        // so the floor holds the RTO there, and not at RFC 6298's 1 s.
        for _ in 0..32 {
            r.sample(10);
        }
        assert_eq!(r.retransmission_timeout(), Duration::from_millis(200));

        // A timeout still backs off from the floor.
        r.on_rto();
        assert_eq!(r.retransmission_timeout(), Duration::from_millis(400));
    }

    #[test]
    fn test_set_get_congestion_control() {
        let mut s = socket_established();

        #[cfg(feature = "socket-tcp-reno")]
        {
            s.set_congestion_control(CongestionControl::Reno);
            assert_eq!(s.congestion_control(), CongestionControl::Reno);
        }

        #[cfg(feature = "socket-tcp-cubic")]
        {
            s.set_congestion_control(CongestionControl::Cubic);
            assert_eq!(s.congestion_control(), CongestionControl::Cubic);
        }

        s.set_congestion_control(CongestionControl::None);
        assert_eq!(s.congestion_control(), CongestionControl::None);
    }

    // =========================================================================================//
    // Timestamp tests
    // =========================================================================================//

    #[test]
    fn test_tsval_established_connection() {
        let mut s = socket_established();
        s.set_tsval_generator(Some(|| 1));

        assert!(s.timestamp_enabled());

        // First roundtrip after establishing.
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                timestamp: Some(TcpTimestampRepr::new(1, 0)),
                ..RECV_TEMPL
            }]
        );
        assert_eq!(s.tx_buffer.len(), 6);
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6),
                timestamp: Some(TcpTimestampRepr::new(500, 1)),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.tx_buffer.len(), 0);
        // Second roundtrip.
        s.send_slice(b"foobar").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1 + 6,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"foobar"[..],
                timestamp: Some(TcpTimestampRepr::new(1, 500)),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1 + 6 + 6),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.tx_buffer.len(), 0);
    }

    #[test]
    fn test_tsval_disabled_in_remote_client() {
        let mut s = socket_listen();
        s.set_tsval_generator(Some(|| 1));
        assert!(s.timestamp_enabled());
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::SynReceived);
        assert_eq!(s.tuple, Some(TUPLE));
        assert!(!s.timestamp_enabled());
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::Established);
        assert_eq!(s.local_seq_no, LOCAL_SEQ + 1);
        assert_eq!(s.remote_seq_no, REMOTE_SEQ + 1);
    }

    #[test]
    fn test_tsval_disabled_in_local_server() {
        let mut s = socket_listen();
        // s.set_timestamp(false); // commented to alert if the default state changes
        assert!(!s.timestamp_enabled());
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: None,
                timestamp: Some(TcpTimestampRepr::new(500, 0)),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::SynReceived);
        assert_eq!(s.tuple, Some(TUPLE));
        assert!(!s.timestamp_enabled());
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: Some(REMOTE_SEQ + 1),
                max_seg_size: Some(BASE_MSS),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                seq_number: REMOTE_SEQ + 1,
                ack_number: Some(LOCAL_SEQ + 1),
                ..SEND_TEMPL
            }
        );
        assert_eq!(s.state(), State::Established);
        assert_eq!(s.local_seq_no, LOCAL_SEQ + 1);
        assert_eq!(s.remote_seq_no, REMOTE_SEQ + 1);
    }

    #[test]
    fn test_tsval_disabled_in_remote_server() {
        let mut s = socket();
        s.set_tsval_generator(Some(|| 1));
        assert!(s.timestamp_enabled());
        s.local_seq_no = LOCAL_SEQ;
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END.port)
            .unwrap();
        assert_eq!(s.tuple, Some(TUPLE));
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                timestamp: Some(TcpTimestampRepr::new(1, 0)),
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                timestamp: None,
                ..SEND_TEMPL
            }
        );
        assert!(!s.timestamp_enabled());
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                timestamp: None,
                ..RECV_TEMPL
            }]
        );
    }

    #[test]
    fn test_tsval_disabled_in_local_client() {
        let mut s = socket();
        // s.set_timestamp(false); // commented to alert if the default state changes
        assert!(!s.timestamp_enabled());
        s.local_seq_no = LOCAL_SEQ;
        s.socket
            .connect(&mut s.cx, REMOTE_END, LOCAL_END.port)
            .unwrap();
        assert_eq!(s.tuple, Some(TUPLE));
        recv!(
            s,
            [TcpRepr {
                control: TcpControl::Syn,
                seq_number: LOCAL_SEQ,
                ack_number: None,
                max_seg_size: Some(BASE_MSS),
                window_scale: Some(0),
                sack_permitted: true,
                ..RECV_TEMPL
            }]
        );
        send!(
            s,
            TcpRepr {
                control: TcpControl::Syn,
                seq_number: REMOTE_SEQ,
                ack_number: Some(LOCAL_SEQ + 1),
                max_seg_size: Some(BASE_MSS - 80),
                window_scale: Some(0),
                timestamp: Some(TcpTimestampRepr::new(500, 0)),
                ..SEND_TEMPL
            }
        );
        assert!(!s.timestamp_enabled());
        s.send_slice(b"abcdef").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abcdef"[..],
                timestamp: None,
                ..RECV_TEMPL
            }]
        );
    }

    // =========================================================================================//
    // Tests for source IP address change.
    // =========================================================================================//

    #[test]
    fn test_established_close_on_src_ip_change() {
        let mut s = socket_established();

        // Verify socket is working normally
        s.send_slice(b"abc").unwrap();
        recv!(
            s,
            [TcpRepr {
                seq_number: LOCAL_SEQ + 1,
                ack_number: Some(REMOTE_SEQ + 1),
                payload: &b"abc"[..],
                ..RECV_TEMPL
            }]
        );

        // Simulate interface IP change - remove the socket's source IP
        // and add a different one.
        let mut new_addrs = heapless::Vec::<IpCidr, IFACE_MAX_ADDR_COUNT>::new();
        new_addrs.push(IpCidr::new(OTHER_ADDR.into(), 24)).unwrap();
        s.cx.set_ip_addrs(new_addrs);

        // The socket's source IP is no longer on the interface.
        // When dispatch() runs, it should detect this and reset the socket
        // silently (no RST sent, since that would use the invalid source IP).
        s.send_slice(b"def").unwrap();
        recv_nothing!(s);
        assert_eq!(s.state, State::Closed);
    }
}
