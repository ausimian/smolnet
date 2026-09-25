use std::collections::VecDeque;

#[cfg(any(test, feature = "fuzzing"))]
use rustler::NewBinary;
#[cfg(all(not(test), not(feature = "fuzzing")))]
use rustler::OwnedBinary;
use rustler::{Env, NifMap, Term};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;

pub struct OutputPacket {
    #[cfg(all(not(test), not(feature = "fuzzing")))]
    inner: OwnedBinary,
    #[cfg(any(test, feature = "fuzzing"))]
    inner: Vec<u8>,
}

impl OutputPacket {
    pub fn zeroed(size: usize) -> Self {
        #[cfg(all(not(test), not(feature = "fuzzing")))]
        let inner = {
            let mut binary = OwnedBinary::new(size).expect("bounded transmit packet allocation");
            binary.as_mut_slice().fill(0);
            binary
        };
        #[cfg(any(test, feature = "fuzzing"))]
        let inner = vec![0; size];

        Self { inner }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[cfg(test)]
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_ref()
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.inner.as_mut()
    }

    pub fn into_term<'a>(self, env: Env<'a>) -> Term<'a> {
        #[cfg(all(not(test), not(feature = "fuzzing")))]
        {
            self.inner.release(env).into()
        }
        #[cfg(any(test, feature = "fuzzing"))]
        {
            NewBinary::from_iter(env, self.inner.into_iter()).into()
        }
    }
}

/// Egress the link has granted that the stack has not yet handed to it.
#[derive(Clone, Copy, Debug, NifMap, PartialEq, Eq)]
pub struct EgressCredit {
    pub packets: usize,
    pub bytes: usize,
}

pub struct BeamDevice {
    mtu: usize,
    receive: VecDeque<Vec<u8>>,
    receive_limit: usize,
    transmit: VecDeque<OutputPacket>,
    transmit_bytes: usize,
    transmit_limit: usize,
    // `None` leaves egress unlimited. Otherwise the transmit queue may grow
    // only while it is smaller than the remaining credit, so smoltcp keeps
    // unsent data in its socket buffers instead of the link dropping it.
    credit: Option<EgressCredit>,
}

impl BeamDevice {
    pub fn new(mtu: usize, receive_limit: usize, credit: Option<EgressCredit>) -> Self {
        Self {
            mtu,
            receive: VecDeque::new(),
            receive_limit,
            transmit: VecDeque::new(),
            transmit_bytes: 0,
            transmit_limit: 0,
            credit,
        }
    }

    /// Adds granted egress. Returns false when egress is unlimited.
    pub fn grant_egress(&mut self, packets: usize, bytes: usize) -> bool {
        let Some(credit) = self.credit.as_mut() else {
            return false;
        };

        credit.packets = credit.packets.saturating_add(packets);
        credit.bytes = credit.bytes.saturating_add(bytes);
        true
    }

    pub fn egress_credit(&self) -> Option<EgressCredit> {
        self.credit
    }

    pub fn begin_call(&mut self, transmit_limit: usize) {
        self.transmit_limit = transmit_limit;
    }

    pub fn enqueue_receive_batch(&mut self, packets: Vec<Vec<u8>>) -> Result<(), ()> {
        let Some(queued) = self.receive.len().checked_add(packets.len()) else {
            return Err(());
        };

        if queued > self.receive_limit {
            return Err(());
        }

        self.receive.extend(packets);
        Ok(())
    }

    pub fn take_receive(&mut self) -> Option<Vec<u8>> {
        self.receive.pop_front()
    }

    pub fn return_receive(&mut self, packet: Vec<u8>) {
        self.receive.push_front(packet);
    }

    pub fn take_transmit(
        &mut self,
        packet_limit: usize,
        byte_limit: usize,
        budget: &mut crate::budget::CallBudget,
    ) -> Vec<OutputPacket> {
        let (packet_limit, byte_limit) = match self.credit {
            Some(credit) => (
                packet_limit.min(credit.packets),
                byte_limit.min(credit.bytes),
            ),
            None => (packet_limit, byte_limit),
        };
        let mut packets = Vec::new();
        let mut bytes = 0usize;

        while packets.len() < packet_limit {
            let Some(packet) = self.transmit.front() else {
                break;
            };

            let Some(next_bytes) = bytes.checked_add(packet.len()) else {
                break;
            };

            if next_bytes > byte_limit {
                break;
            }

            if !budget.checkpoint() {
                break;
            }

            bytes = next_bytes;
            packets.push(self.transmit.pop_front().expect("front packet exists"));
        }

        self.transmit_bytes -= bytes;
        if let Some(credit) = self.credit.as_mut() {
            credit.packets -= packets.len();
            credit.bytes -= bytes;
        }

        packets
    }

    /// Whether the transmit queue may grow within the remaining credit.
    ///
    /// Taking packets reduces the queue and the credit by the same amount, so
    /// this does not change when the stack hands packets to its link.
    pub fn transmit_credit_available(&self) -> bool {
        self.credit.is_none_or(|credit| {
            self.transmit.len() < credit.packets && self.transmit_bytes < credit.bytes
        })
    }

    /// Whether the next queued packet may be handed to the link now.
    pub fn transmit_ready(&self) -> bool {
        self.transmit.front().is_some_and(|packet| {
            self.credit
                .is_none_or(|credit| credit.packets > 0 && packet.len() <= credit.bytes)
        })
    }

    pub fn has_receive(&self) -> bool {
        !self.receive.is_empty()
    }

    pub fn can_receive(&self) -> bool {
        self.transmit.len() < self.transmit_limit
    }

    pub fn queued_packets(&self) -> (usize, usize) {
        (self.receive.len(), self.transmit.len())
    }

    pub fn discard_one(&mut self) -> bool {
        if self.receive.pop_front().is_some() {
            true
        } else if let Some(packet) = self.transmit.pop_front() {
            self.transmit_bytes -= packet.len();
            true
        } else {
            false
        }
    }

    #[cfg(debug_assertions)]
    pub fn test_fill_transmit(
        &mut self,
        packet_count: usize,
        packet_size: usize,
    ) -> Result<(), ()> {
        if !self.transmit.is_empty() {
            return Err(());
        }

        self.transmit
            .extend((0..packet_count).map(|_| OutputPacket::zeroed(packet_size)));
        self.transmit_bytes = packet_count * packet_size;
        Ok(())
    }
}

pub struct BeamRxToken(Vec<u8>);

impl RxToken for BeamRxToken {
    fn consume<R, F>(self, operation: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        operation(&self.0)
    }
}

pub struct BeamTxToken<'a> {
    queue: &'a mut VecDeque<OutputPacket>,
    bytes: &'a mut usize,
}

impl TxToken for BeamTxToken<'_> {
    fn consume<R, F>(self, length: usize, operation: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut packet = OutputPacket::zeroed(length);
        let result = operation(packet.as_mut_slice());
        *self.bytes += packet.len();
        self.queue.push_back(packet);
        result
    }
}

impl BeamDevice {
    fn tx_token(&mut self) -> BeamTxToken<'_> {
        BeamTxToken {
            queue: &mut self.transmit,
            bytes: &mut self.transmit_bytes,
        }
    }
}

impl Device for BeamDevice {
    type RxToken<'a> = BeamRxToken;
    type TxToken<'a> = BeamTxToken<'a>;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.transmit.len() >= self.transmit_limit {
            return None;
        }

        // A reply to an ingress packet may use the whole transmit queue, even
        // without credit, so that ingress is not refused while the link holds
        // egress back. It waits in the queue until credit arrives.
        let packet = self.receive.pop_front()?;
        Some((BeamRxToken(packet), self.tx_token()))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        (self.transmit.len() < self.transmit_limit && self.transmit_credit_available())
            .then(|| self.tx_token())
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.medium = Medium::Ip;
        capabilities.max_transmission_unit = self.mtu;
        // smoltcp clamps every advertised TCP window to max_burst_size segments,
        // for devices whose fixed receive rings silently drop a full window.
        // Ingress here is serial and reports backpressure to the link, so the
        // per-socket receive buffer is the flow-control limit. Leave it unset.
        capabilities.max_burst_size = None;
        capabilities
    }
}

#[cfg(test)]
mod tests {
    use smoltcp::phy::{Device, Medium, TxToken};

    use super::{BeamDevice, EgressCredit};
    use crate::budget::CallBudget;

    #[test]
    fn reports_raw_ip_capabilities() {
        let device = BeamDevice::new(1_500, 4, None);
        let capabilities = device.capabilities();

        assert_eq!(capabilities.medium, Medium::Ip);
        assert_eq!(capabilities.max_transmission_unit, 1_500);
        assert_eq!(capabilities.max_burst_size, None);
    }

    #[test]
    fn receive_and_transmit_queues_are_explicitly_bounded() {
        let mut device = BeamDevice::new(1_500, 2, None);
        device.begin_call(1);

        assert_eq!(
            device.enqueue_receive_batch(vec![vec![0; 40], vec![0; 40]]),
            Ok(())
        );
        assert_eq!(device.enqueue_receive_batch(vec![vec![0; 40]]), Err(()));

        let (_rx, tx) = device.receive(smoltcp::time::Instant::ZERO).unwrap();
        tx.consume(40, |packet| packet[0] = 0x60);

        assert!(device.transmit(smoltcp::time::Instant::ZERO).is_none());
        let mut budget = CallBudget::start(None);
        assert!(device.take_transmit(1, 39, &mut budget).is_empty());

        let mut expected = vec![0; 40];
        expected[0] = 0x60;
        let packets = device.take_transmit(1, 40, &mut budget);
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].as_slice(), expected);
        assert!(device.receive(smoltcp::time::Instant::ZERO).is_some());
    }

    #[test]
    fn transmit_waits_for_packet_and_byte_credit() {
        let credit = EgressCredit {
            packets: 2,
            bytes: 100,
        };
        let mut device = BeamDevice::new(1_500, 1, Some(credit));
        device.begin_call(8);

        device
            .transmit(smoltcp::time::Instant::ZERO)
            .unwrap()
            .consume(60, |_| ());
        device
            .transmit(smoltcp::time::Instant::ZERO)
            .unwrap()
            .consume(60, |_| ());
        // Two packets fill the packet credit and overrun the byte credit.
        assert!(device.transmit(smoltcp::time::Instant::ZERO).is_none());
        assert!(!device.transmit_credit_available());

        assert_eq!(take(&mut device, 8), 1);
        assert_eq!(
            device.egress_credit(),
            Some(EgressCredit {
                packets: 1,
                bytes: 40
            })
        );
        // The second packet is larger than the remaining byte credit.
        assert!(!device.transmit_ready());
        assert_eq!(take(&mut device, 8), 0);

        assert!(device.grant_egress(0, 20));
        assert!(device.transmit_ready());
        assert_eq!(take(&mut device, 8), 1);
        assert_eq!(
            device.egress_credit(),
            Some(EgressCredit {
                packets: 0,
                bytes: 0
            })
        );
        assert!(device.transmit(smoltcp::time::Instant::ZERO).is_none());
    }

    #[test]
    fn ingress_replies_may_queue_beyond_credit() {
        let credit = EgressCredit {
            packets: 0,
            bytes: 0,
        };
        let mut device = BeamDevice::new(1_500, 1, Some(credit));
        device.begin_call(1);

        assert_eq!(device.enqueue_receive_batch(vec![vec![0; 40]]), Ok(()));
        let (_rx, tx) = device.receive(smoltcp::time::Instant::ZERO).unwrap();
        tx.consume(40, |_| ());

        assert_eq!(device.queued_packets(), (0, 1));
        assert!(!device.transmit_ready());
        assert_eq!(take(&mut device, 1), 0);

        assert!(device.grant_egress(1, 40));
        assert_eq!(take(&mut device, 1), 1);
    }

    // A fresh budget per call: the work budget is wall-clock time, and a
    // preempted test thread would otherwise see it expire mid-test.
    fn take(device: &mut BeamDevice, packet_limit: usize) -> usize {
        device
            .take_transmit(packet_limit, 1_500, &mut CallBudget::start(None))
            .len()
    }

    #[test]
    fn unlimited_egress_refuses_grants() {
        let mut device = BeamDevice::new(1_500, 1, None);

        assert!(!device.grant_egress(1, 1));
        assert_eq!(device.egress_credit(), None);
        assert!(device.transmit_credit_available());
    }
}
