use std::collections::VecDeque;

#[cfg(any(test, feature = "fuzzing"))]
use rustler::NewBinary;
#[cfg(all(not(test), not(feature = "fuzzing")))]
use rustler::OwnedBinary;
use rustler::{Env, Term};
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

pub struct BeamDevice {
    mtu: usize,
    receive: VecDeque<Vec<u8>>,
    receive_limit: usize,
    transmit: VecDeque<OutputPacket>,
    transmit_limit: usize,
}

impl BeamDevice {
    pub fn new(mtu: usize, receive_limit: usize) -> Self {
        Self {
            mtu,
            receive: VecDeque::new(),
            receive_limit,
            transmit: VecDeque::new(),
            transmit_limit: 0,
        }
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

        packets
    }

    pub fn has_transmit(&self) -> bool {
        !self.transmit.is_empty()
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
        } else {
            self.transmit.pop_front().is_some()
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

pub struct BeamTxToken<'a>(&'a mut VecDeque<OutputPacket>);

impl TxToken for BeamTxToken<'_> {
    fn consume<R, F>(self, length: usize, operation: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut packet = OutputPacket::zeroed(length);
        let result = operation(packet.as_mut_slice());
        self.0.push_back(packet);
        result
    }
}

impl Device for BeamDevice {
    type RxToken<'a> = BeamRxToken;
    type TxToken<'a> = BeamTxToken<'a>;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.transmit.len() >= self.transmit_limit {
            return None;
        }

        let packet = self.receive.pop_front()?;
        Some((BeamRxToken(packet), BeamTxToken(&mut self.transmit)))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        (self.transmit.len() < self.transmit_limit).then_some(BeamTxToken(&mut self.transmit))
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.medium = Medium::Ip;
        capabilities.max_transmission_unit = self.mtu;
        // smoltcp uses this capability to clamp the advertised TCP receive
        // window. Ingress batching is an ABI boundary optimization, not a TCP
        // flow-control setting, so preserve the established one-segment value.
        capabilities.max_burst_size = Some(1);
        capabilities
    }
}

#[cfg(test)]
mod tests {
    use smoltcp::phy::{Device, Medium, TxToken};

    use super::BeamDevice;
    use crate::budget::CallBudget;

    #[test]
    fn reports_raw_ip_capabilities() {
        let device = BeamDevice::new(1_500, 4);
        let capabilities = device.capabilities();

        assert_eq!(capabilities.medium, Medium::Ip);
        assert_eq!(capabilities.max_transmission_unit, 1_500);
        assert_eq!(capabilities.max_burst_size, Some(1));
    }

    #[test]
    fn receive_and_transmit_queues_are_explicitly_bounded() {
        let mut device = BeamDevice::new(1_500, 2);
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
}
