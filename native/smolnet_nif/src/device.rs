use std::collections::VecDeque;

use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;

#[derive(Debug)]
pub struct BeamDevice {
    mtu: usize,
    receive: VecDeque<Vec<u8>>,
    transmit: VecDeque<Vec<u8>>,
    transmit_limit: usize,
}

impl BeamDevice {
    pub fn new(mtu: usize) -> Self {
        Self {
            mtu,
            receive: VecDeque::new(),
            transmit: VecDeque::new(),
            transmit_limit: 0,
        }
    }

    pub fn begin_call(&mut self, transmit_limit: usize) {
        self.transmit_limit = transmit_limit;
    }

    pub fn enqueue_receive(&mut self, packet: Vec<u8>) -> Result<(), ()> {
        if !self.receive.is_empty() {
            return Err(());
        }

        self.receive.push_back(packet);
        Ok(())
    }

    pub fn take_transmit(&mut self, packet_limit: usize, byte_limit: usize) -> Vec<Vec<u8>> {
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

    pub fn queued_packets(&self) -> (usize, usize) {
        (self.receive.len(), self.transmit.len())
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

pub struct BeamTxToken<'a>(&'a mut VecDeque<Vec<u8>>);

impl TxToken for BeamTxToken<'_> {
    fn consume<R, F>(self, length: usize, operation: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut packet = vec![0; length];
        let result = operation(&mut packet);
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
        capabilities.max_burst_size = Some(1);
        capabilities
    }
}

#[cfg(test)]
mod tests {
    use smoltcp::phy::{Device, Medium, TxToken};

    use super::BeamDevice;

    #[test]
    fn reports_raw_ip_capabilities() {
        let device = BeamDevice::new(1_500);
        let capabilities = device.capabilities();

        assert_eq!(capabilities.medium, Medium::Ip);
        assert_eq!(capabilities.max_transmission_unit, 1_500);
        assert_eq!(capabilities.max_burst_size, Some(1));
    }

    #[test]
    fn receive_and_transmit_queues_are_explicitly_bounded() {
        let mut device = BeamDevice::new(1_500);
        device.begin_call(1);

        assert_eq!(device.enqueue_receive(vec![0; 40]), Ok(()));
        assert_eq!(device.enqueue_receive(vec![0; 40]), Err(()));

        let (_rx, tx) = device.receive(smoltcp::time::Instant::ZERO).unwrap();
        tx.consume(40, |packet| packet[0] = 0x60);

        assert!(device.transmit(smoltcp::time::Instant::ZERO).is_none());
        assert_eq!(device.take_transmit(1, 39), Vec::<Vec<u8>>::new());

        let mut expected = vec![0; 40];
        expected[0] = 0x60;
        assert_eq!(device.take_transmit(1, 40), vec![expected]);
    }
}
