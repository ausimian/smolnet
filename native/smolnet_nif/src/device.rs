use std::collections::VecDeque;

use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::time::Instant;

#[derive(Debug)]
pub struct BeamDevice {
    mtu: usize,
    receive: VecDeque<Vec<u8>>,
    transmit: VecDeque<Vec<u8>>,
}

impl BeamDevice {
    pub fn new(mtu: usize) -> Self {
        Self {
            mtu,
            receive: VecDeque::new(),
            transmit: VecDeque::new(),
        }
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
        let packet = self.receive.pop_front()?;
        Some((BeamRxToken(packet), BeamTxToken(&mut self.transmit)))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(BeamTxToken(&mut self.transmit))
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
    use smoltcp::phy::{Device, Medium};

    use super::BeamDevice;

    #[test]
    fn reports_raw_ip_capabilities() {
        let device = BeamDevice::new(1_500);
        let capabilities = device.capabilities();

        assert_eq!(capabilities.medium, Medium::Ip);
        assert_eq!(capabilities.max_transmission_unit, 1_500);
        assert_eq!(capabilities.max_burst_size, Some(1));
    }
}
