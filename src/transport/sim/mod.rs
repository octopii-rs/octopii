use crate::transport::{Peer, Transport, TransportFut};
use std::net::SocketAddr;
use std::sync::Arc;

mod peer;
mod router;

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub drop_rate: f64,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            drop_rate: 0.0,
            min_delay_ms: 0,
            max_delay_ms: 0,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ReorderConfig {
    pub max_jitter_ms: u64,
    pub probability: f64,
}

pub struct BandwidthCap {
    pub bytes_per_ms: u64,
    pub burst_bytes: u64,
    pub available_bytes: u64,
    pub last_refill_ms: u64,
}

impl BandwidthCap {
    pub fn new(bytes_per_ms: u64, burst_bytes: u64, now_ms: u64) -> Self {
        let burst = burst_bytes.max(bytes_per_ms);
        Self {
            bytes_per_ms,
            burst_bytes: burst,
            available_bytes: burst,
            last_refill_ms: now_ms,
        }
    }

    pub fn refill(&mut self, now_ms: u64) {
        if now_ms <= self.last_refill_ms || self.bytes_per_ms == 0 {
            self.last_refill_ms = now_ms;
            return;
        }
        let delta = now_ms - self.last_refill_ms;
        let add = delta.saturating_mul(self.bytes_per_ms);
        let next = self.available_bytes.saturating_add(add);
        self.available_bytes = next.min(self.burst_bytes);
        self.last_refill_ms = now_ms;
    }
}

#[derive(Default)]
pub struct FaultRule {
    pub drop: bool,
    pub delay_ms: Option<u64>,
    pub reorder: Option<ReorderConfig>,
    pub timeout_ms: Option<u64>,
    pub bandwidth: Option<BandwidthCap>,
}

impl FaultRule {
    pub fn is_empty(&self) -> bool {
        !self.drop
            && self.delay_ms.is_none()
            && self.reorder.is_none()
            && self.timeout_ms.is_none()
            && self.bandwidth.is_none()
    }
}

pub use peer::{SimPeer, SimTransport};
pub use router::SimRouter;

impl Transport for SimTransport {
    fn local_addr(&self) -> crate::error::Result<SocketAddr> {
        SimTransport::local_addr(self)
    }

    fn close(&self) {
        SimTransport::close(self);
    }

    fn is_closed(&self) -> bool {
        SimTransport::is_closed(self)
    }

    fn connect(&self, addr: SocketAddr) -> TransportFut<'_, Arc<dyn Peer>> {
        SimTransport::connect(self, addr)
    }

    fn accept(&self) -> TransportFut<'_, (SocketAddr, Arc<dyn Peer>)> {
        SimTransport::accept(self)
    }
}
