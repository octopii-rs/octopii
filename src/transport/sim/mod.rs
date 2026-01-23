use crate::transport::{Peer, Transport, TransportFut};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;

mod config;
mod fault;
mod peer;
mod router;

pub use config::SimConfig;
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

    fn send(&self, addr: SocketAddr, data: Bytes) -> TransportFut<'_, ()> {
        SimTransport::send(self, addr, data)
    }
}
