use crate::chunk::{ChunkSource, TransferResult};
use crate::error::Result;
use crate::sim_time;
use crate::transport::Transport;
use bytes::Bytes;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

pub struct ShippingLane {
    transport: Arc<dyn Transport>,
}

impl ShippingLane {
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self { transport }
    }

    async fn send_chunk(&self, addr: SocketAddr, chunk: ChunkSource) -> Result<TransferResult> {
        let peer = self.transport.connect(addr).await?;
        let start = sim_time::now();
        match peer.send_chunk_verified(&chunk).await {
            Ok(bytes) => Ok(TransferResult::success(
                addr,
                bytes,
                sim_time::elapsed(start),
            )),
            Err(err) => Ok(TransferResult::failure(addr, err.to_string())),
        }
    }

    pub async fn send_file<P: AsRef<Path>>(
        &self,
        addr: SocketAddr,
        path: P,
    ) -> Result<TransferResult> {
        let chunk = ChunkSource::File(path.as_ref().to_path_buf());
        self.send_chunk(addr, chunk).await
    }

    pub async fn receive_file<P: AsRef<Path>>(
        &self,
        addr: SocketAddr,
        dest: P,
    ) -> Result<TransferResult> {
        let peer = self.transport.connect(addr).await?;
        let start = sim_time::now();
        match peer.recv_chunk_verified_to_file(dest.as_ref()).await? {
            Some(bytes) => Ok(TransferResult::success(
                addr,
                bytes,
                sim_time::elapsed(start),
            )),
            None => Ok(TransferResult::failure(
                addr,
                "connection closed before data transfer".to_string(),
            )),
        }
    }

    pub async fn send_memory(&self, addr: SocketAddr, payload: Bytes) -> Result<TransferResult> {
        let chunk = ChunkSource::Memory(payload);
        self.send_chunk(addr, chunk).await
    }

    pub async fn receive_memory(
        &self,
        addr: SocketAddr,
    ) -> Result<(TransferResult, Option<Bytes>)> {
        let peer = self.transport.connect(addr).await?;
        let start = sim_time::now();
        match peer.recv_chunk_verified().await {
            Ok(Some(bytes)) => {
                let result =
                    TransferResult::success(addr, bytes.len() as u64, sim_time::elapsed(start));
                Ok((result, Some(bytes)))
            }
            Ok(None) => Ok((
                TransferResult::failure(addr, "connection closed before data".to_string()),
                None,
            )),
            Err(err) => Ok((TransferResult::failure(addr, err.to_string()), None)),
        }
    }
}
