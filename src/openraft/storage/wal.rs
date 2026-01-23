#![cfg(feature = "openraft")]

use crate::wal::WriteAheadLog;
use bytes::Bytes;
use std::io;
#[cfg(feature = "simulation")]
use std::time::Duration;
#[cfg(feature = "simulation")]
use tokio::task::yield_now;
#[cfg(feature = "simulation")]
use crate::openraft::sim_runtime;

pub(crate) async fn append_wal_record(wal: &WriteAheadLog, data: Bytes) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        for attempt in 0..20 {
            match wal.append(data.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt == 19 {
                        return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                    }
                    sim_runtime::advance_time(Duration::from_millis(10));
                    yield_now().await;
                }
            }
        }
        return Ok(());
    }
    #[cfg(not(feature = "simulation"))]
    {
        wal.append(data)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }
}
