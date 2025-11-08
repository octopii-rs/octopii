pub mod runtime;

pub mod chunk;
pub mod config;
pub mod error;
pub mod node;
pub mod raft;
pub mod rpc;
pub mod transport;
pub mod wal;

// Re-export main types
pub use chunk::{ChunkSource, TransferResult};
pub use config::Config;
pub use error::{OctopiiError, Result};
pub use node::OctopiiNode;
pub use runtime::OctopiiRuntime;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_creation() {
        let runtime = OctopiiRuntime::new(2);
        let result = runtime.spawn(async { 42 }).await.unwrap();
        assert_eq!(result, 42);
    }
}
