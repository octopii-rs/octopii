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
    async fn test_runtime_from_handle() {
        // Test using handle from current runtime (no nested runtime creation)
        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let result = runtime.spawn(async { 42 }).await.unwrap();
        assert_eq!(result, 42);
    }
}
