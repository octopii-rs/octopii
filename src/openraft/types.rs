#![cfg(feature = "openraft")]

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppEntry(pub Vec<u8>);

impl std::fmt::Display for AppEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AppEntry({} bytes)", self.0.len())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppResponse(pub Vec<u8>);

pub type AppSnapshot = Vec<u8>;

pub type AppNodeId = u64;

#[cfg(feature = "simulation")]
openraft::declare_raft_types!(
    pub AppTypeConfig:
        D = AppEntry,
        R = AppResponse,
        NodeId = AppNodeId,
        AsyncRuntime = crate::sim_runtime::SimRuntime,
);

#[cfg(not(feature = "simulation"))]
openraft::declare_raft_types!(
    pub AppTypeConfig:
        D = AppEntry,
        R = AppResponse,
        NodeId = AppNodeId,
);
