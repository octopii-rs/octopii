#![cfg(feature = "openraft")]

use serde::{Deserialize, Serialize};

/// Application entry payload for OpenRaft.
/// Keep it simple: raw bytes of a command.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppEntry(pub Vec<u8>);

/// Application response for writes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppResponse(pub Vec<u8>);

/// Snapshot bytes container.
pub type AppSnapshot = Vec<u8>;

/// Node ID type.
pub type AppNodeId = u64;

/// OpenRaft type configuration for Octopii.
///
/// NOTE: This mirrors OpenRaft's TypeConfig associated types without binding
/// too tightly to details here. Actual trait impls live in storage/network.
pub struct AppTypeConfig;



