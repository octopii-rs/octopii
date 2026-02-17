//! Placement group example - deterministic node assignment for sharding.
//!
//! get_placement_group(nodes, hash, size) returns a deterministic ordering
//! of nodes for any given hash. Use this to decide which nodes should hold
//! a piece of data. The application controls the hash (tenant_id, user_id,
//! shard number, etc).

use octopii::get_placement_group;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn main() {
    let nodes: Vec<u64> = vec![1, 2, 3, 4, 5];
    println!("Cluster nodes: {:?}\n", nodes);

    println!("--- Basic Usage ---");
    let key = "user:12345";
    let hash = hash_key(key);
    let pg = get_placement_group(&nodes, hash, 3);
    println!("Key '{}' -> hash {:#x} -> PG {:?}", key, hash, pg);
    println!("  Primary: node {}, Replicas: {:?}\n", pg[0], &pg[1..]);

    println!("--- Different Keys ---");
    for key in ["user:1", "user:2", "user:3", "order:100", "order:200"] {
        let pg = get_placement_group(&nodes, hash_key(key), 3);
        println!("  '{}' -> {:?}", key, pg);
    }
    println!();

    println!("--- Deterministic ---");
    let hash = hash_key("important:data");
    for i in 0..3 {
        let pg = get_placement_group(&nodes, hash, 3);
        println!("  Run {}: {:?}", i + 1, pg);
    }
    println!();

    println!("--- Sharding (8 shards, RF=3) ---");
    for shard_id in 0..8u64 {
        let pg = get_placement_group(&nodes, shard_id, 3);
        println!("  Shard {} -> {:?}", shard_id, pg);
    }
    println!();

    println!("--- Application-Defined Keys ---");
    let tenant_id = 42u64;
    let user_id = 12345u64;
    println!("  Tenant {} -> {:?}", tenant_id, get_placement_group(&nodes, tenant_id, 3));
    println!("  User {} -> {:?}", user_id, get_placement_group(&nodes, user_id, 3));
    println!();

    println!("--- Wraparound (3 nodes, 5 slots) ---");
    let small: Vec<u64> = vec![1, 2, 3];
    println!("  {:?}", get_placement_group(&small, 0xDEAD, 5));
}

fn hash_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}
