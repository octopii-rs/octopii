/// Returns a deterministic placement group for the given hash.
/// The same hash always produces the same ordering of nodes.
///
/// Uses a custom Fisher-Yates shuffle with a simple splitmix64 PRNG
/// to guarantee identical results on any platform/version.
pub fn get_placement_group(nodes: &[u64], hash: u64, size: usize) -> Vec<u64> {
    if nodes.is_empty() || size == 0 {
        return Vec::new();
    }

    let mut shuffled = nodes.to_vec();
    let mut state = hash;

    // Fisher-Yates shuffle with splitmix64 PRNG
    for i in (1..shuffled.len()).rev() {
        state = splitmix64(state);
        let j = (state as usize) % (i + 1);
        shuffled.swap(i, j);
    }

    (0..size).map(|i| shuffled[i % shuffled.len()]).collect()
}

/// splitmix64 - simple, fast, well-defined PRNG
/// https://prng.di.unimi.it/splitmix64.c
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic() {
        let nodes = vec![1, 2, 3, 4, 5];
        let pg1 = get_placement_group(&nodes, 0xABCD, 3);
        let pg2 = get_placement_group(&nodes, 0xABCD, 3);
        assert_eq!(pg1, pg2);
    }

    #[test]
    fn test_different_hash_different_result() {
        let nodes = vec![1, 2, 3, 4, 5];
        let pg1 = get_placement_group(&nodes, 0xABCD, 3);
        let pg2 = get_placement_group(&nodes, 0xDEAD, 3);
        assert_ne!(pg1, pg2);
    }

    #[test]
    fn test_wraparound() {
        let nodes = vec![1, 2, 3, 4, 5];
        let pg = get_placement_group(&nodes, 42, 7);
        assert_eq!(pg.len(), 7);
        // First 5 unique, then wraps
        assert_eq!(pg[5], pg[0]);
        assert_eq!(pg[6], pg[1]);
    }

    #[test]
    fn test_size_larger_than_nodes() {
        let nodes = vec![1, 2];
        let pg = get_placement_group(&nodes, 99, 5);
        assert_eq!(pg.len(), 5);
    }

    #[test]
    fn test_empty_nodes() {
        let pg = get_placement_group(&[], 123, 3);
        assert!(pg.is_empty());
    }

    #[test]
    fn test_zero_size() {
        let nodes = vec![1, 2, 3];
        let pg = get_placement_group(&nodes, 123, 0);
        assert!(pg.is_empty());
    }

    #[test]
    fn test_uniform_distribution_3_nodes() {
        let nodes = vec![1, 2, 3];
        let num_samples = 100_000u64;
        let mut counts = std::collections::HashMap::new();

        for hash in 0..num_samples {
            let pg = get_placement_group(&nodes, hash, 1);
            *counts.entry(pg[0]).or_insert(0u64) += 1;
        }

        let expected = num_samples as f64 / nodes.len() as f64;

        // Chi-squared test: sum of (observed - expected)^2 / expected
        // For 2 degrees of freedom (3 nodes - 1), critical value at p=0.01 is 9.21
        let chi_squared: f64 = nodes
            .iter()
            .map(|n| {
                let observed = *counts.get(n).unwrap_or(&0) as f64;
                (observed - expected).powi(2) / expected
            })
            .sum();

        println!("Distribution for 3 nodes over {} samples:", num_samples);
        for node in &nodes {
            let count = counts.get(node).unwrap_or(&0);
            let pct = (*count as f64 / num_samples as f64) * 100.0;
            println!("  Node {}: {} ({:.2}%)", node, count, pct);
        }
        println!("Chi-squared: {:.4} (critical value at p=0.01: 9.21)", chi_squared);

        assert!(
            chi_squared < 9.21,
            "Distribution not uniform! Chi-squared {} >= 9.21",
            chi_squared
        );
    }

    #[test]
    fn test_uniform_distribution_5_nodes() {
        let nodes = vec![1, 2, 3, 4, 5];
        let num_samples = 100_000u64;
        let mut counts = std::collections::HashMap::new();

        for hash in 0..num_samples {
            let pg = get_placement_group(&nodes, hash, 1);
            *counts.entry(pg[0]).or_insert(0u64) += 1;
        }

        let expected = num_samples as f64 / nodes.len() as f64;

        // For 4 degrees of freedom (5 nodes - 1), critical value at p=0.01 is 13.28
        let chi_squared: f64 = nodes
            .iter()
            .map(|n| {
                let observed = *counts.get(n).unwrap_or(&0) as f64;
                (observed - expected).powi(2) / expected
            })
            .sum();

        println!("Distribution for 5 nodes over {} samples:", num_samples);
        for node in &nodes {
            let count = counts.get(node).unwrap_or(&0);
            let pct = (*count as f64 / num_samples as f64) * 100.0;
            println!("  Node {}: {} ({:.2}%)", node, count, pct);
        }
        println!("Chi-squared: {:.4} (critical value at p=0.01: 13.28)", chi_squared);

        assert!(
            chi_squared < 13.28,
            "Distribution not uniform! Chi-squared {} >= 13.28",
            chi_squared
        );
    }

    #[test]
    fn test_uniform_distribution_random_hashes() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let nodes = vec![1, 2, 3];
        let num_samples = 100_000usize;
        let mut counts = std::collections::HashMap::new();

        // Use random-ish string keys like real usage
        for i in 0..num_samples {
            let key = format!("user:{}:profile:data:key{}", i * 7 + 13, i);
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let hash = hasher.finish();

            let pg = get_placement_group(&nodes, hash, 1);
            *counts.entry(pg[0]).or_insert(0u64) += 1;
        }

        let expected = num_samples as f64 / nodes.len() as f64;

        let chi_squared: f64 = nodes
            .iter()
            .map(|n| {
                let observed = *counts.get(n).unwrap_or(&0) as f64;
                (observed - expected).powi(2) / expected
            })
            .sum();

        println!("Distribution for 3 nodes with string keys over {} samples:", num_samples);
        for node in &nodes {
            let count = counts.get(node).unwrap_or(&0);
            let pct = (*count as f64 / num_samples as f64) * 100.0;
            println!("  Node {}: {} ({:.2}%)", node, count, pct);
        }
        println!("Chi-squared: {:.4} (critical value at p=0.01: 9.21)", chi_squared);

        assert!(
            chi_squared < 9.21,
            "Distribution not uniform! Chi-squared {} >= 9.21",
            chi_squared
        );
    }
}
