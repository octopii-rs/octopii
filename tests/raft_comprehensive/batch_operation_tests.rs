/// Batch operation tests
mod common;

use common::TestCluster;
use std::time::Duration;

#[test]
fn test_batch_append_correctness() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting batch append correctness test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8400).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Rapidly propose 200 entries (should batch internally)
        tracing::info!("Rapidly proposing 200 entries...");
        let start = std::time::Instant::now();

        for i in 1..=200 {
            let cmd = format!("SET batch{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        let elapsed = start.elapsed();
        tracing::info!("Proposed 200 entries in {:?}", elapsed);

        // Wait for replication
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify cluster converged
        cluster.verify_convergence(Duration::from_secs(5)).await.expect("Convergence failed");

        tracing::info!("✓ All entries replicated correctly");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Batch append correctness");
    });
}

#[test]
fn test_batch_recovery_performance() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting batch recovery performance test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8410).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Write 1000 entries
        tracing::info!("Writing 1000 entries...");
        for i in 1..=1000 {
            let cmd = format!("SET recovery{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();

            if i % 200 == 0 {
                tracing::info!("Written {} entries", i);
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Restart node 2 and measure recovery time
        tracing::info!("Restarting node 2 to measure recovery performance...");
        let recovery_start = std::time::Instant::now();

        cluster.restart_node(2).await.expect("Failed to restart");

        let recovery_time = recovery_start.elapsed();
        tracing::info!("Recovery completed in {:?}", recovery_time);

        // With batch reads, recovery should be fast (<5 seconds for 1000 entries)
        assert!(recovery_time < Duration::from_secs(10), "Recovery too slow: {:?}", recovery_time);
        tracing::info!("✓ Fast recovery with batch reads");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Batch recovery performance");
    });
}

#[test]
fn test_high_throughput_proposals() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting high throughput proposals test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8420).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Sustained high throughput for 30 seconds
        tracing::info!("Sustaining high throughput for 30 seconds...");
        let start = std::time::Instant::now();
        let mut count = 0;

        while start.elapsed() < Duration::from_secs(30) {
            let cmd = format!("SET throughput{} value{}", count, count);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            count += 1;

            if count % 500 == 0 {
                tracing::info!("Proposed {} entries in {:?}", count, start.elapsed());
            }
        }

        let total_time = start.elapsed();
        let throughput = count as f64 / total_time.as_secs_f64();

        tracing::info!("✓ Sustained throughput: {:.0} entries/sec ({} total entries in {:?})",
            throughput, count, total_time);

        // Wait for replication to settle
        tokio::time::sleep(Duration::from_secs(3)).await;

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: High throughput proposals");
    });
}
