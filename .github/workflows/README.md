# GitHub Workflows for Octopii

This directory contains CI/CD workflows for automated testing and quality checks.

## Workflows Overview

### Main CI Workflows

#### `ci.yml` - Quick CI Check
**Triggers:** All pushes and pull requests
**Purpose:** Fast feedback loop for developers
**Duration:** ~5-10 minutes

Runs:
- Code formatting check (`cargo fmt`)
- Linter checks (`cargo clippy`)
- Build verification
- Quick unit tests
- Basic integration smoke tests

Use this for rapid iteration and early issue detection.

---

#### `nightly.yml` - Comprehensive Nightly Tests
**Triggers:** Daily at 2 AM UTC, manual dispatch
**Purpose:** Full test suite execution
**Duration:** ~2-3 hours

Runs all tests in sequence:
1. Unit tests
2. Integration tests
3. Durability and WAL tests
4. Raft cluster tests (single-threaded)
5. Raft comprehensive tests (single-threaded)
6. Stress and performance tests

### Test-Specific Workflows

#### `unit-tests.yml` - Unit Tests
**Triggers:** All pushes and pull requests
**Runs:**
- Library unit tests (`cargo test --lib`)
- Documentation tests
- Code quality checks (fmt, clippy)

---

#### `integration-tests.yml` - Integration Tests
**Triggers:** All pushes and pull requests
**Runs:**
- Transport layer tests
- RPC tests
- Chunk transfer tests
- Chunk error handling tests
- Integration tests

**Parallelization:** Tests run in parallel where safe

---

#### `raft-cluster-tests.yml` - Raft Cluster Tests
**Triggers:** All pushes and pull requests
**Runs:** Basic Raft cluster functionality tests

**⚠️ Special Requirements:**
- Single-threaded execution (`--test-threads=1`)
- Prevents port conflicts and resource contention
- Timeout: 30 minutes

---

#### `raft-comprehensive-tests.yml` - Comprehensive Raft Tests
**Triggers:** All pushes and pull requests
**Runs:** Full Raft test suite including:
- Chaos tests
- Partition tests
- Durability edge cases
- Linearizability tests
- Custom state machine tests
- Learner tests
- Snapshot transfer tests

**⚠️ Special Requirements:**
- Single-threaded execution (`--test-threads=1`)
- Timeout: 60 minutes
- Cleans up test artifacts after completion

---

#### `durability-tests.yml` - Durability and WAL Tests
**Triggers:** All pushes and pull requests
**Runs:**
- WAL (Write-Ahead Log) tests
- Raft durability tests
- Crash recovery tests

**⚠️ Special Requirements:**
- Raft durability tests run single-threaded
- Timeout: 30 minutes total

---

#### `stress-tests.yml` - Stress and Performance Tests
**Triggers:** Pushes to main/develop, PRs, manual dispatch
**Runs:**
- Chunk parallel stress tests
- Chunk mixed scenarios
- Large transfer tests

**Duration:** ~30 minutes per test category
**Note:** Resource-intensive, may be manually triggered for development branches

---

## Self-Hosted Runner Configuration

All workflows use `runs-on: self-hosted` to utilize your infrastructure.

### Runner Requirements

**Minimum specifications:**
- 4 CPU cores (8+ recommended for stress tests)
- 8 GB RAM (16+ GB recommended)
- 50 GB disk space
- Linux OS (Ubuntu 20.04+ or similar)
- Rust toolchain (installed via workflow)

**Network:**
- Open ephemeral ports for Raft cluster communication
- Internet access for dependency downloads

**Cleanup:**
- Workflows automatically clean up test artifacts
- `/tmp/octopii_test_*`, `/tmp/walrus_*`, etc.

### Setting Up Self-Hosted Runners

1. Go to repository Settings → Actions → Runners
2. Click "New self-hosted runner"
3. Follow instructions to download and configure runner
4. Label runners appropriately (e.g., `linux`, `rust-tests`)

## Test Execution Patterns

### Single-Threaded Tests

Certain tests **must** run single-threaded to avoid conflicts:

```bash
cargo test --test raft_cluster_test -- --test-threads=1
cargo test --test raft_comprehensive_test -- --test-threads=1
cargo test --test raft_durability_test -- --test-threads=1
```

**Why?**
- Port allocation conflicts (multiple nodes binding to same ports)
- Shared WAL directory access
- Resource contention in crash recovery scenarios
- Race conditions in cluster state

### Parallel Tests

These tests can safely run in parallel:
- Unit tests (`--lib`)
- Integration tests (transport, RPC, chunk)
- Stress tests (use different resources)

## Timeouts

Each workflow has configured timeouts to prevent hanging:

| Workflow | Timeout |
|----------|---------|
| Unit Tests | 15 min |
| Integration Tests | 20 min |
| Raft Cluster Tests | 30 min |
| Raft Comprehensive Tests | 60 min |
| Durability Tests | 30 min |
| Stress Tests | 30 min each |
| Nightly (All Tests) | ~3 hours |

## Environment Variables

Standard environment variables used across workflows:

```yaml
env:
  RUST_BACKTRACE: 1      # Full backtraces on panic
  RUST_LOG: info         # Logging level (info for CI, warn for quick checks)
```

## Triggering Workflows Manually

Some workflows support manual triggering via `workflow_dispatch`:

1. Go to Actions tab in GitHub
2. Select the workflow
3. Click "Run workflow"
4. Choose branch and parameters

**Workflows with manual trigger:**
- `stress-tests.yml`
- `nightly.yml`

## Debugging Failed Tests

When tests fail in CI:

1. **Check the logs:**
   - Click on the failed job
   - Expand the failing step
   - Look for `RUST_BACKTRACE` output

2. **Reproduce locally:**
   ```bash
   # Run specific test with same flags
   cargo test --test <test_name> -- --test-threads=1 --nocapture

   # Set environment
   export RUST_BACKTRACE=1
   export RUST_LOG=info
   ```

3. **Check cleanup:**
   - Ensure `/tmp` isn't full
   - Verify no leaked processes from previous runs

4. **Resource constraints:**
   - Monitor CPU/memory during test runs
   - Some stress tests may need resource tuning

## Contributing

When adding new tests:

1. **Determine test category:**
   - Unit → `unit-tests.yml`
   - Integration → `integration-tests.yml`
   - Raft cluster → `raft-cluster-tests.yml` (single-threaded)
   - Stress → `stress-tests.yml`

2. **Add to appropriate workflow**

3. **Consider threading:**
   - Does it need `--test-threads=1`?
   - Can it run in parallel with others?

4. **Set appropriate timeout:**
   - Default: 10 minutes
   - Long tests: 30-60 minutes
   - Stress tests: Up to 30 minutes per category

5. **Clean up resources:**
   - Add cleanup step if test creates files
   - Use `/tmp` for temporary data

## Status Badges

Add to your README.md:

```markdown
![CI](https://github.com/nubskr/octopii/workflows/CI/badge.svg)
![Unit Tests](https://github.com/nubskr/octopii/workflows/Unit%20Tests/badge.svg)
![Raft Tests](https://github.com/nubskr/octopii/workflows/Raft%20Comprehensive%20Tests/badge.svg)
```
