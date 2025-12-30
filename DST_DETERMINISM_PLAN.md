# Deterministic Simulation Plan

## Goal
Make simulation mode fully deterministic by eliminating real OS I/O and
non-deterministic scheduling/order effects.

## Plan (checkboxes)
- [x] Map sim-mode I/O touchpoints and background-thread entry points.
- [ ] Add an in-memory VFS backend for simulation (files + dirs + metadata + deterministic faults).
- [ ] Route sim paths to the in-memory backend (paths, storage, recovery, cleanup).
- [ ] Determinism hardening (ordered iteration, no background threads, sim-only time/RNG).
- [ ] Validation: deterministic replay test + cross-run consistency tests.

## I/O touchpoints (summary)
### Core WAL paths
- `src/wal/wal/vfs.rs`: wraps `std::fs::File`, `OpenOptions`, and directory ops.
- `src/wal/wal/paths.rs`: creates/extends WAL files, syncs directories.
- `src/wal/wal/storage.rs`: `SharedMmap` + fd backend use real files and metadata.
- `src/wal/wal/runtime/walrus.rs`: recovery scans directory entries and reads files.
- `src/wal/wal/runtime/index.rs`: syncs index files.

### Simulation harness
- `src/simulation.rs`: uses temp dirs, remove_dir_all, read_dir, remove_file.
- `tests/simulation.rs`: temp dirs + cleanup (real fs).

### Background and ordering
- Background worker thread is disabled in sim (manual ticks).
- Recovery uses `read_dir` order, which is nondeterministic across OS/filesystems.
