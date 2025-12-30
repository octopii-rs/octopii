#[cfg(feature = "simulation")]
use std::process::{Child, Command};

#[cfg(feature = "simulation")]
use std::time::Duration;

#[cfg(feature = "simulation")]
struct Args {
    child: bool,
    seed: Option<u64>,
    seeds: Vec<u64>,
    seed_start: Option<u64>,
    seed_count: Option<usize>,
    jobs: usize,
    iterations: usize,
    error_rate: f64,
    partial_writes: bool,
    progress_every: Option<usize>,
    quiet_walrus: bool,
    verbose_sim: bool,
}

#[cfg(feature = "simulation")]
fn parse_args() -> Result<Args, String> {
    let mut child = false;
    let mut seed = None;
    let mut seeds = Vec::new();
    let mut seed_start = None;
    let mut seed_count = None;
    let mut jobs = std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1);
    let mut iterations = 5000;
    let mut error_rate = 0.0;
    let mut partial_writes = false;
    let mut progress_every = None;
    let mut quiet_walrus = false;
    let mut verbose_sim = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--child" => child = true,
            "--seed" => {
                let val = args.next().ok_or("--seed requires a value")?;
                seed = Some(val.parse::<u64>().map_err(|_| "--seed must be u64")?);
            }
            "--seeds" => {
                let val = args.next().ok_or("--seeds requires a value")?;
                for part in val.split(',') {
                    if part.trim().is_empty() {
                        continue;
                    }
                    seeds.push(part.trim().parse::<u64>().map_err(|_| "--seeds must be u64 list")?);
                }
            }
            "--seed-start" => {
                let val = args.next().ok_or("--seed-start requires a value")?;
                seed_start = Some(val.parse::<u64>().map_err(|_| "--seed-start must be u64")?);
            }
            "--seed-count" => {
                let val = args.next().ok_or("--seed-count requires a value")?;
                seed_count = Some(val.parse::<usize>().map_err(|_| "--seed-count must be usize")?);
            }
            "--jobs" => {
                let val = args.next().ok_or("--jobs requires a value")?;
                jobs = val.parse::<usize>().map_err(|_| "--jobs must be usize")?;
                if jobs == 0 {
                    return Err("--jobs must be >= 1".to_string());
                }
            }
            "--iterations" => {
                let val = args.next().ok_or("--iterations requires a value")?;
                iterations = val.parse::<usize>().map_err(|_| "--iterations must be usize")?;
            }
            "--error-rate" => {
                let val = args.next().ok_or("--error-rate requires a value")?;
                error_rate = val.parse::<f64>().map_err(|_| "--error-rate must be f64")?;
            }
            "--partial-writes" => partial_writes = true,
            "--progress-every" => {
                let val = args.next().ok_or("--progress-every requires a value")?;
                let parsed = val.parse::<usize>().map_err(|_| "--progress-every must be usize")?;
                if parsed == 0 {
                    return Err("--progress-every must be >= 1".to_string());
                }
                progress_every = Some(parsed);
            }
            "--quiet-walrus" => quiet_walrus = true,
            "--verbose-sim" => verbose_sim = true,
            "--help" | "-h" => {
                return Err(String::new());
            }
            other => return Err(format!("unknown argument: {}", other)),
        }
    }

    Ok(Args {
        child,
        seed,
        seeds,
        seed_start,
        seed_count,
        jobs,
        iterations,
        error_rate,
        partial_writes,
        progress_every,
        quiet_walrus,
        verbose_sim,
    })
}

#[cfg(feature = "simulation")]
fn print_usage() {
    eprintln!(
        "Usage:
  sim_runner --seeds 1,2,3 [--jobs N] [--iterations N] [--error-rate R] [--partial-writes] [--progress-every N] [--quiet-walrus]
  sim_runner --seed-start S --seed-count N [--jobs N] [--iterations N] [--error-rate R] [--partial-writes] [--progress-every N] [--quiet-walrus]
  Flags: --verbose-sim to enable simulation logs"
    );
}

#[cfg(feature = "simulation")]
fn build_seed_list(args: &Args) -> Result<Vec<u64>, String> {
    if !args.seeds.is_empty() {
        return Ok(args.seeds.clone());
    }

    match (args.seed_start, args.seed_count) {
        (Some(start), Some(count)) => Ok((0..count).map(|i| start + i as u64).collect()),
        (None, None) => Err("provide --seeds or --seed-start/--seed-count".to_string()),
        _ => Err("both --seed-start and --seed-count are required".to_string()),
    }
}

#[cfg(feature = "simulation")]
fn spawn_child(seed: u64, args: &Args) -> std::io::Result<Child> {
    let exe = std::env::current_exe()?;
    let mut cmd = Command::new(exe);
    cmd
        .arg("--child")
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--iterations")
        .arg(args.iterations.to_string())
        .arg("--error-rate")
        .arg(args.error_rate.to_string())
        .env("RUST_BACKTRACE", "1");
    if args.partial_writes {
        cmd.arg("--partial-writes");
    }
    if let Some(every) = args.progress_every {
        cmd.env("SIM_PROGRESS_EVERY", every.to_string());
    }
    if args.quiet_walrus {
        cmd.env("WALRUS_QUIET", "1");
    }
    if args.verbose_sim {
        cmd.env("SIM_VERBOSE", "1");
    }
    cmd.spawn()
}

#[cfg(feature = "simulation")]
fn wait_any(running: &mut Vec<(u64, Child)>) -> Option<(u64, std::process::ExitStatus)> {
    for idx in 0..running.len() {
        let (seed, child) = &mut running[idx];
        if let Ok(Some(status)) = child.try_wait() {
            let seed = *seed;
            running.remove(idx);
            return Some((seed, status));
        }
    }
    None
}

#[cfg(feature = "simulation")]
fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(msg) if msg.is_empty() => {
            print_usage();
            std::process::exit(0);
        }
        Err(msg) => {
            eprintln!("error: {}", msg);
            print_usage();
            std::process::exit(2);
        }
    };

    if args.child {
        let seed = args.seed.expect("--seed required for --child");
        octopii::simulation::run_simulation_with_config(
            seed,
            args.iterations,
            args.error_rate,
            args.partial_writes,
        );
        return;
    }

    let seeds = match build_seed_list(&args) {
        Ok(seeds) => seeds,
        Err(msg) => {
            eprintln!("error: {}", msg);
            print_usage();
            std::process::exit(2);
        }
    };

    let mut running: Vec<(u64, Child)> = Vec::new();
    let mut failed = false;

    for seed in seeds {
        while running.len() >= args.jobs {
            if let Some((done_seed, status)) = wait_any(&mut running) {
                if !status.success() {
                    eprintln!("seed {} failed: {}", done_seed, status);
                    failed = true;
                }
            } else {
                std::thread::sleep(Duration::from_millis(100));
            }
        }

        match spawn_child(seed, &args) {
            Ok(child) => running.push((seed, child)),
            Err(err) => {
                eprintln!("failed to spawn seed {}: {}", seed, err);
                failed = true;
            }
        }
    }

    while !running.is_empty() {
        if let Some((done_seed, status)) = wait_any(&mut running) {
            if !status.success() {
                eprintln!("seed {} failed: {}", done_seed, status);
                failed = true;
            }
        } else {
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    if failed {
        std::process::exit(1);
    }
}

#[cfg(not(feature = "simulation"))]
fn main() {
    eprintln!("sim_runner requires --features simulation");
    std::process::exit(2);
}
