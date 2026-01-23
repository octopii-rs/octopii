#[cfg(all(feature = "simulation", feature = "openraft"))]
use openraft::AsyncRuntime;
#[cfg(all(feature = "simulation", feature = "openraft"))]
use openraft::Instant as OpenRaftInstant;
use std::time::Duration;

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub type Instant = crate::sim_runtime::SimInstant;
#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub type Instant = tokio::time::Instant;

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub fn now() -> Instant {
    crate::sim_runtime::now()
}

#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub fn now() -> Instant {
    tokio::time::Instant::now()
}

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub fn sleep(duration: Duration) -> crate::sim_runtime::SimSleep {
    crate::sim_runtime::SimRuntime::sleep(duration)
}

#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub fn sleep(duration: Duration) -> tokio::time::Sleep {
    tokio::time::sleep(duration)
}

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub fn elapsed(start: Instant) -> Duration {
    OpenRaftInstant::elapsed(&start)
}

#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub fn elapsed(start: Instant) -> Duration {
    start.elapsed()
}
