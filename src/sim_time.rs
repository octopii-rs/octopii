use std::future::Future;
use std::time::Duration;
#[cfg(all(feature = "simulation", feature = "openraft"))]
use openraft::Instant as OpenRaftInstant;
#[cfg(all(feature = "simulation", feature = "openraft"))]
use openraft::AsyncRuntime;

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub type Instant = crate::openraft::sim_runtime::SimInstant;
#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub type Instant = tokio::time::Instant;

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub fn now() -> Instant {
    crate::openraft::sim_runtime::now()
}

#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub fn now() -> Instant {
    tokio::time::Instant::now()
}

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub fn sleep(duration: Duration) -> crate::openraft::sim_runtime::SimSleep {
    crate::openraft::sim_runtime::SimRuntime::sleep(duration)
}

#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub fn sleep(duration: Duration) -> tokio::time::Sleep {
    tokio::time::sleep(duration)
}

#[cfg(all(feature = "simulation", feature = "openraft"))]
pub fn timeout<R, F>(
    duration: Duration,
    future: F,
) -> crate::openraft::sim_runtime::SimTimeout<R, F>
where
    F: Future<Output = R> + openraft::OptionalSend,
{
    crate::openraft::sim_runtime::SimRuntime::timeout(duration, future)
}

#[cfg(not(all(feature = "simulation", feature = "openraft")))]
pub fn timeout<R, F>(duration: Duration, future: F) -> tokio::time::Timeout<F>
where
    F: Future<Output = R>,
{
    tokio::time::timeout(duration, future)
}

pub fn elapsed(start: Instant) -> Duration {
    #[cfg(all(feature = "simulation", feature = "openraft"))]
    {
        OpenRaftInstant::elapsed(&start)
    }
    #[cfg(not(all(feature = "simulation", feature = "openraft")))]
    {
        start.elapsed()
    }
}
