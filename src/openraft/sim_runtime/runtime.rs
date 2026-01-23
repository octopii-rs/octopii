use super::channels::{
    SimMpsc, SimMpscUnbounded, SimMutex, SimOneshot, SimWatch,
};
use super::time::{
    now, SimInstant, SimSleep, SimThreadRng, SimTimeout, SimTimeoutError,
};
use openraft::AsyncRuntime;
use openraft::OptionalSend;
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SimRuntime;

impl AsyncRuntime for SimRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = tokio::task::JoinHandle<T>;
    type Sleep = SimSleep;
    type Instant = SimInstant;
    type TimeoutError = SimTimeoutError;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = SimTimeout<R, T>;
    type ThreadLocalRng = SimThreadRng;

    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        tokio::task::spawn(future)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        SimSleep::new(now().saturating_add_duration(duration))
    }

    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        SimSleep::new(deadline)
    }

    fn timeout<R, F: Future<Output = R> + OptionalSend>(duration: Duration, future: F) -> Self::Timeout<R, F> {
        let deadline = now().saturating_add_duration(duration);
        SimTimeout::new(deadline.nanos, future)
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        SimTimeout::new(deadline.nanos, future)
    }

    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    fn thread_rng() -> Self::ThreadLocalRng {
        SimThreadRng
    }

    type Mpsc = SimMpsc;
    type MpscUnbounded = SimMpscUnbounded;
    type Watch = SimWatch;
    type Oneshot = SimOneshot;
    type Mutex<T: OptionalSend + 'static> = SimMutex<T>;
}
