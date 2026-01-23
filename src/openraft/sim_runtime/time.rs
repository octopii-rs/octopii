use openraft::OptionalSend;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct SimInstant {
    pub(crate) nanos: u64,
}

impl SimInstant {
    fn from_nanos(nanos: u64) -> Self {
        Self { nanos }
    }

    pub(crate) fn saturating_add_duration(self, duration: Duration) -> Self {
        let add_ns = duration_to_nanos(duration);
        Self {
            nanos: self.nanos.saturating_add(add_ns),
        }
    }
}

impl openraft::Instant for SimInstant {
    fn now() -> Self {
        SimInstant::from_nanos(sim_now_nanos())
    }
}

impl std::ops::Add<Duration> for SimInstant {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        self.saturating_add_duration(rhs)
    }
}

impl std::ops::AddAssign<Duration> for SimInstant {
    fn add_assign(&mut self, rhs: Duration) {
        *self = self.saturating_add_duration(rhs);
    }
}

impl std::ops::Sub<Duration> for SimInstant {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        let sub_ns = duration_to_nanos(rhs);
        Self {
            nanos: self.nanos.saturating_sub(sub_ns),
        }
    }
}

impl std::ops::SubAssign<Duration> for SimInstant {
    fn sub_assign(&mut self, rhs: Duration) {
        let sub_ns = duration_to_nanos(rhs);
        self.nanos = self.nanos.saturating_sub(sub_ns);
    }
}

impl std::ops::Sub<SimInstant> for SimInstant {
    type Output = Duration;

    fn sub(self, rhs: SimInstant) -> Self::Output {
        match self.nanos.cmp(&rhs.nanos) {
            Ordering::Greater | Ordering::Equal => Duration::from_nanos(self.nanos - rhs.nanos),
            Ordering::Less => Duration::from_nanos(0),
        }
    }
}

#[derive(Debug)]
struct SleepEntry {
    deadline_ns: u64,
    waker: Waker,
}

#[derive(Debug)]
struct SimClock {
    now_ns: u64,
    sleepers: HashMap<u64, SleepEntry>,
    rng: StdRng,
}

impl SimClock {
    fn new(seed: u64, now_ns: u64) -> Self {
        Self {
            now_ns,
            sleepers: HashMap::new(),
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

thread_local! {
    static SIM_CLOCK: RefCell<SimClock> = RefCell::new(SimClock::new(1, 0));
}

static SIM_SLEEP_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

pub fn reset(seed: u64, now_ns: u64) {
    SIM_CLOCK.with(|clock| {
        let mut clock = clock.borrow_mut();
        clock.now_ns = now_ns;
        clock.sleepers.clear();
        clock.rng = StdRng::seed_from_u64(seed);
    });
}

/// Advance simulated time by the given duration.
/// Returns the number of sleepers that were woken up.
pub fn advance_time(duration: Duration) -> usize {
    let delta_ns = duration_to_nanos(duration);
    let ready = SIM_CLOCK.with(|clock| {
        let mut clock = clock.borrow_mut();
        clock.now_ns = clock.now_ns.saturating_add(delta_ns);
        let now = clock.now_ns;
        let mut ready = Vec::new();
        clock.sleepers.retain(|_, entry| {
            if entry.deadline_ns <= now {
                ready.push(entry.waker.clone());
                false
            } else {
                true
            }
        });
        ready
    });

    let woken_count = ready.len();
    for waker in ready {
        waker.wake();
    }
    woken_count
}

pub fn now() -> SimInstant {
    SimInstant::from_nanos(sim_now_nanos())
}

fn sim_now_nanos() -> u64 {
    SIM_CLOCK.with(|clock| clock.borrow().now_ns)
}

fn register_sleep(id: u64, deadline_ns: u64, waker: Waker) {
    SIM_CLOCK.with(|clock| {
        let mut clock = clock.borrow_mut();
        clock.sleepers.insert(id, SleepEntry { deadline_ns, waker });
    });
}

fn unregister_sleep(id: u64) {
    SIM_CLOCK.with(|clock| {
        let mut clock = clock.borrow_mut();
        clock.sleepers.remove(&id);
    });
}

fn duration_to_nanos(duration: Duration) -> u64 {
    let ns = duration.as_nanos();
    if ns > u128::from(u64::MAX) {
        u64::MAX
    } else {
        ns as u64
    }
}

#[derive(Debug)]
pub struct SimSleep {
    id: u64,
    deadline_ns: u64,
}

impl SimSleep {
    pub(crate) fn new(deadline: SimInstant) -> Self {
        let id = SIM_SLEEP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            id,
            deadline_ns: deadline.nanos,
        }
    }
}

impl Future for SimSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = sim_now_nanos();
        if now >= self.deadline_ns {
            unregister_sleep(self.id);
            return Poll::Ready(());
        }
        register_sleep(self.id, self.deadline_ns, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for SimSleep {
    fn drop(&mut self) {
        unregister_sleep(self.id);
    }
}

#[derive(Debug)]
pub struct SimTimeoutError;

impl fmt::Display for SimTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "simulation timeout elapsed")
    }
}

pub struct SimTimeout<R, F> {
    pub(crate) deadline_ns: u64,
    pub(crate) future: F,
    pub(crate) _marker: std::marker::PhantomData<fn() -> R>,
    sleep_id: u64,
}

impl<R, F> SimTimeout<R, F> {
    pub(crate) fn new(deadline_ns: u64, future: F) -> Self {
        let sleep_id = SIM_SLEEP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            deadline_ns,
            future,
            _marker: std::marker::PhantomData,
            sleep_id,
        }
    }
}

impl<R, F> Future for SimTimeout<R, F>
where
    F: Future<Output = R> + OptionalSend,
{
    type Output = Result<R, SimTimeoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if sim_now_nanos() >= this.deadline_ns {
            unregister_sleep(this.sleep_id);
            return Poll::Ready(Err(SimTimeoutError));
        }
        let fut = unsafe { Pin::new_unchecked(&mut this.future) };
        match fut.poll(cx) {
            Poll::Ready(value) => {
                unregister_sleep(this.sleep_id);
                Poll::Ready(Ok(value))
            }
            Poll::Pending => {
                // Register a waker so we get woken when the deadline passes
                register_sleep(this.sleep_id, this.deadline_ns, cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl<R, F> Drop for SimTimeout<R, F> {
    fn drop(&mut self) {
        unregister_sleep(self.sleep_id);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SimThreadRng;

impl RngCore for SimThreadRng {
    fn next_u32(&mut self) -> u32 {
        SIM_CLOCK.with(|clock| clock.borrow_mut().rng.next_u32())
    }

    fn next_u64(&mut self) -> u64 {
        SIM_CLOCK.with(|clock| clock.borrow_mut().rng.next_u64())
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        SIM_CLOCK.with(|clock| clock.borrow_mut().rng.fill_bytes(dest))
    }
}
