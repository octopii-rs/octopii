#![cfg(feature = "simulation")]

use openraft::AsyncRuntime;
use openraft::OptionalSend;
use openraft::OptionalSync;
use openraft::async_runtime::mpsc;
use openraft::async_runtime::mpsc_unbounded;
use openraft::async_runtime::mutex;
use openraft::async_runtime::oneshot;
use openraft::async_runtime::watch;
use openraft::type_config::OneshotSender;
use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::watch as tokio_watch;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct SimInstant {
    nanos: u64,
}

impl SimInstant {
    fn from_nanos(nanos: u64) -> Self {
        Self { nanos }
    }

    fn saturating_add_duration(self, duration: Duration) -> Self {
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

pub fn advance_time(duration: Duration) {
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

    for waker in ready {
        waker.wake();
    }
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
    fn new(deadline: SimInstant) -> Self {
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
    deadline_ns: u64,
    future: F,
    _marker: std::marker::PhantomData<fn() -> R>,
}

impl<R, F> Future for SimTimeout<R, F>
where
    F: Future<Output = R> + OptionalSend,
{
    type Output = Result<R, SimTimeoutError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if sim_now_nanos() >= this.deadline_ns {
            return Poll::Ready(Err(SimTimeoutError));
        }
        let fut = unsafe { Pin::new_unchecked(&mut this.future) };
        match fut.poll(cx) {
            Poll::Ready(value) => Poll::Ready(Ok(value)),
            Poll::Pending => Poll::Pending,
        }
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
        SimTimeout {
            deadline_ns: deadline.nanos,
            future,
            _marker: std::marker::PhantomData,
        }
    }

    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(deadline: Self::Instant, future: F) -> Self::Timeout<R, F> {
        SimTimeout {
            deadline_ns: deadline.nanos,
            future,
            _marker: std::marker::PhantomData,
        }
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

pub struct SimMpsc;

impl mpsc::Mpsc for SimMpsc {
    type Sender<T: OptionalSend> = SimMpscSender<T>;
    type Receiver<T: OptionalSend> = SimMpscReceiver<T>;
    type WeakSender<T: OptionalSend> = SimMpscWeakSender<T>;

    fn channel<T: OptionalSend>(buffer: usize) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = tokio_mpsc::channel(buffer);
        (SimMpscSender(tx), SimMpscReceiver(rx))
    }
}

#[derive(Debug)]
pub struct SimMpscSender<T>(tokio_mpsc::Sender<T>);

pub struct SimMpscReceiver<T>(tokio_mpsc::Receiver<T>);

#[derive(Debug)]
pub struct SimMpscWeakSender<T>(tokio_mpsc::WeakSender<T>);

impl<T> Clone for SimMpscSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for SimMpscWeakSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> mpsc::MpscSender<SimMpsc, T> for SimMpscSender<T>
where
    T: OptionalSend,
{
    fn send(&self, msg: T) -> impl Future<Output = Result<(), mpsc::SendError<T>>> + OptionalSend {
        let sender = self.0.clone();
        async move { sender.send(msg).await.map_err(|e| mpsc::SendError(e.0)) }
    }

    fn downgrade(&self) -> <SimMpsc as mpsc::Mpsc>::WeakSender<T> {
        SimMpscWeakSender(self.0.downgrade())
    }
}

impl<T> mpsc::MpscReceiver<T> for SimMpscReceiver<T>
where
    T: OptionalSend,
{
    fn recv(&mut self) -> impl Future<Output = Option<T>> + OptionalSend {
        self.0.recv()
    }

    fn try_recv(&mut self) -> Result<T, mpsc::TryRecvError> {
        self.0.try_recv().map_err(|e| match e {
            tokio_mpsc::error::TryRecvError::Empty => mpsc::TryRecvError::Empty,
            tokio_mpsc::error::TryRecvError::Disconnected => mpsc::TryRecvError::Disconnected,
        })
    }
}

impl<T> mpsc::MpscWeakSender<SimMpsc, T> for SimMpscWeakSender<T>
where
    T: OptionalSend,
{
    fn upgrade(&self) -> Option<<SimMpsc as mpsc::Mpsc>::Sender<T>> {
        self.0.upgrade().map(SimMpscSender)
    }
}

pub struct SimMpscUnbounded;

impl mpsc_unbounded::MpscUnbounded for SimMpscUnbounded {
    type Sender<T: OptionalSend> = SimMpscUnboundedSender<T>;
    type Receiver<T: OptionalSend> = SimMpscUnboundedReceiver<T>;
    type WeakSender<T: OptionalSend> = SimMpscUnboundedWeakSender<T>;

    fn channel<T: OptionalSend>() -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = tokio_mpsc::unbounded_channel();
        (SimMpscUnboundedSender(tx), SimMpscUnboundedReceiver(rx))
    }
}

#[derive(Debug)]
pub struct SimMpscUnboundedSender<T>(tokio_mpsc::UnboundedSender<T>);

pub struct SimMpscUnboundedReceiver<T>(tokio_mpsc::UnboundedReceiver<T>);

#[derive(Debug)]
pub struct SimMpscUnboundedWeakSender<T>(tokio_mpsc::WeakUnboundedSender<T>);

impl<T> Clone for SimMpscUnboundedSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for SimMpscUnboundedWeakSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> mpsc_unbounded::MpscUnboundedSender<SimMpscUnbounded, T>
    for SimMpscUnboundedSender<T>
where
    T: OptionalSend,
{
    fn send(&self, msg: T) -> Result<(), mpsc_unbounded::SendError<T>> {
        self.0
            .send(msg)
            .map_err(|e| mpsc_unbounded::SendError(e.0))
    }

    fn downgrade(&self) -> <SimMpscUnbounded as mpsc_unbounded::MpscUnbounded>::WeakSender<T> {
        SimMpscUnboundedWeakSender(self.0.downgrade())
    }
}

impl<T> mpsc_unbounded::MpscUnboundedReceiver<T> for SimMpscUnboundedReceiver<T>
where
    T: OptionalSend,
{
    async fn recv(&mut self) -> Option<T> {
        self.0.recv().await
    }

    fn try_recv(&mut self) -> Result<T, mpsc_unbounded::TryRecvError> {
        self.0.try_recv().map_err(|e| match e {
            tokio_mpsc::error::TryRecvError::Empty => mpsc_unbounded::TryRecvError::Empty,
            tokio_mpsc::error::TryRecvError::Disconnected => mpsc_unbounded::TryRecvError::Disconnected,
        })
    }
}

impl<T> mpsc_unbounded::MpscUnboundedWeakSender<SimMpscUnbounded, T>
    for SimMpscUnboundedWeakSender<T>
where
    T: OptionalSend,
{
    fn upgrade(&self) -> Option<<SimMpscUnbounded as mpsc_unbounded::MpscUnbounded>::Sender<T>> {
        self.0.upgrade().map(SimMpscUnboundedSender)
    }
}

pub struct SimWatch;

impl watch::Watch for SimWatch {
    type Sender<T: OptionalSend + OptionalSync> = SimWatchSender<T>;
    type Receiver<T: OptionalSend + OptionalSync> = SimWatchReceiver<T>;
    type Ref<'a, T: OptionalSend + 'a> = tokio_watch::Ref<'a, T>;

    fn channel<T: OptionalSend + OptionalSync>(init: T) -> (Self::Sender<T>, Self::Receiver<T>) {
        let (tx, rx) = tokio_watch::channel(init);
        (SimWatchSender(tx), SimWatchReceiver(rx))
    }
}

#[derive(Debug)]
pub struct SimWatchSender<T>(tokio_watch::Sender<T>);

#[derive(Debug)]
pub struct SimWatchReceiver<T>(tokio_watch::Receiver<T>);

impl<T> Clone for SimWatchSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Clone for SimWatchReceiver<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> SimWatchReceiver<T> {
    pub fn borrow(&self) -> tokio_watch::Ref<'_, T> {
        self.0.borrow()
    }
}

impl<T> watch::WatchSender<SimWatch, T> for SimWatchSender<T>
where
    T: OptionalSend + OptionalSync,
{
    fn send(&self, value: T) -> Result<(), watch::SendError<T>> {
        self.0.send(value).map_err(|e| watch::SendError(e.0))
    }

    fn send_if_modified<F>(&self, modify: F) -> bool
    where
        F: FnOnce(&mut T) -> bool,
    {
        self.0.send_if_modified(modify)
    }

    fn borrow_watched(&self) -> <SimWatch as watch::Watch>::Ref<'_, T> {
        self.0.borrow()
    }
}

impl<T> watch::WatchReceiver<SimWatch, T> for SimWatchReceiver<T>
where
    T: OptionalSend + OptionalSync,
{
    async fn changed(&mut self) -> Result<(), watch::RecvError> {
        self.0.changed().await.map_err(|_| watch::RecvError(()))
    }

    fn borrow_watched(&self) -> <SimWatch as watch::Watch>::Ref<'_, T> {
        self.0.borrow()
    }
}

pub struct SimOneshot;

impl oneshot::Oneshot for SimOneshot {
    type Sender<T: OptionalSend> = SimOneshotSender<T>;
    type Receiver<T: OptionalSend> = SimOneshotReceiver<T>;
    type ReceiverError = tokio::sync::oneshot::error::RecvError;

    fn channel<T>() -> (Self::Sender<T>, Self::Receiver<T>)
    where
        T: OptionalSend,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (SimOneshotSender(tx), SimOneshotReceiver(rx))
    }
}

pub struct SimOneshotSender<T>(tokio::sync::oneshot::Sender<T>);

pub struct SimOneshotReceiver<T>(tokio::sync::oneshot::Receiver<T>);

impl<T> Future for SimOneshotReceiver<T> {
    type Output = Result<T, tokio::sync::oneshot::error::RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll(cx)
    }
}

impl<T> Unpin for SimOneshotReceiver<T> {}

impl<T> OneshotSender<T> for SimOneshotSender<T>
where
    T: OptionalSend,
{
    fn send(self, t: T) -> Result<(), T> {
        self.0.send(t)
    }
}

pub struct SimMutex<T>(tokio::sync::Mutex<T>);

impl<T> mutex::Mutex<T> for SimMutex<T>
where
    T: OptionalSend + 'static,
{
    type Guard<'a> = tokio::sync::MutexGuard<'a, T>;

    fn new(value: T) -> Self {
        SimMutex(tokio::sync::Mutex::new(value))
    }

    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + OptionalSend {
        self.0.lock()
    }
}
