use openraft::async_runtime::{mpsc, mpsc_unbounded, mutex, oneshot, watch};
use openraft::type_config::OneshotSender;
use openraft::{OptionalSend, OptionalSync};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::watch as tokio_watch;

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

impl<T> mpsc_unbounded::MpscUnboundedSender<SimMpscUnbounded, T> for SimMpscUnboundedSender<T>
where
    T: OptionalSend,
{
    fn send(&self, msg: T) -> Result<(), mpsc_unbounded::SendError<T>> {
        self.0.send(msg).map_err(|e| mpsc_unbounded::SendError(e.0))
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
            tokio_mpsc::error::TryRecvError::Disconnected => {
                mpsc_unbounded::TryRecvError::Disconnected
            }
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
