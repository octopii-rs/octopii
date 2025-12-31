use crate::error::{OctopiiError, Result};
use crate::transport::{Peer, Transport, TransportFut};
use bytes::Bytes;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub seed: u64,
    pub drop_rate: f64,
    pub min_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            drop_rate: 0.0,
            min_delay_ms: 0,
            max_delay_ms: 0,
        }
    }
}

#[derive(Clone)]
pub struct SimRouter {
    inner: Arc<Mutex<SimRouterInner>>,
}

impl SimRouter {
    pub fn new(config: SimConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimRouterInner::new(config))),
        }
    }

    pub fn register(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.nodes.entry(addr).or_insert_with(NodeQueue::new);
    }

    pub fn close(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(node) = inner.nodes.get_mut(&addr) {
            node.closed = true;
            node.notify.notify_waiters();
        }
    }

    pub fn set_drop_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.drop_pairs.insert((from, to));
    }

    pub fn set_delay_pair(&self, from: SocketAddr, to: SocketAddr, delay_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.delay_pairs.insert((from, to), delay_ms);
    }

    pub fn set_reorder_pair(&self, from: SocketAddr, to: SocketAddr, max_jitter_ms: u64, probability: f64) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .reorder_pairs
            .insert((from, to), ReorderConfig { max_jitter_ms, probability });
    }

    pub fn clear_reorder_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.reorder_pairs.remove(&(from, to));
    }

    pub fn set_timeout_pair(&self, from: SocketAddr, to: SocketAddr, timeout_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.timeout_pairs.insert((from, to), timeout_ms);
    }

    pub fn clear_timeout_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.timeout_pairs.remove(&(from, to));
    }

    pub fn set_bandwidth_pair(
        &self,
        from: SocketAddr,
        to: SocketAddr,
        bytes_per_ms: u64,
        burst_bytes: u64,
    ) {
        let mut inner = self.inner.lock().unwrap();
        let now_ms = inner.now_ms;
        inner
            .bandwidth_caps
            .insert((from, to), BandwidthCap::new(bytes_per_ms, burst_bytes, now_ms));
    }

    pub fn clear_bandwidth_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.bandwidth_caps.remove(&(from, to));
    }

    pub fn clear_faults(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.drop_pairs.clear();
        inner.delay_pairs.clear();
        inner.reorder_pairs.clear();
        inner.timeout_pairs.clear();
        inner.bandwidth_caps.clear();
        inner.partitions.clear();
    }

    pub fn add_partition(&self, group_a: Vec<SocketAddr>, group_b: Vec<SocketAddr>) {
        let mut inner = self.inner.lock().unwrap();
        let a: HashSet<SocketAddr> = group_a.into_iter().collect();
        let b: HashSet<SocketAddr> = group_b.into_iter().collect();
        inner.partitions.push((a, b));
    }

    pub fn advance_time(&self, delta_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.now_ms = inner.now_ms.saturating_add(delta_ms);
    }

    pub fn now_ms(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.now_ms
    }

    pub fn deliver_ready(&self) {
        let mut ready = Vec::new();
        {
            let mut inner = self.inner.lock().unwrap();
            while let Some(Reverse(msg)) = inner.inflight.peek().cloned() {
                if msg.deliver_at_ms > inner.now_ms {
                    break;
                }
                inner.inflight.pop();
                ready.push(msg);
            }
        }

        for msg in ready {
            let mut inner = self.inner.lock().unwrap();
            if inner.should_timeout(&msg) {
                continue;
            }
            if !inner.reserve_bandwidth(msg.from, msg.to, msg.data.len() as u64) {
                let mut delayed = msg;
                delayed.deliver_at_ms = inner.now_ms.saturating_add(1);
                inner.inflight.push(Reverse(delayed));
                continue;
            }
            if let Some(node) = inner.nodes.get_mut(&msg.to) {
                if node.closed {
                    continue;
                }
                let queue = node.inbox.entry(msg.from).or_default();
                queue.push_back(msg.data);
                if !node.active_peers.contains(&msg.from)
                    && !node.pending_accepts.contains(&msg.from)
                {
                    node.pending_accepts.push_back(msg.from);
                }
                node.notify.notify_waiters();
            }
        }
    }

    fn enqueue(&self, from: SocketAddr, to: SocketAddr, data: Bytes) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.is_blocked(from, to) {
            return Ok(());
        }
        if inner.should_drop(from, to) {
            return Ok(());
        }
        let now_ms = inner.now_ms;
        let delay = inner.delay_for(from, to);
        let jitter = inner.reorder_jitter_for(from, to);
        let deliver_at_ms = now_ms.saturating_add(delay.saturating_add(jitter));
        let seq = inner.next_seq();
        inner
            .inflight
            .push(Reverse(QueuedMsg::new(
                from,
                to,
                data,
                deliver_at_ms,
                now_ms,
                seq,
            )));
        Ok(())
    }

    fn recv_from(&self, local: SocketAddr, remote: SocketAddr) -> Result<Option<Bytes>> {
        let mut inner = self.inner.lock().unwrap();
        let node = inner
            .nodes
            .get_mut(&local)
            .ok_or_else(|| OctopiiError::Transport("sim node not registered".to_string()))?;
        if node.closed {
            return Ok(None);
        }
        let queue = node.inbox.entry(remote).or_default();
        Ok(queue.pop_front())
    }

    fn accept_peer(&self, local: SocketAddr) -> Result<Option<SocketAddr>> {
        let mut inner = self.inner.lock().unwrap();
        let node = inner
            .nodes
            .get_mut(&local)
            .ok_or_else(|| OctopiiError::Transport("sim node not registered".to_string()))?;
        if let Some(peer) = node.pending_accepts.pop_front() {
            node.active_peers.insert(peer);
            return Ok(Some(peer));
        }
        Ok(None)
    }

    fn notify_handle(&self, addr: SocketAddr) -> Option<Arc<Notify>> {
        let inner = self.inner.lock().unwrap();
        inner.nodes.get(&addr).map(|n| Arc::clone(&n.notify))
    }

    fn is_closed(&self, addr: SocketAddr) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .nodes
            .get(&addr)
            .map(|n| n.closed)
            .unwrap_or(true)
    }
}

#[derive(Clone)]
pub struct SimTransport {
    addr: SocketAddr,
    router: SimRouter,
}

impl SimTransport {
    pub fn new(addr: SocketAddr, router: SimRouter) -> Self {
        router.register(addr);
        Self { addr, router }
    }

    pub fn router(&self) -> SimRouter {
        self.router.clone()
    }
}

impl Transport for SimTransport {
    fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.addr)
    }

    fn close(&self) {
        self.router.close(self.addr);
    }

    fn is_closed(&self) -> bool {
        self.router.is_closed(self.addr)
    }

    fn connect(&self, addr: SocketAddr) -> TransportFut<'_, Arc<dyn Peer>> {
        let router = self.router.clone();
        let local = self.addr;
        Box::pin(async move {
            Ok(Arc::new(SimPeer::new(local, addr, router)) as Arc<dyn Peer>)
        })
    }

    fn accept(&self) -> TransportFut<'_, (SocketAddr, Arc<dyn Peer>)> {
        let router = self.router.clone();
        let local = self.addr;
        Box::pin(async move {
            loop {
                if router.is_closed(local) {
                    return Err(OctopiiError::Transport("sim transport closed".to_string()));
                }
                if let Some(peer) = router.accept_peer(local)? {
                    let sim_peer = SimPeer::new(local, peer, router.clone());
                    return Ok((peer, Arc::new(sim_peer) as Arc<dyn Peer>));
                }
                if let Some(notify) = router.notify_handle(local) {
                    notify.notified().await;
                }
            }
        })
    }

    fn send(&self, addr: SocketAddr, data: Bytes) -> TransportFut<'_, ()> {
        let router = self.router.clone();
        let local = self.addr;
        Box::pin(async move { router.enqueue(local, addr, data) })
    }
}

#[derive(Clone)]
struct SimPeer {
    local: SocketAddr,
    remote: SocketAddr,
    router: SimRouter,
}

impl SimPeer {
    fn new(local: SocketAddr, remote: SocketAddr, router: SimRouter) -> Self {
        Self {
            local,
            remote,
            router,
        }
    }
}

impl Peer for SimPeer {
    fn send(&self, data: Bytes) -> TransportFut<'_, ()> {
        let router = self.router.clone();
        let local = self.local;
        let remote = self.remote;
        Box::pin(async move { router.enqueue(local, remote, data) })
    }

    fn recv(&self) -> TransportFut<'_, Option<Bytes>> {
        let router = self.router.clone();
        let local = self.local;
        let remote = self.remote;
        Box::pin(async move {
            loop {
                if router.is_closed(local) {
                    return Ok(None);
                }
                if let Some(data) = router.recv_from(local, remote)? {
                    return Ok(Some(data));
                }
                if let Some(notify) = router.notify_handle(local) {
                    notify.notified().await;
                }
            }
        })
    }

    fn is_closed(&self) -> bool {
        self.router.is_closed(self.local) || self.router.is_closed(self.remote)
    }
}

struct NodeQueue {
    inbox: HashMap<SocketAddr, VecDeque<Bytes>>,
    pending_accepts: VecDeque<SocketAddr>,
    active_peers: HashSet<SocketAddr>,
    notify: Arc<Notify>,
    closed: bool,
}

impl NodeQueue {
    fn new() -> Self {
        Self {
            inbox: HashMap::new(),
            pending_accepts: VecDeque::new(),
            active_peers: HashSet::new(),
            notify: Arc::new(Notify::new()),
            closed: false,
        }
    }
}

#[derive(Clone, Debug)]
struct QueuedMsg {
    from: SocketAddr,
    to: SocketAddr,
    data: Bytes,
    deliver_at_ms: u64,
    enqueued_at_ms: u64,
    seq: u64,
}

impl QueuedMsg {
    fn new(
        from: SocketAddr,
        to: SocketAddr,
        data: Bytes,
        deliver_at_ms: u64,
        enqueued_at_ms: u64,
        seq: u64,
    ) -> Self {
        Self {
            from,
            to,
            data,
            deliver_at_ms,
            enqueued_at_ms,
            seq,
        }
    }
}

impl PartialEq for QueuedMsg {
    fn eq(&self, other: &Self) -> bool {
        (self.deliver_at_ms, self.seq) == (other.deliver_at_ms, other.seq)
    }
}

impl Eq for QueuedMsg {}

impl PartialOrd for QueuedMsg {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedMsg {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.deliver_at_ms, self.seq).cmp(&(other.deliver_at_ms, other.seq))
    }
}

struct SimRouterInner {
    now_ms: u64,
    inflight: BinaryHeap<Reverse<QueuedMsg>>,
    nodes: HashMap<SocketAddr, NodeQueue>,
    drop_pairs: HashSet<(SocketAddr, SocketAddr)>,
    delay_pairs: HashMap<(SocketAddr, SocketAddr), u64>,
    reorder_pairs: HashMap<(SocketAddr, SocketAddr), ReorderConfig>,
    timeout_pairs: HashMap<(SocketAddr, SocketAddr), u64>,
    bandwidth_caps: HashMap<(SocketAddr, SocketAddr), BandwidthCap>,
    partitions: Vec<(HashSet<SocketAddr>, HashSet<SocketAddr>)>,
    rng: SimRng,
    drop_rate: f64,
    min_delay_ms: u64,
    max_delay_ms: u64,
    seq: u64,
}

impl SimRouterInner {
    fn new(config: SimConfig) -> Self {
        Self {
            now_ms: 0,
            inflight: BinaryHeap::new(),
            nodes: HashMap::new(),
            drop_pairs: HashSet::new(),
            delay_pairs: HashMap::new(),
            reorder_pairs: HashMap::new(),
            timeout_pairs: HashMap::new(),
            bandwidth_caps: HashMap::new(),
            partitions: Vec::new(),
            rng: SimRng::new(config.seed),
            drop_rate: config.drop_rate,
            min_delay_ms: config.min_delay_ms,
            max_delay_ms: config.max_delay_ms,
            seq: 0,
        }
    }

    fn next_seq(&mut self) -> u64 {
        self.seq = self.seq.wrapping_add(1);
        self.seq
    }

    fn delay_for(&mut self, from: SocketAddr, to: SocketAddr) -> u64 {
        if let Some(delay) = self.delay_pairs.get(&(from, to)) {
            return *delay;
        }
        if self.max_delay_ms <= self.min_delay_ms {
            return self.min_delay_ms;
        }
        let span = self.max_delay_ms - self.min_delay_ms;
        self.min_delay_ms + (self.rng.next_u64() % (span + 1))
    }

    fn reorder_jitter_for(&mut self, from: SocketAddr, to: SocketAddr) -> u64 {
        let Some(cfg) = self.reorder_pairs.get(&(from, to)) else {
            return 0;
        };
        if cfg.probability <= 0.0 || cfg.max_jitter_ms == 0 {
            return 0;
        }
        let limit = (u64::MAX as f64 * cfg.probability) as u64;
        if self.rng.next_u64() >= limit {
            return 0;
        }
        self.rng.next_range(cfg.max_jitter_ms + 1)
    }

    fn should_drop(&mut self, from: SocketAddr, to: SocketAddr) -> bool {
        if self.drop_pairs.contains(&(from, to)) {
            return true;
        }
        if self.drop_rate <= 0.0 {
            return false;
        }
        let limit = (u64::MAX as f64 * self.drop_rate) as u64;
        self.rng.next_u64() < limit
    }

    fn is_blocked(&self, from: SocketAddr, to: SocketAddr) -> bool {
        for (a, b) in &self.partitions {
            if (a.contains(&from) && b.contains(&to)) || (a.contains(&to) && b.contains(&from)) {
                return true;
            }
        }
        false
    }

    fn should_timeout(&self, msg: &QueuedMsg) -> bool {
        let Some(timeout_ms) = self.timeout_pairs.get(&(msg.from, msg.to)) else {
            return false;
        };
        self.now_ms.saturating_sub(msg.enqueued_at_ms) > *timeout_ms
    }

    fn reserve_bandwidth(&mut self, from: SocketAddr, to: SocketAddr, bytes: u64) -> bool {
        let Some(cap) = self.bandwidth_caps.get_mut(&(from, to)) else {
            return true;
        };
        if cap.bytes_per_ms == 0 {
            return false;
        }
        cap.refill(self.now_ms);
        if cap.available_bytes >= bytes {
            cap.available_bytes -= bytes;
            return true;
        }
        false
    }
}

#[derive(Clone, Copy)]
struct ReorderConfig {
    max_jitter_ms: u64,
    probability: f64,
}

struct BandwidthCap {
    bytes_per_ms: u64,
    burst_bytes: u64,
    available_bytes: u64,
    last_refill_ms: u64,
}

impl BandwidthCap {
    fn new(bytes_per_ms: u64, burst_bytes: u64, now_ms: u64) -> Self {
        let burst = burst_bytes.max(bytes_per_ms);
        Self {
            bytes_per_ms,
            burst_bytes: burst,
            available_bytes: burst,
            last_refill_ms: now_ms,
        }
    }

    fn refill(&mut self, now_ms: u64) {
        if now_ms <= self.last_refill_ms || self.bytes_per_ms == 0 {
            self.last_refill_ms = now_ms;
            return;
        }
        let delta = now_ms - self.last_refill_ms;
        let add = delta.saturating_mul(self.bytes_per_ms);
        let next = self.available_bytes.saturating_add(add);
        self.available_bytes = next.min(self.burst_bytes);
        self.last_refill_ms = now_ms;
    }
}

struct SimRng {
    state: u64,
}

impl SimRng {
    fn new(seed: u64) -> Self {
        let mut rng = Self { state: 0 };
        rng.state = seed.wrapping_add(0x9E3779B97F4A7C15);
        rng.next_u64();
        rng
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_range(&mut self, upper_exclusive: u64) -> u64 {
        if upper_exclusive <= 1 {
            return 0;
        }
        self.next_u64() % upper_exclusive
    }
}
