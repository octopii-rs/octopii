use super::config::SimConfig;
use super::fault::{BandwidthCap, FaultRule, ReorderConfig};
use super::rng::SimRng;
use crate::error::{OctopiiError, Result};
use bytes::Bytes;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct SimRouter {
    pub(crate) inner: Arc<Mutex<SimRouterInner>>,
}

impl SimRouter {
    pub fn new(config: SimConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimRouterInner::new(config))),
        }
    }

    pub fn register(&self, addr: SocketAddr) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let epoch = inner
            .nodes
            .entry(addr)
            .and_modify(|node| {
                node.epoch = node.epoch.saturating_add(1);
                node.inbox.clear();
                node.pending_accepts.clear();
                node.active_peers.clear();
                node.closed = false;
                node.notify.notify_waiters();
            })
            .or_insert_with(NodeQueue::new)
            .epoch;
        epoch
    }

    pub fn close(&self, addr: SocketAddr, epoch: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(node) = inner.nodes.get_mut(&addr) {
            if node.epoch == epoch {
                node.closed = true;
                node.notify.notify_waiters();
            }
        }
    }

    pub fn set_drop_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.rule_mut((from, to)).drop = true;
    }

    pub fn set_delay_pair(&self, from: SocketAddr, to: SocketAddr, delay_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.rule_mut((from, to)).delay_ms = Some(delay_ms);
    }

    pub fn set_reorder_pair(&self, from: SocketAddr, to: SocketAddr, max_jitter_ms: u64, probability: f64) {
        let mut inner = self.inner.lock().unwrap();
        inner.rule_mut((from, to)).reorder = Some(ReorderConfig { max_jitter_ms, probability });
    }

    pub fn clear_reorder_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.clear_rule_field((from, to), |r| r.reorder = None);
    }

    pub fn set_timeout_pair(&self, from: SocketAddr, to: SocketAddr, timeout_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.rule_mut((from, to)).timeout_ms = Some(timeout_ms);
    }

    pub fn clear_timeout_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.clear_rule_field((from, to), |r| r.timeout_ms = None);
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
        inner.rule_mut((from, to)).bandwidth = Some(BandwidthCap::new(bytes_per_ms, burst_bytes, now_ms));
    }

    pub fn clear_bandwidth_pair(&self, from: SocketAddr, to: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.clear_rule_field((from, to), |r| r.bandwidth = None);
    }

    pub fn clear_faults(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.pair_rules.clear();
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
            let from_ok = inner
                .nodes
                .get(&msg.from)
                .map(|n| !n.closed && n.epoch == msg.from_epoch)
                .unwrap_or(false);
            let to_ok = inner
                .nodes
                .get(&msg.to)
                .map(|n| !n.closed && n.epoch == msg.to_epoch)
                .unwrap_or(false);
            if !from_ok || !to_ok {
                continue;
            }
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
                if node.closed || node.epoch != msg.to_epoch {
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

    pub fn enqueue(
        &self,
        from: SocketAddr,
        from_epoch: u64,
        to: SocketAddr,
        to_epoch: u64,
        data: Bytes,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let from_ok = inner
            .nodes
            .get(&from)
            .map(|n| !n.closed && n.epoch == from_epoch)
            .unwrap_or(false);
        let to_ok = inner
            .nodes
            .get(&to)
            .map(|n| !n.closed && n.epoch == to_epoch)
            .unwrap_or(false);
        if !from_ok || !to_ok {
            return Ok(());
        }
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
                from_epoch,
                to_epoch,
            )));
        Ok(())
    }

    pub fn recv_from(&self, local: SocketAddr, local_epoch: u64, remote: SocketAddr) -> Result<Option<Bytes>> {
        let mut inner = self.inner.lock().unwrap();
        let node = inner
            .nodes
            .get_mut(&local)
            .ok_or_else(|| OctopiiError::Transport("sim node not registered".to_string()))?;
        if node.closed || node.epoch != local_epoch {
            return Ok(None);
        }
        let queue = node.inbox.entry(remote).or_default();
        Ok(queue.pop_front())
    }

    pub fn accept_peer(&self, local: SocketAddr) -> Result<Option<SocketAddr>> {
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

    pub fn notify_handle(&self, addr: SocketAddr, epoch: u64) -> Option<Arc<Notify>> {
        let inner = self.inner.lock().unwrap();
        inner
            .nodes
            .get(&addr)
            .and_then(|n| (n.epoch == epoch).then(|| Arc::clone(&n.notify)))
    }

    pub fn is_closed(&self, addr: SocketAddr, epoch: u64) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .nodes
            .get(&addr)
            .map(|n| n.closed || n.epoch != epoch)
            .unwrap_or(true)
    }

    pub fn epoch_of(&self, addr: SocketAddr) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.nodes.get(&addr).map(|n| n.epoch).unwrap_or(0)
    }
}

pub(crate) struct SimRouterInner {
    pub(crate) now_ms: u64,
    pub(crate) inflight: BinaryHeap<Reverse<QueuedMsg>>,
    pub(crate) nodes: HashMap<SocketAddr, NodeQueue>,
    pub(crate) pair_rules: HashMap<(SocketAddr, SocketAddr), FaultRule>,
    pub(crate) partitions: Vec<(HashSet<SocketAddr>, HashSet<SocketAddr>)>,
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
            pair_rules: HashMap::new(),
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

    fn is_blocked(&self, from: SocketAddr, to: SocketAddr) -> bool {
        for (a, b) in &self.partitions {
            if (a.contains(&from) && b.contains(&to)) || (a.contains(&to) && b.contains(&from)) {
                return true;
            }
        }
        false
    }

    fn should_timeout(&self, msg: &QueuedMsg) -> bool {
        let Some(timeout_ms) = self
            .pair_rules
            .get(&(msg.from, msg.to))
            .and_then(|r| r.timeout_ms)
        else {
            return false;
        };
        self.now_ms.saturating_sub(msg.enqueued_at_ms) > timeout_ms
    }

    fn reserve_bandwidth(&mut self, from: SocketAddr, to: SocketAddr, bytes: u64) -> bool {
        let Some(cap) = self
            .pair_rules
            .get_mut(&(from, to))
            .and_then(|r| r.bandwidth.as_mut())
        else {
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

    fn rule_mut(&mut self, key: (SocketAddr, SocketAddr)) -> &mut FaultRule {
        self.pair_rules.entry(key).or_default()
    }

    fn clear_rule_field(&mut self, key: (SocketAddr, SocketAddr), f: impl FnOnce(&mut FaultRule)) {
        if let Some(rule) = self.pair_rules.get_mut(&key) {
            f(rule);
            if rule.is_empty() {
                self.pair_rules.remove(&key);
            }
        }
    }

    fn delay_for(&mut self, from: SocketAddr, to: SocketAddr) -> u64 {
        if let Some(delay) = self.pair_rules.get(&(from, to)).and_then(|r| r.delay_ms) {
            return delay;
        }
        if self.max_delay_ms <= self.min_delay_ms {
            return self.min_delay_ms;
        }
        let span = self.max_delay_ms - self.min_delay_ms;
        self.min_delay_ms + (self.rng.next_u64() % (span + 1))
    }

    fn reorder_jitter_for(&mut self, from: SocketAddr, to: SocketAddr) -> u64 {
        let Some(cfg) = self.pair_rules.get(&(from, to)).and_then(|r| r.reorder) else {
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
        if self
            .pair_rules
            .get(&(from, to))
            .map(|r| r.drop)
            .unwrap_or(false)
        {
            return true;
        }
        if self.drop_rate <= 0.0 {
            return false;
        }
        let limit = (u64::MAX as f64 * self.drop_rate) as u64;
        self.rng.next_u64() < limit
    }
}

pub(crate) struct NodeQueue {
    pub(crate) inbox: HashMap<SocketAddr, VecDeque<Bytes>>,
    pub(crate) pending_accepts: VecDeque<SocketAddr>,
    pub(crate) active_peers: HashSet<SocketAddr>,
    pub(crate) notify: Arc<Notify>,
    pub(crate) closed: bool,
    pub(crate) epoch: u64,
}

impl NodeQueue {
    fn new() -> Self {
        Self {
            inbox: HashMap::new(),
            pending_accepts: VecDeque::new(),
            active_peers: HashSet::new(),
            notify: Arc::new(Notify::new()),
            closed: false,
            epoch: 1,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QueuedMsg {
    pub(crate) from: SocketAddr,
    pub(crate) to: SocketAddr,
    pub(crate) data: Bytes,
    pub(crate) deliver_at_ms: u64,
    pub(crate) enqueued_at_ms: u64,
    pub(crate) seq: u64,
    pub(crate) from_epoch: u64,
    pub(crate) to_epoch: u64,
}

impl QueuedMsg {
    fn new(
        from: SocketAddr,
        to: SocketAddr,
        data: Bytes,
        deliver_at_ms: u64,
        enqueued_at_ms: u64,
        seq: u64,
        from_epoch: u64,
        to_epoch: u64,
    ) -> Self {
        Self {
            from,
            to,
            data,
            deliver_at_ms,
            enqueued_at_ms,
            seq,
            from_epoch,
            to_epoch,
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
