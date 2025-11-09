// Network simulation filters ported from TiKV
// Source: tikv/components/test_raftstore/src/transport_simulate.rs
// Licensed under Apache-2.0

use raft::eraftpb::Message;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

pub type Result<T> = std::result::Result<T, FilterError>;

#[derive(Debug, Clone)]
pub struct FilterError {
    pub reason: String,
}

impl FilterError {
    pub fn filtered() -> Self {
        FilterError {
            reason: "Message filtered".to_string(),
        }
    }
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter error: {}", self.reason)
    }
}

impl std::error::Error for FilterError {}

/// Check if messages vector is empty and return error if so.
pub fn check_messages(msgs: &[Message]) -> Result<()> {
    if msgs.is_empty() {
        Err(FilterError::filtered())
    } else {
        Ok(())
    }
}

/// Filter trait for message filtering.
///
/// Filters can intercept and modify messages before they are sent.
/// This enables network simulation for testing (partitions, packet loss, delays, etc.)
pub trait Filter: Send + Sync {
    /// Called before sending messages. Can modify or filter the message list.
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()>;

    /// Called after sending messages. Can modify the result.
    fn after(&self, res: Result<()>) -> Result<()> {
        res
    }
}

/// Drop packets randomly based on a rate (0-100).
///
/// # Example
/// ```
/// let filter = DropPacketFilter::new(30); // Drop 30% of packets
/// ```
#[derive(Clone)]
pub struct DropPacketFilter {
    /// Drop rate from 0-100 (percentage)
    pub rate: u32,
}

impl DropPacketFilter {
    pub fn new(rate: u32) -> Self {
        assert!(rate <= 100, "Drop rate must be 0-100");
        DropPacketFilter { rate }
    }
}

impl Filter for DropPacketFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        msgs.retain(|_| rand::random::<u32>() % 100u32 >= self.rate);
        check_messages(msgs)
    }
}

/// Delay all messages by a fixed duration.
///
/// # Example
/// ```
/// let filter = DelayFilter::new(Duration::from_millis(100));
/// ```
#[derive(Clone)]
pub struct DelayFilter {
    pub duration: Duration,
}

impl DelayFilter {
    pub fn new(duration: Duration) -> Self {
        DelayFilter { duration }
    }
}

impl Filter for DelayFilter {
    fn before(&self, _: &mut Vec<Message>) -> Result<()> {
        std::thread::sleep(self.duration);
        Ok(())
    }
}

/// Filter messages going to specific nodes (used for partitions).
///
/// # Example
/// ```
/// let filter = PartitionFilter::new(vec![2, 3]); // Block messages to nodes 2 and 3
/// ```
#[derive(Clone)]
pub struct PartitionFilter {
    pub node_ids: Vec<u64>,
}

impl PartitionFilter {
    pub fn new(node_ids: Vec<u64>) -> Self {
        PartitionFilter { node_ids }
    }
}

impl Filter for PartitionFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        // Filter out messages going to partitioned nodes
        msgs.retain(|m| !self.node_ids.contains(&m.to));
        check_messages(msgs)
    }
}

/// Factory trait for generating filters per node.
pub trait FilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<dyn Filter>>;
}

/// Create partition between two groups of nodes.
///
/// Nodes in group1 cannot communicate with nodes in group2 and vice versa.
///
/// # Example
/// ```
/// let factory = PartitionFilterFactory::new(vec![1], vec![2, 3]);
/// // Node 1 isolated from nodes 2 and 3
/// ```
pub struct PartitionFilterFactory {
    s1: Vec<u64>,
    s2: Vec<u64>,
}

impl PartitionFilterFactory {
    pub fn new(s1: Vec<u64>, s2: Vec<u64>) -> Self {
        PartitionFilterFactory { s1, s2 }
    }
}

impl FilterFactory for PartitionFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<dyn Filter>> {
        if self.s1.contains(&node_id) {
            // This node is in s1, so block messages to s2
            return vec![Box::new(PartitionFilter {
                node_ids: self.s2.clone(),
            })];
        }
        // This node is in s2, so block messages to s1
        vec![Box::new(PartitionFilter {
            node_ids: self.s1.clone(),
        })]
    }
}

/// Completely isolate a single node from the cluster.
///
/// # Example
/// ```
/// let factory = IsolationFilterFactory::new(2);
/// // Node 2 cannot send or receive any messages
/// ```
pub struct IsolationFilterFactory {
    node_id: u64,
}

impl IsolationFilterFactory {
    pub fn new(node_id: u64) -> Self {
        IsolationFilterFactory { node_id }
    }
}

impl FilterFactory for IsolationFilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<dyn Filter>> {
        if node_id == self.node_id {
            // This is the isolated node - drop all outgoing messages
            return vec![Box::new(DropPacketFilter { rate: 100 })];
        }
        // Other nodes - block messages to the isolated node
        vec![Box::new(PartitionFilter {
            node_ids: vec![self.node_id],
        })]
    }
}

/// Direction for message filtering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Recv,
    Send,
    Both,
}

impl Direction {
    pub fn is_recv(self) -> bool {
        matches!(self, Direction::Recv | Direction::Both)
    }

    pub fn is_send(self) -> bool {
        matches!(self, Direction::Send | Direction::Both)
    }
}

/// Filter messages by message type.
///
/// This is useful for testing specific Raft message scenarios.
///
/// # Example
/// ```
/// let filter = MessageTypeFilter::new(MessageType::MsgAppend);
/// // Drop all MsgAppend messages
/// ```
#[derive(Clone)]
pub struct MessageTypeFilter {
    pub msg_type: raft::eraftpb::MessageType,
}

impl MessageTypeFilter {
    pub fn new(msg_type: raft::eraftpb::MessageType) -> Self {
        MessageTypeFilter { msg_type }
    }
}

impl Filter for MessageTypeFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        msgs.retain(|m| m.msg_type != self.msg_type);
        check_messages(msgs)
    }
}

/// Filter that can be controlled dynamically.
///
/// Allows enabling/disabling filtering at runtime via an AtomicBool.
///
/// # Example
/// ```
/// let active = Arc::new(AtomicBool::new(true));
/// let filter = ConditionalFilter::new(
///     active.clone(),
///     Box::new(DropPacketFilter::new(100))
/// );
/// // Later: active.store(false, Ordering::SeqCst); // Disable filtering
/// ```
pub struct ConditionalFilter {
    active: Arc<AtomicBool>,
    inner: Box<dyn Filter>,
}

impl ConditionalFilter {
    pub fn new(active: Arc<AtomicBool>, inner: Box<dyn Filter>) -> Self {
        ConditionalFilter { active, inner }
    }
}

impl Filter for ConditionalFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        if self.active.load(Ordering::SeqCst) {
            self.inner.before(msgs)
        } else {
            Ok(())
        }
    }

    fn after(&self, res: Result<()>) -> Result<()> {
        if self.active.load(Ordering::SeqCst) {
            self.inner.after(res)
        } else {
            res
        }
    }
}

/// Filter that allows a limited number of messages through.
///
/// Useful for testing specific scenarios like "allow 3 heartbeats then partition".
///
/// # Example
/// ```
/// let filter = CountFilter::new(5); // Allow 5 messages, then block all
/// ```
pub struct CountFilter {
    remaining: Arc<AtomicUsize>,
}

impl CountFilter {
    pub fn new(count: usize) -> Self {
        CountFilter {
            remaining: Arc::new(AtomicUsize::new(count)),
        }
    }

    pub fn remaining(&self) -> usize {
        self.remaining.load(Ordering::SeqCst)
    }
}

impl Filter for CountFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        let mut allowed = Vec::new();
        for msg in msgs.drain(..) {
            loop {
                let left = self.remaining.load(Ordering::SeqCst);
                if left == 0 {
                    break;
                }
                if self
                    .remaining
                    .compare_exchange(left, left - 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    allowed.push(msg);
                    break;
                }
            }
        }
        *msgs = allowed;
        check_messages(msgs)
    }
}

/// Duplicate messages to test idempotency.
///
/// TiKV tests this to ensure the Raft implementation properly handles duplicate messages
/// that can occur in real networks (e.g., due to retransmissions).
///
/// # Example
/// ```
/// let filter = MessageDuplicationFilter::new(50); // Duplicate 50% of messages
/// ```
#[derive(Clone)]
pub struct MessageDuplicationFilter {
    /// Duplication rate from 0-100 (percentage)
    pub rate: u32,
}

impl MessageDuplicationFilter {
    pub fn new(rate: u32) -> Self {
        assert!(rate <= 100, "Duplication rate must be 0-100");
        MessageDuplicationFilter { rate }
    }
}

impl Filter for MessageDuplicationFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        let mut duplicated = Vec::new();
        for msg in msgs.iter() {
            duplicated.push(msg.clone());
            // Randomly duplicate this message
            if rand::random::<u32>() % 100u32 < self.rate {
                duplicated.push(msg.clone());
            }
        }
        *msgs = duplicated;
        Ok(())
    }
}

/// Reorder messages to simulate out-of-order network delivery.
///
/// TiKV tests this to ensure Raft handles messages arriving out of order,
/// which is common in real networks.
///
/// # Example
/// ```
/// let filter = MessageReorderFilter::new(); // Randomly reorder messages
/// ```
#[derive(Clone)]
pub struct MessageReorderFilter;

impl MessageReorderFilter {
    pub fn new() -> Self {
        MessageReorderFilter
    }
}

impl Filter for MessageReorderFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        msgs.shuffle(&mut rng);
        Ok(())
    }
}

/// Throttle message delivery to simulate a slow follower.
///
/// This is useful for testing how Raft handles followers that are slow to process messages,
/// which can trigger snapshot transfers.
///
/// # Example
/// ```
/// let filter = ThrottleFilter::new(Duration::from_millis(100), 2);
/// // Allow 2 messages per 100ms
/// ```
pub struct ThrottleFilter {
    interval: Duration,
    max_per_interval: usize,
    last_reset: Arc<std::sync::Mutex<std::time::Instant>>,
    count: Arc<AtomicUsize>,
}

impl ThrottleFilter {
    pub fn new(interval: Duration, max_per_interval: usize) -> Self {
        ThrottleFilter {
            interval,
            max_per_interval,
            last_reset: Arc::new(std::sync::Mutex::new(std::time::Instant::now())),
            count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Filter for ThrottleFilter {
    fn before(&self, msgs: &mut Vec<Message>) -> Result<()> {
        let mut last_reset = self.last_reset.lock().unwrap();

        // Check if we need to reset the interval
        if last_reset.elapsed() >= self.interval {
            *last_reset = std::time::Instant::now();
            self.count.store(0, Ordering::SeqCst);
        }

        // Limit messages to max_per_interval
        let current = self.count.load(Ordering::SeqCst);
        let available = self.max_per_interval.saturating_sub(current);

        if available == 0 {
            // Throttle - drop all messages
            msgs.clear();
            return Err(FilterError {
                reason: "Throttled".to_string(),
            });
        }

        // Allow up to 'available' messages
        if msgs.len() > available {
            msgs.truncate(available);
        }

        self.count.fetch_add(msgs.len(), Ordering::SeqCst);
        check_messages(msgs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::{Message, MessageType};

    fn create_test_message(from: u64, to: u64, msg_type: MessageType) -> Message {
        let mut msg = Message::default();
        msg.from = from;
        msg.to = to;
        msg.msg_type = msg_type;
        msg
    }

    #[test]
    fn test_drop_packet_filter() {
        let filter = DropPacketFilter::new(100); // Drop all
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 3, MessageType::MsgHeartbeat),
        ];

        let result = filter.before(&mut msgs);
        assert!(result.is_err());
        assert_eq!(msgs.len(), 0);
    }

    #[test]
    fn test_delay_filter() {
        let filter = DelayFilter::new(Duration::from_millis(10));
        let mut msgs = vec![create_test_message(1, 2, MessageType::MsgHeartbeat)];

        let start = std::time::Instant::now();
        filter.before(&mut msgs).unwrap();
        assert!(start.elapsed() >= Duration::from_millis(10));
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn test_partition_filter() {
        let filter = PartitionFilter::new(vec![2, 3]);
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 3, MessageType::MsgHeartbeat),
            create_test_message(1, 4, MessageType::MsgHeartbeat),
        ];

        filter.before(&mut msgs).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].to, 4);
    }

    #[test]
    fn test_partition_factory() {
        let factory = PartitionFilterFactory::new(vec![1], vec![2, 3]);

        // Node 1's filter should block messages to 2 and 3
        let filters = factory.generate(1);
        assert_eq!(filters.len(), 1);

        // Node 2's filter should block messages to 1
        let filters = factory.generate(2);
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_isolation_factory() {
        let factory = IsolationFilterFactory::new(2);

        // Node 2 (isolated) should drop all outgoing
        let filters = factory.generate(2);
        assert_eq!(filters.len(), 1);

        // Other nodes should block messages to node 2
        let filters = factory.generate(1);
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn test_message_type_filter() {
        let filter = MessageTypeFilter::new(MessageType::MsgHeartbeat);
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 2, MessageType::MsgAppend),
        ];

        filter.before(&mut msgs).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].msg_type, MessageType::MsgAppend);
    }

    #[test]
    fn test_conditional_filter() {
        let active = Arc::new(AtomicBool::new(true));
        let filter = ConditionalFilter::new(
            active.clone(),
            Box::new(DropPacketFilter::new(100)),
        );

        let mut msgs = vec![create_test_message(1, 2, MessageType::MsgHeartbeat)];

        // Filter is active - should drop
        assert!(filter.before(&mut msgs).is_err());
        assert_eq!(msgs.len(), 0);

        // Disable filter
        active.store(false, Ordering::SeqCst);
        let mut msgs = vec![create_test_message(1, 2, MessageType::MsgHeartbeat)];

        // Filter inactive - should pass through
        assert!(filter.before(&mut msgs).is_ok());
        assert_eq!(msgs.len(), 1);
    }

    #[test]
    fn test_count_filter() {
        let filter = CountFilter::new(2);
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 2, MessageType::MsgHeartbeat),
        ];

        filter.before(&mut msgs).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(filter.remaining(), 0);

        // Next call should block all
        let mut msgs = vec![create_test_message(1, 2, MessageType::MsgHeartbeat)];
        assert!(filter.before(&mut msgs).is_err());
    }

    #[test]
    fn test_message_duplication_filter() {
        let filter = MessageDuplicationFilter::new(100); // Duplicate all messages
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 3, MessageType::MsgHeartbeat),
        ];

        filter.before(&mut msgs).unwrap();
        // With 100% duplication rate, we should have 4 messages (2 original + 2 duplicates)
        assert_eq!(msgs.len(), 4);
    }

    #[test]
    fn test_message_duplication_filter_zero_rate() {
        let filter = MessageDuplicationFilter::new(0); // No duplication
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 3, MessageType::MsgHeartbeat),
        ];

        filter.before(&mut msgs).unwrap();
        // With 0% duplication rate, should have original 2 messages
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn test_message_reorder_filter() {
        let filter = MessageReorderFilter::new();
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 3, MessageType::MsgAppend),
            create_test_message(1, 4, MessageType::MsgRequestVote),
            create_test_message(1, 5, MessageType::MsgSnapshot),
        ];

        filter.before(&mut msgs).unwrap();
        // Messages should still be there, just potentially reordered
        assert_eq!(msgs.len(), 4);
        // Verify all original messages are present (order may vary)
        let to_values: Vec<u64> = msgs.iter().map(|m| m.to).collect();
        assert!(to_values.contains(&2));
        assert!(to_values.contains(&3));
        assert!(to_values.contains(&4));
        assert!(to_values.contains(&5));
    }

    #[test]
    fn test_throttle_filter() {
        let filter = ThrottleFilter::new(Duration::from_millis(100), 2);

        // First batch - should allow 2 messages
        let mut msgs = vec![
            create_test_message(1, 2, MessageType::MsgHeartbeat),
            create_test_message(1, 3, MessageType::MsgHeartbeat),
            create_test_message(1, 4, MessageType::MsgHeartbeat),
        ];
        filter.before(&mut msgs).unwrap();
        assert_eq!(msgs.len(), 2);

        // Second batch immediately - should be throttled
        let mut msgs = vec![create_test_message(1, 2, MessageType::MsgHeartbeat)];
        assert!(filter.before(&mut msgs).is_err());

        // Wait for interval to reset
        std::thread::sleep(Duration::from_millis(150));

        // Should allow messages again
        let mut msgs = vec![create_test_message(1, 2, MessageType::MsgHeartbeat)];
        assert!(filter.before(&mut msgs).is_ok());
        assert_eq!(msgs.len(), 1);
    }
}
