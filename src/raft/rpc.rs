use crate::rpc::{RequestPayload, ResponsePayload};
use bytes::Bytes;
use raft::prelude::*;

/// Convert a Raft Message to RPC RequestPayload
pub fn raft_message_to_rpc(msg: &Message) -> Option<RequestPayload> {
    match msg.get_msg_type() {
        MessageType::MsgAppend => {
            tracing::debug!(
                "Converting MsgAppend: from={}, to={}, term={}, log_term={}, index={}, commit={}, entries={}",
                msg.from,
                msg.to,
                msg.term,
                msg.log_term,
                msg.index,
                msg.commit,
                msg.entries.len()
            );

            Some(RequestPayload::AppendEntries {
                term: msg.term,
                leader_id: msg.from,
                prev_log_index: msg.index,
                prev_log_term: msg.log_term,
                entries: msg.entries.iter().map(|e| Bytes::copy_from_slice(&e.data)).collect(),
                leader_commit: msg.commit,
            })
        }
        MessageType::MsgRequestVote => {
            tracing::debug!(
                "Converting MsgRequestVote: from={}, to={}, term={}, log_term={}, index={}",
                msg.from,
                msg.to,
                msg.term,
                msg.log_term,
                msg.index
            );

            Some(RequestPayload::RequestVote {
                term: msg.term,
                candidate_id: msg.from,
                last_log_index: msg.index,
                last_log_term: msg.log_term,
            })
        }
        MessageType::MsgHeartbeat => {
            tracing::trace!(
                "Converting MsgHeartbeat: from={}, to={}, term={}, commit={}",
                msg.from,
                msg.to,
                msg.term,
                msg.commit
            );

            // Heartbeats are like AppendEntries with no entries
            Some(RequestPayload::AppendEntries {
                term: msg.term,
                leader_id: msg.from,
                prev_log_index: msg.index,
                prev_log_term: msg.log_term,
                entries: vec![],
                leader_commit: msg.commit,
            })
        }
        _ => {
            tracing::warn!(
                "Unsupported Raft message type for RPC: {:?}",
                msg.get_msg_type()
            );
            None
        }
    }
}

/// Convert RPC RequestPayload to Raft Message
pub fn rpc_to_raft_message(_from: u64, to: u64, payload: &RequestPayload) -> Option<Message> {
    match payload {
        RequestPayload::AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            tracing::debug!(
                "Converting RPC AppendEntries to Raft: from={}, to={}, term={}, entries={}",
                leader_id,
                to,
                term,
                entries.len()
            );

            let msg_type = if entries.is_empty() {
                MessageType::MsgHeartbeat
            } else {
                MessageType::MsgAppend
            };

            let raft_entries: Vec<Entry> = entries
                .iter()
                .enumerate()
                .map(|(i, data)| Entry {
                    entry_type: EntryType::EntryNormal.into(),
                    term: *term,
                    index: prev_log_index + 1 + i as u64,
                    data: data.to_vec().into(),
                    ..Default::default()
                })
                .collect();

            let mut msg = Message::default();
            msg.set_msg_type(msg_type);
            msg.from = *leader_id;
            msg.to = to;
            msg.term = *term;
            msg.log_term = *prev_log_term;
            msg.index = *prev_log_index;
            msg.commit = *leader_commit;
            msg.entries = raft_entries.into();

            Some(msg)
        }
        RequestPayload::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => {
            tracing::debug!(
                "Converting RPC RequestVote to Raft: from={}, to={}, term={}",
                candidate_id,
                to,
                term
            );

            let mut msg = Message::default();
            msg.set_msg_type(MessageType::MsgRequestVote);
            msg.from = *candidate_id;
            msg.to = to;
            msg.term = *term;
            msg.log_term = *last_log_term;
            msg.index = *last_log_index;

            Some(msg)
        }
        _ => None,
    }
}

/// Convert Raft Message response to RPC ResponsePayload
pub fn raft_response_to_rpc(msg: &Message) -> Option<ResponsePayload> {
    match msg.get_msg_type() {
        MessageType::MsgAppendResponse | MessageType::MsgHeartbeatResponse => {
            tracing::debug!(
                "Converting MsgAppendResponse: from={}, to={}, term={}, reject={}",
                msg.from,
                msg.to,
                msg.term,
                msg.reject
            );

            Some(ResponsePayload::AppendEntriesResponse {
                term: msg.term,
                success: !msg.reject,
            })
        }
        MessageType::MsgRequestVoteResponse => {
            tracing::debug!(
                "Converting MsgRequestVoteResponse: from={}, to={}, term={}, reject={}",
                msg.from,
                msg.to,
                msg.term,
                msg.reject
            );

            Some(ResponsePayload::RequestVoteResponse {
                term: msg.term,
                vote_granted: !msg.reject,
            })
        }
        _ => None,
    }
}

/// Convert RPC ResponsePayload to Raft Message
pub fn rpc_response_to_raft(from: u64, to: u64, request_type: MessageType, payload: &ResponsePayload) -> Option<Message> {
    match payload {
        ResponsePayload::AppendEntriesResponse { term, success } => {
            tracing::debug!(
                "Converting RPC AppendEntriesResponse to Raft: from={}, to={}, term={}, success={}",
                from,
                to,
                term,
                success
            );

            let msg_type = match request_type {
                MessageType::MsgHeartbeat => MessageType::MsgHeartbeatResponse,
                _ => MessageType::MsgAppendResponse,
            };

            let mut msg = Message::default();
            msg.set_msg_type(msg_type);
            msg.from = from;
            msg.to = to;
            msg.term = *term;
            msg.reject = !success;

            Some(msg)
        }
        ResponsePayload::RequestVoteResponse { term, vote_granted } => {
            tracing::debug!(
                "Converting RPC RequestVoteResponse to Raft: from={}, to={}, term={}, granted={}",
                from,
                to,
                term,
                vote_granted
            );

            let mut msg = Message::default();
            msg.set_msg_type(MessageType::MsgRequestVoteResponse);
            msg.from = from;
            msg.to = to;
            msg.term = *term;
            msg.reject = !vote_granted;

            Some(msg)
        }
        _ => None,
    }
}
