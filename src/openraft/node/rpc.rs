#![cfg(feature = "openraft")]

use super::OpenRaftNode;
use crate::openraft::types::AppTypeConfig;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;

impl OpenRaftNode {
    async fn decode_call<Req, Resp, Fut, Err>(
        data: &Bytes,
        f: impl FnOnce(Req) -> Fut,
    ) -> Option<Vec<u8>>
    where
        Req: DeserializeOwned,
        Resp: Serialize,
        Fut: Future<Output = std::result::Result<Resp, Err>>,
    {
        let req = bincode::deserialize::<Req>(data).ok()?;
        let resp = f(req).await.ok()?;
        bincode::serialize(&resp).ok()
    }

    pub(crate) async fn set_openraft_request_handler(&self) {
        eprintln!("[node {}] registering openraft request handler", self.config.node_id);
        let raft_clone = self.raft.clone();
        self.rpc
            .set_request_handler(move |req| {
                let raft = raft_clone.clone();
                async move {
                    match req.payload {
                        crate::rpc::RequestPayload::OpenRaft { kind, data } => {
                            eprintln!("[openraft rpc handler] recv kind={}", kind);
                            let response_data = match kind.as_str() {
                                "append_entries" => {
                                    OpenRaftNode::decode_call::<
                                        openraft::raft::AppendEntriesRequest<AppTypeConfig>,
                                        openraft::raft::AppendEntriesResponse<AppTypeConfig>,
                                        _,
                                        _,
                                    >(&data, |req| raft.append_entries(req))
                                    .await
                                }
                                "vote" => {
                                    OpenRaftNode::decode_call::<
                                        openraft::raft::VoteRequest<AppTypeConfig>,
                                        openraft::raft::VoteResponse<AppTypeConfig>,
                                        _,
                                        _,
                                    >(&data, |req| raft.vote(req))
                                    .await
                                }
                                "install_snapshot" => {
                                    OpenRaftNode::decode_call::<
                                        openraft::raft::InstallSnapshotRequest<AppTypeConfig>,
                                        openraft::raft::InstallSnapshotResponse<AppTypeConfig>,
                                        _,
                                        _,
                                    >(&data, |req| raft.install_snapshot(req))
                                    .await
                                }
                                _ => None,
                            }
                            .unwrap_or_default();

                            crate::rpc::ResponsePayload::OpenRaft {
                                kind,
                                data: bytes::Bytes::from(response_data),
                            }
                        }
                        _ => crate::rpc::ResponsePayload::CustomResponse {
                            success: false,
                            data: bytes::Bytes::new(),
                        },
                    }
                }
            })
            .await;
    }
}
