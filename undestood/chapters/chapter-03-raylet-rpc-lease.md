# Chapter 03: Raylet RPC 接入（Worker Lease 入口）

## 必看源码
- `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`
- `/home/runner/work/ray/ray/src/ray/protobuf/node_manager.proto`

## 基础代码流（必要）
1. RPC: `RequestWorkerLease` 进入 `NodeManager::HandleRequestWorkerLease`
2. 若 lease 已 granted，直接复用已有 worker 地址返回
3. 若 caller 已死亡，直接 canceled
4. 若 lease 在排队中，追加 reply callback（幂等）
5. `worker_pool_.PrestartWorkers(...)`
6. `cluster_lease_manager_.QueueAndScheduleLease(...)`

## 必理解协议字段
- `RequestWorkerLeaseRequest.lease_spec`
- `RequestWorkerLeaseReply.canceled/rejected/failure_type`
- `CancelWorkerLease` 用于撤销未完成租约
