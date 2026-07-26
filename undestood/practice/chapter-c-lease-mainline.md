# Practice C: Lease 主链

## 目标
吃透 RequestWorkerLease 到 grant/spillback/cancel 的主流程。

## 必做代码流
- `NodeManager::HandleRequestWorkerLease`
- `ClusterLeaseManager::QueueAndScheduleLease`
- `LocalLeaseManager::GrantScheduledLeasesToWorkers`

## 必看源码
- `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/local_lease_manager.cc`
