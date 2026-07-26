# Chapter 04: Lease 调度主流程（Cluster + Local）

## 必看源码
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/local_lease_manager.cc`

## 基础代码流（必要）
1. Cluster 侧：`QueueAndScheduleLease` 入队
2. Cluster 侧：`ScheduleAndGrantLeases` 调 `GetBestSchedulableNode`
3. 本地命中则进入 LocalLeaseManager；否则 spillback 到其他 raylet
4. Local 侧：`WaitForLeaseArgsRequests` 等待参数依赖
5. Local 侧：`GrantScheduledLeasesToWorkers` 分配 worker 并授予 lease
6. 参数或资源长期不满足时：`SpillWaitingLeases` / cancel

## 必理解队列
- `leases_to_schedule_`（待调度）
- `infeasible_leases_`（当前不可调度）
- `waiting_lease_queue_`（依赖未就绪）
- `leases_to_grant_`（可授予）
