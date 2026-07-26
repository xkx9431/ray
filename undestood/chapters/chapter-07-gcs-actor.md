# Chapter 07: GCS 在 Actor 调度中的角色

## 必看源码
- `/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_scheduler.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`

## 基础代码流（必要）
1. `GcsActorManager::CreateActor` 进入 actor 生命周期管理
2. `gcs_actor_scheduler_->Schedule(actor)` 选择节点
3. `LeaseWorkerFromNode(...)` 向 raylet 发 `RequestWorkerLease`
4. `HandleWorkerLeaseReply(...)` 处理 granted/rejected/canceled
5. granted 后 `CreateActorOnWorker(...)` 真正创建 actor
6. 失败时回到 `OnActorSchedulingFailed` 做重试/恢复

## 必理解点
- 普通任务主要由 raylet 主导；actor 生命周期由 GCS 强协调
- 节点故障时，actor 重建路径通常先经过 GCS 再落到 raylet
