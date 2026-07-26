# Practice E: Actor + GCS 协同调度

## 目标
打通 actor 的创建、租约、创建成功/失败恢复链路。

## 必做代码流
- `GcsActorManager::CreateActor`
- `GcsActorScheduler::Schedule` -> `LeaseWorkerFromNode`
- `HandleWorkerLeaseReply` -> `CreateActorOnWorker`

## 必看源码
- `/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_scheduler.cc`
