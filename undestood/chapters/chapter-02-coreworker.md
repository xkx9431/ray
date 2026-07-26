# Chapter 02: CoreWorker 层（任务构建与发送）

## 必看源码
- `/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_manager.cc`

## 基础代码流（必要）
1. `CoreWorker::SubmitTask` 构建 `TaskSpecification`
2. `task_manager_->AddPendingTask(...)` 记录 pending
3. `normal_task_submitter_->SubmitTask(...)` 发送 lease 请求
4. 返回 `ObjectReference` 给 Python `ObjectRef`

## Actor 必要流
1. `CoreWorker::CreateActor` 构建 actor creation task
2. 注册 actor handle 与 metadata
3. `actor_task_submitter_->SubmitActorCreationTask(...)`
4. `CoreWorker::SubmitActorTask(...)` 提交 actor 方法任务
