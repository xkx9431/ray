# Practice B: CoreWorker 任务规格流

## 目标
理解 task spec 如何被构建并进入 pending。

## 必做代码流
- `CoreWorker::SubmitTask` -> `BuildCommonTaskSpec` -> `AddPendingTask`
- `CoreWorker::CreateActor` -> actor task spec -> submitter

## 必看源码
- `/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_manager.cc`
