# Chapter 06: WorkerPool（执行供给层）

## 必看源码
- `/home/runner/work/ray/ray/src/ray/raylet/worker_pool.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/worker_pool.h`

## 基础代码流（必要）
1. Raylet 启动后 `WorkerPool::Start`
2. 根据配置预启动 worker（降低首任务延迟）
3. 调度阶段 `PrestartWorkers(...)` 补充 worker
4. lease 授予时从 pool 分配匹配 worker
5. worker 退出/异常后清理并触发后续调度补偿

## 关键理解
- WorkerPool 决定“能否及时拿到执行进程”
- 调度策略选中节点后，能否快速执行依赖 WorkerPool 状态
