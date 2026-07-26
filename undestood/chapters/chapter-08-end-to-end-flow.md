# Chapter 08: 端到端调用链（`f.remote()`）

## 必看源码（按顺序）
1. `/home/runner/work/ray/ray/python/ray/remote_function.py`
2. `/home/runner/work/ray/ray/python/ray/_raylet.pyx`
3. `/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc`
4. `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`
5. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc`
6. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/local_lease_manager.cc`
7. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.cc`
8. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc`

## 基础代码流（必要）
1. Python `remote()` 生成任务提交参数
2. CoreWorker 构建 `TaskSpecification` 并发起 lease
3. Raylet 接收 lease，请求入队并调度
4. 资源策略选择执行节点
5. 本地授予 worker 或 spillback
6. worker 执行，结果回传为 `ObjectRef`
7. `ray.get` 拉取对象完成闭环
