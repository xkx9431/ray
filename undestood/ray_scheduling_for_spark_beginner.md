# 从 Spark 到 Ray：任务调度源码学习与实践路径（面向初学者）

> 目标：让有 Spark 经验的同学，能够快速建立 Ray 心智模型，并且可以顺着源码读懂“任务是如何被调度和执行”的。

## 0. 先建立 Spark -> Ray 对照表（5 分钟）

| Spark 概念 | Ray 对应概念 | 关键源码入口 |
|---|---|---|
| Driver | Python Driver + CoreWorker | `/home/runner/work/ray/ray/python/ray/_private/worker.py`, `/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc` |
| Executor | Ray Worker 进程 | `/home/runner/work/ray/ray/src/ray/raylet/worker_pool.cc` |
| Cluster Manager + Scheduler | Raylet + 调度策略 + GCS | `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`, `/home/runner/work/ray/ray/src/ray/raylet/scheduling/*`, `/home/runner/work/ray/ray/src/ray/gcs/*` |
| Stage/Task 提交 | `@ray.remote`/Actor 提交 | `/home/runner/work/ray/ray/python/ray/remote_function.py`, `/home/runner/work/ray/ray/python/ray/actor.py` |
| 任务资源匹配 | 资源请求 + 调度策略 + spillback | `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.cc`, `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/*` |
| 结果拉取（action） | `ray.get`/ObjectRef | `/home/runner/work/ray/ray/python/ray/_private/worker.py`, `/home/runner/work/ray/ray/python/ray/_raylet.pyx` |

---

## 1. 按项目结构看“任务调度”关键代码路径

## 第 1 章：Python API 层（用户调用入口）

### 1.1 普通任务入口
- `@ray.remote` 函数最终走到：
  - `/home/runner/work/ray/ray/python/ray/remote_function.py`
  - 关键函数：`RemoteFunction._remote`（构造 task options、资源、调度策略）
  - 提交点：`worker.core_worker.submit_task(...)`

### 1.2 Actor 入口
- Actor 创建与方法调用：
  - `/home/runner/work/ray/ray/python/ray/actor.py`
  - 关键函数：`ActorClass._remote`（创建 actor）
  - 创建提交：`worker.core_worker.create_actor(...)`
  - 方法调用提交：`worker.core_worker.submit_actor_task(...)`

### 1.3 Python/C++ 桥接层
- Cython bridge：
  - `/home/runner/work/ray/ray/python/ray/_raylet.pyx`
  - 关键函数：
    - `submit_task` -> `CCoreWorkerProcess.GetCoreWorker().SubmitTask(...)`
    - `create_actor` -> `...CreateActor(...)`
    - `submit_actor_task` -> `...SubmitActorTask(...)`

---

## 第 2 章：CoreWorker 层（客户端侧任务构建与发送）

- 文件：`/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc`
- 关键函数：
  - `CoreWorker::SubmitTask`
  - `CoreWorker::CreateActor`
  - `CoreWorker::SubmitActorTask`

### 2.1 你会看到的关键动作
1. 构造 `TaskSpecification`（含资源、重试、label、fallback 等）
2. `task_manager_->AddPendingTask(...)` 记录 pending
3. 通过 submitter 发送：
   - 普通任务：`normal_task_submitter_->SubmitTask(...)`
   - actor 任务：`actor_task_submitter_->SubmitTask(...)`

### 2.2 推荐同步阅读文件
- `/home/runner/work/ray/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_manager.cc`

---

## 第 3 章：Raylet RPC 接入层（接收 lease 请求）

- 文件：`/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`
- 核心入口：`NodeManager::HandleRequestWorkerLease`

### 3.1 该入口核心逻辑
1. lease 去重（已 granted 直接返回 worker 地址）
2. caller 是否死亡检查（快速 cancel）
3. 若 lease 已在队列中，追加 reply callback（幂等）
4. `worker_pool_.PrestartWorkers(...)`
5. `cluster_lease_manager_.QueueAndScheduleLease(...)`

### 3.2 RPC 协议定义
- `/home/runner/work/ray/ray/src/ray/protobuf/node_manager.proto`
  - `RequestWorkerLeaseRequest`
  - `RequestWorkerLeaseReply`
  - `rpc RequestWorkerLease(...)`
  - `rpc CancelWorkerLease(...)`

---

## 第 4 章：Lease 调度主流程（Cluster + Local 两级）

### 4.1 Cluster 级调度（选节点）
- 文件：`/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc`
- 关键函数：
  - `QueueAndScheduleLease`
  - `ScheduleAndGrantLeases`
  - `TryScheduleInfeasibleLease`
  - `ScheduleOnNode`

作用：
- 维护 `leases_to_schedule_` 与 `infeasible_leases_`
- 先做“选哪个节点”，再决定本地授予还是 spillback

### 4.2 Local 级调度（本地队列与授予 worker）
- 文件：`/home/runner/work/ray/ray/src/ray/raylet/scheduling/local_lease_manager.cc`
- 关键函数：
  - `QueueAndScheduleLease`
  - `WaitForLeaseArgsRequests`
  - `GrantScheduledLeasesToWorkers`
  - `SpillWaitingLeases`
  - `Grant`

作用：
- 参数依赖未就绪时先等待
- 本地资源可用时分配 worker 并授予
- 资源紧张时触发 spillback

---

## 第 5 章：资源与策略层（真正决定“选谁”）

### 5.1 调度资源总入口
- 文件：`/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.cc`
- 关键函数：
  - `GetBestSchedulableNode(...)`（多重重载）
  - `IsSchedulable(...)`

关键点：
- 根据 scheduling strategy（DEFAULT/SPREAD/NODE_AFFINITY/PG/NODE_LABEL）选择分支
- 综合可行性（feasible）与可用性（available）
- 支持 label selector + fallback strategy

### 5.2 策略分发器
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/composite_scheduling_policy.cc`

### 5.3 默认混合策略（最值得先读）
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc`
- 关键机制：
  - 节点可行性判断
  - 基于关键资源利用率打分
  - top-k 随机（降低热点）
  - local node 优先与 spillback 协调

---

## 第 6 章：Worker 供给层（调度结果如何落地执行）

- 文件：`/home/runner/work/ray/ray/src/ray/raylet/worker_pool.cc`
- 关键职责：
  - worker 启动、缓存、复用
  - `PrestartWorkers` 降低调度延迟
  - runtime env 与语言 worker 管理

这层对应 Spark 中 executor slot/进程供给问题，但 Ray 粒度更细（worker 生命周期和 runtime env 绑定更紧）。

---

## 第 7 章：GCS 在调度中的角色（重点看 Actor）

### 7.1 Actor 调度器
- 文件：`/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_scheduler.cc`
- 关键函数：
  - `Schedule`
  - `SelectForwardingNode`
  - `LeaseWorkerFromNode`
  - `HandleWorkerLeaseReply`
  - `CreateActorOnWorker`

### 7.2 Actor 管理器
- 文件：`/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- 关键函数：
  - `CreateActor`
  - `SchedulePendingActors`
  - `OnActorSchedulingFailed`
  - `OnActorCreationSuccess`

理解重点：普通 task 主要在 raylet 调度，actor 的生命周期协调强依赖 GCS。

---

## 第 8 章：从一次 `f.remote()` 出发的完整调用链（源码级）

1. 用户调用：`f.remote()`
   - `/home/runner/work/ray/ray/python/ray/remote_function.py` `RemoteFunction._remote`
2. Python 提交到 CoreWorker
   - `/home/runner/work/ray/ray/python/ray/_raylet.pyx` `submit_task`
3. C++ CoreWorker 构建任务并发送
   - `/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc` `CoreWorker::SubmitTask`
4. Raylet 收到 `RequestWorkerLease`
   - `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc` `HandleRequestWorkerLease`
5. ClusterLeaseManager 选节点
   - `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc` `ScheduleAndGrantLeases`
6. ClusterResourceScheduler + Policy 给出节点
   - `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.cc`
   - `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc`
7. 本地可跑则进入 LocalLeaseManager 授予 worker；否则 spillback
   - `/home/runner/work/ray/ray/src/ray/raylet/scheduling/local_lease_manager.cc`
8. Worker 执行并返回 ObjectRef
   - 回到 CoreWorker / Python `ray.get`

---

## 2. 实践路径（undestood 章节化学习路线）

## Chapter A：环境与入口熟悉（半天）

### 学习目标
- 明确 Python API 到 C++ 调度链路入口。

### 必读源码
- `/home/runner/work/ray/ray/python/ray/__init__.py`
- `/home/runner/work/ray/ray/python/ray/remote_function.py`
- `/home/runner/work/ray/ray/python/ray/actor.py`
- `/home/runner/work/ray/ray/python/ray/_raylet.pyx`

### 实践任务
1. 写最小 `@ray.remote` 与 actor 示例（本地运行）
2. 对照 `_remote` / `submit_task` 参数，记录每个参数语义
3. 重点理解 scheduling_strategy、resources、num_returns 如何下传

### 章节产出
- 你自己的“API 参数 -> 调度行为”对照表。

---

## Chapter B：CoreWorker 任务构建（1 天）

### 学习目标
- 看懂 task/actor task spec 是怎么组装并进入 pending 的。

### 必读源码
- `/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_manager.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_submission/normal_task_submitter.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/task_submission/actor_task_submitter.cc`

### 实践任务
1. 跟踪 `CoreWorker::SubmitTask` 中 `TaskSpecBuilder` 字段
2. 对比普通任务和 actor 任务提交流程差异
3. 画出 pending -> submitted -> completed 的状态流

### 章节产出
- 一张 CoreWorker 状态机草图。

---

## Chapter C：Raylet lease 调度主线（1~2 天）

### 学习目标
- 吃透 `HandleRequestWorkerLease -> QueueAndScheduleLease -> Grant/Spillback`。

### 必读源码
- `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/local_lease_manager.cc`
- `/home/runner/work/ray/ray/src/ray/protobuf/node_manager.proto`

### 实践任务
1. 找到 lease 幂等处理代码并解释为什么要追加 callback
2. 找到 caller 死亡时的 cancel 路径
3. 找到“参数依赖未就绪 -> waiting queue”的路径
4. 找到 spillback 触发条件

### 章节产出
- 一张 lease 生命周期时序图（driver/core worker/raylet/worker）。

---

## Chapter D：调度策略与资源选择（1 天）

### 学习目标
- 理解默认策略为何是“混合打分 + top-k 随机 + 本地优先”。

### 必读源码
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/composite_scheduling_policy.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc`

### 实践任务
1. 对比 DEFAULT 与 SPREAD 的路径分支
2. 找出 NodeAffinity/NodeLabel 的分支入口
3. 解释 feasible 与 available 的区别
4. 解释 preferred node 与 force spillback 的关系

### 章节产出
- 一页“策略选择树（if/else 决策树）”。

---

## Chapter E：Actor 与 GCS 协同调度（1 天）

### 学习目标
- 看懂 Actor 从 GCS 发起 lease 再到 raylet 落地执行。

### 必读源码
- `/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_manager.cc`
- `/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_scheduler.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`

### 实践任务
1. 跟踪 `CreateActor -> Schedule -> LeaseWorkerFromNode` 调用链
2. 看 `HandleWorkerLeaseReply` 的 granted/rejected/canceled 分支
3. 分析 actor 重试、节点故障时重调度路径

### 章节产出
- “Actor 生命周期与调度恢复”流程图。

---

## Chapter F：用测试反向理解实现（持续）

### 建议先读的测试
- `/home/runner/work/ray/ray/src/ray/raylet/tests/node_manager_test.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/tests/local_lease_manager_test.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/tests/hybrid_scheduling_policy_test.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/tests/core_worker_test.cc`

### 实践任务
1. 每读完一章，挑 2~3 个测试用例对照
2. 用测试名反推“这个场景为何存在”
3. 优先关注：幂等、取消、资源不足、节点故障、spillback

---

## 3. 快速掌握建议（Spark 背景专用）

1. **先抓主链，不先抠细节**：先跑通第 8 章调用链。
2. **把“lease”当作核心抽象**：Ray 调度关键不是 stage 划分，而是 lease 获取与兑现。
3. **把“本地优先 + spillback”当作默认策略心智**：这比 Spark 的中心化调度器更分布式。
4. **优先吃透 actor 路径**：很多 Ray 高阶能力（Serve、长期状态）都依赖 actor。
5. **读测试比读注释更快**：先看失败场景和边界场景。

---

## 4. 建议的阅读顺序（最短路径）

1. `/home/runner/work/ray/ray/python/ray/remote_function.py`
2. `/home/runner/work/ray/ray/python/ray/_raylet.pyx`
3. `/home/runner/work/ray/ray/src/ray/core_worker/core_worker.cc`
4. `/home/runner/work/ray/ray/src/ray/raylet/node_manager.cc`
5. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_lease_manager.cc`
6. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/local_lease_manager.cc`
7. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.cc`
8. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc`
9. `/home/runner/work/ray/ray/src/ray/gcs/actor/gcs_actor_scheduler.cc`
10. `/home/runner/work/ray/ray/src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc`

---

## 5. 你学完后应达到的“可验证能力”

- 能从 `f.remote()` 一路讲到 worker 被授予并执行。
- 能解释 lease pending/infeasible/waiting/granted/canceled 几种状态。
- 能解释为何会 spillback、何时本地优先。
- 能定位普通任务与 actor 调度的源码差异。
- 能根据报错快速定位到 Python/CoreWorker/Raylet/GCS 哪一层。

