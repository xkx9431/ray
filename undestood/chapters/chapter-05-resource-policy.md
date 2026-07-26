# Chapter 05: 资源调度与策略选择

## 必看源码
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/composite_scheduling_policy.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc`

## 基础代码流（必要）
1. `GetBestSchedulableNode(...)` 接收资源请求 + 策略
2. 根据策略分流到 `CompositeSchedulingPolicy::Schedule`
3. 默认走 `HybridSchedulingPolicy::Schedule`
4. Hybrid 评估：feasible -> available -> 打分 -> top-k 选择
5. 若本地可用且允许本地优先，则直接返回本地
6. 无可用节点时，返回 infeasible 或可行但暂不可用节点

## 必理解术语
- feasible: 资源形状理论可满足
- available: 当前可立即分配
- spillback: 本节点不执行，转发给其他节点
