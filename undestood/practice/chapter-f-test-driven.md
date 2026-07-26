# Practice F: 用测试反向理解实现

## 目标
用测试场景快速理解真实边界行为。

## 必做代码流
- lease 幂等与取消
- infeasible 与 spillback
- worker 授予与回收
- actor 调度失败恢复

## 必看测试
- `/home/runner/work/ray/ray/src/ray/raylet/tests/node_manager_test.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/tests/cluster_lease_manager_test.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/tests/local_lease_manager_test.cc`
- `/home/runner/work/ray/ray/src/ray/raylet/scheduling/policy/tests/hybrid_scheduling_policy_test.cc`
- `/home/runner/work/ray/ray/src/ray/core_worker/tests/core_worker_test.cc`
