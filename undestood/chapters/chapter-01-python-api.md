# Chapter 01: Python API 层（用户调用入口）

## 必看源码
- `/home/runner/work/ray/ray/python/ray/remote_function.py`
- `/home/runner/work/ray/ray/python/ray/actor.py`
- `/home/runner/work/ray/ray/python/ray/_raylet.pyx`

## 基础代码流（必要）
1. `f.remote()` -> `RemoteFunction._remote(...)`
2. 组装 task options（resources、scheduling_strategy、num_returns）
3. `worker.core_worker.submit_task(...)`
4. 进入 Cython 桥接 `_raylet.pyx::submit_task`

## Actor 对应流
1. `ActorClass._remote(...)`
2. `worker.core_worker.create_actor(...)`
3. `actor_handle.method.remote(...)`
4. `worker.core_worker.submit_actor_task(...)`
