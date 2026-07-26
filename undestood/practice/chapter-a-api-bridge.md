# Practice A: API 到桥接层

## 目标
打通 Python API 到 Cython 桥接的最小主链。

## 必做代码流
- `RemoteFunction._remote` -> `_raylet.pyx::submit_task`
- `ActorClass._remote` -> `_raylet.pyx::create_actor`

## 必看源码
- `/home/runner/work/ray/ray/python/ray/remote_function.py`
- `/home/runner/work/ray/ray/python/ray/actor.py`
- `/home/runner/work/ray/ray/python/ray/_raylet.pyx`
