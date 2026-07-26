# Ray Scheduling Learning Docs (Split Version)

This folder splits the original single document into chapter files.

## Core chapters
- [Chapter 01 - Python API Entry](./chapters/chapter-01-python-api.md)
- [Chapter 02 - CoreWorker Submission](./chapters/chapter-02-coreworker.md)
- [Chapter 03 - Raylet RPC Lease Entry](./chapters/chapter-03-raylet-rpc-lease.md)
- [Chapter 04 - Cluster/Local Lease Scheduling](./chapters/chapter-04-lease-scheduling.md)
- [Chapter 05 - Resource Scheduler and Policies](./chapters/chapter-05-resource-policy.md)
- [Chapter 06 - Worker Pool Supply](./chapters/chapter-06-worker-pool.md)
- [Chapter 07 - GCS Actor Scheduling](./chapters/chapter-07-gcs-actor.md)
- [Chapter 08 - End-to-End Task Flow](./chapters/chapter-08-end-to-end-flow.md)

## Practice chapters
- [Practice A - API to Bridge](./practice/chapter-a-api-bridge.md)
- [Practice B - CoreWorker Spec Flow](./practice/chapter-b-coreworker-spec.md)
- [Practice C - Lease Mainline](./practice/chapter-c-lease-mainline.md)
- [Practice D - Policy Decision Path](./practice/chapter-d-policy-decision.md)
- [Practice E - Actor + GCS Path](./practice/chapter-e-actor-gcs.md)
- [Practice F - Test-Driven Source Reading](./practice/chapter-f-test-driven.md)

## Spark -> Ray quick mapping
- Driver -> Python Driver + CoreWorker
- Executor -> Ray Worker process
- Scheduler -> Raylet + scheduling policy + GCS
- Task submit -> `@ray.remote` / actor task submit
