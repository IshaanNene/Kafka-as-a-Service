# Design 0001: The cluster-state engine

- Status: implemented (first slice), Phase 1
- Date: 2026-10-31
- Code: `core/snapshot`, `core/engine`, `cmd/kaas/watch.go`

## Problem

Every KaaS surface (TUI, CLI, agent, API) needs the same answer to "what does this cluster look like right now?" Kafka offers no metadata watch API, so the only option is polling. Naive tools poll everything every second on every screen, and on a 100k-partition cluster that load lands on the brokers and the controller exactly when operators are already looking at a problem.

## Goals

- One immutable `Snapshot` per cluster that every surface renders. No screen reads live client state.
- Metadata visible in the UI within **5 s** of a change on the cluster (Phase 1 freshness target).
- Admin load bounded by a per-cluster budget that the operator sets, not by how many screens are open.
- Changes surfaced as typed events (`LeaderMoved`, `ISRShrunk`, ...) for toasts, alerts and the web read model.
- Stays responsive at **100k partitions**.

## Non-goals (this slice)

- Consumer groups and lag. The lag engine (signature system 1) builds on this engine's offsets.
- Topic configs, ACLs and quotas.
- Screen rendering. The TUI's 60 fps frame cap belongs to `ui/`; the engine only guarantees that a slow reader never blocks it.

## Design

### Snapshots

`snapshot.Snapshot` holds brokers, controller, topics and partitions (leader, replicas, ISR, offline replicas, log-start and log-end offsets). Topics and partitions are sorted, so lookups use binary search and diffs use a merge join. Each topic carries `OffsetsAt`, so a screen can show how old a number is instead of hiding it.

A published snapshot is never mutated. When only offsets change, the engine copies the snapshot struct and the touched topics' partition slices and shares everything else.

### One loop owns every request

`Engine.Run` is the only goroutine that talks to the cluster. On each tick (the fast interval, 1 s by default) it decides what is due:

| Data | Refreshed every | Cost to the budget |
|---|---|---|
| Metadata (all topics) | 5 s | 1 request |
| Offsets of watched topics | 1 s | 2 × distinct leaders |
| Offsets of every other topic | 30 s | 2 × distinct leaders |

Screens call `Watch(topics...)` for what they show. `Refresh()` forces metadata and watched offsets on the next pass. It writes to a channel of capacity 1, so any number of concurrent calls collapse into one refresh.

Offsets use kadm's `ListStartOffsets` and `ListEndOffsets`, which send one request per partition leader. The budget charges exactly that. The 100k benchmark below counts 6 requests per full pass on 3 brokers, matching the model.

### Budget

`Budget` is a token bucket (`golang.org/x/time/rate`, 20 req/s with a burst of 40 by default). Every refresh pays before it runs. When the bucket is empty, the refresh is skipped, not queued. Data gets older, the UI shows its age, and the cluster sees less load at the moment it may be struggling. `Stats()` exposes tokens, granted and denied for `kaas watch -v` and the future debug overlay.

### Events

`Diff(prev, next)` is a pure function that emits events in a deterministic order: brokers, controller, then topics by name and partitions by ID. The first snapshot is a baseline and produces no events. Offset movement is not an event. Events are computed only when metadata was refreshed.

### Subscriptions

A `Subscription` is a single-slot mailbox. It keeps only the newest snapshot, because renderers draw the latest state, and accumulates events up to 4,096, counting any beyond that in `DroppedEvents`. Delivery never blocks the engine loop.

## A finding: kadm.Metadata is cached

`kadm.Metadata` goes through `kgo.Client.RequestCachedMetadata`, which serves topic data up to `MetadataMinAge` old (5 s by default). Polling every 5 s through a 5 s cache can serve data close to 10 s old, which breaks the freshness target. Tests caught it: topics created on kfake never appeared within the test timeout.

kadm's only bypass also asks the broker for authorized operations on every topic, which is expensive on large clusters with ACLs. So `snapshot.Fetch` sends a raw `MetadataRequest` through `kmsg.Requestor` and decodes it itself. As a side effect, topics that return an error are skipped instead of failing the whole snapshot.

## Measurements

100k partitions (1,000 topics × 100) on 3 brokers, in-process kfake, Apple M-series, `go test -bench . ./core/engine`:

| Operation | Time | Allocated |
|---|---|---|
| `Diff`, 1% of leaders moved | 0.98 ms | 24 KB, 10 allocs |
| `Fetch` (metadata request, decode, build) | 33.6 ms | 132 MB, 633k allocs |
| Full offsets pass (start and end) | 89.9 ms, 6 requests | 307 MB |

kfake runs in-process, so these timings include the fake broker's own work; they are upper bounds on client cost, not network measurements.

## Risks and follow-ups

- **Allocation on fetch.** 132 MB per metadata fetch is about 26 MB/s of garbage at a 5 s interval. Most of it is kmsg decoding. Options: reuse decode buffers, or switch to `DescribeTopicPartitions` (KIP-966) with pagination on Kafka 3.9+. Measure before choosing.
- **Slow tier at 100k partitions** reads every partition's offsets at once. It is cheap in requests (6) but costly in allocation (307 MB). Spread it across ticks if GC pauses show up in the TUI frame-time benchmark.
- **ISR events are only unit-tested.** kfake always reports ISR = replicas, so ISR shrink and expand are covered by pure `Diff` tests and still need a fault-injection test against a real cluster (Toxiproxy or a stopped broker).
- **Interest scheduling is per topic, not per partition.** Fine for current screens; the partition matrix may need finer interests.

## Alternatives rejected

- **Reading `__consumer_offsets` directly (Burrow-style)** couples KaaS to broker internals. Admin APIs are the stable contract.
- **A goroutine per screen polling on its own** multiplies load by open screens and makes the budget impossible to enforce.
- **Queuing refreshes when over budget** turns a struggling cluster into a backlog that lands all at once when it recovers.
