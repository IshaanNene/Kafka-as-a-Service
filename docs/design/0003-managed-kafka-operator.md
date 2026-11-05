# Design 0003: managed Kafka on Kubernetes

Phase 3. A team asks for a Kafka cluster and gets one; it restarts safely when asked and grows and shrinks with its load. The console shows these clusters next to the ones it monitors.

## Goals

- Create a cluster from a few lines: Kafka version, disk per broker, and either a broker count or an autoscaling policy.
- Roll every broker on request without taking partitions offline.
- Size the broker pool to measured load, adding brokers at once and removing them only after load has stayed low, with partitions moved first.
- Create, roll and resize from the console, behind the same guards as applying a GitOps plan.

## Non-goals

- Running brokers. Strimzi already does that well: pods, storage, certificates, ordered restarts, Cruise Control. Reimplementing it would be a large surface with nothing to show for it.
- Changing the KRaft controller quorum. It is fixed at three, which survives one loss.
- Topic, user and ACL management. GitOps (design 0002) already covers them.
- Deleting clusters from the console. `kubectl delete kafkaservice` is deliberate enough; a button is not.

## The resource

```yaml
apiVersion: kaas.io/v1alpha1
kind: KafkaService
metadata: {name: orders, namespace: kafka}
spec:
  version: 4.2.0
  brokers: 3          # the size when autoscale is unset
  storage: 20Gi       # per broker
  config: {}          # broker config over the defaults
  autoscale: {min: 3, max: 9, target: 20000, scaleDownAfter: 10m}
  restartAt: null     # set to now to roll
```

`status` carries the phase (`provisioning`, `ready`, `rolling`, `scaling`, `degraded`, `failed`), a message, the broker count being run towards, every broker's state, roll progress, an hour of load samples, the autoscaling recommendation and its reason, the last 20 things that happened, and a `Ready` condition.

Names are limited to 20 characters. Strimzi's pod names repeat the name (`orders-orders-controllers-12`), and pods are hostnames, which stop at 63 characters.

## What the operator renders

For each KafkaService, owned by it, so deleting the service deletes them:

| Strimzi resource | Name | What |
|---|---|---|
| `Kafka` | `orders` | Version, a plain internal listener, durable defaults and Cruise Control with `autoRebalance` for `add-brokers` and `remove-brokers` |
| `KafkaNodePool` | `orders-controllers` | Three KRaft controllers, 10Gi of metadata each |
| `KafkaNodePool` | `orders-brokers` | The brokers, `spec.storage` each, `deleteClaim: false` |
| `KafkaRebalance` | `orders-rebalance` | The template Strimzi copies for each automatic rebalance |

The defaults keep three copies with two required for a write (`min.insync.replicas`), or fewer when there are fewer brokers. Internal topics take their replication factor when they're created, so a cluster started with one broker keeps one copy of those.

Writes compare what's there with what's wanted, and only on the fields the operator sets: fields the API server or Strimzi default don't count as differences. A steady cluster costs no writes.

## Roll

Setting `spec.restartAt` to a time after `status.rolledAt` starts a roll. The operator annotates every controller and broker pod created before that time with `strimzi.io/manual-rolling-update`. Strimzi then restarts them in its own safe order: controllers first, then brokers, each one only once the partitions it leads are back in sync. A pod counts as done once it has been recreated after `restartAt` and is ready again. When all are done, `rolledAt` is set and the roll is recorded in history. While a roll runs, the operator reconciles every five seconds so progress is current.

## Autoscale

Load is the rate of change of the sum of high-water marks across every partition outside Kafka's own topics. That is one `ListOffsets` request per broker per measurement, whatever the partition count. It is taken every 30 seconds, once the cluster has been ready.

Two pure functions in `controlplane/lifecycle` decide the size; the operator and the console demo share them:

- **`Recommend`.** `ceil(load / target)`, clamped to `[min, max]`.
- **`Decide`.**
  - It goes up at once to the latest recommendation.
  - It goes down only when every recommendation over `scaleDownAfter` asked for fewer brokers, and then only to the highest of them, so a dip between bursts doesn't shed brokers about to be needed again.
  - It waits for a full window of history before shrinking, so a restarted operator doesn't read its empty memory as a quiet hour.

Recommendations live in `status.marks`, trimmed to the window, so they survive a restart.

Sizing is held while a roll runs, and until the last change has finished. Strimzi's auto-rebalance moves partitions:

- on to brokers just added;
- off brokers before they are removed. Strimzi refuses to remove a broker that still holds partitions.

New brokers are ready a minute or so before Strimzi starts moving partitions on to them, so ready pods don't mean a resize is done. When the broker count changes, the operator notes the time of the Kafka's last auto-rebalance transition. The service stays `scaling` until the auto-rebalance is idle again after a different transition, with nothing left in its queue (`status.autoRebalance.modes`). Strimzi can go idle between two rebalances for one resize, with the second still queued. Strimzi's timestamps are compared only with each other: a resize in the same second as Strimzi's first idle state would otherwise read as already rebalanced.

## The console

`controlplane/lifecycle.Manager` is the contract:

- `Services`
- `Create`
- `Roll`
- `Scale` (a fixed size; turns autoscaling off)
- `SetAutoscale`

There are two implementations:

- **`operator.Manager`.** Writes KafkaService specs through the Kubernetes API. The console never touches Strimzi resources. It needs only the `kaas-console` role: get, list, watch, create and update on `kafkaservices`.
- **`lifecycle.Demo`.** Acts out the same life in memory, compressed into seconds, for `kaas console --demo` and the console's tests.

`kaas console --kube --namespace kafka` adds the pages. Writes need `--allow-apply`, JSON, the console's own origin and, for a roll, the cluster's name typed.

| Route | |
|---|---|
| `GET /api/services` | Every managed cluster, where they run, and which are also in the monitored fleet |
| `POST /api/services` | Create (`lifecycle.Spec`) |
| `POST /api/services/{name}/roll` | Roll, with `{"confirm": name}` |
| `POST /api/services/{name}/scale` | `{"brokers": n}` |
| `POST /api/services/{name}/autoscale` | `{"autoscale": policy}`, or `null` to turn it off |

Validation errors are 400. A missing cluster is 404. A taken name, or a change while another runs, is 409.

## Alternatives considered

- **Our own broker StatefulSets.** This is the whole Strimzi surface again: ordered restarts, certificates, KRaft bootstrapping, storage migration.
- **Generated clients and deep copies (controller-gen).** This adds a code-generation step to a repository that has none. The types are small, and JSON round-trip deep copies can't fall out of step with them.
- **The console talking to the operator over HTTP.** The KafkaService is already the API: `kubectl` and the console go through the same object, RBAC and audit log.
- **Autoscaling on CPU.** Brokers are rarely CPU-bound before disk or network. Records a second is what teams reason about and what the target is set in.

## Testing

- `controlplane/lifecycle`:
  - table tests for `Recommend` and `Decide`;
  - a property test: `Decide` stays in bounds, never lags a rising recommendation, and never sheds below any recommendation in the window;
  - demo tests driving each phase through simulated time.
- `operator`: the reconciler against controller-runtime's fake client, with a test that plays Strimzi (ready conditions, pods per pool). It covers rendering, a steady pass writing nothing, provisioning to ready, autoscaling up and then down after the window, a manual resize, and a roll pod by pod.
- `controlplane/console`: the services API and its guards, against the demo manager.

Against real Strimzi it is manual for now (`task operator:dev`).
