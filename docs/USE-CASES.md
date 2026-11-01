# Use cases: how kaas helps a company run Kafka

Kafka tends to become critical before it becomes understood. The people who depend on it (application teams, data engineers, on-call SREs, QA) usually reach it through JVM shell scripts, a web console someone has to host, or a colleague who knows the commands. `kaas` gives each of them a fast, safe, scriptable tool, so fewer questions need an expert and fewer mistakes reach production.

Each scenario below has the problem, how `kaas` handles it, and what changes for the team.

---

## 1. On-call: "Kafka is down" at 3 a.m.

**The problem.** An alert fires: consumers are lagging and a service can't connect. The on-call engineer has to work out whether it's the network, a broker, a partition or the consumers, before anyone can act.

**With kaas.**

```bash
kaas snapshot                 # is anything offline or under-replicated? problems sort first
kaas watch -t orders          # are leaders moving or ISRs shrinking right now?
kaas brokers                  # is one broker carrying too many leaders?
kaas lag --top 10             # which groups are behind, and is it getting worse?
```

If the cluster can't be reached at all, the error screen says which layer failed (DNS, TCP, the Kafka protocol or metadata) and what to try. That separates "the VPN dropped" from "the controller is down" in seconds.

**What changes.** Diagnosis no longer depends on remembering `kafka-topics --describe --under-replicated-partitions` flags. The first five minutes of an incident produce answers instead of command-line archaeology.

---

## 2. Replaying data after a bad deploy

**The problem.** A consumer shipped a bug at 09:00 and wrote bad results for two hours. The team needs to reprocess from 09:00, and to know exactly what that means, before moving offsets on a production group.

**With kaas.**

```bash
kaas offsets find orders --time 2026-10-31T09:00                 # where 09:00 starts, per partition
kaas offsets reset billing -t orders --to 2026-10-31T09:00 --dry-run   # the plan: records re-read per partition
kaas offsets reset billing -t orders --to 2026-10-31T09:00       # type yes to commit
```

The reset refuses while the group still has members, shows every partition's move with the record count, and commits only after you type `yes`. If the problem started with a record shape rather than a time, `--match` finds the first matching record by bisecting.

**What changes.** A replay that used to need a reviewed runbook and a careful engineer becomes a plan anyone can read before it runs.

---

## 3. Getting data in and out of Kafka without writing a service

**The problem.** Teams keep writing small services to move data:
- a webhook into a topic;
- a topic into a file for analysts;
- one topic filtered into another;
- a partner's API into Kafka.

Each is code to build, deploy, monitor and own. Kafka Connect solves this for many systems, but it is a cluster to run in its own right.

**With kaas pipe.**

```bash
# Partner webhooks into a topic: each request answered only once its records are stored
kaas pipe --from listen://:8080/ingest --to kafka:partner-events

# An analyst's daily extract, from where yesterday's left off
kaas pipe --from 'kafka:orders?group=daily-extract&stop=end' --to file:/exports/orders.jsonl

# High-value EU orders to a fraud service, plus an audit topic
kaas pipe -f pipelines/fraud-feed.yaml
```

Delivery is at least once: a consumer group commits only what every sink wrote. Filters are one CEL expression. Anything the built-ins don't cover becomes a plugin, a script that speaks JSON lines. The pipeline itself is a YAML file in git.

**What changes.** Simple integrations go from a service per integration to a reviewed config file. Kafka Connect is left for the integrations that really need it.

---

## 4. Testing with production-shaped data

**The problem.** Staging runs on synthetic data that is too clean, and bugs show up only in production. Copying production data across by hand is slow and error-prone.

**With kaas.**

```bash
# Sample an hour of production…
kaas consume orders -b prod:9092 --from 1h --exit -o json > sample.jsonl
# …and replay it into staging: keys, headers and binary values intact
kaas pipe --from file:sample.jsonl --to 'kafka:orders?brokers=staging:9092'

# Or leave out records that carry personal data
kaas pipe --from file:sample.jsonl --where '!has(value.email)' --to 'kafka:orders?brokers=staging:9092'
```

**What changes.** Staging sees the real key distribution, headers and edge cases. Reproducing a production bug becomes two commands.

---

## 5. Capacity planning and performance regressions

**The problem.** Nobody knows how much headroom the cluster has, or whether lz4 or zstd suits this workload. Producer settings get tuned by folklore.

**With kaas.**

```bash
kaas bench produce -t perf --size 100,1024,10240 --acks 1,all --compression none,lz4,zstd
kaas bench consume -t perf --fetch-size 64KB,1MB,8MB
kaas pipe --from 'gen:orders?rate=20000' --to kafka:perf --for 30m   # sustained load while you watch kaas top
```

The matrix runs each combination with its own producer and reports p50, p99 and max latency next to throughput. With `-o json` the results can be stored and compared release over release.

**What changes.** Settings are chosen from measurements on your own cluster. A regression shows up as a number in CI instead of an outage.

---

## 6. Debugging a slow or misbehaving client

**The problem.** A service takes 20 seconds to start consuming, or one producer is slow. Broker metrics are aggregated across every client, so they can't show a single client's experience.

**With kaas.**

```bash
kaas sniff --trace --client-id billing-svc     # point the service at localhost:19092
kaas sniff --profile --api produce --api fetch # where the time goes, as a flame graph
```

The trace shows each request the client makes as it starts:
- its group join and the `MEMBER_ID_REQUIRED` round trip;
- metadata refreshes;
- the first produce;
- the time each one spent connecting, queued, at the broker and on the way back.

**What changes.** "Kafka is slow" becomes a specific answer, such as a long poll, a coordinator lookup or one broker's queue. The fix can be argued from data.

---

## 7. Safer operations and less key-person risk

**The problem.** Only a few people are trusted to delete topics or change retention, because the stock tools give no preview and no undo. Changes are hard to review.

**With kaas.**
- **Topic deletes** need the name typed, then give five seconds to undo.
- **Partition increases** say what happens to keyed records and which groups will rebalance.
- **Config edits** are validated by the controller and shown as a diff before they apply.
- **Scripts** must pass `--yes`, and every change can print JSON for an audit log.

**What changes.** More people can make routine changes safely, reviews have a concrete plan to look at, and the experts spend less time as a human safety check.

---

## 8. Platform teams standardising Kafka access

**The problem.** Every team installs different tools in different versions, with different defaults. Some of them hammer the controller.

**With kaas.**
- **One binary.** It is signed (cosign), with an SBOM and SLSA provenance, and shipped from one release pipeline.
- **One request budget per cluster.** `kaas` skips refreshes rather than queueing them, so twenty engineers watching a cluster don't load it twenty times over.
- **Stable JSON and exit codes.** Internal tooling, runbooks and CI gates can rely on them. For example, `kaas lag -o json` makes a deploy gate on consumer lag a single line.

**What changes.** One supported tool replaces a pile of scripts, and the platform team can reason about the load its users put on the cluster.

---

## 9. Many clusters, managed from git

**The problem.** A platform team runs a dozen clusters across regions and environments. Topics, ACLs and quotas are created by hand or by tickets, so nobody can say what production *should* look like. A stale ACL from a retired job lingers for years. Each morning someone checks every cluster's dashboard in turn.

**With kaas.**

```bash
kaas console --fleet fleet.yaml                       # every cluster, health and drift, on one page
kaas plan  -f gitops/prod-eu.yaml --exit-code         # CI on each pull request
kaas apply -f gitops/prod-eu.yaml --plan "$id" --yes  # CI after merge
```

- **Desired state in git.** Each cluster's topics, configs, ACLs and quotas live in a file in git, so a change is a pull request.
- **Plans for review.** The plan shows reviewers each change with its risk. "orders keeps less data" is marked caution, and a revoked ACL is marked destructive.
- **Policies enforced.** Rules like "production topics have three replicas" block a plan before it reaches a reviewer.
- **Only what was reviewed.** An apply runs only the reviewed plan, and grants new access before revoking old access.
- **One page for the fleet.** The console shows every cluster's health, throughput, lag and drift on one page, and its feed shows a leader moving in Singapore next to a plan applied in Frankfurt.

**What changes.**
- The file in git is the answer to "what should production look like?".
- Drift is a red number on a card, not a surprise in an incident.
- Access reviews read sentences, not `kafka-acls` output.

---

## What kaas does not do (yet)

- **Schema contracts in pipes.** The console shows each cluster's Schema Registry subjects and compatibility, but pipes don't decode Avro or Protobuf, and schemas aren't part of the desired-state file yet. A `--where 'has(value.field)'` filter can gate a pipe on shape.
- **Exactly-once pipes.** Pipes are at least once. Downstream systems should be idempotent by key, as Kafka consumers generally must be.
- **Login.** The console has no login or roles yet, so it stays on localhost unless it sits behind an authenticating proxy. OIDC is next.
- **TLS and SASL to brokers.** Not configurable yet, for the CLI or the fleet file.
- **Managed Kafka on Kubernetes.** Creating, rolling and autoscaling clusters is Phase 3. Its clusters will show up in the same console.
