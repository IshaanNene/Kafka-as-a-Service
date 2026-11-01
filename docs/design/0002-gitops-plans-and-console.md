# Design 0002: GitOps plans and the fleet console

Phase 2 of KaaS manages many clusters from git. This note covers four pieces:
- the desired-state file;
- how a plan is made, checked and applied;
- the fleet console that shows it all;
- what is deliberately left for later.

## Desired state

Each cluster has one YAML file, reviewed in pull requests like any other code:

```yaml
cluster: prod-eu
topics:
  orders:
    partitions: 12
    replication: 3
    configs: {retention.ms: 7d, cleanup.policy: delete}
acls:
  - principal: User:billing
    resource: topic
    name: orders
    operations: [read, describe]
quotas:
  - user: analytics
    values: {consumer_byte_rate: 10MiB}
policies:
  - name: replicated
    rule: topic.replication >= 3
    message: production topics need three replicas
prune: {acls: true}
```

- **Durations and sizes are written as people say them**, such as `7d` or `10MiB`, and normalised when the file is parsed.
- **Unknown keys are errors**, so a misspelt `partitons:` fails the plan instead of being ignored.
- **A file manages only the kinds it declares.** Without an `acls:` key, the cluster's ACLs are left alone even with `prune.acls`.
- **Nothing is deleted unless `prune` names that kind.** Live objects the file doesn't mention are counted as "not in the file".
- **A topic that declares `configs` has all its topic-level configs managed.** A config set on the cluster but missing from the file is planned for removal. Leave `configs` out to manage only shape.
- **Policies** are CEL rules over `topic.name`, `.partitions`, `.replication` and `.configs`, with a cost limit (ADR-0005). A violation blocks the plan.

## A plan

`core/plan` makes a plan in five steps.

1. **Read** (`plan.Read`). One snapshot from the cluster-state engine, configs for the declared topics, then ACLs (exact DescribeACLs) and quotas.
2. **Diff** (`plan.Diff`). A pure function from desired and live state to ordered operations, so it is fully tested without a cluster. Each op has:
   - a kind;
   - a target;
   - from and to values;
   - a sentence saying what it does;
   - a risk.

   Risks:
   - **safe**: adds something; nothing that works now stops working.
   - **caution**: changes behaviour. That covers more partitions (keyed ordering), less retention, unset configs, and lower or removed quotas.
   - **destructive**: removes data or access, such as a topic delete or a revoked ACL.

   Things a plan can't do are *warnings*, not ops. For example, Kafka can't remove partitions, and changing the replication factor needs a reassignment.
3. **Check** (`plan.Check`). Each op is sent to the controller with `validateOnly` where Kafka supports it: topic creation, partition growth, configs, quotas. A refusal is recorded on the op and blocks the plan.
4. **ID.** A plan's ID is a hash of its ops. `kaas apply --plan ID` and the console's apply both re-plan first and refuse when the ID has changed. What runs is always what was reviewed.
5. **Apply** (`plan.Apply`). Changes run in a fixed order:
   1. create topics
   2. add partitions
   3. set and unset configs (one request per topic)
   4. grant ACLs
   5. set and remove quotas
   6. revoke ACLs
   7. delete topics

   New grants land before stale ones are revoked, so a migration never leaves a client without access. Apply stops at the first failure and reports what was done.

### In CI

```bash
kaas plan -f gitops/prod-eu.yaml --exit-code        # on the PR: 0 in sync, 5 changes, 1 blocked
kaas plan -f gitops/prod-eu.yaml -o json > plan.json # for a bot to comment
kaas apply -f gitops/prod-eu.yaml --plan "$(jq -r .id plan.json)" --yes   # after merge
```

## The console

`kaas console` serves the fleet (ADR-0008).

### Runtime

- Each cluster in the fleet file gets a `Cluster` running the same engine the CLI uses, with its own request budget. The console adds four slower loops on top:
  - throughput per topic, measured from offset growth;
  - consumer groups and lag (10s);
  - ACLs, quotas, disk and schemas (60s);
  - the plan (30s, re-read from the file each time).
- **Health is computed, not configured.**
  - Unreachable after three missed metadata intervals.
  - Critical on an offline partition.
  - Degraded on an under-replicated partition.
  - Lag and drift are shown on each cluster's card, not counted as health.
- **Availability** comes from a health sample every 10 seconds, kept for an hour. Each minute of the status strip shows the worst sample in it; availability is the share of samples that were healthy or degraded.
- **Freshness** is a group's lag divided by the rate its topics are written at, so lag is said in time rather than records.
- **Cost** is an estimate from the optional `pricing` in the fleet file: brokers × hourly price × 730, plus stored GiB × monthly price.
- **Recommendations** (`insights.go`) are recomputed from live state on every read, never stored, so each goes away with its cause. Each says what it costs the business, links to the page that shows it and gives a `kaas` command. They are sorted critical, warning, then advice.
- **Events** come from the engine's typed events (leader moves, ISR changes, brokers joining or leaving), plus drift, applies and reachability. They live in a ring of 500 and stream over SSE with `Last-Event-ID` replay, so a reconnecting browser misses nothing in the ring.
- **Schemas** are read from each cluster's Schema Registry (Confluent-compatible REST API): subjects, their latest version, version count, and compatibility, global or per subject.

### API

| Route | Returns |
|---|---|
| `GET /api/meta` | Version, demo, whether apply is allowed |
| `GET /api/fleet` | A card per cluster (with availability, uptime slots, freshness and cost), fleet totals, produced and consumed history, recommendations, recent events |
| `GET /api/events` | Server-sent events |
| `GET /api/clusters/{name}` | Brokers, topics, groups with data age, events, recommendations, the request budget |
| `GET /api/clusters/{name}/topics/{topic}` | Partitions and the groups reading it |
| `GET /api/clusters/{name}/access` | ACLs and quotas |
| `GET /api/clusters/{name}/schemas` | Subjects |
| `GET /api/clusters/{name}/plan` | The current plan and whether it can be applied |
| `POST /api/clusters/{name}/apply` | `{"plan": ID, "confirm": name}` |

Lists are always arrays, never `null`.

Apply errors:
- 403 when apply isn't allowed, or the request is cross-origin;
- 415 when the body isn't JSON;
- 400 when the name typed is wrong;
- 409 when the plan changed, or another apply is running;
- 422 when the plan is blocked;
- 502 when Kafka refused.

### The web app

- **Pages.** Overview, Recommendations, Changes (every cluster's drift on one page), Activity, and per cluster: overview, topics with a partition map, consumers, access, schemas and GitOps.
- **Design tokens.** Every colour is a CSS custom property in `app.css`, defined once for dark and once for light; components never use literal colours. Surfaces are neutral graphite. The lime accent marks primary actions, the selected page and the brand, never health. Health uses semantic pairs (success, warning, critical, information), each a foreground and a subtle background; recommendations use violet. Text keeps 4.5:1 contrast on every surface.
- **Updates.** Data is polled every 2.5 seconds and events are streamed. A rendering batch per animation frame and a keyed morph keep the page still while numbers change underneath.
- **Charts.**
  - Fixed series colours everywhere: produced is teal, consumed is violet and dashed, healthy capacity is green. The pair passes the colour-vision checks in both themes.
  - A recessive grid.
  - A crosshair and tooltip on hover.
  - Sparklines scaled to their own range, so a quiet cluster's shape still shows.
- **Navigation.** ⌘K or `/` jumps to any cluster, topic, group or page.

## Not done yet

| Gap | Plan |
|---|---|
| Login | OIDC in front of the console, with roles: view, plan, apply. Until then, localhost only with `--allow-apply` |
| TLS and SASL to brokers | The franz-go options exist; the fleet file needs fields for them |
| Audit log | Applies are events today. ADR-0002's Postgres and outbox will keep them, with who and which commit |
| Schemas as desired state | Subjects and compatibility are shown; declaring them in the file comes next |
| Replication-factor changes | Planned as a warning today; a reassignment op with throttles comes next |
| Phase 3 | The operator reports its clusters to this console, which gains create, roll and autoscale pages |
