# KaaS — Kafka As A Service

**Know what your Kafka cluster is doing, and why it broke, without leaving the terminal.**

![The kaas terminal UI: live topics, partitions, record counts and a cluster event feed](img/tui-topics.svg)

KaaS is one Go codebase that grows into three products:

1. **`kaas`**: a fast, single-binary CLI and terminal UI for any Apache Kafka cluster. *Available now.*
2. **A management plane**: one place to run many clusters, with GitOps plans, schemas, ACLs and quotas. *Phase 2.*
3. **Managed Kafka on Kubernetes**: create, roll and autoscale clusters as a service. *Phase 3.*

All three sit on the same cluster-state engine. That means the screens you learn today become the ones you use to operate a fleet later.

> Every image on this page is a real screen. `task screenshots` drives the actual commands against an in-memory Kafka cluster and saves what they draw. Nothing here is a mock-up.

---

## Why KaaS

| What Kafka teams deal with today | What `kaas` does instead |
|---|---|
| JVM shell scripts that take seconds to start and print walls of text | One static Go binary that starts instantly, with a screen built for each job |
| "Connection to node -1 could not be established" | A layer-by-layer probe that says *which* layer broke (DNS, TCP, protocol or metadata) and what to try |
| Polling `kafka-topics --describe` in a loop to catch a leader move | A live event stream: leader moves, ISR shrinks, topics created and deleted, as typed events |
| `kafka-producer-perf-test`, one setting at a time | A benchmark matrix (sizes × acks × compression) with live latency histograms |
| Tools that hammer the controller when 20 people run them | A per-cluster request budget: refreshes are skipped, never queued, so `kaas` can't become the incident |
| Pretty output that breaks your scripts | Screens on a terminal; stable text on pipes; JSON with `-o json`; fixed exit codes |

### Built to production standards

- **Bounded load.** One loop owns every request to a cluster, behind a token bucket. The engine diffs a 100,000-partition snapshot in about 1 ms ([design 0001](design/0001-cluster-state-engine.md)).
- **Tested against real Kafka.** Unit and property tests run on an in-memory cluster. Integration tests run against Apache Kafka 3.9, 4.2 and 4.4 in containers. Every screen has golden tests and is fit-tested from 40 to 200 columns. Live screens fail CI if they truncate anything.
- **Supply chain.** Releases are signed with cosign and ship an SBOM and SLSA provenance.
- **Accessible.** `NO_COLOR` keeps the layout and drops the colour, and status shapes carry the meaning. `KAAS_REDUCED_MOTION=1` stops animation.

---

## Install

```bash
go install github.com/IshaanNene/Kafka-as-a-Service/cmd/kaas@latest
```

Point it at any cluster with `-b`, or set it once:

```bash
export KAAS_BROKERS=kafka-0.prod:9092,kafka-1.prod:9092
```

No cluster handy? Bring up a local three-broker KRaft cluster on kind (needs Docker):

```bash
task dev && task dev:forward
```

---

## User guide

### Every command explains itself

`kaas --help` and `kaas help <command>` open a command palette. It lists the command's subcommands and flags with their defaults, and below it, worked examples.

![kaas help: the command palette with flags and an examples session](img/help-kaas-bench-produce.svg)

On a pipe, you get cobra's standard plain help instead, so `kaas --help | grep` works as usual.

### Connecting

Before a session opens, `kaas` probes the cluster one layer at a time and shows the progress as a boot log. When the cluster answers, the log is erased. When it doesn't, the log stays, so you can see where it stopped.

![The connect probe: DNS, TCP, Kafka protocol and metadata, each with its timing](img/connect.svg)

### `kaas`: the terminal UI

Run `kaas` with no command. The first screen lists every topic with its partitions, under-replicated count, records retained and how fresh its offsets are. The cluster's event feed runs underneath.

| Key | Does |
|---|---|
| `↑` `↓` / `j` `k` | Move |
| `enter` | Open the topic: partitions, leaders, replicas, ISR, offsets |
| `esc` | Back |
| `/` | Filter topics (internal topics only when the filter starts with `_`) |
| `:` | Command palette: `:topics`, `:brokers`, `:topic <name>`, or just `:<name>` |
| `?` | Help |
| `ctrl+d` | Request-budget overlay |
| `q` | Quit |

![A topic's partitions: leader, replicas, in-sync replicas and offsets per partition](img/tui-topic.svg)

The partition you're looking at is watched closely, with its offsets refreshed every second. Everything else refreshes on a slower interval. `:brokers` shows each broker's leadership load and marks the controller:

![The brokers screen](img/tui-brokers.svg)

A typo doesn't dead-end. It shows the nearest real names:

![The TUI 404 with did-you-mean suggestions](img/tui-404.svg)

### `kaas watch`: what changed, as it happens

```bash
kaas watch -t orders -t payments
```

Each `-t` topic gets a records-per-second sparkline and its retained count. Cluster events arrive as cards, and each card's bottom edge burns down as it ages: leader moves, ISR shrink and expand, topics and partitions added or deleted, brokers joining or leaving, controller elections. The footer shows the request budget, so you can see what watching costs the cluster.

![kaas watch: sparklines per topic and a stack of event cards](img/watch.svg)

For scripts, use `kaas watch -o json` (one object per line) or pipe the output for plain timestamped lines.

### `kaas snapshot`: one look, for scripts too

```bash
kaas snapshot                  # every topic, problems first
kaas snapshot --sort records   # biggest first (also: name, partitions)
kaas snapshot -o json | jq .   # everything: brokers, topics, partitions, offsets
```

One row per topic: partitions, replication factor, under-replicated and offline partitions, records retained, and a status dot. Offline topics sort first, then under-replicated ones. The strip underneath totals the cluster and draws leaders per broker, so an unbalanced cluster shows at a glance. On narrow terminals the least important columns drop out before anything is cut.

![kaas snapshot: topics with status dots, and cluster totals underneath](img/snapshot.svg)

### `kaas bench produce`: measure, don't guess

The kaas version of `kafka-producer-perf-test`, as a matrix:

```bash
kaas bench produce -t orders --size 100,1024 --acks 1,all --compression none,lz4
```

Every combination runs with its own producer. The table fills in case by case, with a live counter on the running case, a latency histogram and a throughput chart.

![kaas bench produce: results table, latency histogram and throughput chart](img/bench.svg)

- **Accurate first records.** Each case sends one untimed warm-up record, so connection and producer-ID setup don't inflate the first latencies.
- **Rate cap.** `--throughput` caps records per second, like the perf tool.
- **On a pipe:** one summary line per case, in the perf tool's terms.
- **With `-o json`:** every percentile and the full histogram.

### When something is wrong

Errors are screens too, and each one says what to do next.

**Cluster unreachable.** One row per seed broker, with a dot for each probe layer. The headline is the layer that broke:

![The 503 page: a rack of seeds with probe dots, the error, and next steps](img/unreachable.svg)

**A name that doesn't exist.** The closest real names come from the live cluster:

![The 404 page for an unknown topic, suggesting the real one](img/not-found-topic.svg)

**A bad flag or value.** The flags you set, the bad one highlighted, and why:

![The validation screen for an invalid --acks value](img/invalid-flag.svg)

### Output and exit codes

| Where output goes | What you get |
|---|---|
| A terminal | The screens above |
| A terminal with `NO_COLOR` | The same layout, monochrome |
| A pipe, a file, `TERM=dumb` | Plain text: results, or `kaas: <message>` plus `hint:` lines |
| `-o json` | JSON results; errors as `{"error":{"code","exit","message","hints","detail"}}` |

| Exit code | Meaning |
|---|---|
| `0` | Success |
| `1` | Unclassified failure |
| `2` | Usage: a bad flag or argument |
| `3` | Not found: a command, topic or group that doesn't exist |
| `4` | Unavailable: the cluster could not be reached |
| `130` | Interrupted |

### Configuration

| Setting | Flag | Environment | Default |
|---|---|---|---|
| Seed brokers | `-b`, `--brokers` | `KAAS_BROKERS` | `localhost:9092` |
| Theme | `--theme magic\|amber\|ice` | `KAAS_THEME` | `magic`: the colours of the original ascii-magic pieces |
| No colour | | `NO_COLOR=1` | |
| No animation | | `KAAS_REDUCED_MOTION=1` | |

### Troubleshooting

| The probe stops at | It means | Try |
|---|---|---|
| **dns** | The host name doesn't resolve | Check `--brokers`, VPN, `/etc/hosts` |
| **tcp** (refused) | Nothing is listening there | Is Kafka up? Is it the right port? For kind: `task dev:forward` |
| **api** (no answer in time) | Connected, but the broker didn't respond | The broker may be starting, overloaded or cut off from its controller |
| **api** (protocol error) | Something answered, but not as Kafka | Wrong port, or the listener needs TLS/SASL |
| **metadata** | Brokers answered but metadata didn't | Controller trouble; check broker logs |

---

## What's coming

Each upcoming command already has its screen designed: 75 of ascii-magic's 79 UI layouts are assigned to commands. See them all, side by side with the screens already built, in the [screen gallery](SCREENS.md). Highlights:

| Command | Screen |
|---|---|
| `kaas lag` | A live leaderboard of consumer groups by lag, with an "all caught up" card at zero |
| `kaas consume` | Records streaming in like a chat channel: time, partition, key, value |
| `kaas offsets reset` | A step-by-step wizard with a preview of every offset change before anything moves |
| `kaas offsets find --time` | A bisect hunt for the offset at a timestamp, or the first bad record |
| `kaas sniff` | A packet capture of Kafka protocol requests, colour-coded by API |
| `kaas doctor` | A dot per health check, then per-topic health bars |
| `kaas plan` / `apply` | A plan diff of topics, ACLs and quotas, then a rolling apply with an abort countdown |
| `kaas reassign` | A download-queue view of every partition move, with speed and ETA |
| `kaas cluster roll` | A broker-by-broker restart gated on ISR, with a maintenance page for clients |
| `kaas fleet` | Every cluster's status at a glance |

---

## For contributors

```bash
task test           # unit, property and kfake tests (race detector on)
task lint           # golangci-lint, including the core/ import boundary
task screenshots    # regenerate every image on this page from real runs
```

Architecture decisions live in [docs/adr](adr/), and the screen design system in [ADR-0007](adr/0007-command-screens.md). Screen layouts are Go ports of [ascii-magic](https://www.ascii-magic.com/ascii) UI pieces (MIT).

*Apache Kafka is a trademark of the Apache Software Foundation. KaaS is not affiliated with or endorsed by the ASF.*
