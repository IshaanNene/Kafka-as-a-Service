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

### `kaas topics`: what each topic keeps, and what it costs

```bash
kaas topics                         # every topic, problems first
kaas topics 'payments.*' --sort disk
kaas topics --tree                  # names grouped by prefix
kaas topics --internal              # include __consumer_offsets and friends
```

Each topic shows its partitions and replication factor. It also shows records retained, disk used across every replica, retention and cleanup policy. Retention or cleanup that a topic sets for itself is purple. Grey values are inherited from the broker, so you can spot a topic that quietly keeps data forever.

![kaas topics: retention and cleanup per topic, topic-level settings in purple](img/topics.svg)

With `--tree`, names split on `.` (or `--sep`) into folders, and single-child chains merge. Each topic is a leaf with its status, size, and a flag: `!` offline, `U` under-replicated, `C` compacted.

![kaas topics --tree: topic prefixes as folders](img/topics-tree.svg)

The pattern is a glob. A plain name that isn't a topic gets the 404 page with the closest real names. On a pipe the table prints raw numbers (records, `DISK_BYTES`), so `sort -n` and `awk` work. `-o json` gives `retentionMs`, `cleanupPolicy`, `diskBytes` and the rest per topic.

#### `kaas topics describe`: one topic, partition by partition

```bash
kaas topics describe orders
kaas topics describe orders -o json | jq '.partitions[] | select(.isr | length < 3) | .id'
```

Every partition's leader, replicas, in-sync replicas, offsets, records and size, with `>>` on the one that needs attention (offline, then under-replicated, else the largest). The layout shows one bar per broker: each replica is a segment in its partition's colour, sized by its bytes on that broker. `█` is a leader, `▒` a follower and `░` an out-of-sync replica, so skew and lagging replicas show at a glance. Below are the leader spread, how many partitions are in sync, and the configs the topic sets itself.

![kaas topics describe: partitions, replicas per broker sized by bytes, and the topic's own settings](img/topics-describe.svg)

On a pipe it prints the `kafka-topics --describe` layout, so existing `grep` habits still work.

#### `kaas topics alter`: add partitions

```bash
kaas topics alter orders --partitions 12            # preview, type yes, write
kaas topics alter orders --partitions 12 --dry-run  # the controller checks it; nothing is written
kaas topics alter orders --partitions 12 --yes -o json
```

Kafka can add partitions to a topic but never remove them, so `alter` shows the plan before it changes anything. The plan is the describe screen with the new partitions added in green. They have no leader yet because the controller places them when they're written.

Below the table, the plan spells out what changes:
- Keys hash over the new count from then on, so a key's new records can land on a different partition from its old ones.
- The groups reading the topic rebalance to pick up the new partitions.

You type `yes` to write it. The table is then redrawn in place with the new partitions' real leaders and replicas.

![kaas topics alter: two new partitions in green before writing, with what changes for keys and readers](img/topics-alter.svg)

Without a terminal to answer the prompt, `--yes` is required, so a piped `yes` can't confirm by accident. Asking for fewer partitions than the topic has is a usage error (exit 2). Asking for the count it already has does nothing and exits 0, so a script can rerun it safely. On a pipe, it prints one line, such as `orders: 6 → 12 partitions, added 6,7,8,9,10,11`.

### `kaas brokers`: who does the work

The cluster as a process tree: every broker, what it leads, and the topics it leads most.

```bash
kaas brokers            # brokers and their top 5 topics
kaas brokers --top 0    # brokers only
```

Each broker row shows the partitions it leads, the replicas it holds, and its disk use across all log directories. The controller is marked, and replicas out of sync show as `N behind` in orange. A broker that partitions still name but metadata no longer lists is shown in red as `offline`. Leader counts well above the even share turn orange. **Skew** in the footer is the busiest broker's leaders over the even share, where `1.00x` is balanced. Internal topics fold into one row unless you pass `--internal`.

![kaas brokers: the cluster, each broker's leaders, replicas and disk, and the topics it leads](img/brokers.svg)

On a pipe it prints one line per broker with a `STATUS` of `controller`, `online` or `offline`. With `-o json` you get every broker's topics with the partition IDs it leads.

### `kaas groups`: who is behind, and for how long

Every consumer group, worst lag first, refreshing in place like a task manager:

```bash
kaas groups                      # live, every 2s; Ctrl-C to stop
kaas groups 'billing-*' --sort rate
kaas groups --once               # one read and exit
```

Each row is a group's state, members, the topics it reads and how many records it is behind, with a bar for its share of the worst lag. The rate comes from committed offsets between refreshes, and **Catch up** is lag divided by rate: `stalled` when its members are not committing, `no members` when nobody is consuming, `caught up` at zero. Rebalancing groups show in purple. Below the rows are the totals and a chart of total lag over time, red wherever it grew.

![kaas groups: groups by lag with bars, consume rates, catch-up times and a chart of total lag](img/groups.svg)

On a pipe it prints one line per group with raw lag numbers. With `-o json` you get every group's lag, committed offsets and topics. A single read has no rate, since the rate needs two reads.

### `kaas lag`: the backlog, ranked

Where `kaas groups` shows each group, `kaas lag` ranks every group·topic pair by how far behind it is:

```bash
kaas lag                    # live board, every 2s
kaas lag 'billing*' --top 5
```

The top three pairs get medals. Each row has a bar for its share of the worst lag, and a trend against three refreshes ago: `▲` in red means the backlog grew, `▼` in green means it shrank. Below the rows, a live chart draws total lag over time with the minimum, average and maximum, and how fast the shown groups consume.

![kaas lag: group·topic pairs ranked by lag, with a live chart of total lag](img/lag.svg)

When every group is at the head of its topics, the board turns into this:

![kaas lag when nothing is behind: all caught up](img/lag-caught-up.svg)

With `-o json` you get `[{group, topic, lag}]`, worst first. That makes a deploy gate a one-liner:

```bash
test "$(kaas lag -o json | jq '[.[].lag] | add // 0')" -eq 0
```

### `kaas consume`: read a topic like a channel

```bash
kaas consume orders                       # new records as they arrive
kaas consume orders --from -20            # the last 20 of each partition, then follow
kaas consume orders --from 1h -p 2 --exit # the last hour of partition 2, then stop
```

Each record is a line, the way a chat channel shows messages: its time, its partition and offset, its key in its partition's colour, then its value, which wraps under itself. Lines that open with `-->` show where each partition starts and how far behind its end that is. A tombstone shows in red, and headers show in purple under their record. Binary values show as their size, so they never garble the terminal. The panel on the right counts records per partition.

![kaas consume: records as a chat channel, with a partition list and a status bar](img/consume.svg)

`kaas consume` never joins a consumer group and never commits, so reading never moves anyone's offsets. `--from` takes `end` (the default), `beginning`, `-N`, a duration like `15m`, or an RFC 3339 time. `-n` stops after a number of records, and `--exit` stops at the end each partition had when the read began.

On a pipe it prints one raw value per line, like `kafka-console-consumer`. With `-o json` you get one object per line: `partition`, `offset`, `timestamp`, `key`, `value` and `headers`, with binary keys and values base64-encoded and marked as such. `kaas tail` is the same command.

### `kaas clients`: who is reading and writing

```bash
kaas clients                 # live, busiest first
kaas clients 'billing*'      # by client.id, group, transactional.id or topic
kaas clients --once --all    # one read, idle producers included
```

This uses the same task-manager frame as `kaas groups`, with one row per client the cluster can see.

- **Consumers** are consumer group members. Each row shows its `client.id`, the host the broker saw, its group, how many partitions it's assigned, and how far behind it is.
- **Producers** are idempotent or transactional producers, from `DescribeProducers` (KIP-664). A transactional producer is shown by its `transactional.id`; any other producer by its producer ID. Its status is `writing`, `in txn`, or how long it has been idle.

![kaas clients: consumer members and producers by records per second](img/clients.svg)

Kafka has no API that lists connections or request rates; those live only in broker JMX metrics. So the rate here is records per second, measured between two refreshes: a member's offset commits, and a producer's sequence numbers (which count records). A member's rate comes in steps because most clients commit every 5s, so it reads `0/s` between commits and then catches up. Rates are only compared on partitions a client had in both refreshes, so a rebalance doesn't show up as a burst. A single read (`--once`, a pipe, or `-o json`) has no rates.

Some clients never show:

- **Producers with idempotence turned off.** It's on by default in current clients.
- **Readers without a group,** including `kaas consume`.

Kafka keeps a producer's state for `producer.id.expiration.ms` (a day by default), so producers that haven't written for 10 minutes are hidden unless you pass `--all`.

### `kaas top`: which partitions are working

```bash
kaas top                            # live, busiest writers first
kaas top 'payments.*' --sort bytes  # the payments topics, by disk growth
kaas top --once --top 10            # two readings, then print the top 10
```

This is htop for partitions. Each broker gets a meter showing its share of the records written. The `Read` gauge compares records read with records written, summed over groups: two groups keeping pace read `2.0×`. Below that, every partition has a row:

- `IN/s`: records written, from its log-end offset.
- `OUT/s`: records read, from consumer group commits.
- `BYTES/s`: how fast its log grows on the leader.
- `SIZE`: the log's size on the leader.

The sorted column is marked `▐ ▼`, and the busiest row is yellow. `--sort` takes `in` (the default), `out`, `bytes`, `size`, or `partition`.

![kaas top: a meter per broker, then partitions sorted by records in](img/top.svg)

Rates are the difference between two readings, so the first live frame shows `-` until the second reading. `--once`, a pipe, and `-o json` all wait one `--interval` (2s by default) for that second reading before printing. The JSON has one object per partition: `topic`, `partition`, `leader`, `in_per_sec`, `out_per_sec`, `bytes_per_sec`, `size_bytes`, and `end_offset`. A negative value means unknown, for example when the cluster refuses `DescribeLogDirs`. `OUT/s` counts only groups that commit; `kaas consume` never commits, so its reads don't show up here.

### `kaas produce`: write records

```bash
kaas produce orders                                   # compose one in the editor
kaas produce orders -k user-17 --value '{"order":1043}' -H source=cli
kaas produce orders --key-separator ':' < orders.txt  # one record per line
```

On a terminal, with nothing piped in, `kaas produce` opens an editor. The value, key and headers each get a tab, and `tab` moves between them. JSON is highlighted as you type: keys in purple, strings in light blue, numbers and literals in blue. `enter` keeps your indentation and indents after `{` or `[`, and a closing bracket steps back out.

The status bar checks the value as you type and points at the first problem by line and column. `ctrl-s` refuses to send until the problem is fixed. A pretty-printed JSON value is sent compacted, so the stored record has no editor newlines. After a send, the buffers stay as they are and the status bar shows where the record landed, so you can change one field and send again. When you quit, each send is printed as a line.

![kaas produce: composing a JSON value in the editor, with key and headers tabs](img/produce.svg)

On a pipe, each line is one record, the way `kafka-console-producer` reads them. With `--key-separator`, the text before the separator is the key; a line without the separator is an error that names the line, so a record never silently loses its key. `--value` sends one record without reading stdin. `-H key=value` adds a header to every record, and `-p` picks the partition. Pipe output is silent unless you pass `-o json`, which prints `{topic, partition, offset, timestamp}` for each record.

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

### `kaas bench consume`: the read side

The kaas version of `kafka-consumer-perf-test`. Fetch sizes and waits multiply into cases, the same way:

```bash
kaas bench consume -t orders --records 100000 --fetch-size 64KB,1MB,8MB --max-wait 100ms,500ms
```

Each case reads from the beginning of the topic with its own client, so no case is served from another case's buffer. It never joins a group and never commits. A case reads `--records`, or the whole topic if it holds fewer. If the topic is empty, `kaas bench produce` fills it.

![kaas bench consume: fetch sizes compared, with poll waits and throughput](img/bench-consume.svg)

The latencies here are poll waits: how long the reader waited for each batch. A poll served from data already fetched takes microseconds, and one that waits on the broker takes a round trip. Small fetches show more of the second kind. `--fetch-size` takes sizes like `64KB` or `1MB` per partition. On a pipe you get one line per case, and with `-o json` you get the full results, including `polls`.

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
