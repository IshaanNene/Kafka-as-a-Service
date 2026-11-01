# Usage guide

Every `kaas` command: what it shows, its flags, and what it prints for scripts. Every image here is a real screen, drawn by `task screenshots` driving the actual commands against an in-memory Kafka cluster.

- **Start:** [Install](#install) · [Help](#every-command-explains-itself) · [Connecting](#connecting) · [The terminal UI](#kaas-the-terminal-ui)
- **Observe:** [watch](#kaas-watch-what-changed-as-it-happens) · [snapshot](#kaas-snapshot-one-look-for-scripts-too) · [topics](#kaas-topics-what-each-topic-keeps-and-what-it-costs) · [brokers](#kaas-brokers-who-does-the-work) · [groups](#kaas-groups-who-is-behind-and-for-how-long) · [lag](#kaas-lag-the-backlog-ranked) · [clients](#kaas-clients-who-is-reading-and-writing) · [top](#kaas-top-which-partitions-are-working) · [segments](#kaas-segments-what-a-partitions-log-holds) · [config](#kaas-config-what-a-topic-or-broker-is-set-to)
- **Operate:** [topics create, alter, delete](#kaas-topics-create-make-a-topic) · [offsets reset](#kaas-offsets-reset-read-again-or-skip-ahead) · [offsets find](#kaas-offsets-find-where-a-moment-or-a-problem-starts) · [config edit](#changing-configs-kaas-config-edit)
- **Data:** [consume](#kaas-consume-read-a-topic-like-a-channel) · [produce](#kaas-produce-write-records) · [pipe](#kaas-pipe-move-records-between-kafka-files-http-and-plugins) · [plugins](#plugins)
- **Measure and debug:** [bench](#kaas-bench-produce-measure-dont-guess) · [sniff](#kaas-sniff-watch-the-requests-clients-send)
- **Many clusters (Phase 2):** [console](#kaas-console-the-fleet-in-a-browser) · [fleet file](#the-fleet-file) · [plan and apply](#gitops-kaas-plan-and-kaas-apply) · [desired-state file](#the-desired-state-file)
- **Reference:** [Output and exit codes](#output-and-exit-codes) · [Configuration](#configuration) · [Troubleshooting](#troubleshooting)

## Install

```bash
go install github.com/IshaanNene/Kafka-as-a-Service/cmd/kaas@latest
export KAAS_BROKERS=kafka-0.prod:9092,kafka-1.prod:9092   # or -b on each command
```

No cluster handy? `task dev && task dev:forward` brings up a three-broker KRaft cluster on kind (needs Docker).


## Every command explains itself

`kaas --help` and `kaas help <command>` open a command palette. It lists the command's subcommands and flags with their defaults, and below it, worked examples.

![kaas help: the command palette with flags and an examples session](img/help-kaas-bench-produce.svg)

On a pipe, you get cobra's standard plain help instead, so `kaas --help | grep` works as usual.

## Connecting

Before a session opens, `kaas` probes the cluster one layer at a time and shows the progress as a boot log. When the cluster answers, the log is erased. When it doesn't, the log stays, so you can see where it stopped.

![The connect probe: DNS, TCP, Kafka protocol and metadata, each with its timing](img/connect.svg)

## `kaas`: the terminal UI

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

## `kaas watch`: what changed, as it happens

```bash
kaas watch -t orders -t payments
```

Each `-t` topic gets a records-per-second sparkline and its retained count. Cluster events arrive as cards, and each card's bottom edge burns down as it ages: leader moves, ISR shrink and expand, topics and partitions added or deleted, brokers joining or leaving, controller elections. The footer shows the request budget, so you can see what watching costs the cluster.

![kaas watch: sparklines per topic and a stack of event cards](img/watch.svg)

For scripts, use `kaas watch -o json` (one object per line) or pipe the output for plain timestamped lines.

## `kaas snapshot`: one look, for scripts too

```bash
kaas snapshot                  # every topic, problems first
kaas snapshot --sort records   # biggest first (also: name, partitions)
kaas snapshot -o json | jq .   # everything: brokers, topics, partitions, offsets
```

One row per topic: partitions, replication factor, under-replicated and offline partitions, records retained, and a status dot. Offline topics sort first, then under-replicated ones. The strip underneath totals the cluster and draws leaders per broker, so an unbalanced cluster shows at a glance. On narrow terminals the least important columns drop out before anything is cut.

![kaas snapshot: topics with status dots, and cluster totals underneath](img/snapshot.svg)

## `kaas topics`: what each topic keeps, and what it costs

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

### `kaas topics describe`: one topic, partition by partition

```bash
kaas topics describe orders
kaas topics describe orders -o json | jq '.partitions[] | select(.isr | length < 3) | .id'
```

Every partition's leader, replicas, in-sync replicas, offsets, records and size, with `>>` on the one that needs attention (offline, then under-replicated, else the largest). The layout shows one bar per broker: each replica is a segment in its partition's colour, sized by its bytes on that broker. `█` is a leader, `▒` a follower and `░` an out-of-sync replica, so skew and lagging replicas show at a glance. Below are the leader spread, how many partitions are in sync, and the configs the topic sets itself.

![kaas topics describe: partitions, replicas per broker sized by bytes, and the topic's own settings](img/topics-describe.svg)

On a pipe it prints the `kafka-topics --describe` layout, so existing `grep` habits still work.

### `kaas topics create`: make a topic

```bash
kaas topics create                                  # step by step
kaas topics create payments -p 12 -r 3 -c cleanup.policy=compact
kaas topics create payments -p 12 --if-not-exists -o json   # idempotent, for scripts
```

On a terminal without `--partitions`, `create` walks through four steps, each checked before you move on:
1. **Name.** The name is checked against Kafka's rules and the topics you already have, including names that would collide in Kafka's metrics, where `.` and `_` become the same. You also see where the name sits in its family, for example `payments › eu › refunds` and how many topics share `payments.eu`.
2. **Layout.** The partition count and replication factor start from the cluster's own `num.partitions` and `default.replication.factor`, which kaas gets by asking the controller. You see how many replicas that makes per broker, and RF 1 comes with a warning.
3. **Configs.** Retention, cleanup, min in-sync replicas and compression are cards you change with `←` `→`. A card turns green when it differs from the broker.
4. **Create.** The controller checks the whole topic before anything is created. `enter` then creates it, and a bar fills as each partition gets a leader.

![kaas topics create: the configs step, with retention and cleanup set](img/topics-create.svg)

With `--partitions`, or without a terminal, the flags say it all: `-p` and `-r` (0 takes the broker's default) and repeatable `-c key=value`. `--dry-run` has the controller check the topic without creating it. A topic that already exists exits 1, unless you pass `--if-not-exists`. `-o json` gives `{topic, id, partitions, replicationFactor, configs, created, dryRun}`.

### `kaas topics delete`: remove topics, with a moment to undo

```bash
kaas topics delete orders.v1                 # type the name, then 5s to undo
kaas topics delete orders.v1 orders.v2       # type "delete 2 topics"
kaas topics delete orders.v1 --yes -o json   # scripts: no dialog, no wait
```

Kafka can't bring a deleted topic back, so on a terminal `delete` makes you look before anything happens:
- **The dialog** opens over the topic's own describe page, dimmed, and says what goes with it: the records, the size on disk, the partitions, and the consumer groups still reading it.
- **You type the topic's name** to arm the red button. For several topics you type `delete N topics`.
- **For 5 seconds** the bin stays open with an Undo timer draining. `u` or `esc` cancels, and nothing has been sent yet; `enter` deletes now.
- **Once the window ends**, kaas sends the delete. It waits until the cluster no longer lists the topic before the lid shuts.

<table><tr><td width="62%"><img src="img/topics-delete.svg" alt="kaas topics delete: the dialog over the dimmed topic, the name half typed"></td><td><img src="img/topics-delete-done.svg" alt="kaas topics delete: the bin closed, topic deleted"></td></tr></table>

Without a terminal to type into, `--yes` is required, and the delete is sent at once. With a terminal but stdout piped, kaas asks for the name on stderr instead of opening the dialog. An unknown topic exits 3. Internal topics such as `__consumer_offsets` are refused (exit 2). If a topic couldn't be deleted, for example because the brokers set `delete.topic.enable=false`, kaas exits 1 with the controller's reason. `-o json` prints `[{topic, deleted, error}]`.

### `kaas topics alter`: add partitions

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

## `kaas brokers`: who does the work

The cluster as a process tree: every broker, what it leads, and the topics it leads most.

```bash
kaas brokers            # brokers and their top 5 topics
kaas brokers --top 0    # brokers only
```

Each broker row shows the partitions it leads, the replicas it holds, and its disk use across all log directories. The controller is marked, and replicas out of sync show as `N behind` in orange. A broker that partitions still name but metadata no longer lists is shown in red as `offline`. Leader counts well above the even share turn orange. **Skew** in the footer is the busiest broker's leaders over the even share, where `1.00x` is balanced. Internal topics fold into one row unless you pass `--internal`.

![kaas brokers: the cluster, each broker's leaders, replicas and disk, and the topics it leads](img/brokers.svg)

On a pipe it prints one line per broker with a `STATUS` of `controller`, `online` or `offline`. With `-o json` you get every broker's topics with the partition IDs it leads.

## `kaas groups`: who is behind, and for how long

Every consumer group, worst lag first, refreshing in place like a task manager:

```bash
kaas groups                      # live, every 2s; Ctrl-C to stop
kaas groups 'billing-*' --sort rate
kaas groups --once               # one read and exit
```

Each row is a group's state, members, the topics it reads and how many records it is behind, with a bar for its share of the worst lag. The rate comes from committed offsets between refreshes, and **Catch up** is lag divided by rate: `stalled` when its members are not committing, `no members` when nobody is consuming, `caught up` at zero. Rebalancing groups show in purple. Below the rows are the totals and a chart of total lag over time, red wherever it grew.

![kaas groups: groups by lag with bars, consume rates, catch-up times and a chart of total lag](img/groups.svg)

On a pipe it prints one line per group with raw lag numbers. With `-o json` you get every group's lag, committed offsets and topics. A single read has no rate, since the rate needs two reads.

## `kaas lag`: the backlog, ranked

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

## `kaas offsets reset`: read again, or skip ahead

```bash
kaas offsets reset                                    # ask for each piece
kaas offsets reset billing -t orders --to 2h          # read the last two hours again
kaas offsets reset billing --all-topics --to latest   # skip the backlog
kaas offsets reset billing -t orders --to -500 --dry-run
```

`--to` takes `earliest` or `latest`, an offset (`1043`), a shift from the committed offset (`+100`, `-500`), a time (`2026-10-31T09:00`, local unless it names a zone), or how long ago (`90m`, `2h`, `7d`). Time targets ask the cluster for each partition's first record at or after that moment, the way a consumer would. A target past either end of a partition's log is moved to that end, as the broker requires, and the plan marks it `clamped`.

On a terminal, anything the flags leave out is asked for in a stepper. First you pick the group from a list showing state, topics and lag. A group with members can't be picked. Then you tick its topics and choose where to. The last step asks the cluster for the plan and sums it up before showing it in full.

![kaas offsets reset: the stepper at where to, with time and 2h](img/offsets-reset-wizard.svg)

The plan follows the infra-plan-diff layout. `~` is a committed offset that moves, old in red and new in green, with how many records the group reads again or skips. `+` is a partition the group has never committed, which gets its first offset. A partition that doesn't move is counted, not listed. Nothing is committed until you type `yes`. The commit lines that follow are the coordinator's actual answers, and a partition it refuses is named.

![kaas offsets reset: the plan for billing two hours back, confirmed and committed](img/offsets-reset.svg)

Kafka only takes commits from a group with no members, so kaas checks first. A group that is in use stops with who its members are, and nothing is sent. A group name kaas doesn't know is planned as new, all `+`, so a typo shows up in the plan before you confirm. Without a terminal, `--yes` is required. `-o json` prints `{group, state, to, dryRun, applied, moves}`, where each move has `from` (null before a first commit), `to`, and `change` (`add`, `change`, `same` or `skip`).

## `kaas offsets find`: where a moment, or a problem, starts

```bash
kaas offsets find orders --time 2026-10-31T09:00     # every partition
kaas offsets find orders --time 2h -p 0
kaas offsets find orders --match '"version":2'       # the first record with the new schema
```

`kaas offsets find` bisects each partition. It reads the record in the middle of what's left, keeps the half the answer is in, and repeats, so 12,480 records take about 14 reads. Each read is one Fetch from the partition's leader. Nothing is consumed or committed. The rail shows each read as it lands, green for before and red for at or after, with a bracket under what's left. The log under the rail shows every read: its offset, time, how long it took, and the record.

![kaas offsets find: a bisect over orders p0 for the first record at or after a time](img/offsets-find.svg)

With `--time`, kaas also asks the broker's time index the same question. They agree unless producers stamped records out of order, and then kaas tells you so. The last line is the `kaas offsets reset` command that starts a group reading from that moment.

`--match` finds the first record whose value matches a regular expression, for questions like "where did the bad records start". A bisect assumes that once records match, they keep matching. So kaas reads the next three records, and if one of them doesn't match, it warns that there may be earlier matches.

A pipe prints `topic partition offset timestamp` per partition, or `-` when nothing qualifies. `-o json` adds `reads` and, for `--time`, the broker's `indexOffset`.

## `kaas consume`: read a topic like a channel

```bash
kaas consume orders                       # new records as they arrive
kaas consume orders --from -20            # the last 20 of each partition, then follow
kaas consume orders --from 1h -p 2 --exit # the last hour of partition 2, then stop
```

Each record is a line, the way a chat channel shows messages: its time, its partition and offset, its key in its partition's colour, then its value, which wraps under itself. Lines that open with `-->` show where each partition starts and how far behind its end that is. A tombstone shows in red, and headers show in purple under their record. Binary values show as their size, so they never garble the terminal. The panel on the right counts records per partition.

![kaas consume: records as a chat channel, with a partition list and a status bar](img/consume.svg)

`kaas consume` never joins a consumer group and never commits, so reading never moves anyone's offsets. `--from` takes `end` (the default), `beginning`, `-N`, a duration like `15m`, or an RFC 3339 time. `-n` stops after a number of records, and `--exit` stops at the end each partition had when the read began.

On a pipe it prints one raw value per line, like `kafka-console-consumer`. With `-o json` you get one object per line: `partition`, `offset`, `timestamp`, `key`, `value` and `headers`, with binary keys and values base64-encoded and marked as such. `kaas tail` is the same command.

### A time window: `--to` and `--pick`

```bash
kaas consume orders --from 2026-10-26T00:00:00Z --to 2026-10-31T00:00:00Z
kaas consume orders --from 3h --to 1h -o json > window.jsonl
kaas consume orders --pick
```

`--to` stops each partition before its first record at or after a time, so a window reads exactly what it holds and then exits. It needs a `--from` time (a duration or an RFC 3339 time) that comes before it, and it can't be in the future.

`--pick` opens a calendar of two months to choose the days instead. Days before the oldest record the topic keeps, and days after today, are faint and can't be picked; days that hold nothing are dimmed. Move with the arrows, press enter on the first day and on the last, and the footer shows how many records the window holds, counted from the broker's time index rather than guessed. Enter again reads it in the channel view. A `--from` and `--to` given with `--pick` start the calendar on that window.

![kaas consume --pick: two months with a picked range and its record count](img/consume-pick.svg)

## `kaas clients`: who is reading and writing

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

## `kaas top`: which partitions are working

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

## `kaas sniff`: watch the requests clients send

```bash
kaas sniff                                   # a proxy on localhost:19092
kaas consume orders --brokers localhost:19092  # in another terminal: read through it
kaas sniff --api produce --api fetch         # only some requests
kaas sniff -n 100 -o json | jq -r .api       # the first 100, for scripts
```

`kaas sniff` runs a proxy in front of the cluster. Point a client at its `--listen` address instead of a broker, and every request it sends shows up as a line: when it came, the client's address, the broker it went to, its API in that API's colour, its size, and what it asks for, decoded (`orders p0,p2 · 24 records · acks all` for a produce, `orders p0@1200` for a fetch, `billing · generation 3` for a heartbeat). Produce is orange, fetch blue, metadata yellow, offsets cyan, groups purple, transactions green, and requests that change the cluster red.

![kaas sniff: requests through the proxy, coloured by API, with the selected one's bytes](img/sniff.svg)

↑ and ↓ select a request: the panel below shows its version, its size and its answer's, how long the broker took, which client sent it, and its first 32 bytes, with the API key highlighted. Enter follows the newest again. The bars count the last 200 requests by API.

Each broker gets a listener of its own, on the ports after `--listen`'s. The proxy rewrites the broker addresses in the answers that carry them (Metadata, FindCoordinator, DescribeCluster, and the leader hints in newer Produce and Fetch answers) so a client's later connections come through it too. Everything else is forwarded as it came. Clients that don't use the proxy aren't touched. When the screen falls behind a busy client it counts requests as dropped from the screen; it never slows the traffic.

On a pipe each request is a line, and `-o json` gives `n`, `time`, `client`, `client_id`, `broker`, `api`, `key`, `version`, `bytes`, `response_bytes`, `took_ms`, `answered` and `info`.

`--client-id` keeps only the requests from clients with that client ID.

### Where a client's time goes: `--trace`

```bash
kaas sniff --trace --client-id billing-svc       # its first 10 seconds
kaas sniff --trace --for 30s -o json > trace.jsonl
```

`--trace` starts with the first request through the proxy and records `--for` (10 seconds by default), then stops and keeps the waterfall on screen. Each request is a bar on one time axis, split into the stages the proxy sees:

- **connect**: dialing the broker, on a connection's first request
- **send**: the request's bytes, in from the client and on to the broker
- **queue**: waiting behind earlier requests on the same connection, which a broker answers in order
- **wait**: the broker working on it, a fetch's long poll included
- **ack**: the answer's bytes, back to the client

![kaas sniff --trace: a service starting up, stage by stage, with its group joined and its first produce acked](img/sniff-trace.svg)

A request whose answer carries an error, such as the `MEMBER_ID_REQUIRED` a first JoinGroup gets, shows as `error` in red with the error's name. Lines mark when the client's group was joined and when its first produce was acknowledged. ↑↓ select a request to read its stages in the footer, ←→ switch between kinds (produce, fetch, metadata, groups, offsets, transactions, admin), and enter follows the newest. On a pipe each answered request is a line, and `-o json` gives each stage in milliseconds since the trace's first request.

### Where the time goes: `--profile`

```bash
kaas sniff --profile                              # until q
kaas sniff --profile --api produce --api fetch    # only those
kaas sniff --profile --for 1m > kafka.folded      # folded stacks, for speedscope or flamegraph.pl
```

`--profile` adds every request's stages up into a flame graph: all at the bottom, then each API, the broker it went to, its topic, and its stages on top, each frame as wide as its share of the time. ←→ move between frames side by side, ↑ goes up the stack and ↓ back down, and the footer shows the selected frame's share of the time, how many requests went through it, and their average and p99. Enter zooms into a frame, esc zooms back out, and r starts over.

![kaas sniff --profile: produce and fetch time by broker, topic and stage, as a flame graph](img/sniff-profile.svg)

A fetch can spend its time polling rather than serving: a consumer that has caught up asks for records that don't exist yet, and the broker holds the fetch until some are written or its wait runs out. The profile tells the two apart. A fetch answered with nothing spent all its wait as an `idle poll`. A fetch whose first record was written after the fetch reached the broker spent the time until then as a `long poll`, going by the records' timestamps. Both are hidden at first, so the graph shows the time spent serving. Press i to show them.

`--profile` records until q, or for `--for` when you give it. On a pipe it prints the profile when it ends, as folded stacks (one line per stage with its time in microseconds), or as a tree with `-o json`.

## `kaas segments`: what a partition's log holds

```bash
kaas segments orders                  # the partition with the most bytes on disk
kaas segments users -p 3              # a compacted topic's partition 3
kaas segments orders --budget 1GB     # read up to 1GB whole rather than sampling
kaas segments orders -o json | jq '{density, holes}'
```

`kaas segments` maps one partition's log as a grid of cells, each a range of offsets from the log start to the end, read from the record batches the leader serves. A cell is solid where every offset still holds a record and thins out (▓▒░) where compaction removed some. A dot is a hole, offsets no batch covers. Colours say what the batches hold: plain records, transactions that committed or aborted, ones still open, and the markers that end them.

![kaas segments: a payments log of plain batches and transactions, one in four aborted, after its oldest offsets were deleted](img/segments.svg)

The panel adds the records, the bytes on disk (from the broker's log dirs, or estimated from the first batches when it won't say), the batches, the holes and the largest, the transactions, the codecs, and roughly how many segment files that makes at the topic's `segment.bytes`. Kafka doesn't show segment files over its protocol, so the map is by offset, not by file.

A log that fits in `--budget` (64MB by default) is read whole. A bigger one is sampled, one small fetch a cell: then the records are an estimate and holes can't be counted. Nothing is consumed or committed. On a pipe it prints a line per stat, and `-o json` adds every cell.

## `kaas config`: what a topic or broker is set to

```bash
kaas config orders                 # every config, in sections
kaas config orders --set           # only what isn't Kafka's default
kaas config --broker 1             # a broker's
kaas config orders -o json | jq '.[] | select(.source == "topic")'
```

`kaas config` lists a topic's configs under retention, cleanup, segments, messages and replication, or a broker's with `--broker`. A value that isn't Kafka's default is drawn bright and tagged with where it is set: `topic`, a `broker` override the topic inherits, a `cluster`-wide default, or the broker's server.properties (`file`). Switches show true and false. Durations sit on a slider from a second to a year, sizes from a kilobyte to a terabyte, and `min.insync.replicas` against the replication factor, so an outlier stands out.

![kaas config: a payments topic with its retention, cleanup and durability set, and a broker's compression it inherits](img/config.svg)

↑↓ move, and the foot shows the selected config's value, where it comes from, what it would be without it, and the broker's documentation for it. `s` shows only what is set, and q quits. On a pipe it prints a line per config with its source; `-o json` adds the default and section. Sensitive values, such as passwords, are never shown.

### Changing configs: `kaas config edit`

```bash
kaas config edit orders                     # in the editor
kaas config edit --broker 1                 # a broker's dynamic configs
kaas config edit orders --set retention.ms=3d --unset cleanup.policy --dry-run
kaas config edit orders --set retention.ms=3d -o json
```

`kaas config edit` opens the configs as a file, one `key = value` a line under the same sections. A line without `#` is set on the topic (or dynamically on the broker). Comment it out or delete it to go back to the default. Durations take `3d`, `12h` or `500ms`, and sizes `1GiB`, as well as Kafka's own numbers.

![kaas config edit: retention cut to a day, larger records allowed and min.insync.replicas unset, checked by the cluster and waiting for y](img/config-edit.svg)

The editor is modal, as vi is:
- **Insert:** `i`, `a`, `o` and `O`; Esc goes back to normal mode.
- **Move:** `h j k l`, `w b`, `0 $` and `gg G`.
- **Delete and change:** `x`, `dd` and `D` delete; `cw` changes a word; `u` undoes.

`:w` checks every change with the cluster without applying it, then shows them: `~` changed, `+` set, `-` unset and what it falls back to. `y` applies them and `n` keeps editing. A key the cluster doesn't list is refused with the nearest real one, and so is a read-only key. Other commands:
- `:wq` applies and quits.
- `:q!` leaves without applying.
- `:e!` starts over from what the cluster has.
- `:%s/old/new/g` replaces text.

For a script, `--set key=value` and `--unset key` make the change without the editor, `--dry-run` only checks it, and `-o json` lists the changes.

## `kaas produce`: write records

```bash
kaas produce orders                                   # compose one in the editor
kaas produce orders -k user-17 --value '{"order":1043}' -H source=cli
kaas produce orders --key-separator ':' < orders.txt  # one record per line
```

On a terminal, with nothing piped in, `kaas produce` opens an editor. The value, key and headers each get a tab, and `tab` moves between them. JSON is highlighted as you type: keys in purple, strings in light blue, numbers and literals in blue. `enter` keeps your indentation and indents after `{` or `[`, and a closing bracket steps back out.

The status bar checks the value as you type and points at the first problem by line and column. `ctrl-s` refuses to send until the problem is fixed. A pretty-printed JSON value is sent compacted, so the stored record has no editor newlines. After a send, the buffers stay as they are and the status bar shows where the record landed, so you can change one field and send again. When you quit, each send is printed as a line.

![kaas produce: composing a JSON value in the editor, with key and headers tabs](img/produce.svg)

On a pipe, each line is one record, the way `kafka-console-producer` reads them. With `--key-separator`, the text before the separator is the key; a line without the separator is an error that names the line, so a record never silently loses its key. `--value` sends one record without reading stdin. `-H key=value` adds a header to every record, and `-p` picks the partition. Pipe output is silent unless you pass `-o json`, which prints `{topic, partition, offset, timestamp}` for each record.

## `kaas produce --file`: upload files

```bash
kaas produce orders -f orders-2026-10.jsonl -f orders-2026-11.jsonl
kaas produce orders --key-separator ':' -f refunds.txt -o json > receipts.jsonl
```

`-f` sends each line of a file as a record, with the same rules as a pipe: `--key-separator`, `-H` and `-p` apply to every line, a trailing `\r` is dropped, and a line can be up to 16 MiB. Repeat `-f` for more files. Four files send at a time, the rest wait in the queue.

On a terminal, each file gets a row: how much has been read and sent, its records per second, and a bar. The bar measures bytes actually read, not a guess from line counts. When every file is in, the list turns into the tray, which shows the file count, total size and records, and the `kaas consume` to read them back. `ctrl-c` stops and leaves the list where it was. Records already sent stay sent.

![kaas produce --file: five files uploading into orders, one done and four sending](img/produce-file.svg)

![kaas produce --file: the tray once every file is in](img/produce-file-done.svg)

A file that fails does not stop the others. The tray turns red and names the failed file and why, and `kaas` exits 1. A missing file, a directory, or `--value` with `-f` is a usage error (exit 2) before anything is sent. A pipe prints `path: N records` per file. `-o json` prints one object per record with `file` and `line` added, so each record can be traced back to its source.

## `kaas bench produce`: measure, don't guess

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

## `kaas bench consume`: the read side

The kaas version of `kafka-consumer-perf-test`. Fetch sizes and waits multiply into cases, the same way:

```bash
kaas bench consume -t orders --records 100000 --fetch-size 64KB,1MB,8MB --max-wait 100ms,500ms
```

Each case reads from the beginning of the topic with its own client, so no case is served from another case's buffer. It never joins a group and never commits. A case reads `--records`, or the whole topic if it holds fewer. If the topic is empty, `kaas bench produce` fills it.

![kaas bench consume: fetch sizes compared, with poll waits and throughput](img/bench-consume.svg)

The latencies here are poll waits: how long the reader waited for each batch. A poll served from data already fetched takes microseconds, and one that waits on the broker takes a round trip. Small fetches show more of the second kind. `--fetch-size` takes sizes like `64KB` or `1MB` per partition. On a pipe you get one line per case, and with `-o json` you get the full results, including `polls`.

## `kaas pipe`: move records between Kafka, files, HTTP and plugins

```bash
kaas pipe --from 'kafka:orders?stop=end' --to file:orders.jsonl          # export a topic, then stop
kaas pipe --from file:sample.jsonl --to 'kafka:orders?brokers=staging:9092'  # replay a sample elsewhere
kaas pipe --from 'kafka:orders?group=eu-big' \
  --where 'value.amount > 100 && headers["region"] == "eu"' \
  --to https://hooks.example.com/orders --to kafka:audit                    # filter, fan out
kaas pipe --from 'gen:payments?rate=2000' --to kafka:payments --for 10m  # load test
kaas pipe -f pipelines/orders-to-lake.yaml                               # a pipeline kept in git
```

A pipe reads batches from one or more **sources** (`--from`, repeatable: they're read at the same time) and writes every record to each **sink** (`--to`, repeatable). A batch is acknowledged to its source only after every sink has written it:
- a Kafka consumer group commits only offsets whose records landed;
- an HTTP sender gets its `200` only once its records are stored;
- a plugin is told which records it can forget.

If a sink fails, or you press Ctrl-C mid-write, the next run reads the unacknowledged records again. Delivery is **at least once**, and nothing acknowledged is lost. A run stopped with Ctrl-C still acknowledges what it had already written, so stopping and starting a pipe doesn't repeat work.

On a terminal the run draws as a data flow, on stderr so stdout stays free for records. The sources come in on the left, the transform stage sits in the middle, and the sinks are on the right, with records moving along the paths and counters along the bottom.

![kaas pipe: two generated feeds into a topic and a file, filtered, with records in flight](img/pipe.svg)

Elsewhere it prints one summary line when it ends, such as `pipe: read 1,000 · wrote 950 · filtered 50 · rejected 0 · 2s`. With `-o json` it prints the final counts per source and sink. `--max N` stops after N records are written, and `--for 30s` stops after a time.

### Endpoints

Every endpoint is a URL. Parameters go in its query string.

| Spec | As a source | As a sink |
|---|---|---|
| `kafka:<topic>` | Reads the topic. `group=` reads as a consumer group that commits only what was written and holds rebalances while a batch is in flight; without a group nothing is committed. `from=start\|end` (end by default). `stop=end` stops at the end offsets the topic had when the pipe started, for a bounded copy. `brokers=` reads another cluster. `batch=` (500). | Produces with the idempotent producer, `acks=all` by default, and waits for every record's acknowledgement. `acks=leader` trades durability for speed. Record timestamps are kept; `time=now` stamps them anew. |
| `file:<path>` | Reads JSON lines (`.jsonl`, `.ndjson`, `.json`), CSV (`.csv`) or text (anything else); `format=` overrides. | Appends in the same formats and syncs each batch to disk before it's acknowledged. `truncate=true` starts the file over. |
| `stdin`, `stdout`, `-` | The process's own streams, JSON lines by default, `?format=text\|csv` | |
| `https://…`, `http://…` | | POSTs each batch as a JSON array (`format=jsonl` for NDJSON). 429 and 5xx answers are retried with backoff, honouring `Retry-After`, up to `retries=` (5). Other 4xx answers fail at once. `header=Name:value` adds a request header. |
| `listen://<host:port>/<path>` | Serves POSTs of JSON lines, a JSON array or `text/plain` lines. Each request is a batch, answered `200 {"written":N}` once every sink wrote it, or `503` if the pipe stopped first, so the sender knows to retry. | |
| `gen:orders\|clicks\|payments\|iot` | Made-up JSON records for demos and load tests. `rate=` records a second (0 for as fast as the sinks take them), `count=` in all, `seed=` for the same records every run. | |
| anything else | A [plugin](#plugins) named `kaas-source-<scheme>` | A plugin named `kaas-sink-<scheme>` |

Records in JSON lines have the shape `kaas consume -o json` writes: `topic`, `partition`, `offset`, `timestamp`, `key`, `value`, `headers`, and `encoding: "base64"` when the key or value is binary (both are then base64). A sample taken with `kaas consume -o json` therefore replays through a pipe unchanged. CSV files have a `key,value,timestamp` header; any other column becomes a record header.

Credentials in a spec (a URL's password, `header=`, `token=`, `password=` and the like) are shown as `***` on screen, in stats and in errors.

### `--where`: filter with CEL

`--where` keeps the records a [CEL](https://cel.dev) expression passes. The expression sees:

| Name | Is |
|---|---|
| `key`, `raw` | The key and the value, as text |
| `value` | The value parsed as JSON when it is a JSON object or array, else its text |
| `headers` | A map of header names to values |
| `topic`, `partition`, `offset`, `time` | Where the record came from, when its source knows |

```bash
--where 'value.status == "placed" && value.amount >= 100.0'
--where '"vip" in value.tags'
--where 'has(value.order_id)'                  # a shape check, as a schema gate
--where 'headers["region"] == "eu" || key.startsWith("eu-")'
--where 'time > timestamp("2026-10-31T00:00:00Z")'
```

Records the expression is false for are **filtered**. Records it can't evaluate, such as a field the value doesn't have, are **rejected**. Both are dropped, counted separately, and still acknowledged, so a bad record can't block the pipe. Each evaluation has a cost limit, so an expression can't spin on a huge value.

### Pipeline files

`-f` reads a pipeline from YAML. Flags given as well add sources and sinks, and override the rest.

```yaml
name: orders-to-lake          # shown on the screen and in -o json
brokers: kafka-0.prod:9092    # unless -b is given
from:
  - kafka:orders?group=orders-to-lake&from=start
to:
  - file:/data/lake/orders.jsonl
  - https://hooks.example.com/orders?header=Authorization:Bearer%20$TOKEN
where: value.status != "cancelled"
max: 0                        # no limit
for: ""                       # until Ctrl-C or the sources end
plugins: [./plugins]          # directories to find plugins in first
```

An unknown key in the file is an error, so a misspelt `form:` doesn't silently read nothing.

### Plugins

A scheme kaas doesn't know runs a plugin: an executable named `kaas-source-<scheme>` or `kaas-sink-<scheme>`. kaas looks in `--plugin-dir`, then `$KAAS_PLUGIN_PATH`, then `~/.config/kaas/plugins`, then `PATH`. The plugin gets the whole spec as its only argument and in `$KAAS_SPEC`, and the cluster in `$KAAS_BROKERS`. It speaks JSON lines, so it can be a few lines of shell, Python or anything else. Its stderr passes through to kaas's stderr.

**A source plugin** writes one record a line to stdout, in the JSON-lines shape above, with an optional `"id"`. Once the record is written everywhere, kaas writes `{"ack": <id>}` to the plugin's stdin. The plugin ends the source by exiting; a line `{"error": "…"}` fails the pipe with that message.

```bash
#!/bin/sh
# kaas-source-tickets: open tickets from an API, acknowledged by id
curl -s "https://tickets.example.com/api/open" |
  jq -c '.[] | {id: .id, key: .project, value: (. | tostring)}'
```

**A sink plugin** reads records from stdin, one a line, each with a `"seq"` counting up from 1. It writes `{"ack": <seq>}` once everything up to that record is durable, or `{"error": "…", "seq": <seq>}` to fail the write. kaas waits for the ack of a batch's last record before acknowledging the batch to its source.

```python
#!/usr/bin/env python3
# kaas-sink-ledger: append records to a database, acknowledging each commit
import json, sys, sqlite3
db = sqlite3.connect("ledger.db")
db.execute("create table if not exists r (k text, v text)")
for line in sys.stdin:
    rec = json.loads(line)
    db.execute("insert into r values (?, ?)", (rec.get("key"), rec.get("value")))
    db.commit()
    print(json.dumps({"ack": rec["seq"]}), flush=True)
```

`kaas pipe plugins` lists the built-in schemes and every plugin it finds, with where each lives. A Go program embedding kaas can add its own endpoint kinds with `pipe.Register`, as `connectors/httpio` does for the HTTP ones.

## `kaas console`: the fleet in a browser

```bash
kaas console --demo                          # five made-up clusters in four regions, with live traffic
kaas console --fleet fleet.yaml              # your fleet
kaas console -b kafka-0:9092 --desired gitops/prod.yaml   # one cluster and its desired state
```

It prints `kaas console  http://127.0.0.1:7070  (5 clusters, read-only)`; open that address.

![Overview: availability, produced and consumed, data freshness and cost, the fleet on a map, recommendations, traffic and every cluster's last hour](img/console-fleet.png)

| Page | What it shows |
|---|---|
| **Overview** | What the fleet is doing for the business: availability over the last hour, records produced and consumed, data freshness, an estimated monthly cost, every cluster on a world map with its share of traffic, the top recommendations, produced against consumed over time, and a row per cluster with a minute-by-minute status strip |
| **Recommendations** | Things worth acting on, worst first, each with what it costs, where to look and the `kaas` command that shows or fixes it. See [Recommendations](#recommendations) |
| **A cluster** | Availability and the last hour minute by minute, produced, consumed, freshness and retained data, traffic, the cluster's recommendations, a treemap of where the traffic goes, brokers with their share of leaders, and the cluster's own events |
| **Topics** | Every topic with partitions, replication, records, rate and retention. Click one for its partition map (leaders and replicas per broker) and the groups reading it |
| **Consumers** | Groups ranked by lag, with data age, consume rate and time to catch up |
| **Access** | ACLs by principal, as sentences ("allow User:billing read on topic orders"), and quotas by user and client |
| **Schemas** | Schema Registry subjects with type, versions and compatibility, and the latest schema highlighted |
| **GitOps** | The plan from the desired-state file: each change by risk, policy results, warnings, and Apply |
| **Changes** | Drift across the whole fleet on one page |
| **Activity** | Every event, filterable by cluster and severity |

![The consumers tab: groups ranked by lag with rate and time to catch up](img/console-consumers.png)

**The numbers.**
- *Availability* is the share of the last hour's health samples (one every 10 seconds) in which the cluster answered with every partition online. The overview averages it over the fleet.
- *Data freshness* is how far behind a consumer group is in time: its lag divided by the rate its topics are written at. The overview shows the group furthest behind.
- *Est. run cost* is brokers × the hourly broker price × 730 hours, plus stored GiB × the monthly price, from `pricing` in the fleet file. Without `pricing` the overview shows the consumer backlog instead.

Press **⌘K** or **/** to jump to any cluster, topic, group or page. Light, dark and system themes are in the sidebar.

![The console in its light theme](img/console-light.png)

| Flag | Default | |
|---|---|---|
| `--fleet` | | The fleet file |
| `--desired` | | Without `--fleet`: the desired-state file for the `-b` cluster |
| `--name` | `local` | Without `--fleet`: what to call the `-b` cluster |
| `--listen` | `127.0.0.1:7070` (`KAAS_CONSOLE_LISTEN`) | Address to serve on |
| `--allow-apply` | off | Let the GitOps tab apply plans. Needs a localhost `--listen` |
| `--demo` | off | Start in-memory clusters with traffic, ACLs, quotas, schemas and drift |

**Security.** The console has no login yet:
- It listens on localhost, and is read-only unless `--allow-apply` is given.
- `--allow-apply` refuses any address other than localhost.
- Listening beyond localhost prints a warning. Put the console behind an authenticating proxy in that case.
- An apply needs the cluster's name typed, a same-origin request, and the plan ID that was on screen. If the cluster or the file changed in between, nothing is applied and the new plan is shown.
- Pages are served with a strict content security policy.

### Recommendations

Recommendations are read from each cluster's live state on every refresh, so one disappears as soon as its cause does.

| Kind | Severity | When |
|---|---|---|
| Not answering | critical | No broker has answered for three metadata intervals |
| Partitions offline | critical | A partition has no leader |
| Under-replicated | warning | Replicas have fallen out of sync |
| One copy in production | warning | A topic with replication factor 1 on a multi-broker production cluster |
| Stopped consumer | warning | A group with no members and a growing backlog |
| Stale data | warning | A group reading data a minute old or more |
| Can't apply | warning | The desired-state file is blocked by a policy, or the plan is destructive |
| Drift | advice | The cluster differs from its desired-state file |
| Unread topic | advice | A topic written at 1 record/s or more that no group reads |
| Leader skew | advice | A broker leads 1.5 times its even share of six or more partitions |

![Recommendations, worst first, each with its impact and a command](img/console-insights.png)

### The fleet file

```yaml
name: Acme Commerce              # shown in the sidebar and the overview; default Fleet
pricing:                         # optional: an estimated monthly cost per cluster
  broker_hour: 0.21              # per broker per hour
  gib_month: 0.10                # per GiB stored, all replicas, per month
  currency: USD                  # default USD
clusters:
  - name: prod-eu
    brokers: [kafka-0.prod-eu:9092, kafka-1.prod-eu:9092]
    env: production              # production, staging or development: a badge on the card
    region: eu-west-1
    desired: gitops/prod-eu.yaml # relative to this file
    schema_registry: https://schemas.prod-eu:8081   # credentials as https://user:pass@…
  - name: staging
    brokers: [kafka.staging:9092]
    env: staging
```

Names must be unique. Unknown keys are errors.

## GitOps: `kaas plan` and `kaas apply`

Keep each cluster's topics, ACLs and quotas in a file in git. `kaas plan` says what it would take to make the cluster match. `kaas apply` does it.

```bash
kaas plan -f gitops/prod-eu.yaml -b kafka-0.prod-eu:9092
```

```
Plan 05a84a7f9082d0f7 for prod-eu: 4 changes (2 safe, 1 caution, 1 destructive)

  + create-topic   [safe]        payments
      6 partitions × 3 replicas, 1 config
  ~ set-config     [caution]     orders retention.ms  604800000 → 172800000
      changed on the topic; less data is kept
  + create-acl     [safe]        allow User:billing read on topic payments
      grants User:billing read
  - delete-acl     [destructive] allow User:legacy-etl write on topic orders
      User:legacy-etl loses write on orders

Not in the file, left alone: 3 topics, 0 ACLs, 0 quotas
```

Each change is marked:

| Sign | Meaning |
|---|---|
| `+` | Adds |
| `~` | Changes |
| `-` | Removes |

Each change also has a risk:

| Risk | Covers |
|---|---|
| safe | Adds something; nothing that works now stops working |
| caution | Changes behaviour: added partitions (keyed ordering), less retention, unset configs, lower or removed quotas |
| destructive | Removes data or access: topic deletes and revoked ACLs |

Every change is first sent to the controller as a validate-only request. One it refuses blocks the plan, as does a policy violation.

| Flag | |
|---|---|
| `-f`, `--file` | The desired-state file (required) |
| `-o json` | The plan as JSON, for a bot to comment on a pull request |
| `--exit-code` | Exit 5 when there are changes, so CI can tell drift from failure |

| `kaas plan` exits | When |
|---|---|
| `0` | The cluster matches, or there are changes and `--exit-code` isn't set |
| `5` | There are changes, with `--exit-code` |
| `1` | The plan is blocked by a policy, an error or the controller |
| `2` | The file is missing or invalid |

`kaas apply` makes the same plan and applies it in a safe order:
1. Create topics.
2. Add partitions.
3. Change configs.
4. Grant ACLs.
5. Set quotas.
6. Revoke ACLs.
7. Delete topics.

New grants land before old ones go. It stops at the first failure and prints what was done. On a terminal you confirm by typing the cluster's name. In CI, pass `--yes` and the reviewed plan's ID:

```bash
id=$(kaas plan -f gitops/prod-eu.yaml -o json | jq -r .id)     # on the pull request
kaas apply -f gitops/prod-eu.yaml --plan "$id" --yes            # after it merges
```

If the cluster or the file changed between the two, the ID differs and nothing is applied.

### The desired-state file

```yaml
cluster: prod-eu
topics:
  orders:
    partitions: 12
    replication: 3
    configs: {retention.ms: 7d, cleanup.policy: delete, max.message.bytes: 1MiB}
  audit:
    partitions: 3
    replication: 3                 # no configs key: its configs aren't managed
acls:
  - principal: User:billing
    resource: topic                # topic, group, cluster, transactional-id
    name: orders
    pattern: literal               # or prefixed
    operations: [read, describe]
  - principal: User:billing
    resource: group
    name: billing-
    pattern: prefixed
    operation: read
quotas:
  - user: analytics
    values: {consumer_byte_rate: 10MiB, request_percentage: "50"}
  - default_client_id: true
    values: {producer_byte_rate: 5MiB}
policies:
  - name: replicated
    rule: topic.replication >= 3
    message: production topics need three replicas
  - name: bounded-retention
    rule: "!('retention.ms' in topic.configs) || topic.configs['retention.ms'] != '-1'"
prune:
  topics: false                    # never delete topics not in this file
  acls: true                       # revoke ACLs not in this file
  quotas: true
```

- **Values.** Durations and sizes can be written as `7d` or `1MiB`.
- **Topic configs.** A topic with `configs` has every topic-level config managed: one set on the cluster but missing here is planned for removal. Leave `configs` out to manage only partitions and replication.
- **ACLs and quotas.** Managed only when the file has an `acls` or `quotas` key, and removed only when `prune` says so.
- **Policies.** CEL rules every declared topic must pass. They see `topic.name`, `topic.partitions`, `topic.replication` and `topic.configs`.
- **Changes Kafka can't make are warnings, not changes.** Partitions can't be removed, and a replication-factor change needs a reassignment.
- **Unknown keys are errors.**

## When something is wrong

Errors are screens too, and each one says what to do next.

**Cluster unreachable.** One row per seed broker, with a dot for each probe layer. The headline is the layer that broke:

![The 503 page: a rack of seeds with probe dots, the error, and next steps](img/unreachable.svg)

**A name that doesn't exist.** The closest real names come from the live cluster:

![The 404 page for an unknown topic, suggesting the real one](img/not-found-topic.svg)

**A bad flag or value.** The flags you set, the bad one highlighted, and why:

![The validation screen for an invalid --acks value](img/invalid-flag.svg)

## Output and exit codes

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

## Configuration

| Setting | Flag | Environment | Default |
|---|---|---|---|
| Seed brokers | `-b`, `--brokers` | `KAAS_BROKERS` | `localhost:9092` |
| Theme | `--theme magic\|amber\|ice` | `KAAS_THEME` | `magic`: the colours of the original ascii-magic pieces |
| No colour | | `NO_COLOR=1` | |
| No animation | | `KAAS_REDUCED_MOTION=1` | |

## Troubleshooting

| The probe stops at | It means | Try |
|---|---|---|
| **dns** | The host name doesn't resolve | Check `--brokers`, VPN, `/etc/hosts` |
| **tcp** (refused) | Nothing is listening there | Is Kafka up? Is it the right port? For kind: `task dev:forward` |
| **api** (no answer in time) | Connected, but the broker didn't respond | The broker may be starting, overloaded or cut off from its controller |
| **api** (protocol error) | Something answered, but not as Kafka | Wrong port, or the listener needs TLS/SASL |
| **metadata** | Brokers answered but metadata didn't | Controller trouble; check broker logs |
