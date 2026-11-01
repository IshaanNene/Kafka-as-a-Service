# Design 0002: A screen for every command

Status: accepted · 2026-10-31 · see ADR-0007

Every `kaas` command draws its own screen on a terminal. Each screen is a Go port of one ascii-magic UI piece's layout, filled with live cluster data (`ui/kit`, `ui/screens`). Pipes get plain text and `-o json` gets JSON, so scripts never see a screen.

This document maps all 79 pieces in ascii-magic's **UI** category to the command or state that uses them. It also maps the **Empty States** and **Live Data** pieces where a command needs a state card or a chart. Piece names link to `https://www.ascii-magic.com/ascii/<slug>`. To see every piece as an image next to its command, open the [screen gallery](../SCREENS.md), which is generated from these tables (`task gallery`).

**Status** is one of:
- **built**: the screen is shipped.
- **P1**: Phase 1, the developer tool.
- **P2**: Phase 2, the management plane.
- **P3**: Phase 3, managed Kafka.
- **unused**: no honest Kafka fit, kept out on purpose.

## Shared screens: every command

| Piece | Used for | Status |
|---|---|---|
| command-palette | `--help` and `kaas help <cmd>`: the command's subcommands and flags | built |
| terminal-session | Examples under every help screen | built |
| boot-sequence | Connecting: DNS → TCP → ApiVersions → Metadata, with timings | built |
| page-500 | Cluster unreachable (503), internal error (500): per-seed probe rack | built |
| page-404 | Unknown command, topic, group or context, with "did you mean" | built |
| form-validation-errors | Bad flags and values, the bad one highlighted with the reason | built |
| modal-dialog | Destructive confirm: type the name to delete a topic, group or records | built (topics; groups and records next) |
| progress-loader | Multi-partition progress (delete records, offset resets) | P1 |
| spinner-set | One spinner for short waits; `KAAS_REDUCED_MOTION` freezes it | P1 |
| skeleton-loader | TUI screens before the first snapshot arrives | P1 |
| keyboard-keypress | TUI `?` overlay: the keys light up as you press them | P1 |

## Phase 1 commands

| Command | Piece(s) | What it shows | Status |
|---|---|---|---|
| `kaas` / `kaas tui` | split-pane-session, window-tiling-manager | Topics, events and meters in panes; panes tile as you open more | P1 (TUI built; tiling next) |
| `kaas snapshot` | data-table | Topics, partitions and URPs, sortable | **built** |
| `kaas watch` | toast-stack, sparkline-row (Live Data) | Events slide in as toasts; one records/sec sparkline per `-t` topic | **built** |
| `kaas bench produce` | benchmark-table, histogram (Live Data), network-traffic (Live Data) | One row per case (size × acks × compression), a live counter, a latency histogram and a throughput graph | **built** |
| `kaas bench consume` | benchmark-table, network-traffic | Same, for fetch: a case per fetch size × max wait, with poll waits | **built** |
| `kaas topics` | data-table, file-tree (`--tree`) | Topic list with retention, cleanup and disk; prefix hierarchy as a tree | **built** |
| `kaas topics describe` | disk-partitioner | Partitions laid out per broker, leaders and replicas | **built** |
| `kaas topics alter --partitions` | disk-partitioner | New partitions in green before writing; a typed yes; the topic as written | **built** |
| `kaas topics create` | onboarding-stepper | Name → partitions and replication from the cluster's defaults → configs → the controller's check → create | **built** |
| `kaas topics delete` | modal-dialog, empty-trash-lid | Type the name over the dimmed topic; 5s to undo before the delete is sent; the bin closes once the cluster lets go | **built** |
| `kaas brokers` | process-tree, gauge-cluster (Live Data) | Broker → leaders → partitions tree; disk, network and request gauges | **built** (tree; gauges next) |
| `kaas groups` | process-task-manager | Groups and members with lag and rate, re-sorting live | **built** |
| `kaas lag` | leaderboard, line-chart-live (Live Data), empty-all-caught-up | Groups ranked by lag; lag over time; "all caught up" at zero | **built** |
| `kaas top` | process-sorter | Partitions by records and bytes in/out, with a meter per broker | **built** |
| `kaas clients` | process-task-manager | Group members and idempotent producers by records/s (Kafka exposes no connection list or request rates) | **built** |
| `kaas consume` / `tail` | relay-chat-channel | Records stream in like a channel: time, partition, key, value | **built** |
| `kaas produce` | code-editor | Compose key, value and headers with JSON highlighting; pipes send a record per line | **built** |
| `kaas produce --file` | upload-dropzone, empty-upload-tray | Each file's lines upload as records, four files at a time, with bytes read, records/s and a bar per file; the tray when they are all in | **built** |
| `kaas offsets reset` | onboarding-stepper, infra-plan-diff | A stepper for whatever the flags leave out (group → topics → where to → the plan's effect); then every partition's committed offset → its new one as a plan diff, a typed yes, and the coordinator's answer per topic | **built** |
| `kaas offsets find` | bisect-hunt | A real bisect per partition, one Fetch per read: the first record at or after a time (checked against the broker's time index), or the first value matching a regex (checked by reading on past it) | **built** |
| `kaas consume --from --to` | date-range-picker | `--to` stops a `--from` read at a time; `--pick` chooses the days on two months of calendar, with days the topic no longer keeps faint and the window's exact record count from the broker's time index | **built** |
| `kaas sniff` | packet-sniffer | A proxy in front of the cluster that decodes every request through it, coloured by API, with the selected one's path and first bytes and the mix of recent APIs; it rewrites the broker addresses in Metadata, FindCoordinator, DescribeCluster and leader hints so clients stay on it | **built** |
| `kaas sniff --trace` | network-waterfall | A client's requests from its first, timed by the proxy stage by stage (connect, send, queue behind earlier requests on the connection, the broker's wait, ack), with the errors answers carry and lines where its group was joined and its first write acked | **built** |
| `kaas sniff --profile` | flame-graph | Every request's stages added up by API, broker and topic, each frame as wide as its share of the time, with a frame's count, average and p99; a fetch's polling (empty answers, and the wait before its first record was written) is told apart from serving and hidden until asked for; folded stacks on a pipe | **built** |
| `kaas segments` | heap-map | A partition's log segments: size, compaction, holes | P1 |
| `kaas config` | settings-toggles | Topic and broker configs, with non-defaults flagged | P1 |
| `kaas config edit` | modal-editor | Edit configs in a vi-style buffer; diff before apply | P1 |
| `kaas doctor` | test-runner-dots, coverage-report | A dot per check; per-topic health bars by threshold | P1 |
| `kaas doctor network` | dns-resolver-chain | Seed → metadata → advertised listener → leader, hop by hop | P1 |
| `kaas doctor auth` | dialup-handshake | TLS and SASL handshake, step by step | P1 |
| `kaas capacity` | spreadsheet-recalc | Edit throughput or retention; disk and broker counts recalculate | P1 |
| `kaas query` | sql-query-runner | CEL filter over a topic; results fill a grid | P1 |
| `kaas filter test` | ide-debugger-stepping | Step a CEL expression over sample records, with locals | P1 |
| `kaas search` | search-autocomplete | Find topics, groups and contexts as you type | P1 |
| `kaas ctx` | tab-bar | Contexts as tabs; switch clusters | P1 |
| `kaas ctx use` | ssh-login | Connect banner: cluster ID, version, brokers, topics | P1 |
| `kaas init` | bios-setup, empty-welcome-door, empty-onboarding-flag | First-run setup: contexts, auth, theme | P1 |
| `kaas theme` | color-picker-sliders | Edit or preview a theme's roles | P1 |
| `kaas version` | readme-badges | Version, commit, Go version and build date as badges | P1 |
| `kaas self-update` | release-published | New version and its release notes | P1 |
| `kaas lint` | compiler-output | Policy and config warnings, pointing at the offending line | P1 |
| `kaas trace <key>` | delivery-route-tracker | Follow a key across topics, hop by hop | P1 |
| `kaas dlq` | inbox-list, empty-inbox-plane | Dead-letter records like mail; an empty card when clean | P1 |

## Phase 2: management plane

| Command | Piece(s) | What it shows | Status |
|---|---|---|---|
| `kaas plan` | infra-plan-diff | Topics, ACLs and quotas to add, change or destroy | P2 |
| `kaas plan` (drift) | merge-conflict-resolver | Desired vs live, hunk by hunk: keep ours, take theirs | P2 |
| `kaas plan edit` | rebase-todo | Reorder, skip or squash plan steps | P2 |
| `kaas apply` | cluster-rollout, empty-rocket-countdown, empty-task-confetti | Countdown to abort; changes roll out; done card | P2 |
| `kaas change` | pr-merged | A change request: checks, approvals, merged | P2 |
| `kaas schema` | git-log-graph | Subject versions and compatibility as a graph | P2 |
| `kaas schema register` | package-publish | Pack, check compatibility, publish to the registry | P2 |
| `kaas acl` | twin-pane-commander | Principals on the left, resources on the right; grants copied across | P2 |
| `kaas quota` | speed-gauge (Live Data) | Produce and fetch quota as a gauge against use | P2 |
| `kaas user create` | password-strength-meter | SCRAM credential with a strength meter | P2 |
| `kaas login` | login-form, otp-code-entry | Sign in, then MFA or a device code | P2 |
| `kaas reassign` | download-queue | Each partition move with size, speed and ETA | P2 |
| `kaas rebalance` | kanban-board | Partitions move between broker columns | P2 |
| `kaas gateway log` | api-request-inspector | Gateway requests with method, status and timing bars | P2 |
| `kaas fleet` | smart-home-panel, world-map-pings (Live Data) | Every cluster's status at a glance; traffic by region | P2 |
| `kaas fleet --carousel` | weather-city-widget | Slides cluster to cluster with its health "forecast" | P2 |
| `kaas history` | git-log-graph | Config and ACL changes over time | P2 |
| `kaas mirror` | twin-pane-commander | Copy topics between clusters, with progress | P2 |

## Phase 3: managed Kafka

| Command | Piece(s) | What it shows | Status |
|---|---|---|---|
| `kaas install` | install-log | Operator install: resolve, fetch, apply | P3 |
| `kaas cluster create` | image-layer-pull, empty-construction-crane | Images pull layer by layer; provisioning card | P3 |
| `kaas cluster roll` | cluster-rollout, maintenance-mode | Broker-by-broker restart with ISR gating; a "be right back" page for clients | P3 |
| `kaas cluster autoscale` | pod-autoscaler | Load pushes CPU up; brokers are added | P3 |
| `kaas cluster maintenance` | calendar-month | Maintenance windows on a calendar | P3 |
| `kaas exec <broker>` | ssh-login | Shell into a broker pod | P3 |
| `kaas agent` | post-memory-test | Agent self-test: disks, memory, cluster handoff | P3 |
| `kaas plans` | pricing-toggle-table | Managed tiers, monthly vs yearly | P3 |
| `kaas billing` | checkout-payment-flow, empty-payment-success, empty-payment-failed | Upgrade a plan | P3 |
| `kaas calc` | calculator-app | Quick partition and throughput arithmetic | P3 |

## States (Empty States category)

| Piece | State |
|---|---|
| empty-offline-plug | TUI: connection lost mid-session |
| empty-reconnecting | TUI: reconnect attempts climbing |
| empty-rate-limited | Request budget exhausted; refresh in N s |
| empty-access-denied | Authorization failed (ACL) |
| empty-session-expired | Token expired; log in again |
| empty-no-results | Filter or search matched nothing |
| empty-cobweb-folder | Cluster has no topics yet |
| empty-smoking-server | A broker is down |
| empty-broken-link-cloud | Agent or gateway disconnected |
| empty-clipboard-copied | `ctrl+y` copied the screen's CLI command |
| empty-cloud-saved | Context saved |

## Unused, on purpose

These have no honest Kafka meaning and are not forced in:

| Piece | Why |
|---|---|
| chat-thread | A chat UI; `consume` already uses relay-chat-channel |
| spectrum-music-player | Music |
| star-rating-review | Product reviews |
| repo-star-button | Repository stars |

That puts 75 of the 79 UI pieces to use.

## How a screen is built

1. Print the piece's still frames (`tools/asciipack`) and port its layout to `ui/screens` using `ui/kit` parts.
   Port its colours too: the default `magic` theme is the pieces' own palette, so use the role the piece uses for each part (grey frames and titles, white values, blue bars, green for better, orange warnings, red errors) and the theme's `Chart`, `Hot` and `Cool` ramps for its gradients.
2. Feed it from the engine, the probe or the command's own measurements. No canned data.
3. Add golden tests (monochrome) and a fit test (colour, 40 to 200 columns).
4. Write the plain and JSON output for non-TTY, with exit codes from ADR-0007.
