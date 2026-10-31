# Kafka As A Service (KaaS)

One Go codebase that ships three surfaces in order: a Kafka CLI/TUI (`kaas`), a multi-cluster management plane, then managed Kafka on Kubernetes. All three sit on one shared cluster-state engine in `core/`.

> Status: **Phase 1 — Developer tool.** See the [design docs](docs/design/) and [ADRs](docs/adr/).

## Quick start

```bash
go install github.com/IshaanNene/Kafka-as-a-Service/cmd/kaas@latest
kaas -b localhost:9092                   # terminal UI (: command · / filter · ? help)
kaas snapshot -b localhost:9092          # one-shot cluster summary
kaas watch -b localhost:9092 -t orders   # stream events and record growth
kaas bench produce -t orders --size 100,1024 --compression none,lz4   # throughput and latency matrix
kaas help watch                          # usage and examples
```

On a terminal every command draws its own screens: help as a command palette with an examples session, a boot log while it connects, and error pages that say what broke (an unreachable cluster shows each seed's DNS → TCP → protocol → metadata probe; a typo gets the names you probably meant). Pipes get plain text and `-o json` gets JSON errors. Exit codes are stable: 0 ok, 1 failure, 2 usage, 3 not found, 4 unavailable (ADR-0007). [Design 0002](docs/design/0002-command-screens.md) maps every ascii-magic UI piece to the command that uses it.

Themes: `--theme amber` (Amber Phosphor, default) or `--theme ice`. `NO_COLOR` keeps the layout without colour; `KAAS_REDUCED_MOTION=1` freezes animation.

## Develop

Requires Go 1.26, [Task](https://taskfile.dev), golangci-lint v2, and Docker for integration tests and the local stack.

```bash
task test               # unit + kfake tests
task test:integration   # Testcontainers; KAAS_KAFKA_IMAGE=apache/kafka:4.4.0 to pick a version
task lint               # includes the core/ import boundary (ADR-0003)
task dev                # kind + Strimzi + 3-broker KRaft cluster
```

## Layout

```
cmd/            kaas (agent, gateway, api, operator to come)
core/           cluster-state engine; no UI or transport imports
ui/             TUI, shared command screens, grid kit and themes (Phase 1)
proto/          Protobuf APIs (Phase 2)
controlplane/   API services, reconcilers, outbox relay (Phase 2)
dataplane/      agent, gateway, sniff decoder
operator/       controller-runtime operator over Strimzi (Phase 3)
web/            React dashboard (Phase 2)
deploy/         kind configs, Helm charts
test/           kfake/Testcontainers harness, simulation, chaos, benchmarks
docs/           design docs, ADRs, runbooks
```

Screen layouts are Go ports of [ascii-magic](https://www.ascii-magic.com/ascii) UI pieces (MIT), drawn from live data (ADR-0007). A few decorative pieces are baked into frame packs by `task asciipack` (ADR-0006). Its licence and notice ship in `ui/asciipack/packs/`.

Apache Kafka is a trademark of the Apache Software Foundation. KaaS is not affiliated with or endorsed by the ASF.
