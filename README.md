# Kafka As A Service (KaaS)

One Go codebase that ships three surfaces in order: a Kafka CLI/TUI (`kaas`), a multi-cluster management plane, then managed Kafka on Kubernetes. All three sit on one shared cluster-state engine in `core/`.

> Status: **Phase 1 — Developer tool.** See the [design docs](docs/design/) and [ADRs](docs/adr/).

## Quick start

```bash
go install github.com/IshaanNene/Kafka-as-a-Service/cmd/kaas@latest
kaas snapshot -b localhost:9092          # one-shot cluster summary
kaas watch -b localhost:9092 -t orders   # stream events and record growth
```

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
ui/             Bubble Tea screens and theme engine (Phase 1)
proto/          Protobuf APIs (Phase 2)
controlplane/   API services, reconcilers, outbox relay (Phase 2)
dataplane/      agent, gateway, sniff decoder
operator/       controller-runtime operator over Strimzi (Phase 3)
web/            React dashboard (Phase 2)
deploy/         kind configs, Helm charts
test/           kfake/Testcontainers harness, simulation, chaos, benchmarks
docs/           design docs, ADRs, runbooks
```

Apache Kafka is a trademark of the Apache Software Foundation. KaaS is not affiliated with or endorsed by the ASF.
