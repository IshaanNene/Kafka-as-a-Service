# ADR-0001: franz-go as the only Kafka client

- Status: accepted
- Date: 2026-10-31

## Context
KaaS needs a Kafka client for the CLI, agent and API, plus raw protocol structs for `kaas sniff` and the gateway. Release binaries must cross-compile statically.

## Decision
Use franz-go (`kgo`, `kadm`, `kmsg`) everywhere, and `kfake` as the in-memory test cluster.

## Alternatives considered
- confluent-kafka-go: needs cgo and librdkafka, breaking static cross-compiles.
- sarama: weaker coverage of new KIPs and no reusable protocol package.

## Consequences
One client, one set of protocol structs for client, sniffer and gateway. We depend on franz-go's pace of KIP support; CI on three Kafka lines catches drift.
