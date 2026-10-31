# ADR-0002: Postgres as source of truth, Kafka as backbone via outbox

- Status: accepted
- Date: 2026-10-31

## Context
Control-plane state needs transactions and constraints. Audit, metering and read models need an ordered, replayable stream of facts.

## Decision
Desired state lives in Postgres. Each mutation writes desired state and an outbox row in one transaction; a relay publishes outbox rows to backbone topics (`cp.events.v1`, `cp.ops.v1`, ...).

## Alternatives considered
- Kafka as the database (event sourcing only): constraints and ad-hoc queries become hard.
- Dual writes to Postgres and Kafka: lose atomicity on partial failure.

## Consequences
At-least-once publication; consumers dedupe on (aggregate ID, version). The relay is a new component to operate and monitor (outbox age is an SLI).
