# ADR-0003: core/ imports no UI or transport

- Status: accepted
- Date: 2026-10-31

## Context
The TUI, agent, API and MCP server must share identical cluster-state semantics (tenet 2: one core, many surfaces).

## Decision
`core/` may not import `ui/`, `controlplane/`, `dataplane/`, UI libraries, `net/http`, gRPC or Connect. golangci-lint's depguard enforces this in CI (`.golangci.yml`, rule `core-boundary`).

## Alternatives considered
- Convention only: erodes under deadline pressure.
- Separate Go module for core: adds versioning overhead before it is needed.

## Consequences
Core stays unit-testable against kfake and embeddable in the agent. Surfaces adapt core types rather than core adapting to them.

Pipe endpoints that need a transport, such as the HTTP webhook sink and `listen://` source, live in `connectors/` and register themselves with `core/pipe` (`pipe.Register`). The pipe engine and its Kafka, file and generator endpoints stay in core.
