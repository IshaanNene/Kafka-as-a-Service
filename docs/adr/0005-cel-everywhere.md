# ADR-0005: CEL for filters, policies and alert rules

- Status: accepted
- Date: 2026-10-31

## Context
Message filters in the TUI, plan-time policies and alert rules all need a user-written expression language, evaluated safely on untrusted input.

## Decision
Use CEL via cel-go for all three, with cost limits on every evaluation.

## Alternatives considered
- OPA/Rego: strong for policy, awkward for per-message filters, and a second language for users.
- Embedded scripting (Lua, Starlark): Turing-complete, harder to bound.

## Consequences
One language to learn and document. CEL always terminates and cost limits bound CPU, which defends against filter-based resource exhaustion (threat model item 5). Kubernetes made the same choice for admission policies.
