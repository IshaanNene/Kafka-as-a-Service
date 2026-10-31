# ADR-0004: Go toolchain and Taskfile, not Bazel

- Status: accepted
- Date: 2026-10-31

## Context
One engineer, one Go module today, a TypeScript web app later.

## Decision
Use the Go toolchain with a Taskfile for orchestration. Revisit Bazel only if CI builds exceed about 10 minutes.

## Alternatives considered
- Bazel: hermetic and cached, but heavy setup and maintenance cost for a solo project.
- Make: works, but Taskfile is cross-platform and self-documenting (`task --list`).

## Consequences
Low setup cost. Build caching relies on Go's own cache and CI cache actions.
