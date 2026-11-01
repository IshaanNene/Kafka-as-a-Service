# ADR-0008: Phases 2 and 3 get a web console, embedded in the binary

- Status: accepted
- Date: 2026-11-01

## Context
The terminal UI suits one engineer looking at one cluster. The management plane (Phase 2) and managed Kafka on Kubernetes (Phase 3) have different readers:
- platform teams comparing many clusters;
- reviewers reading a GitOps plan;
- managers looking at a fleet's health.

They expect a page they can open, link to and leave on a wall screen. Hosted Kafka consoles also set the bar for how a fleet should look.

## Decision
- **The TUI is Phase 1 only.** Fleet, GitOps, schemas, ACLs, quotas and (in Phase 3) cluster lifecycle are web pages, served by `kaas console`.
- **No build step.** The app is plain ES modules, CSS and self-hosted fonts in `controlplane/console/web`, embedded with `//go:embed`.
  - `kaas` stays one static binary.
  - `go install` still works.
  - There is no `node_modules` in the repository.
  - A small `h()` with a keyed morph renderer stands in for a framework, and keeps focus and scroll while data streams in.
  - Node is used only to run the JavaScript unit tests.
- **The server is a thin adapter** in `controlplane/console`. Plan, ACL and quota logic lives in `core/` (ADR-0003), and the HTTP schema-registry client lives in `controlplane/schemas`.
- **A brand of its own.**
  - Dark "chitin" surfaces with one signature colour, volt (`#d4ff3a`), and violet iris for data.
  - Bricolage Grotesque for display, Instrument Sans for the interface, Martian Mono for numbers. All three are OFL and shipped in the binary.
  - Status colours (ok, warning, critical) are reserved for state and always come with an icon and a word.
  - A light "paper" theme, and the system setting by default.
- **Safe by default.**
  - Listens on localhost.
  - Read-only unless `--allow-apply`, which refuses a non-loopback address because there is no login yet.
  - An apply needs JSON, a same-origin request and the cluster's name typed, plus the plan ID the user reviewed. If the cluster or the file has changed since, the server answers 409 instead of applying something else.
  - A strict CSP (`default-src 'self'`, no inline script or style), `nosniff`, `no-referrer` and COOP.

## Alternatives considered
- **React or Svelte with a bundler.** This means a toolchain, a lockfile and supply-chain surface for a few thousand lines of UI. It may come back if the UI outgrows the morph renderer.
- **Extending the TUI to many clusters.** It can't be linked to, shared or left on a wall screen, and reviewers of a plan shouldn't need a terminal.
- **A separate frontend service.** It would be a second deployable and a second version to keep in step with the API it reads.

## Consequences
- One binary serves both the CLI and the console. `kaas console --demo` shows the whole thing without a cluster.
- No login yet, so anything beyond localhost needs an authenticating proxy. OIDC is next.
- Phase 3's operator reports its clusters to the same console. Create, roll and autoscale become pages next to Fleet and Changes, not a new app.
