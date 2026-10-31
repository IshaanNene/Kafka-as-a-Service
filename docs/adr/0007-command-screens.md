# ADR-0007: Every command gets its own screen, ported to Go and fed live data

- Status: accepted
- Date: 2026-10-31

## Context
ADR-0006 replays ascii-magic pieces as baked frame loops. That suits decoration, but a canned loop can't show your cluster. The design system's UI pieces (command palette, 500 page, form validation, boot log, terminal session and around 75 more) are layouts. A Kafka tool's help, errors and progress should look like them and carry real data: the broker that refused, the flag that was wrong, the topic you probably meant.

## Decision
- **`ui/kit`** holds the parts the templates are built from: rounded and heavy boxes, dotted rules, block digits, buttons, status dots and verdict marks. Every colour comes from a theme role.
- **`ui/screens`** ports the shared layouts and renders them from data:
  - help → command palette, from cobra's commands and flags
  - examples → terminal session, from each command's `Example`
  - unreachable cluster → 500 page, from a layered probe
  - unknown command or topic → 404 page, with fuzzy suggestions
  - bad flags → form validation
  - connecting → boot log
- **`core/probe`** checks a cluster one layer at a time (DNS, TCP, ApiVersions, Metadata). A failure therefore names the layer that broke.
- Command-specific screens (topics table, lag board, plan diff, and so on) follow the same pattern as those commands land.

Output contract:

| Where output goes | What it gets |
| --- | --- |
| Interactive terminal | The screens. |
| `NO_COLOR` | The same layout, monochrome, with dot shapes carrying state. |
| Pipes and `TERM=dumb` | Cobra's plain help and `kaas: message` plus `hint:` lines. |
| `-o json` | Errors as `{"error":{code,exit,message,hints,detail}}`. |

Exit codes are fixed:

| Code | Meaning |
| --- | --- |
| 0 | ok |
| 1 | failure |
| 2 | usage |
| 3 | not found |
| 4 | unavailable |
| 130 | interrupted |

## Alternatives considered
- Keep replaying baked packs (ADR-0006) for these screens. They can't show data. Packs stay only where pure decoration is wanted.
- Use cobra's default help and plain errors everywhere. They are stable, which is why pipes still get them, but on a terminal they give the operator less to act on.

## Consequences
Screens are golden-tested in monochrome, and fit-tested in colour from 40 to 200 columns. The probe adds one ApiVersions round trip per seed before a session opens, a few milliseconds locally. On success its boot log is erased, so only failures leave a trace.
