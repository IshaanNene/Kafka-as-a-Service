# ADR-0006: Bake ascii-magic pieces into frame packs

- Status: accepted
- Date: 2026-10-31

## Context
The design system reuses pieces from ascii-magic (MIT, `ascii-magic-art` on npm). Each piece is a JavaScript module whose `draw(t)` returns a frame. The `kaas` binary must stay a single static Go executable with no JavaScript runtime, and play smoothly inside a 16 ms frame budget.

## Decision
At build time, `tools/asciipack/bake.mjs` runs each chosen piece in Node and samples one loop (at most 15 fps) into a gzipped JSON frame pack: the piece's size, fps and palette, plus each frame's characters and colour keys. Packs are committed under `ui/asciipack/packs/` and embedded with `go:embed`. At runtime, playback is just picking a frame by elapsed time, then colouring it with lipgloss. Bubble Tea or `colorprofile` downsample the colours to what the terminal supports.

`ascii-magic-art` is pinned to 1.1.0 by an npm lockfile and installed with `--ignore-scripts`. Its LICENSE and NOTICE are copied beside the packs. The two Unsplash-derived scenes the NOTICE excludes from MIT are never baked.

## Alternatives considered
- Embed a JavaScript engine (goja) and run pieces live: adds a runtime and per-frame CPU, and drags JavaScript into the binary.
- Port every piece to Go by hand: right for screens that show live data (and planned for those), wasteful for decorative ones.

## Consequences
Six pieces cost about 160 KiB in the binary. Animations loop rather than run forever, so a piece whose loop length is not one of its own periods jumps once per loop. Changing a piece means re-running `task asciipack`, which needs Node; CI and normal builds don't.
