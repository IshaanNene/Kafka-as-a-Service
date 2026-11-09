# Marketing site: visual spec

Working spec for `site/`. Part 1 is what was measured on the reference (langfuse.com at 1440×900 and 375×812). Part 2 is what KaaS does with it. Nothing visual or written is taken from the reference: only its level of care.

## 1. Reference analysis

| Area | Observed |
| --- | --- |
| Layout | Docs-like three-column shell: fixed 240px left rail, 840px centre column, right table of contents. Structural hairlines run the full height. Sections 650–850px tall, so one idea per screen. |
| Type | H1 68/71, weight 400, three deliberate lines. H2 only 32/37, left aligned. Body 16/24, supporting text in muted warm grey. Mono at 10–11px for labels and metadata. |
| Colour | Warm greys: page `#EDEDE8`, panels `#F6F6F3`, ink `#222220`, lines around `#CFCFC9`. One saturated highlight, used only behind a headline phrase and tiny "new" tags. |
| Density | High information per screen, small type, small gaps inside panels, generous gaps between sections. |
| Borders | 1px everywhere, radius 0–2px. Panels carry corner ticks; grids carry crosshairs at intersections. Shadow only on buttons, and faint. |
| Diagrams | Square boxes, elbow connectors with small solid arrowheads, boxed mono labels on the lines. Isometric line drawings inside feature cells. |
| Product | Real screenshots in thin frames, tab row of plain words above them. |
| Composition | Each section is its own arrangement: flow diagram, tabbed shot, loop diagram, bento, 2×2 with crosshairs, ruled two-column list, Q&A split. |
| Mobile | Rails drop, diagrams restack vertically with down arrows, same frame language, headings about 32px. |

Why it works: the page behaves like a technical document that happens to sell. Restraint in colour lets the one highlight do all the emphasis.

## 2. Creative direction: the page is a log

KaaS is about Kafka, so the page is structured like a partition. Each section is a **record at an offset**. The left rail is the log itself: as you read, the committed offset advances and consumer lag falls to zero. Everything else follows from an engineer's notebook: paper, ink, ruled lines, one orange highlighter.

Differences from the reference, on purpose: one rail, not two, and a wider canvas for asymmetric layouts; a log-based navigation concept; a controlled dark band for the "inside your walls" architecture and for terminals; horizontal log rows as the core visual motif, reused in the hero, the boundary diagram and the rail.

## 3. Design system

**Colour tokens** (light / dark)

| Token | Light | Dark | Use |
| --- | --- | --- | --- |
| `--shell` | `#eae8e0` | `#141310` | Rail, nav, page |
| `--bg` | `#f1efe8` | `#191815` | Reading column |
| `--panel` | `#f8f6f1` | `#201f1b` | Frames |
| `--ink` | `#23211d` | `#efece4` | Text |
| `--ink-2` | `#5b584f` | `#b7b3a8` | Supporting text |
| `--ink-3` | `#68655b` | `#a09c91` | Mono labels |
| `--line` | `#d2cfc4` | `#37352e` | Hairlines |
| `--line-2` | `#a39f92` | `#5a574d` | Emphasised lines |
| `--hl` | `#ffab66` | `#ff8a3c` | Highlighter, headlines only |
| `--acc` | `#e8590c` | `#ff7a2f` | Graphic accent: records in motion, active state |
| `--acc-t` | `#ab3a06` | `#ff9a5c` | Accent as text |

Semantic ok, warn and bad appear only inside product surfaces.

**Type**: display Bricolage Grotesque 500; body Instrument Sans 400; labels Martian Mono. Scale: H1 clamp(40–66)/1.04, H2 clamp(29–37)/1.14, final H2 clamp(36–56), H3 17/1.3, body 15.5/1.55, small 13.5, label 10.5 mono.

**Spacing**: 4px base; 4, 8, 12, 16, 24, 32, 48, 72, 112. Section gap 112 desktop, 72 mobile.

**Widths**: shell 1520 max, rail 236, reading column fluid with 56px gutters, prose 56ch.

**Borders and radius**: 1px hairlines; radius 2px on controls, 0 on frames; corner ticks on primary frames; crosshairs at grid intersections on desktop. Button shadow only.

**Buttons**: 38px high, 2px radius, label plus a right-hand arrow cell. Primary is ink on paper, secondary is panel with hairline. Orange is never a button fill except on the dark band.

**Navigation**: 56px bar with hairline, logo cell the width of the rail, dropdowns of square rows, sheet under 1000px.

**Section layouts**: hero (asymmetric headline with live wall), quadrant plus text, dark boundary diagram, sticky scroll story, tabbed product frame with engine strip, crosshair grid plus isometric chain, rate card with invoice band, Q&A split, dark final band.

**Motion**: only to explain. Hero replay is scrubbable; reveals are 8px and 400ms once; reduced motion gets the static end state.

**Breakpoints**: 1180 rail drops; 1000 nav collapses; 760 layouts stack; tested down to 375.
