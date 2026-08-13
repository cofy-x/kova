# Development Conventions

Stability rules (behavior, CLI surface, worktrees) live in [AGENTS.md](../AGENTS.md); this file covers only day-to-day conventions.

## Code

- Non-trivial shell logic lives in `scripts/`; `Makefile` targets stay thin wrappers.
- Prefer existing package boundaries and helper APIs over new abstractions.
- `kova` stays a CGO-free, cross-platform client. Linux build execution, LMDB state, and daemon APIs belong to `kovad` and its runtime image.

## Charts and Examples

- Chart templates contain no environment-specific registry addresses. Dockerfile defaults use official upstream sources; generic proxy, download-base, base-image, and `GOPROXY` overrides belong to consuming environments.
- Examples use the documented local registry addresses: `localhost:5002` on the host, `host.docker.internal:5002` from Pods and build outputs.

## Markdown

- English prose: one sentence per line. Chinese prose: one paragraph per line with fullwidth punctuation; never wrap CJK text at a fixed column.
- Link syntax, code blocks, tables, and URLs stay intact on one line.
