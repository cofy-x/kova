# Development Conventions

- Prefer putting non-trivial shell logic in `scripts/`; keep `Makefile`
  targets as thin wrappers.
- Keep behavior stable unless the user explicitly asks for behavior changes.
- Preserve the CLI surface and daemon API paths unless a migration is planned.
- Do not put environment-specific registry addresses in chart templates.
- Be careful with dirty worktrees. Do not revert user changes unless asked.
- Prefer existing package boundaries and helper APIs over introducing new
  abstractions.
- Keep examples aligned with the documented local registry addresses:
  `localhost:5002` on the host and `host.docker.internal:5002` from Pods or
  build outputs.
- Wrap English Markdown prose at about 80 columns. Keep link labels concise and
  descriptive; do not force-wrap code blocks, tables, URLs, or other structures
  whose meaning or readability depends on staying intact.
