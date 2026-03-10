# Documentation

This repository keeps product and architecture documentation under `docs/`.

## Layout

- `docs/spec/`
  - Canonical Markdown specifications and product documents.
- `docs/exports/`
  - Generated exports derived from the Markdown spec set.
- `docs/archive/`
  - Historical or legacy materials retained for reference.

## Source Of Truth

The source of truth for product definition and architecture is the Markdown set in `docs/spec/`.

Current spec entry points:

- [`spec/vision.md`](spec/vision.md)
- [`spec/architecture.md`](spec/architecture.md)
- [`spec/v0.1-plan.md`](spec/v0.1-plan.md)
- [`spec/glossary.md`](spec/glossary.md)

## Export Policy

The latest consolidated DOCX export is tracked at:

- [`exports/Crawfish-PRD.docx`](exports/Crawfish-PRD.docx)

To regenerate it, run:

```bash
python3 scripts/export_docset.py
```

## Archive Policy

Historical materials are retained under `docs/archive/` for provenance and comparison, but they are not editable specs.

