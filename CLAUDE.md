# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Reads the [WorldPop API v3](https://api.worldpop.org/v3/) country by country and creates per-country
population and age/sex datasets (with showcases) in HDX. Resources are URLs to WorldPop files, so no
files are stored. Dataset text comes from the "WorldPop Dataset Metadata" Google Sheet. Runs yearly.

Extra inputs besides the HDX config: `GSHEET_AUTH` (Google service account JSON, or `--gsheet-auth`)
and `WORLDPOP_API_KEY` (or `--worldpop-api-key`; anonymous limits apply without it).

## Commands

Environment setup (Python 3.13, managed with `uv`):

```shell
uv sync
```

Run the pipeline (requires `~/.hdx_configuration.yaml` with an HDX key, and `~/.useragents.yaml`
with a `hdx-scraper-worldpop` entry — see README.md):

```shell
uv run python -m hdx.scraper.worldpop
```

Run all tests (with coverage, configured in `pyproject.toml`):

```shell
uv run pytest
```

Lint and format:

```shell
uv run ruff check
uv run ruff format
```

Pre-commit (runs ruff on every commit once installed):

```shell
pre-commit install
pre-commit run --all-files
```

Build:

```shell
uv build
```

Adding a dependency: add it to `project.dependencies` in `pyproject.toml` (or `[dependency-groups]`
for test-only deps), then run `uv lock --upgrade` to refresh `uv.lock` (pre-commit also does this
automatically on commit).

## Architecture

- `__main__.py`: entry point. Checks WorldPop organisation access, sets up rate-limited downloaders,
  loops over countries and calls `create_in_hdx` for each dataset and showcase.
- `pipeline.py`: `Pipeline` fetches the country list and file metadata from the API and builds
  datasets and showcases per country.
- `dataset_generator.py`: `DatasetGenerator` builds one dataset and showcase, filling description,
  licence and citation placeholders from the sheet metadata and API.
- `sheet.py`: `Sheet` reads the metadata Google Sheet via `gspread`.
- `config/`: `project_configuration.yaml` and `hdx_dataset_static.yaml`.

## Code Style

- Formatted with `ruff` via pre-commit hooks. After changing any Python code, run:

```bash
pre-commit run --all-files
```

- Python ≥ 3.13

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag
  tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions,
  external systems). Search or read the actual source first. If you lack verified information, say
  so rather than speculate.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific
issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements
in the same commit unless they are directly required by the fix. Unrelated changes obscure the
intent of the fix and complicate review and blame.

## Decision Records

Non-trivial design decisions are recorded in `docs/decisions/` (see `docs/decisions/README.md`) —
the distilled decision, not the full planning narrative, belongs here.
