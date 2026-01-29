# AGENTS guidelines

## Repository map
- Code lives in `src/yads/`; tests mirror modules under `tests/` with `test_<behavior>` names. Specs sit in `spec/`; docs and executable examples live in `docs/src/`. CI helpers are under `ci/`.
- Do not edit generated doc blocks directly; example sources are in `docs/src/examples/` and are injected into Markdown via markers.

## Environment and tooling
- Use Python 3.10+ with `uv`; never call `python`/`pip` directly. Install dev deps and hooks with `make install` (frozen lock) or `make install-all` (all extras).
- Run commands through `make` or `uv run --group dev <cmd>`; `uv sync --group dev` updates the env, `uv sync --all-groups` pulls every optional extra when needed.
- Typical cycle: `make lint`, `make format`, `make test`, `make test-cov`, `make build`. `make help` lists all targets.
- Use `uvx ruff check src/ tests/ ci/` and `uvx ruff format src/ tests/ ci/` (wired into `make lint/format`). Type check locally with `uv run --group dev pyright src/`.

## Coding standards
- Follow PEP 8 with a 90-character limit. Maintain strict typing (Pyright `strict`); prefer `TypedDict`, `Protocol`, and dataclasses over raw dicts. Fix all type errors and missing imports.
- Add Google-style docstrings for any user-facing function/class/module; keep internal helpers self-documenting. Use explicit imports for `spec`/`types` to avoid collisions.
- When adding code snipets as examples in Google-style docstrings, use the code in backticks so they are renderd as code blocks.
- Keep code deterministic and semantics-preserving per the design philosophy; surface explicit warnings/errors for lossy conversions.

## Testing expectations
- Add or update tests alongside code changes. Use `make test` for the suite and `make test-cov` when touching schema logic or converters.
- Optional dependency matrices: run `make test-dependency DEP=<name> VER=<x.y.z>` (or `make test-dependency-all`) for compatibility changes. Run `make test-integration DIALECT=<spark|duckdb>` (or `make test-integration-all`) when touching SQL converters.
- Clean artifacts with `make clean` (or `make clean-all`) if needed.

## Documentation and examples
- Public behavior changes require docs updates in `docs/` or `spec/`; leave `README.md` alone unless explicitly asked.
- Executable examples live in `docs/src/examples/` as `ExampleDefinition` objects. Markdown uses `<!-- BEGIN:example <id> <slug> -->` / `<!-- END:... -->` blocks; treat them as generated.
- Refresh snippets with `make sync-examples FILE=<path>` or `make sync-examples-all`. Do not hand-edit generated code/output blocks; adjust example definitions instead.

## Spec validation and data contracts
- Validate specs before merging: `uv run --group dev check-jsonschema --schema spec/yads_spec_latest.json <file>`.
- Keep optional dependency support aligned with `pyproject.toml` version ranges; add new groups via `uv add` and update CI matrices/configs when extending compatibility.

## Git and PR flow
- Branch from `main` (e.g., `feature/<topic>` or `fix/<issue>`); rebase frequently and avoid merge commits. One focused change per branch.
- Use Conventional Commit-style PR titles/messages (`feat:`, `fix:`, `docs:`). Run lint/tests before pushing; ensure CI parity with the commands above.
- Do not commit secrets or credentials. Keep TODOs tied to an issue reference.

## CI signals
- CI runs pytest across Python 3.10–3.14; Pyright runs on 3.10. Dependency and integration workflows use Docker and matrices from `ci/dependency-tests/versions.json` and `ci/integration-tests/config.json`. Match local runs to these when relevant.
