# Repository Guidelines

## Project Structure & Module Organization
- `src/`: Core modules (e.g., `src/migration/`, `src/utils/`).
- `scripts/`: Executables and entry points (e.g., `scripts/run_migration.py`).
- `tests/`: Unit/integration tests mirroring `src/` layout.
- `config/`: Configuration files (`.env.example`, YAML/JSON templates).
- `data/` and `logs/`: Optional inputs/outputs, backups, and run logs.

## Build, Test, and Development Commands
- Create env: `python -m venv .venv && source .venv/bin/activate`.
- Install deps: `pip install -r requirements.txt -r requirements-dev.txt`.
- Run locally: `python scripts/run_migration.py --dry-run` (safe preview).
- Lint/format: `ruff check . && black .`.
- Tests: `pytest -q` or `pytest --cov=src --cov-report=term-missing`.

## Coding Style & Naming Conventions
- Python 3.10+; 4-space indent; UTF-8; Unix line endings.
- Use type hints and docstrings for public functions.
- Naming: `snake_case` for functions/vars, `PascalCase` for classes, `UPPER_SNAKE` for constants, modules under `src/` are lowercase.
- Keep modules focused; avoid side effects at import time; prefer pure functions in `src/`, thin CLIs in `scripts/`.

## Testing Guidelines
- Framework: `pytest` with fixtures for I/O and external services.
- Location: tests in `tests/` mirroring `src/` (e.g., `tests/migration/test_runner.py`).
- Names: files `test_*.py`, functions `test_*` with clear Arrange/Act/Assert sections.
- Coverage: target ≥80%; include edge cases (empty inputs, idempotency, retries).
- Run selective: `pytest -k keyword -q` during development.

## Commit & Pull Request Guidelines
- Commits follow Conventional Commits: `feat:`, `fix:`, `chore:`, `refactor:`, `test:`, `docs:`.
- Keep commits small and scoped; include rationale in the body if non-obvious.
- PRs must include: purpose, approach, testing steps, sample logs/output (include `--dry-run` results), risk/rollback plan, and linked issues.
- Request review when CI/lint/tests pass and PR is rebased.

## Security & Configuration Tips
- Never commit secrets; use `.env` and provide `.env.example` with placeholders.
- Support `--dry-run`, `--backup-path`, and explicit confirmation flags for destructive actions.
- Log to `logs/` with timestamps; avoid printing secrets.
- Design migrations to be idempotent and resumable; snapshot/backup before changes.

