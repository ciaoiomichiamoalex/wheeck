# Project State

**Project:** wheeck — PDF delivery-note (DDT) recording pipeline and Excel reporting
**Last activity:** 2026-08-09 - Completed quick task 260809-plo: transaction management with rollback

## Current Status

Existing codebase, mapped on 2026-08-09 into `.planning/codebase/`. No GSD roadmap — this project
uses GSD in quick-task mode only.

## Stack

Python 3.14, pypdfium2 (PDF text extraction), openpyxl (Excel reporting), pyodbc via the external
`core.Querier` wrapper, openrouteservice (geocoding), PostgreSQL (schema `wheeck`).

The `core` package (`Querier`, `decode_json`, `get_logger`) is external and unvendored — it lives at
`C:/source/core` and is resolved through `PYTHONPATH`/IDE SDK path.

### Blockers/Concerns

- `run()`'s post-commit steps (backup copy and the move into `DDTs/recorded/`, `recording_docs.py:634-639`)
  are still outside any guard: an `OSError` there aborts the whole batch after the document was committed
  and leaves it as `<name>.recording.pdf`, which `PATTERN_WORKING_DOC` never matches again. Pre-existing;
  the `.failed.pdf` idiom added for the rollback restore would close it.

- No test suite exists in the repository (see `.planning/codebase/TESTING.md`).
- `src/main.py` swallows every exception via a bare `except Exception`, so job failures are logged
  but never surfaced to the scheduler.

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260809-plo | Transaction management: rollback on mid-document error, commit when all pages succeed | 2026-08-09 | 840eca4 | Needs Review | [260809-plo-aggiungi-gestione-transazione-con-rollba](./quick/260809-plo-aggiungi-gestione-transazione-con-rollba/) |
