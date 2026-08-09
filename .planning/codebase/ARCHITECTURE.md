<!-- refreshed: 2026-08-09 -->
# Architecture

**Analysis Date:** 2026-08-09

## System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                     Entry Point / CLI                        │
│                       `src/main.py`                          │
└──────────────────────────────┬───────────────────────────────┘
                                │ wheeck.run()
                                ▼
┌─────────────────────────────────────────────────────────────┐
│              Recording Pipeline (PDF -> DB)                  │
│              `src/wheeck/recording_docs.py`                  │
├──────────────────────────┬────────────────────────────────────┤
│  PDF text extraction     │  Regex field extraction            │
│  (pypdfium2)              │  (constants.PATTERN_*)             │
└──────────────┬────────────┴─────────────┬──────────────────────┘
               │                           │
               ▼                           ▼
┌───────────────────────────┐   ┌──────────────────────────────┐
│   Geocoding / Distance     │   │   Similarity / validation    │
│   `src/geo/geo.py`          │   │   (difflib in recording_docs) │
│   (openrouteservice API)    │   │                              │
└──────────────┬───────────┘   └──────────────┬───────────────┘
               │                               │
               ▼                               ▼
┌─────────────────────────────────────────────────────────────┐
│          Data Access (external `core.Querier`, pyodbc)       │
│          Queries defined in `src/wheeck/constants.py`         │
└──────────────────────────────┬───────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────┐
│         PostgreSQL schema `wheeck` (`scheme/wheeck.sql`)      │
│  delivery / delivery_discard / delivery_warning / views       │
└──────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│        Reporting (Excel output)                              │
│        `src/wheeck/overview_docs.py` (openpyxl)               │
│        writes to `res/` from `scheme/overview.xlsx`,          │
│        `scheme/summary.xlsx` templates                        │
└─────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Entry point | Bootstraps logging and invokes the recording pipeline | `src/main.py` |
| Package facade | Re-exports the public API of the `wheeck` package | `src/wheeck/__init__.py` |
| Constants/config | Path constants, regex patterns, SQL query strings, Excel format map | `src/wheeck/constants.py` |
| Recording pipeline | Orchestrates document scanning, extraction, persistence, discard handling, backups | `src/wheeck/recording_docs.py` |
| Reporting | Generates/updates monthly overview and yearly summary Excel workbooks | `src/wheeck/overview_docs.py` |
| Geocoding | Wraps OpenRouteService client for address search and distance calculation | `src/geo/geo.py` |
| Geo package facade | Exposes `GeoMap` at package level | `src/geo/__init__.py` |
| Database schema | Defines tables, constraints, indexes and reporting views | `scheme/wheeck.sql` |
| External DB library | Provides `Querier` (pyodbc wrapper), `decode_json`, `get_logger` — NOT part of this repo | external `core` package (resolved via IDE/PYTHONPATH, no lockfile entry) |

## Pattern Overview

**Overall:** Batch-processing ETL/pipeline script, not a web service. A single `run()` entry point walks a working directory of PDF files, extracts structured data via regex, enriches it with a geocoding API call, persists to PostgreSQL, and finally renders Excel reports from database views.

**Key Characteristics:**
- Single-process, synchronous, sequential execution — no concurrency or async I/O.
- File-system-as-queue: work items are discovered by scanning `DDTs/` and consumed by renaming/moving files (`.recording.pdf` → `discarded/` or `recorded/`).
- Regex-driven extraction from PDF text layers is the core "business logic" (`src/wheeck/constants.py` `PATTERN_*`, applied in `src/wheeck/recording_docs.py::doc_scanner`).
- Persistence is entirely SQL-string based (no ORM); queries are defined as module-level constants and executed through an external `Querier` wrapper.
- Reporting layer reads back from SQL views/aggregates and writes into pre-built `.xlsx` templates (`scheme/overview.xlsx`, `scheme/summary.xlsx`) rather than generating spreadsheets from scratch.
- Configuration (API keys, vehicle/driver roster, backup directory) is externalized to JSON files under `config/`, loaded via `decode_json` (from the external `core` package).

## Layers

**Entry/orchestration layer:**
- Purpose: Process bootstrap, logger initialization, top-level exception handling.
- Location: `src/main.py`
- Contains: A single `if __name__ == '__main__':` guard calling `wheeck.run()`.
- Depends on: `wheeck` package, external `core.get_logger`.
- Used by: Invoked directly as the process entry point (e.g. `python src/main.py` or an equivalent scheduled task).

**Pipeline/domain layer:**
- Purpose: Document discovery, PDF text extraction, regex-based field parsing, duplicate/gap detection, discard handling, warning-message recording.
- Location: `src/wheeck/recording_docs.py`
- Contains: `Delivery` dataclass, `MessageGenre` enum, `doc_scanner`, `discard_doc`, `check_duplicate`, `check_gaps`, `check_similarity`, `save_warning`, `run`.
- Depends on: `src/wheeck/constants.py` (patterns/queries/paths), `src/geo/geo.py` (`GeoMap`), external `core.Querier`/`decode_json`/`get_logger`, `pypdfium2`.
- Used by: `src/main.py`, `src/wheeck/__init__.py` (re-exported).

**Reporting layer:**
- Purpose: Renders Excel overviews (monthly deliveries) and summaries (yearly vehicle trip gaps) from database query results.
- Location: `src/wheeck/overview_docs.py`
- Contains: `generate_overview`, `generate_summary`, `generate_current`.
- Depends on: `src/wheeck/constants.py`, external `core.Querier`/`decode_json`/`get_logger`, `openpyxl`.
- Used by: `src/wheeck/recording_docs.py::run` (calls `generate_current` after a batch), `src/wheeck/__init__.py` (re-exported).

**Integration layer:**
- Purpose: Wraps the OpenRouteService HTTP API for geocoding and driving-distance calculation.
- Location: `src/geo/geo.py`
- Contains: `GeoMap` class (`search`, `get_distance`, `get_distance_from_coords`).
- Depends on: `openrouteservice` package, external `core.decode_json`/`get_logger`.
- Used by: `src/wheeck/recording_docs.py::Delivery.calculate_distance`.

**Configuration/constants layer:**
- Purpose: Centralizes filesystem paths, regex patterns, SQL text, and Excel formatting maps as module-level constants (no classes/functions).
- Location: `src/wheeck/constants.py`
- Contains: `PATH_*`, `PATTERN_*`, `QUERY_*`, `OVERVIEW_*`, `CMD_BACKUP_DB`.
- Depends on: Nothing internal (only stdlib `datetime`, `pathlib`).
- Used by: Every other module in `src/wheeck/` and indirectly `src/geo/geo.py` (via `wheeck.PATH_LOG`).

**Data layer (external system):**
- Purpose: Durable storage for delivery records, discard/warning audit trail, and reporting views.
- Location: `scheme/wheeck.sql` (DDL), `scheme/wheeck.dump` (binary dump), `config/querier.json` (connection config, contents not read/quoted — treat as credentials).
- Contains: Tables `wheeck.delivery`, `wheeck.delivery_history`, `wheeck.delivery_discard`, `wheeck.delivery_warning`, and views `wheeck.vw_discard_message`, `wheeck.vw_gap_message`, `wheeck.vw_delivery_gap`.
- Depends on: PostgreSQL server (backup via `pg_dump`, see `CMD_BACKUP_DB` in `src/wheeck/constants.py`).
- Used by: All query execution in `src/wheeck/recording_docs.py` and `src/wheeck/overview_docs.py` through the external `core.Querier`.

## Data Flow

### Primary Request Path (document recording job)

1. `run()` lists files in `DDTs/` matching `PATTERN_WORKING_DOC`, one document at a time (`src/wheeck/recording_docs.py:445-477`).
2. Each matched file is renamed to a `.recording.pdf` working copy, then passed to `doc_scanner` (`src/wheeck/recording_docs.py:463-468`).
3. `doc_scanner` opens the PDF with `pypdfium2`, iterates pages, extracts text, and builds a `Delivery` dataclass per page using the `PATTERN_*` regexes from `src/wheeck/constants.py` (`src/wheeck/recording_docs.py:112-240`).
4. Vehicle/driver values are normalized against `config/wheeck.json` `members` list via `check_similarity` (fuzzy match with `difflib.SequenceMatcher`) (`src/wheeck/recording_docs.py:196-232`).
5. If the delivery city's distance is not already cached in the DB (`QUERY_GET_DISTANCE`), `Delivery.calculate_distance` calls `GeoMap.search`/`get_distance_from_coords` against the OpenRouteService API, then sleeps 1.5s to respect rate limits (`src/wheeck/recording_docs.py:62-72, 270-280`).
6. Valid records are inserted via `QUERY_INSERT_DELIVERY`; duplicate/invalid pages are written to `wheeck.delivery_discard` (`QUERY_INSERT_DISCARD`) with a warning message (`QUERY_INSERT_WARNING`) and the offending page is split into its own PDF under `DDTs/discarded/` by `discard_doc` (`src/wheeck/recording_docs.py:282-326, 329-355`).
7. After all documents in the batch are processed, `check_gaps` looks for missing `document_number` sequences via `QUERY_CHECK_GAPS` (backed by the `wheeck.vw_delivery_gap`/`wheeck.vw_gap_message` views) and records gap warnings (`src/wheeck/recording_docs.py:380-396`).
8. `generate_current` (from `src/wheeck/overview_docs.py`) regenerates Excel overviews/summaries for any year/month touched by today's recordings, then the pipeline shells out to `pg_dump` to refresh the schema backup (`src/wheeck/recording_docs.py:479-500`).

### Reporting Flow (Excel generation)

1. `generate_overview(year, month)` queries `wheeck.delivery` via `QUERY_GET_OVERVIEW`, opens the prior month's workbook (or the `scheme/overview.xlsx` template if none exists), writes rows into the `consegne` sheet, and refreshes calendar-day cells in the `cifre`/`litri`/`cifre manuale`/`litri manuale` sheets (`src/wheeck/overview_docs.py:15-59`).
2. `generate_summary(year)` queries yearly vehicle-trip pairs via `QUERY_GET_SUMMARY` (window functions partitioning by vehicle/delivery date) and writes them into `scheme/summary.xlsx`'s `viaggi` sheet (`src/wheeck/overview_docs.py:62-95`).
3. Output workbooks are written to `res/` (a symlink to an external directory outside the repo, excluded from git via `.gitignore`) (`src/wheeck/constants.py:7`, `.gitignore`).

**State Management:**
- No in-memory application state persists between runs; all durable state lives in PostgreSQL (`scheme/wheeck.sql`) and the filesystem (`DDTs/discarded`, `DDTs/recorded`, `res/`).
- Per-document/page state is held transiently in the `Delivery` dataclass and the anonymous `DiscardedPages` tracker object inside `doc_scanner` (`src/wheeck/recording_docs.py:117-125`).
- `Querier` instances are created and explicitly `del`eted per logical unit of work (per document, per validation check, per warning save) rather than being held open for the whole run — see repeated `querier = Querier(...); ...; del querier` blocks throughout `src/wheeck/recording_docs.py`.

## Key Abstractions

**`Delivery` dataclass:**
- Purpose: Represents all fields extracted from one PDF page, plus bookkeeping fields (`document_source`, `page_number`, `recording_date`, `id_warning_message`).
- Location: `src/wheeck/recording_docs.py:28-83`.
- Pattern: Fields are populated incrementally via regex matches and mutated in place as `doc_scanner` progresses; `charge()` bulk-assigns from a dict (used when restoring a previously-discarded record); `__str__` serializes to JSON for logging, normalizing `date`/`datetime`/`Decimal` types.

**`MessageGenre` enum:**
- Purpose: Defines the three warning message categories (`DISCARD`, `GAP`, `WARNING`) with their `%`-style format templates.
- Location: `src/wheeck/recording_docs.py:86-92`.
- Pattern: `save_warning(message_genre, **kwargs)` fills the enum's template string and persists it via `QUERY_INSERT_WARNING`.

**`GeoMap`:**
- Purpose: Encapsulates an `openrouteservice.Client` and exposes address search / distance calculation with consistent error handling (API, HTTP, timeout).
- Location: `src/geo/geo.py:12-103`.
- Pattern: Instantiated per-calculation (`GeoMap(PATH_CFG_PRJ)` in `Delivery.calculate_distance`) rather than shared/cached across the run.

**Constants-as-config module:**
- Purpose: `src/wheeck/constants.py` acts as the single source of truth for filesystem layout, regex contracts against the PDF text format, and all SQL text — no query building happens elsewhere.
- Examples: `PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`/`PATTERN_CITY_SX`, `QUERY_INSERT_DELIVERY`, `QUERY_GET_SUMMARY`.
- Pattern: Flat module-level constants, no namespacing/classes; every consumer imports the specific names it needs.

## Entry Points

**Process entry point:**
- Location: `src/main.py`
- Triggers: Manual or scheduled execution of the Python interpreter against this file (`python src/main.py`); no CLI args are parsed.
- Responsibilities: Initialize the logger against `wheeck.PATH_LOG`, call `wheeck.run()`, and log any unhandled exception via `logger.exception` (process does not re-raise, so a crash is logged but exits silently as far as calling infrastructure is concerned).

**Package-level API (`src/wheeck/__init__.py`):**
- Re-exports `PATH_CFG`, `PATH_LOG`, `PATH_RES`, `PATH_SCHEME`, `generate_current`, `generate_overview`, `generate_summary`, `check_duplicate`, `check_gaps`, `check_similarity`, `discard_doc`, `doc_scanner`, `run`, `save_warning`.
- This is effectively the public surface for any external caller (e.g. `src/main.py`, or an interactive session) instead of importing submodules directly.

## Architectural Constraints

- **Threading:** Single-threaded, fully synchronous. The 1.5s `time.sleep` in `doc_scanner` (`src/wheeck/recording_docs.py:280`) is a hard rate-limit throttle for the OpenRouteService free tier (40 calls/min) and blocks the whole pipeline while waiting.
- **Global state:** No module-level singletons for DB/HTTP clients; `Querier` and `GeoMap` are constructed and torn down per call site. `logger` objects are module-level (`src/wheeck/recording_docs.py:25`, `src/wheeck/overview_docs.py:12`, `src/geo/geo.py:9`) but stateless beyond the underlying logging handlers configured by the external `core.get_logger`.
- **External, unvendored dependency:** `core` (providing `Querier`, `decode_json`, `get_logger`) is imported in `src/geo/geo.py`, `src/main.py`, `src/wheeck/overview_docs.py`, and `src/wheeck/recording_docs.py` but does not exist anywhere in this repository, is absent from `requirements.txt`, and has no matching entry in `.venv/Lib/site-packages`. It must be resolvable on `PYTHONPATH`/IDE SDK path from outside this repo for the code to run; any architectural change to logging or DB access must be coordinated with that external package.
- **Filesystem-as-state:** The pipeline mutates the working directory as it runs (moves/renames files between `DDTs/`, `DDTs/recorded/`, `DDTs/discarded/`); re-running after a crash mid-batch requires manual inspection of file suffixes (`.recording.pdf`) to determine where processing stopped.
- **Windows path assumptions:** `res` is a symlink (`res -> /c/Users/Alex/MEGA/AGM/consegne/overviews`) and `CMD_BACKUP_DB` invokes `pg_dump` via `subprocess.run(..., shell=True)`, both tying execution to the current Windows/Git-Bash environment described in `.idea/wheeck.iml`.

## Anti-Patterns

### Bare `Exception` catch at the top level

**What happens:** `src/main.py` wraps the entire `wheeck.run()` call in `try: ... except Exception: logger.exception(...)`.
**Why it's wrong:** Any failure — including programming errors, unexpected data shapes, or DB connectivity loss mid-batch — is swallowed after logging; the process exits 0-like with no signal to a scheduler/monitor that the job failed.
**Do this instead:** Re-raise (or `sys.exit(1)`) after logging so calling infrastructure (cron, Task Scheduler, CI) can detect failure; reserve broad catches for a narrower boundary if a "log and continue" is truly intended.

### Manual resource lifecycle via `del querier`

**What happens:** Throughout `src/wheeck/recording_docs.py` (e.g. lines 107/325, 367/377, 387/395, 434/441) a `Querier` is constructed and explicitly deleted with `del querier` instead of using a context manager.
**Why it's wrong:** `del` only decrements a reference count; it does not guarantee the connection is closed if an exception occurs between construction and `del`, or if `Querier` doesn't implement `__del__`-based cleanup. Errors inside the `try`-less blocks leave connections however the external `core.Querier` implementation happens to leave them.
**Do this instead:** Use `with Querier(...) as querier:` (if the external library supports the context-manager protocol) so cleanup happens deterministically even on exception.

## Error Handling

**Strategy:** Defensive, log-and-continue at the field level (regex misses mark a page for discard rather than crashing), combined with a single unguarded top-level catch in `src/main.py`.

**Patterns:**
- Regex extraction failures accumulate into `discarded_pages.on_error` as a comma-joined string of failed pattern names rather than raising (`src/wheeck/recording_docs.py:146-151, 172-175, 184-187, 236-239`).
- External API failures in `GeoMap` are caught by specific `openrouteservice.exceptions` types (`ApiError`, `HTTPError`, `Timeout`), logged with context, and converted to a `None` return rather than propagated (`src/geo/geo.py:44-52, 95-102`).
- DB write failures are detected by checking `querier.run(...).rows != 1` and logged at `critical` level, but execution continues to the next step rather than aborting the batch (`src/wheeck/recording_docs.py:285-293, 307-308, 320-322`).
- The `pg_dump` backup step catches only `subprocess.CalledProcessError` and logs the stderr; it does not fail the overall `run()` (`src/wheeck/recording_docs.py:488-500`).

## Cross-Cutting Concerns

**Logging:** Every module obtains a module-scoped logger via the external `core.get_logger(PATH_LOG, __name__)` pattern (`src/wheeck/recording_docs.py:25`, `src/wheeck/overview_docs.py:12`, `src/geo/geo.py:9`); log files are written under `log/` (see dated files like `log/2026_07_05.log`), one implied per invocation/day based on filenames observed in `log/`.
**Validation:** Purely regex- and lookup-table-based (against `config/wheeck.json` `members`); no schema/type validation library is used. Data integrity is additionally enforced at the database layer via `CHECK`/`UNIQUE` constraints in `scheme/wheeck.sql`.
**Authentication:** Not applicable to the pipeline itself (no HTTP server); the only auth concern is the OpenRouteService API key and the Postgres connection string, both stored in `config/*.json` files (`config/wheeck.json`, `config/querier.json`) and never logged or hard-coded in source.

---

*Architecture analysis: 2026-08-09*
