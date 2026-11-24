<!-- generated-by: gsd-doc-writer -->
<!-- refreshed: 2026-11-22 -->
# Architecture

**Analysis Date:** 2026-11-22

## System Overview

wheeck is a single-process batch ETL application that records deliveries from Italian transport documents (DDT, *documento di trasporto*). Its inputs are the DDT PDFs queued in `DDTs/` and the JSON configuration in `config/*.local.json`. Each run extracts the text of every page with `pypdfium2` and parses it with regular expressions. It resolves driving distances through OpenRouteService and writes the deliveries, discards and warnings to the PostgreSQL database `wheeck` (schema `public`) with one transaction per document. Its outputs are the database rows, single-page discard PDFs in `DDTs/discarded/`, monthly and yearly Excel workbooks in `res/`, and a `pg_dump` backup in `scheme/`. The code is layered: a thin entry script, the procedural application package `wheeck`, and two small infrastructure packages, `core` (database and logging) and `geo` (OpenRouteService).

## Component Diagram

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                         Entry point (batch run)                          │
│                    `src/main.py` -> `wheeck.run()`                       │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Application package `wheeck`                        │
├──────────────────────────────┬──────────────────────┬───────────────────┤
│  Recording pipeline          │  Excel reporting     │  Constants        │
│  `src/wheeck/                │  `src/wheeck/        │  (paths, regex,   │
│   recording_docs.py`         │   overview_docs.py`  │   SQL text)       │
│  run / doc_scanner /         │  generate_current /  │  `src/wheeck/     │
│  resolve_distances / ...     │  overview / summary  │   constants.py`   │
└──────────┬──────────┬────────┴──────────┬───────────┴───────────────────┘
           │          │                   │
           ▼          ▼                   ▼
┌────────────────────────┐  ┌──────────────────────────────────────────────┐
│ Geo wrapper `geo`      │  │ Shared infrastructure `core`                 │
│ `src/geo/geo.py`       │  │ `src/core/querier.py` (pyodbc Querier)       │
│ GeoMap (ORS client)    │  │ `src/core/common.py` (decode_json, logger)   │
└──────────┬─────────────┘  └───────────────────┬──────────────────────────┘
           │                                    │
           ▼                                    ▼
┌────────────────────────┐  ┌──────────────────────────────────────────────┐
│ OpenRouteService API   │  │ PostgreSQL database `wheeck` (via ODBC)      │
│ (geocode + directions) │  │ `sql/` scripts: tables, views, trigger       │
└────────────────────────┘  └──────────────────────────────────────────────┘

Filesystem side effects: `DDTs/` (input queue, discarded/, recorded/), `res/` (Excel
output), `scheme/` (Excel templates + pg_dump backups), `log/` (daily log files).
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| Entry script | Creates the `main` logger, calls `wheeck.run()`, logs any unhandled exception | `src/main.py` |
| Batch orchestrator `run()` | Walks `DDTs/`, renames each matching PDF to `.recording.pdf`, scans it, archives or requeues it, then runs gap check, Excel refresh and DB backup | `src/wheeck/recording_docs.py:593` |
| Document scanner `doc_scanner()` | Extracts page text, resolves distances, opens one DB transaction per document, parses every page with regexes, inserts deliveries/discards/warnings, commits or rolls back | `src/wheeck/recording_docs.py:159` |
| Distance resolver `resolve_distances()` | Collects distinct delivery cities, reuses distances already stored in `public.delivery`, otherwise calls OpenRouteService with a 1.5 s throttle | `src/wheeck/recording_docs.py:119` |
| Domain model `Delivery` | Dataclass holding the fields extracted from one PDF page; field order matches the INSERT column order | `src/wheeck/recording_docs.py:30` |
| Warning messages `MessageGenre` | Enum of message templates (`DISCARD`, `GAP`, `WARNING`) written to `public.delivery_warning` | `src/wheeck/recording_docs.py:81` |
| Helpers | `discard_doc`, `discarded_doc_path`, `check_duplicate`, `check_gaps`, `check_similarity`, `save_warning`, `city_distance` | `src/wheeck/recording_docs.py:103-590` |
| Excel reporting | Monthly overview (`res/YYYY_MM.xlsx`) and yearly trip summary (`res/YYYY_TRIPS.xlsx`) filled from SQL into templates in `scheme/` | `src/wheeck/overview_docs.py` |
| Constants | Project paths, config paths, filename and text regexes, `pg_dump` command, Excel number formats, every SQL statement | `src/wheeck/constants.py` |
| DB access | `Querier` (pyodbc, chained `run().fetch()`, commit/rollback, Excel export) and `LowQuerier` (sqlite3 variant) | `src/core/querier.py` |
| Common utilities | `decode_json` (JSON file filter), `get_logger` (daily file + console logger) | `src/core/common.py` |
| Geo wrapper | `GeoMap.search`, `get_distance`, `get_distance_from_coords` over the `openrouteservice` client | `src/geo/geo.py` |
| Database DDL | Tables, views, trigger function, role | `sql/` |

## Pattern Overview

**Overall:** Single-process batch ETL script (PDF -> regex extraction -> PostgreSQL -> Excel reports), organised as a procedural application package on top of two small infrastructure packages.

**Key Characteristics:**
- Procedural module functions, no service classes or dependency injection. The only classes are the `Delivery` dataclass, the `MessageGenre` enum, `RecordingError`, `Querier`/`LowQuerier` and `GeoMap`.
- One database transaction per PDF document (`doc_scanner`), with compensating filesystem cleanup on rollback.
- The filesystem acts as the job queue: a document's state is encoded in its filename and folder.
- All SQL lives as string constants in `src/wheeck/constants.py`; the Python code never builds SQL inline except the `IN (%s)` placeholder expansion in `generate_summary` (`src/wheeck/overview_docs.py:79`).
- Business logic is split between Python and the database: gap detection, discard-message parsing and discard-status sync live in `sql/views/` and `sql/triggers/`.

## Layers

**Entry layer:**
- Purpose: Start the batch run and catch any unhandled exception.
- Location: `src/main.py`
- Contains: Logger setup and a single `wheeck.run()` call.
- Depends on: `wheeck`, `core`
- Used by: Manual run as a script (`python src/main.py`). Running the script puts `src/` on `sys.path`, from any working directory.

**Application layer (`wheeck`):**
- Purpose: DDT recording pipeline, gap checking, Excel report generation.
- Location: `src/wheeck/`
- Contains: `recording_docs.py` (pipeline), `overview_docs.py` (Excel), `constants.py` (paths, regex, SQL), `__init__.py` (public re-exports).
- Depends on: `core` (`Querier`, `decode_json`, `get_logger`), `geo` (`GeoMap`), `pypdfium2`, `openpyxl`.
- Used by: `src/main.py`.

**Infrastructure layer (`core`, `geo`):**
- Purpose: Reusable wrappers around pyodbc/sqlite3, JSON config reading, logging, and the OpenRouteService client.
- Location: `src/core/`, `src/geo/`
- Contains: `Querier`, `LowQuerier`, `decode_json`, `get_logger`, `GeoMap`.
- Depends on: `pyodbc`, `sqlite3`, `openpyxl`, `openrouteservice`. `core` has no project imports. `geo` imports `wheeck.PATH_LOG` (see Architectural Constraints).
- Used by: `wheeck`.

**Persistence layer (PostgreSQL database `wheeck`, schema `public`):**
- Purpose: Store deliveries, discards, warnings and historical deliveries; derive gaps and parsed messages through views.
- Location: `sql/` (installed by hand with the psql `\ir` meta-command, see the README)
- Contains: tables `delivery`, `delivery_history`, `delivery_warning`, `delivery_discard`; views `vw_discard_message`, `vw_gap_message`, `vw_delivery_gap`; function `sync_discard_status` and trigger `trg_sync_discard_status`; role script `sql/roles/wheeck.sql`.
- Depends on: PostgreSQL.
- Used by: SQL constants in `src/wheeck/constants.py` and `CMD_BACKUP_DB` (`pg_dump`).

## Data Flow

### Primary Request Path (one batch run)

1. `src/main.py:4` creates the `main` logger; `src/main.py:7` calls `wheeck.run()`.
2. `run()` lists files in `DDTs/` (`src/wheeck/recording_docs.py:598`) and skips names that do not match `PATTERN_WORKING_DOC` (`src/wheeck/recording_docs.py:604`).
3. Each matching file `X.pdf` is renamed to `X.recording.pdf` (`src/wheeck/recording_docs.py:611-612`).
4. `doc_scanner()` extracts the text of every page with `pypdfium2` and closes the PDF handle (`src/wheeck/recording_docs.py:179-182`).
5. `resolve_distances()` runs before any transaction: distinct cities are looked up with `QUERY_GET_DISTANCE`, and missing ones are computed through `city_distance()` -> `GeoMap` with a 1.5 s sleep (`src/wheeck/recording_docs.py:143-153`).
6. A `Querier` with auto-commit off opens the document transaction (`src/wheeck/recording_docs.py:190`).
7. For every page: regexes `PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`/`PATTERN_CITY_SX`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE` fill a `Delivery` (`src/wheeck/recording_docs.py:215-316`). Vehicle and driver are normalised against `members` in `config/wheeck.local.json` with `check_similarity()` (`SequenceMatcher`, >0.5 ratio). Unknown values write a `WARNING` message.
8. `check_duplicate()` runs `QUERY_CHECK_DUPLICATE`; the distance comes from the pre-resolved map (`src/wheeck/recording_docs.py:342-354`).
9. A clean page inserts into `public.delivery` (`src/wheeck/recording_docs.py:359`). A page that fails a regex writes a `DISCARD` message plus a `public.delivery_discard` row, and extracts the page to `DDTs/discarded/X_Pnnn.pdf` (`src/wheeck/recording_docs.py:368-405`). A duplicate page (`CHECK_DUPLICATE`, `id_warning_message = -1`) gets only the discard PDF, with no warning and no `delivery_discard` row (`src/wheeck/recording_docs.py:342-346`).
10. Open gap warnings matching the page's document number are closed with `QUERY_UPDATE_WARNING` (`src/wheeck/recording_docs.py:409-416`).
11. Commit is the last statement of the `try` (`src/wheeck/recording_docs.py:420`). Any exception rolls back, deletes the discard PDFs created by this run, and re-raises (`src/wheeck/recording_docs.py:423-435`).
12. On success `run()` copies the file to `backup_dir` (if configured) and moves it to `DDTs/recorded/X.recorded.pdf` (`src/wheeck/recording_docs.py:634-639`). On failure it restores `X.pdf`, or renames to `X.failed.pdf` if the restore fails (`src/wheeck/recording_docs.py:617-630`).
13. After at least one document, `run()` calls `check_gaps()`, `generate_current()` and the `pg_dump` backup into `scheme/wheeck.dump` (`src/wheeck/recording_docs.py:641-662`).

### Discard Re-entry Flow

1. `run()` scans only `DDTs/`, not `DDTs/discarded/`. A discarded single-page PDF `X_Pnnn.pdf` is processed again only when it is placed back in `DDTs/`.
2. `PATTERN_WORKING_DOC` accepts the `_Pnnn` suffix; `PATTERN_DISCARD_DOC` recognises it as a discard document (`src/wheeck/recording_docs.py:319`).
3. If the page still fails parsing, `QUERY_GET_DISCARD` loads the open (`status IS TRUE`) discard row. The row is used only if none of its selected columns is NULL, so the missing values must be filled in `public.delivery_discard` first. `Delivery.charge()` then copies the values and the page is treated as clean (`src/wheeck/recording_docs.py:328-339`). With NULLs left, the page stays discarded and keeps its existing warning id.
4. The delivery is inserted and `QUERY_UPDATE_WARNING` sets the `DISCARD` warning `status = FALSE` (`src/wheeck/recording_docs.py:363`).
5. Trigger `trg_sync_discard_status` (`sql/triggers/trg_sync_discard_status.sql`) runs `public.sync_discard_status()` (`sql/functions/sync_discard_status.sql`), which copies the status to `delivery_discard`.
6. Discard documents are never copied to `backup_dir` (`src/wheeck/recording_docs.py:634`).

### Gap Detection Flow

1. `vw_delivery_gap` (`sql/views/vw_delivery_gap.sql`) generates every document number between MIN and MAX per year over `delivery UNION ALL delivery_history`, and returns the missing ones with an `is_discard` flag joined from `vw_discard_message`.
2. `check_gaps()` runs `QUERY_CHECK_GAPS` (gaps that are not discards and have no open `GAP` message) and writes one `GAP` warning per row (`src/wheeck/recording_docs.py:518-534`).
3. A later document filling the gap closes the warning through `QUERY_GET_GAP` + `QUERY_UPDATE_WARNING` (step 10 of the primary path).

### Excel Report Flow

1. `generate_current()` selects the (year, month) pairs recorded in the last 24 h with `QUERY_GET_LAST_OVERVIEWS` (`src/wheeck/overview_docs.py:98-110`).
2. `generate_overview(year, month)` loads `res/YYYY_MM.xlsx`, or the template `scheme/overview.xlsx` if the file does not exist yet (the templates are gitignored, see Directory Structure Rationale). It fills sheet `consegne` and writes the month's dates into sheets `cifre`, `litri`, `cifre manuale`, `litri manuale` (`src/wheeck/overview_docs.py:15-59`).
3. `generate_summary(year)` always starts from `scheme/summary.xlsx`. It fills sheet `viaggi` with the longest trip per day for each vehicle, side by side. `QUERY_GET_SUMMARY` takes exactly two vehicle parameters, so `members` must list exactly two vehicles. Deliveries by members listed without a vehicle are excluded (`src/wheeck/overview_docs.py:62-95`).

**State Management:**
- Persistent state lives in PostgreSQL (`wheeck` database) and in the filenames and folders under `DDTs/`.
- In-process state: the `Delivery` instance per page, an anonymous `DiscardedPages` counter object per document (`src/wheeck/recording_docs.py:194-202`), and the `discarded_docs` list used for rollback cleanup.
- Config is re-read from disk with `decode_json(PATH_CFG_PRJ)` on every use (per page in the vehicle check, per city in `city_distance`). Only the DB config is cached (`Querier._cache_config`).

## Key Abstractions

**Querier (DB session):**
- Purpose: One pyodbc connection plus cursor, with chained `run(query, *args).fetch(genre)`, `rows` holding the last `rowcount`, `save_changes(True/False)` for commit/rollback, and `save_excel()` export.
- Examples: `src/core/querier.py:14`; used in `src/wheeck/recording_docs.py:143,190,504,525,579` and `src/wheeck/overview_docs.py:25,69,102`.
- Pattern: Thin wrapper. Connection config comes from `config/querier.local.json` (entry `name == "main"`). There is no `close()`: release a connection with `del querier`, which triggers `__del__`. Control flow branches on `.rows` after SELECTs (for example `if querier.run(QUERY_GET_DISTANCE, city).rows == 1`), which relies on the ODBC driver returning a row count for SELECT.

**Shared vs private querier:**
- Purpose: Helpers take an optional `querier`. With a shared querier they run inside the document transaction. Without one they open and close a private connection. `save_warning()` is the only helper with a failure path: with a shared querier it raises `RecordingError`, and with a private auto-commit connection it returns the `-1` sentinel. `check_duplicate()` returns a bool in both modes.
- Examples: `check_duplicate()` (`src/wheeck/recording_docs.py:490`), `save_warning()` (`src/wheeck/recording_docs.py:561`).
- Pattern: `own_querier = querier is None` then `if own_querier: del querier` at the end.

**Delivery dataclass:**
- Purpose: The record extracted from one page.
- Examples: `src/wheeck/recording_docs.py:30-78`
- Pattern: Field order is a contract. `astuple(delivery)[:-1]` feeds `QUERY_INSERT_DELIVERY` (13 columns) and `astuple(delivery)` feeds `QUERY_INSERT_DISCARD` (14 columns, `id_warning_message` last). Add new fields only together with matching column changes in both queries.

**MessageGenre messages:**
- Purpose: Human-readable warning text that is also machine-parsed.
- Examples: `src/wheeck/recording_docs.py:81-93`, parsed by `sql/views/vw_discard_message.sql` and `sql/views/vw_gap_message.sql`.
- Pattern: `%`-format templates filled by `save_warning(genre, **kwargs)`.

**Filename state machine:**
- Purpose: Track document processing state without a DB table.
- States: `DDTs/YYYY_MM_DDT_NNNN_NNNN.pdf` (queued) -> `DDTs/...recording.pdf` (in progress) -> `DDTs/recorded/...recorded.pdf` (done). Failure restores `DDTs/....pdf` or, as a last resort, `DDTs/....failed.pdf`. Discarded pages go to `DDTs/discarded/..._Pnnn.pdf`.
- Pattern: Regexes `PATTERN_WORKING_DOC` and `PATTERN_DISCARD_DOC` in `src/wheeck/constants.py:18-19`.

**GeoMap:**
- Purpose: Geocode an address and compute driving distance in km.
- Examples: `src/geo/geo.py:12`
- Pattern: Returns `None` on any `openrouteservice` API, HTTP or timeout error and logs it. Callers treat `None` as an unknown distance (nullable `distance` column).

## Directory Structure Rationale

The code is split by responsibility: `src/core/` and `src/geo/` are reusable wrappers with no business logic, and `src/wheeck/` holds everything specific to DDT recording. Runtime data lives in top-level folders next to `src/`, resolved from `PATH_PRJ` in `src/wheeck/constants.py`. See `docs/STRUCTURE.md` for the file-level layout.

```text
wheeck/
├── src/
│   ├── main.py        # Entry script for the batch run
│   ├── wheeck/        # Application package: recording pipeline, Excel reports, constants and SQL
│   ├── core/          # Infrastructure: Querier (pyodbc), LowQuerier (sqlite3), decode_json, get_logger
│   └── geo/           # Infrastructure: GeoMap over the openrouteservice client
├── sql/               # PostgreSQL DDL scripts, one file per object, run in order with psql \ir
├── config/            # Committed JSON templates; the real *.local.json copies are gitignored
├── scheme/            # Excel templates (overview.xlsx, summary.xlsx) and pg_dump backups
├── DDTs/              # Input queue, with discarded/ and recorded/ subfolders
├── res/               # Generated Excel workbooks
└── log/               # Daily log files
```

- `sql/` keeps one file per table, view, function, trigger and role, so each object can be reviewed and re-applied on its own. The README lists the creation order as psql `\ir` lines. The repository tracks no install script.
- `config/` separates committed templates from gitignored real values, so credentials and the OpenRouteService key stay out of the repository.
- `DDTs/`, `res/` and `log/` are gitignored. So are `*.xlsx` and `*.dump`, which means the templates in `scheme/` are not in the repository and must be supplied locally.
- The code never creates folders. `DDTs/discarded/`, `DDTs/recorded/`, `res/` and `log/` must exist before the first run. If `log/` is missing, `get_logger` writes to a file named `log` in the project root instead.

## Entry Points

**Batch run:**
- Location: `src/main.py`
- Triggers: Manual run as a script (`python src/main.py`; the PyCharm run config `main` uses working directory `src/`). Running the script puts `src/` on `sys.path`, so `core`, `geo` and `wheeck` resolve as top-level packages. No scheduler config is committed.
- Responsibilities: Process every queued DDT, then gap check, Excel refresh and DB backup.

**Library functions (manual / interactive use):**
- Location: `src/wheeck/__init__.py`
- Triggers: Import from a Python shell started in `src/`.
- Responsibilities: Re-exports `generate_overview`, `generate_summary`, `generate_current`, `check_gaps`, `check_duplicate`, `check_similarity`, `discard_doc`, `doc_scanner`, `save_warning`, `run`, `RecordingError` and the path constants, for regenerating a report or re-running a step by hand.

**Database install:**
- Location: the `\ir` sequence in the README (Installation, step 3). The repository tracks no install script.
- Triggers: run by hand at a `psql` prompt started from the repository root.
- Responsibilities: Creates tables, function, trigger, role and views in schema `public` of the database `wheeck`, in dependency order. The database must exist first: no script creates it.

## Architectural Constraints

- **Threading:** Single-threaded, synchronous. Documents are processed one at a time; OpenRouteService calls are throttled with `time.sleep(1.5)` (40 calls/min limit).
- **Transaction boundary:** One transaction per document. Keep network calls (geocoding) and file reads outside it. `resolve_distances()` exists because a long-open transaction hits PostgreSQL's `idle_in_transaction_session_timeout`. The commit must stay the last statement inside the `try` of `doc_scanner`, and the `except` must re-raise. Otherwise `run()` archives a document that was not recorded.
- **Filesystem is not transactional:** Every file written inside the transaction must be registered for cleanup before it is written (pattern: `discarded_docs.append(...)` before `discard_doc(...)`, `src/wheeck/recording_docs.py:398-402`).
- **Windows file locks:** PDF handles must be closed before `run()` renames files. `doc_scanner` and `discard_doc` close `pypdfium2` documents in `finally` blocks.
- **Global state:**
  - Module-level loggers: `get_logger(PATH_LOG, __name__)` runs at import in `src/wheeck/recording_docs.py:26`, `src/wheeck/overview_docs.py:12` and `src/geo/geo.py:9`, and creates `log/YYYY_MM_DD.log` as a side effect of importing.
  - `Querier._cache_config` is a class-level dict cache of DB configs (`src/core/querier.py:19`).
  - Default arguments are evaluated once at import: `doc_scanner(job_begin=datetime.now())` and `generate_overview(year=date.today().year, month=date.today().month)`. Always pass these explicitly from long-lived sessions.
- **Circular imports:** `wheeck/__init__.py` -> `recording_docs` -> `geo` -> `geo/geo.py` -> `from wheeck import PATH_LOG`. It works only because `src/wheeck/__init__.py:1` binds the constants before importing `recording_docs`. Running `import geo` before `import wheeck` fails with `ImportError: cannot import name 'GeoMap' from 'geo'`. Always import `wheeck` first, and keep the constants import on line 1 of `src/wheeck/__init__.py`.
- **Import root:** Packages are imported as top-level names (`from core import ...`), so `src/` must be on `sys.path`. Running `src/main.py` as a script does this. Interactive imports need `src/` as the working directory or on `PYTHONPATH`. There is no `pyproject.toml` or installable package. Project paths are computed from `src/wheeck/constants.py` (`Path(__file__).resolve().parents[2]`), so data folders resolve independently of the working directory.
- **Configuration files:** The real configs are `config/wheeck.local.json` and `config/querier.local.json` (gitignored). The committed `config/wheeck.json` and `config/querier.json` are templates with null values. The code reads only the `.local.json` files (`src/wheeck/constants.py:11-12`).
- **Backup command is separate from DB config:** `CMD_BACKUP_DB` (`src/wheeck/constants.py:27`) hardcodes `pg_dump -U postgres -h 127.0.0.1 -p 5432 -d wheeck` with `-w` (never prompt for a password). It needs non-interactive auth for `postgres` (`.pgpass`, `PGPASSWORD` or trust auth) and does not read `config/querier.local.json`.
- **Report assumptions:** `QUERY_GET_SUMMARY` takes exactly two vehicle parameters, and its `vehicle_driver NOT IN (%s)` list is built from `members` entries without a vehicle. `members` must list exactly two vehicles and at least one driver-only entry, or `generate_summary` sends invalid SQL (`NOT IN ()`) or the wrong number of parameters.
- **Unused code paths:** Nothing in `src/wheeck/` uses `LowQuerier` or `Querier.save_excel` (`src/core/querier.py`). No Python code writes `public.delivery_history`; only `sql/views/vw_delivery_gap.sql` reads it.

## Anti-Patterns

### Changing message templates without updating the parsing views

**What happens:** `MessageGenre.DISCARD` and `MessageGenre.GAP` (`src/wheeck/recording_docs.py:91-92`) produce text that `sql/views/vw_discard_message.sql` and `sql/views/vw_gap_message.sql` parse back with `STRPOS`/`SUBSTRING` on literal markers (`'[number: '`, `', genre: '`, `' of year '`, ...). `vw_delivery_gap` and `QUERY_CHECK_GAPS`/`QUERY_GET_GAP` depend on those views.
**Why it's wrong:** Any wording change silently breaks gap detection and discard matching. A `%d` instead of `%s` for a value that can be `None` raises `TypeError` inside the document transaction and rolls back the whole document.
**Do this instead:** Treat the template strings and the views as one contract. Change both in the same commit, keep `%(document_number)s` for nullable values, and keep `NULLIF(..., 'None')` in the view.

### Writing to the DB without the shared transaction querier

**What happens:** Helpers called with `querier=None` open a private connection. `save_warning()` in that mode uses auto-commit.
**Why it's wrong:** Inside `doc_scanner`, a private connection commits outside the document transaction, so a rollback leaves orphan warnings behind.
**Do this instead:** Inside `doc_scanner`, always pass `querier=querier` (see `save_warning(..., querier=querier, ...)` at `src/wheeck/recording_docs.py:287`). Use the private mode only from standalone steps such as `check_gaps()`.

### Checking write results without raising

**What happens:** Every INSERT or UPDATE inside the document transaction checks `.rows != 1`. Most branches raise `RecordingError`, but the `QUERY_UPDATE_WARNING` failures at `src/wheeck/recording_docs.py:363-365` and `414-416` only log `critical`.
**Why it's wrong:** Non-raising branches commit partial state.
**Do this instead:** For new writes in `doc_scanner`, follow `if querier.run(...).rows != 1: logger.critical(...); raise RecordingError('<STEP>')` (`src/wheeck/recording_docs.py:359-362`).

### Inline SQL or regex in functions

**What happens:** SQL text or regexes written directly inside functions. The codebase keeps them all in `src/wheeck/constants.py`; the only dynamic SQL is the `IN (%s)` placeholder expansion in `generate_summary`.
**Why it's wrong:** Scattered SQL and regexes cannot be reviewed in one place against `sql/` and real DDT text.
**Do this instead:** Add new SQL as `QUERY_*` and new regexes as `PATTERN_*` in `src/wheeck/constants.py`, with `?` placeholders (pyodbc qmark style).

## Error Handling

**Strategy:** Fail the whole document and requeue it. Page-level parse failures are data, not errors.

**Patterns:**
- Page-level regex failures do not raise. They accumulate in `discarded_pages.on_error`, and the page becomes a `DISCARD` warning plus a `delivery_discard` row plus a single-page PDF.
- DB write failures inside the document transaction raise `RecordingError('<STEP>')` (`src/wheeck/recording_docs.py:96`). `doc_scanner` catches `Exception`, rolls back in its own nested `try` (so a dead connection cannot mask the original error), removes new discard PDFs and re-raises.
- `run()` catches the re-raised exception per document, logs it with `logger.exception`, restores the original filename (fallback `.failed.pdf`) and continues with the next document.
- `src/main.py` is the last-resort catch: `logger.exception('unhandled exception')`.
- External API failures (`GeoMap`) are caught, logged and turned into `None`.
- `pg_dump` failures (`subprocess.CalledProcessError`) are logged as `critical`. A backup smaller than the previous dump is discarded (`src/wheeck/recording_docs.py:653-660`).

## Cross-Cutting Concerns

**Logging:** `core.get_logger(PATH_LOG, name)` writes INFO and above to `log/YYYY_MM_DD.log` (format `%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s - %(message)s`), and ERROR and above to the console. Every module creates its own logger at import with `__name__`. Messages use `%`-style lazy arguments and the convention `'doing X on page %d of %s...'` / `'... ok!'`.
**Validation:** Regex extraction in `doc_scanner`, fuzzy vehicle and driver matching against `members` (`check_similarity`), duplicate check (`QUERY_CHECK_DUPLICATE`), and DB constraints (`UNIQUE`, `CHECK > 0`, `NOT NULL` in `sql/tables/*.sql`) as the final guard.
**Authentication:** The app connects to PostgreSQL over ODBC with the credentials in `config/querier.local.json`. The template `config/querier.json` names user `wheeck`, the role created by `sql/roles/wheeck.sql` (SELECT, INSERT and UPDATE only, no DELETE). `pg_dump` runs as `postgres` with `-w`, so it needs non-interactive auth. The OpenRouteService key is `ors_api_key` in `config/wheeck.local.json`.

---

*Architecture analysis: 2026-11-22*
