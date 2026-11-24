<!-- generated-by: codebase-mapper -->
# Codebase Structure

**Analysis Date:** 2026-11-22

## Directory Layout

```
wheeck/
├── src/                        # All Python code; import root (`python src/main.py`)
│   ├── main.py                 # Entry script: logger + wheeck.run()
│   ├── core/                   # Generic infrastructure (no project imports)
│   │   ├── __init__.py         # Re-exports decode_json, get_logger, LowQuerier, Querier
│   │   ├── common.py           # decode_json(), get_logger()
│   │   └── querier.py          # Querier (pyodbc), LowQuerier (sqlite3)
│   ├── geo/                    # OpenRouteService wrapper
│   │   ├── __init__.py         # Re-exports GeoMap
│   │   └── geo.py              # GeoMap: search, get_distance, get_distance_from_coords
│   └── wheeck/                 # Application package
│       ├── __init__.py         # Re-exports public API; constants import MUST stay first
│       ├── constants.py        # Paths, regex patterns, SQL queries, pg_dump command, Excel formats
│       ├── recording_docs.py   # DDT pipeline: run, doc_scanner, Delivery, MessageGenre, helpers
│       └── overview_docs.py    # Excel overview/summary generation
├── sql/                        # PostgreSQL DDL, one file per object
│   ├── tables/                 # delivery, delivery_history, delivery_warning, delivery_discard
│   ├── functions/              # sync_discard_status (PL/pgSQL trigger function)
│   ├── triggers/               # trg_sync_discard_status
│   ├── roles/wheeck.sql        # Application DB role and grants
│   └── views/                  # vw_discard_message, vw_gap_message, vw_delivery_gap
├── config/                     # JSON configuration
│   ├── wheeck.json             # Template: ors_api_key, departure_coords, members, backup_dir
│   ├── querier.json            # Template: ODBC connection list (entry "main")
│   ├── wheeck.local.json       # Real project config (gitignored, read by the code)
│   └── querier.local.json      # Real DB config (gitignored, read by the code)
├── DDTs/                       # Runtime input queue (gitignored)
│   ├── discarded/              # Single-page PDFs of pages that failed extraction
│   └── recorded/               # Successfully recorded PDFs (*.recorded.pdf)
├── scheme/                     # Excel templates + DB dump (all content gitignored)
├── res/                        # Excel output; symlink to a folder outside the repo (gitignored)
├── log/                        # Daily log files YYYY_MM_DD.log (gitignored)
├── docs/                       # Project documentation (this file)
├── requirements.txt            # pip dependencies (openpyxl, openrouteservice, pyodbc, pypdfium2)
├── README.md                   # Module overview and config setup
└── .gitignore
```

## Directory Purposes

**`src/`:**
- Purpose: Python source root and `sys.path` root, so `core`, `geo` and `wheeck` import as top-level packages. Running `src/main.py` as a script adds it to `sys.path`. Interactive sessions must start in `src/` or set `PYTHONPATH=src`.
- Contains: `main.py` and three packages.
- Key files: `src/main.py`

**`src/core/`:**
- Purpose: Project-independent helpers that can be reused in other projects: DB access, JSON config reading, logging.
- Contains: `Querier`/`LowQuerier` classes, `decode_json`, `get_logger`.
- Key files: `src/core/querier.py`, `src/core/common.py`
- Rule: Never import `wheeck` or `geo` from here.

**`src/geo/`:**
- Purpose: OpenRouteService client wrapper (geocoding and driving distance).
- Contains: `GeoMap` class.
- Key files: `src/geo/geo.py`
- Note: It imports `PATH_LOG` from `wheeck`, which creates an import cycle. Import `wheeck` before `geo`.

**`src/wheeck/`:**
- Purpose: The DDT recording application: PDF text extraction, regex parsing, DB recording, gap detection, Excel reports.
- Contains: Pipeline functions, the domain dataclass and enum, every SQL string and regex.
- Key files: `src/wheeck/recording_docs.py`, `src/wheeck/constants.py`, `src/wheeck/overview_docs.py`

**`sql/`:**
- Purpose: Full DDL for PostgreSQL database `wheeck` (schema `public`). Each object has its own file, grouped by object kind.
- Contains: `.sql` scripts only. Install them in dependency order with the psql `\ir` meta-command (sequence in the README); the database must exist first, because no script creates it. No install script is tracked.
- Key files: `sql/views/vw_delivery_gap.sql` (gap logic), `sql/views/vw_discard_message.sql` (parses `DISCARD` message text)

**`config/`:**
- Purpose: Runtime configuration. Committed `*.json` files are templates with null values. The code reads only the gitignored `*.local.json` copies (`src/wheeck/constants.py:11-12`).
- Contains: `wheeck*.json` (ORS API key, departure coordinates, `members` list of `{vehicle, driver}`, optional `backup_dir`) and `querier*.json` (list of named ODBC connections).
- Key files: `config/wheeck.json`, `config/querier.json`

**`DDTs/`:**
- Purpose: Filesystem job queue. Drop `YYYY_MM_DDT_NNNN_NNNN.pdf` here; `run()` processes every matching file.
- Contains: Queued `*.pdf`, in-progress `*.recording.pdf`, failed `*.failed.pdf`, plus subfolders `discarded/` and `recorded/`.
- Key files: None committed. The folder and both subfolders must exist at runtime.

**`scheme/`:**
- Purpose: Excel templates read by `src/wheeck/overview_docs.py`, and the target of `pg_dump` backups.
- Contains: `overview.xlsx` (sheets `consegne`, `cifre`, `litri`, `cifre manuale`, `litri manuale`; symlink to a file outside the repo), `summary.xlsx` (sheet `viaggi`), `wheeck.dump` (latest DB backup), and the temporary `wheeck.bak.dump`.
- Key files: `scheme/overview.xlsx`, `scheme/summary.xlsx`

**`res/`:**
- Purpose: Generated Excel reports `YYYY_MM.xlsx` (monthly overview) and `YYYY_TRIPS.xlsx` (yearly trip summary).
- Contains: Output only. The path is a symlink to a folder outside the repo.

**`log/`:**
- Purpose: Daily log files written by `core.get_logger`.
- Contains: `YYYY_MM_DD.log`. A file is created at import time, even when no document is processed.

## Key File Locations

**Entry Points:**
- `src/main.py`: Batch entry. Run as a script: `python src/main.py`.
- `src/wheeck/recording_docs.py` `run()` (line 593): Pipeline orchestrator.
- `src/wheeck/__init__.py`: Public API for interactive use (`generate_overview`, `generate_summary`, `check_gaps`, ...).
- `\ir` sequence in `README.md`: Database install (no install script is tracked).

**Configuration:**
- `src/wheeck/constants.py`: All project paths (`PATH_*`), config file paths (`PATH_CFG_PRJ`, `PATH_CFG_DB`), regexes (`PATTERN_*`), SQL (`QUERY_*`), `CMD_BACKUP_DB`, Excel formats (`OVERVIEW_*`).
- `config/wheeck.json` / `config/wheeck.local.json`: Project settings template and real copy.
- `config/querier.json` / `config/querier.local.json`: DB connection template and real copy.
- `requirements.txt`: Python dependencies.
- `.gitignore`: Ignores `.venv`, `.idea`, `DDTs/`, `log/`, `res/`, `*.xlsx`, `*.dump`, `config/*.local.json`.

**Core Logic:**
- `src/wheeck/recording_docs.py`: `doc_scanner()` (per-document transaction and page parsing), `resolve_distances()`, `Delivery`, `MessageGenre`, `RecordingError`, `check_*`, `save_warning`, `discard_doc`.
- `src/wheeck/overview_docs.py`: `generate_current()`, `generate_overview()`, `generate_summary()`.
- `src/core/querier.py`: DB wrapper used by every DB call.
- `src/geo/geo.py`: Distance calculation.
- `sql/views/vw_delivery_gap.sql`, `sql/views/vw_discard_message.sql`, `sql/views/vw_gap_message.sql`: Gap detection and message parsing logic in SQL.
- `sql/functions/sync_discard_status.sql` + `sql/triggers/trg_sync_discard_status.sql`: Sync discard status from warning status.

**Testing:**
- Not detected. There is no test directory, test framework or test config.

## Naming Conventions

**Files:**
- Python modules: `snake_case.py`. Modules that work on documents end in `_docs.py` (`recording_docs.py`, `overview_docs.py`).
- SQL files: one object per file, named exactly as the DB object: `sql/tables/delivery_warning.sql`, `sql/views/vw_gap_message.sql`, `sql/triggers/trg_sync_discard_status.sql`.
- DDT input PDFs: `YYYY_MM_DDT_<first doc number:4>_<last doc number:4>.pdf` (for example `2024_01_DDT_0001_0267.pdf`), enforced by `PATTERN_WORKING_DOC`.
- Discarded pages: `<source stem>_P<page:3>.pdf` (for example `2024_01_DDT_0001_0267_P005.pdf`), built by `discarded_doc_path()`.
- Processing states: `.recording.pdf` (in progress), `.recorded.pdf` (done, in `DDTs/recorded/`), `.failed.pdf` (restore failed).
- Reports: `res/YYYY_MM.xlsx`, `res/YYYY_TRIPS.xlsx`. Logs: `log/YYYY_MM_DD.log`.
- Config: `<name>.json` committed template, `<name>.local.json` real gitignored copy.

**Directories:**
- Python packages: short lowercase single word (`core`, `geo`, `wheeck`).
- `sql/` subfolders: plural object kind (`tables`, `views`, `functions`, `triggers`, `roles`).

**Identifiers:**
- Constants in `src/wheeck/constants.py` use a kind prefix: `PATH_*`, `PATTERN_*`, `QUERY_<VERB>_<OBJECT>` (`QUERY_INSERT_DELIVERY`, `QUERY_GET_GAP`, `QUERY_CHECK_DUPLICATE`), `CMD_*`, `OVERVIEW_*`.
- DB objects: views `vw_*`, triggers `trg_*`, constraints `pk_<table>_<col>`, `fk_<ref>_<col>`, `uq_<table>_<cols>`, `chk_<table>_<col>`, indexes `idx_<table>_<cols>`.
- Each package `__init__.py` re-exports its public names and defines `__version__`.

## Where to Add New Code

**New extraction field from DDT pages:**
- Regex: add `PATTERN_<FIELD>` to `src/wheeck/constants.py`.
- Parsing: add a search block in the page loop of `doc_scanner()` in `src/wheeck/recording_docs.py`. Follow the existing pattern: log the search, on match set the `delivery.<field>`, on failure append the pattern name to `discarded_pages.on_error`.
- Model: add the field to `Delivery` (`src/wheeck/recording_docs.py:30`) at the matching column position.
- SQL: update `QUERY_INSERT_DELIVERY`, `QUERY_INSERT_DISCARD` and `QUERY_GET_DISCARD` in `src/wheeck/constants.py`.
- Schema: add the column to `sql/tables/delivery.sql`, `sql/tables/delivery_discard.sql` and `sql/tables/delivery_history.sql`. `vw_delivery_gap` uses `SELECT *` over a `UNION ALL` of `delivery` and `delivery_history`, so both tables must keep identical column lists.
- Tests: no test suite exists. Verify against sample PDFs in `DDTs/`.

**New pipeline step (run once per batch):**
- Implementation: a function in `src/wheeck/recording_docs.py`, called from the `if job_begin:` block at the end of `run()`.
- Export it from `src/wheeck/__init__.py` if it should be callable by hand.

**New report:**
- Implementation: a `generate_<name>()` function in `src/wheeck/overview_docs.py`, using `Querier(cfg_in=PATH_CFG_DB)`, a template in `scheme/` (`PATH_SCHEME`), and output to `res/` (`PATH_RES`).
- SQL: `QUERY_GET_<NAME>` in `src/wheeck/constants.py`.
- Wiring: call it from `generate_current()` and export it from `src/wheeck/__init__.py`.

**New SQL query:**
- Add it to `src/wheeck/constants.py` as a `QUERY_*` triple-quoted string starting with `"""\`, with schema-qualified names (`public.<table>`) and `?` placeholders.

**New DB object (table, view, function, trigger):**
- File: `sql/<kind>/<object_name>.sql`. Views and functions start with `DROP ... IF EXISTS` and then `CREATE OR REPLACE`. Tables use `CREATE TABLE IF NOT EXISTS` with named constraints.
- Wiring: add a `\ir sql/<kind>/<object_name>.sql` line to the install sequence in `README.md`, after every object it depends on.

**New external service wrapper:**
- Implementation: a new top-level package `src/<name>/` with `<name>.py` holding the class and `__init__.py` re-exporting it with `__version__`, like `src/geo/`.
- Config: read keys from `config/wheeck.local.json` through `decode_json(PATH_CFG_PRJ)`, and add the key with a `null` value to the template `config/wheeck.json`.
- Avoid importing `wheeck` from the new package. Take the log path as a parameter, or use `core.get_logger` with a path passed in.

**Utilities:**
- Generic, project-independent helpers: `src/core/common.py`, exported in `src/core/__init__.py`.
- DDT-specific helpers: `src/wheeck/recording_docs.py`.

## Special Directories

**`DDTs/`:**
- Purpose: Input queue and archive of processed PDFs.
- Generated: Partly (`discarded/` PDFs and the renamed files in `recorded/` are produced by the app).
- Committed: No (gitignored). The folder and both subfolders must be created by hand.

**`scheme/`:**
- Purpose: Excel templates and `pg_dump` output.
- Generated: `wheeck.dump` / `wheeck.bak.dump` are generated. The `.xlsx` templates are hand-made.
- Committed: No. All content matches the `*.xlsx` and `*.dump` ignore patterns, but the folder is required at runtime.

**`res/`:**
- Purpose: Report output.
- Generated: Yes.
- Committed: No (gitignored; symlink to a folder outside the repo).

**`log/`:**
- Purpose: Daily logs.
- Generated: Yes.
- Committed: No (gitignored).

**`config/*.local.json`:**
- Purpose: Real configuration with secrets (ORS API key, DB password).
- Generated: No, copied by hand from the templates.
- Committed: No (gitignored).

**`.venv/`, `.idea/`, `__pycache__/`:**
- Purpose: Virtual environment, PyCharm project, bytecode cache.
- Generated: Yes.
- Committed: No.

---

*Structure analysis: 2026-11-22*
