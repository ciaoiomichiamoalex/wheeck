# Codebase Structure

**Analysis Date:** 2026-08-09

## Directory Layout

```
wheeck/
├── src/                    # Python source root (IDE source folder, see .idea/wheeck.iml)
│   ├── main.py             # Process entry point
│   ├── wheeck/             # Core domain package (pipeline + reporting)
│   │   ├── __init__.py     # Public API re-exports
│   │   ├── constants.py    # Paths, regex patterns, SQL queries, Excel formats
│   │   ├── recording_docs.py  # PDF scanning, extraction, persistence, discard handling
│   │   └── overview_docs.py   # Excel overview/summary generation
│   └── geo/                # Geocoding integration package
│       ├── __init__.py     # Exposes GeoMap
│       └── geo.py          # OpenRouteService client wrapper
├── config/                 # JSON configuration (not committed with secrets exposed in git history — treat as sensitive)
│   ├── wheeck.json         # API key, departure coords, vehicle/driver roster, backup_dir
│   └── querier.json        # Database connection configuration (external `core.Querier` input)
├── scheme/                 # Database schema, dumps, and Excel templates
│   ├── wheeck.sql          # DDL for tables/views (source of truth for schema)
│   ├── wheeck.dump         # Binary pg_dump backup (generated, gitignored via *.dump)
│   └── summary.xlsx        # Excel template consumed by generate_summary (gitignored via *.xlsx)
├── DDTs/                   # Working directory for PDF documents (gitignored)
│   ├── discarded/          # Single-page PDFs split out for records that failed validation
│   └── recorded/           # Fully processed source PDFs (renamed *.recorded.pdf)
├── log/                    # Dated log files written by the external `core.get_logger` (gitignored)
├── res -> (symlink)        # Symlink to an external directory outside the repo for Excel outputs (gitignored)
├── requirements.txt        # Pinned third-party dependencies (openpyxl, openrouteservice, pyodbc, pypdfium2)
├── README.md                # High-level module descriptions
└── .venv/                   # Local virtual environment (gitignored)
```

## Directory Purposes

**`src/wheeck/`:**
- Purpose: Core domain logic — the delivery-extraction pipeline and Excel reporting.
- Contains: Dataclasses/enums (`Delivery`, `MessageGenre`), orchestration functions (`run`, `doc_scanner`), reporting functions (`generate_overview`, `generate_summary`, `generate_current`), and the shared constants module.
- Key files: `src/wheeck/recording_docs.py`, `src/wheeck/overview_docs.py`, `src/wheeck/constants.py`, `src/wheeck/__init__.py`.

**`src/geo/`:**
- Purpose: Isolates the third-party geocoding/distance API integration from the domain logic.
- Contains: A single `GeoMap` class wrapping `openrouteservice`.
- Key files: `src/geo/geo.py`, `src/geo/__init__.py`.

**`config/`:**
- Purpose: Runtime configuration external to source code (API keys, DB connection, business roster of vehicles/drivers, backup path).
- Contains: Two JSON files, both consumed via the external `core.decode_json` helper.
- Key files: `config/wheeck.json` (read by `PATH_CFG_PRJ` constant in `src/wheeck/constants.py:10`), `config/querier.json` (read by `Querier(cfg_in=PATH_CFG, ...)` calls throughout `src/wheeck/recording_docs.py` and `src/wheeck/overview_docs.py`).

**`scheme/`:**
- Purpose: Database schema definition and Excel report templates/backups.
- Contains: `wheeck.sql` (DDL), `.dump`/`.bak.dump` backups produced by `pg_dump` (see `CMD_BACKUP_DB` in `src/wheeck/constants.py:25`), and `.xlsx` templates (`overview.xlsx` referenced in code but not present in the current directory listing — verify before assuming it exists; `summary.xlsx` is present).
- Key files: `scheme/wheeck.sql`.

**`DDTs/`:**
- Purpose: Filesystem-based work queue for the recording pipeline.
- Contains: Incoming PDFs (root of `DDTs/`, matched by `PATTERN_WORKING_DOC` in `src/wheeck/constants.py:16`), `discarded/` (single-page extracts for failed validations, matched by `PATTERN_DISCARD_DOC`), `recorded/` (fully processed originals renamed to `*.recorded.pdf`).
- Key files: none (data only); paths defined by `PATH_WORKING_DIR`, `PATH_DISCARDED_DIR`, `PATH_RECORDED_DIR` in `src/wheeck/constants.py:12-14`.

**`log/`:**
- Purpose: Application log output, one file per run/day (e.g. `log/2026_07_05.log`).
- Contains: Text logs written through the external `core.get_logger(PATH_LOG, __name__)` pattern.
- Key files: none tracked as source; directory path defined by `PATH_LOG` in `src/wheeck/constants.py:6`.

## Key File Locations

**Entry Points:**
- `src/main.py`: Process bootstrap and top-level exception logging.

**Configuration:**
- `src/wheeck/constants.py`: All path constants, regex patterns, SQL query text, Excel format map, and the `pg_dump` backup command template.
- `config/wheeck.json`, `config/querier.json`: Runtime secrets/config (do not quote contents in documentation or commits).

**Core Logic:**
- `src/wheeck/recording_docs.py`: Extraction/persistence pipeline (`Delivery`, `doc_scanner`, `run`, `check_duplicate`, `check_gaps`, `check_similarity`, `save_warning`, `discard_doc`).
- `src/wheeck/overview_docs.py`: Excel report generation (`generate_overview`, `generate_summary`, `generate_current`).
- `src/geo/geo.py`: `GeoMap` geocoding/distance wrapper.

**Testing:**
- Not detected. No test files, test framework config, or `tests/` directory exist in this repository.

## Naming Conventions

**Files:**
- Snake_case Python module names matching their primary responsibility noun/verb pair: `recording_docs.py`, `overview_docs.py`, `constants.py`, `geo.py`.
- PDF working files follow a strict pattern encoded in `src/wheeck/constants.py`: `YYYY_MM_DDT_####_####[_P###]*.pdf` for incoming docs (`PATTERN_WORKING_DOC`), `*_P###.pdf` for discarded single pages (`PATTERN_DISCARD_DOC`), and `*.recorded.pdf` for fully processed originals.

**Directories:**
- Lowercase, singular-domain-noun package names (`geo`, `wheeck`) under `src/`.
- Plural, domain-meaningful top-level directories for data (`DDTs`, `log`, `config`, `scheme`).

**Code identifiers (observed in `src/wheeck/constants.py`, `src/wheeck/recording_docs.py`):**
- Module-level constants: `SCREAMING_SNAKE_CASE` grouped by prefix — `PATH_*` (filesystem locations), `PATTERN_*` (compiled-at-use regexes), `QUERY_*` (SQL text), `OVERVIEW_*` (Excel formatting), `CMD_*` (shell commands).
- Functions/methods/variables: `snake_case` (`doc_scanner`, `check_duplicate`, `working_doc_name`).
- Classes: `PascalCase` (`Delivery`, `GeoMap`, `MessageGenre`).
- Type hints are used consistently on function signatures and dataclass fields, including modern `|` union syntax (`str | Path`, `float | None`).

## Where to Add New Code

**New extraction pattern or SQL query:**
- Add the regex/query constant to `src/wheeck/constants.py` following the existing `PATTERN_*`/`QUERY_*` naming prefix, then import and use it from `src/wheeck/recording_docs.py` or `src/wheeck/overview_docs.py`.

**New pipeline step (e.g. another validation or enrichment stage):**
- Implement as a function in `src/wheeck/recording_docs.py`, call it from `doc_scanner` or `run`, and export it from `src/wheeck/__init__.py` if it should be part of the public API.

**New report type:**
- Add a `generate_*` function to `src/wheeck/overview_docs.py` following the pattern of `generate_overview`/`generate_summary` (query via `Querier`, load/mutate an `openpyxl` workbook from a `scheme/*.xlsx` template, save to `res/` via `PATH_RES`).

**New external integration (e.g. a different mapping provider):**
- Create a new package under `src/` (mirroring `src/geo/`) with an `__init__.py` re-export and an implementation module; wrap provider-specific exceptions into logged, `None`-returning methods as done in `src/geo/geo.py`.

**Utilities:**
- No dedicated `utils`/`helpers` module exists in this repo; shared low-level helpers (`Querier`, `decode_json`, `get_logger`) live in the external, unvendored `core` package, imported the same way from every module (`from core import ...`).

**Tests:**
- No existing test directory or framework. If adding tests, create a `tests/` (or `src/tests/`) directory and a corresponding test runner dependency in `requirements.txt`; no conventions currently exist to follow.

## Special Directories

**`DDTs/`:**
- Purpose: Live filesystem work-queue and processed-document archive for the pipeline.
- Generated: Partially — `discarded/` contents are generated by `discard_doc` (`src/wheeck/recording_docs.py:329-355`); `recorded/` contents are moved/renamed originals.
- Committed: Yes, PDF files are currently tracked in git despite `DDTs/` being listed in `.gitignore` (pre-existing tracked files are unaffected by a later-added ignore rule) — new files dropped into this directory going forward will be ignored by git.

**`log/`:**
- Purpose: Runtime log output.
- Generated: Yes, by `core.get_logger`.
- Committed: Contents are currently tracked in git despite the `/log/` rule in `.gitignore`, for the same pre-existing-tracked-file reason as `DDTs/`.

**`res/`:**
- Purpose: Destination for generated Excel overview/summary workbooks.
- Generated: No (it is a symlink itself); its target directory's contents (`*.xlsx` files) are generated by `src/wheeck/overview_docs.py`.
- Committed: No — excluded via `/res/` in `.gitignore`, and it points outside the repository (`res -> /c/Users/Alex/MEGA/AGM/consegne/overviews`).

**`.venv/`:**
- Purpose: Local Python virtual environment (Python 3.14 per `.idea/misc.xml`).
- Generated: Yes, by the environment/package manager.
- Committed: No — excluded via `/.venv/` in `.gitignore`.

---

*Structure analysis: 2026-08-09*
