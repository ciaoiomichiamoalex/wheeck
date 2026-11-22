<!-- generated-by: codebase-mapper -->
# Technology Stack

**Analysis Date:** 2026-11-22

## Languages

**Primary:**
- Python 3.12+ (developed on 3.15.1): all application code under `src/` (`src/main.py`, `src/core/`, `src/geo/`, `src/wheeck/`)

**Secondary:**
- SQL (PostgreSQL dialect, including PL/pgSQL): the database schema under `db/`
  - DDL scripts: `db/schema/wheeck.sql`, `db/tables/*.sql`, `db/views/*.sql`
  - PL/pgSQL trigger function: `db/functions/sync_discard_status.sql`
  - Trigger: `db/triggers/trg_sync_discard_status.sql`
  - Role and grants: `db/roles/wheeck.sql`
- Embedded SQL strings: every query the app runs is a module-level constant in `src/wheeck/constants.py` (`QUERY_*`). They use `?` placeholders, which is the pyodbc/ODBC parameter style.
- psql meta-commands: `db/install.sql` chains the object scripts with `\ir`, so you must run it with `psql`.

**Why the Python floor is 3.12** (nothing in the repo pins the version):
- `sqlite3.connect(..., autocommit=...)` in `src/core/querier.py` (line 227) needs Python 3.12 or later.
- `typing.Self` in `src/core/querier.py` needs Python 3.11 or later.
- `pyodbc` 5.5.0 declares `Requires-Python: >=3.11`.
- `match` statements (`src/core/querier.py`) and `X | Y` type unions (all modules) need Python 3.10 or later.

## Runtime

**Environment:**
- CPython. The local virtualenv `.venv/` (gitignored) is CPython 3.15.1 on Windows.
- Target OS: Windows.
  - Code comments in `src/wheeck/recording_docs.py` (lines 175-177 and 619-621) cover Windows file-sharing violations on rename.
  - The ODBC driver name `PostgreSQL ANSI` in `config/querier.json` is the Windows psqlODBC driver name.
- Launch: `src/main.py`. Imports are top-level package names (`import wheeck`, `from core import ...`, `from geo import ...`), so `src/` must be on `sys.path`.
  - The IDE run configuration in `.idea/workspace.xml` uses working directory `src/` and script `src/main.py`.
  - Equivalent shell command: `cd src && python main.py`.

**Package Manager:**
- pip (26.2.2 in `.venv`)
- Manifest: `requirements.txt`, compatible-release pins (`~=`) only.
- Lockfile: missing. There is no `pyproject.toml`, `setup.py`, `poetry.lock` or `requirements.lock`, so transitive dependency versions are not pinned.

## Frameworks

**Core:**
- None. This is a plain Python batch script with no application framework.

**Testing:**
- Not detected. There is no test framework, test directory or test config in the repo.

**Build/Dev:**
- No build step. The app runs straight from source.
- JetBrains PyCharm project files in `.idea/` (gitignored). `.idea/wheeck.iml` marks `src/` as the source root and uses the `.venv` interpreter.

## Key Dependencies

Direct dependencies, from `requirements.txt`:

**Critical:**
- `pypdfium2` ~=5.5.0 (installed 5.5.0): PDF text extraction and page splitting.
  - `pypdfium2.PdfDocument(...)`, `page.get_textpage().get_text_bounded()`
  - `PdfDocument.new()`, `import_pages`, `save`
  - All of this is in `src/wheeck/recording_docs.py` (`doc_scanner`, `discard_doc`).
- `pyodbc` ~=5.5.0 (installed 5.5.0): the only path to the PostgreSQL database, wrapped by `Querier` in `src/core/querier.py`.
- `openrouteservice` ~=2.3.3 (installed 2.3.3): geocoding and driving distance, wrapped by `GeoMap` in `src/geo/geo.py`.
- `openpyxl` ~=3.1.5 (installed 3.1.5): reads and writes the Excel overview and summary workbooks.
  - Used in `src/wheeck/overview_docs.py`.
  - Also used by `Querier.save_excel` in `src/core/querier.py`.

**Infrastructure (transitive, installed in `.venv`, not pinned):**
- `requests` 2.34.2, `urllib3` 2.8.0, `idna` 3.20, `charset_normalizer` 3.5.2, `certifi` 2026.11.20: the HTTP stack under `openrouteservice`.
- `et_xmlfile` 2.0.0: an `openpyxl` dependency.

**Standard library modules that carry real logic:**
- `re`: all DDT field extraction. The patterns are `PATTERN_*` in `src/wheeck/constants.py`.
- `difflib.SequenceMatcher`: fuzzy matching of vehicle plates and driver names (`check_similarity` in `src/wheeck/recording_docs.py`).
- `dataclasses`: the `Delivery` record (`src/wheeck/recording_docs.py`).
- `logging`: one file handler per day plus an ERROR-level console handler (`get_logger` in `src/core/common.py`).
- `subprocess`: runs `pg_dump` for the database backup (`run` in `src/wheeck/recording_docs.py`).
- `sqlite3`: the `LowQuerier` class in `src/core/querier.py`. It is exported from `src/core/__init__.py` but the app never uses it.
- `json`: config loading through `decode_json` in `src/core/common.py`.

**Internal package versions (`__version__`):**
- `src/core/__init__.py`: 1.0.3
- `src/geo/__init__.py`: 1.0.1
- `src/wheeck/__init__.py`: 1.0.3

## Configuration

**Environment:**
- The app reads no environment variables. There are no `os.environ` or `getenv` calls in `src/`.
- Configuration comes from JSON files in `config/`. The paths are resolved in `src/wheeck/constants.py`.
  - `config/wheeck.json`: committed template. Its real copy is `config/wheeck.local.json` (gitignored), read through `PATH_CFG_PRJ`. Keys:
    - `ors_api_key`: OpenRouteService API key.
    - `departure_coords`: start point for distance calculation. It is passed to ORS directions next to the GeoJSON coordinates `GeoMap.search` returns, so use GeoJSON order `[lon, lat]`. The `GeoMap.search` docstring in `src/geo/geo.py` says latitude first.
    - `members`: list of `{vehicle, driver}` entries. They are the allowed plates and drivers for fuzzy matching.
      - Deliveries by drivers of vehicle-less members are excluded from the yearly summary.
      - `QUERY_GET_SUMMARY` has exactly two `vehicle = ?` slots, so it expects exactly two vehicles.
    - `backup_dir`: optional directory that receives a copy of every recorded PDF.
  - `config/querier.json`: committed template. Its real copy is `config/querier.local.json` (gitignored), read through `PATH_CFG_DB`. It holds a list of named connections. The app uses the `main` connection, with keys `name`, `driver`, `server`, `port`, `database`, `user`, `password`.
- `decode_json` re-reads the config files on every call. `Querier` caches its parsed connection config in the class attribute `Querier._cache_config`.
- Hardcoded settings in `src/wheeck/constants.py`:
  - All project paths (`PATH_*`) derive from `Path(__file__).resolve().parents[2]`.
  - Regex patterns for file names and DDT fields (`PATTERN_*`).
  - The `pg_dump` backup command (`CMD_BACKUP_DB`), with fixed host `127.0.0.1`, port `5432`, user `postgres` and database `postgres`.
  - Excel number formats and the font (`OVERVIEW_*`).

**Build:**
- No build configuration.
- `requirements.txt` is the only dependency manifest.
- `.gitignore` excludes `/.idea/`, `/.venv/`, `__pycache__/`, `/DDTs/`, `/log/`, `/res/`, `*.dump`, `*.xlsx` and `/config/*.local.json`.

## Platform Requirements

**Development:**
- Python 3.12 or later, with a virtualenv: `python -m venv .venv`, then `.venv\Scripts\pip install -r requirements.txt`.
- psqlODBC installed, registered under the driver name given in `config/querier.local.json`. The template uses `PostgreSQL ANSI`.
- A PostgreSQL server reachable at the configured `server`/`port`. The template uses `127.0.0.1:5432`, database `postgres`.
- The schema must be installed once with `psql -f db/install.sql` as a superuser. `db/schema/wheeck.sql` runs `DROP SCHEMA IF EXISTS wheeck CASCADE`, so re-running the install wipes all data.
- PostgreSQL client tools on `PATH`:
  - `psql`, for the install.
  - `pg_dump`, for the automatic backup at the end of each run. It runs once any file name in `DDTs/` matches `PATTERN_WORKING_DOC`, even if that document rolled back.
- Copy both templates to their `*.local.json` siblings and fill in real values.
- Gitignored runtime paths a fresh clone lacks. Create them before the first run:
  - `DDTs/`: input folder. If it is missing, `run()` finds no documents and exits quietly.
  - `DDTs/discarded/` and `DDTs/recorded/`: must exist. If they are missing, saving a discard PDF fails and rolls the document back, and the final move of a recorded PDF raises after the commit.
  - `log/`: must exist as a directory. If it is missing, `get_logger` in `src/core/common.py` creates a plain file named `log` at the project root and appends every run to it.
  - `res/`: output folder for the generated workbooks. Locally it is a symlink to a folder outside the repo.
  - `scheme/overview.xlsx`: workbook template with sheets `consegne`, `cifre`, `litri`, `cifre manuale`, `litri manuale`. It is gitignored as `*.xlsx` and locally a symlink to a folder outside the repo.
  - `scheme/summary.xlsx`: workbook template with sheet `viaggi`. It is gitignored as `*.xlsx`.

**Production:**
- A single Windows workstation runs the batch manually or on a schedule. No scheduler definition is committed.
- PostgreSQL runs on the same machine. Both the template config and the hardcoded `pg_dump` command target `127.0.0.1`.
- Outbound HTTPS to `api.openrouteservice.org` is required for any new delivery city.
- No container, service or server deployment exists.

---

*Stack analysis: 2026-11-22*
