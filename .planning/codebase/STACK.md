# Technology Stack

**Analysis Date:** 2026-08-09

## Languages

**Primary:**
- Python 3.14 (see `.venv/pyvenv.cfg`, `version_info = 3.14.3.final.0`) - entire codebase

**Secondary:**
- SQL (PostgreSQL dialect) - schema, views and hand-written parametrized queries in `scheme/wheeck.sql` and `src/wheeck/constants.py`

## Runtime

**Environment:**
- CPython 3.14.3, `pythoncore-3.14-64` base install (`.venv/pyvenv.cfg`)
- Windows-oriented paths and tooling (backslash-free `pathlib.Path` usage, `pg_dump` invoked via `subprocess.run(..., shell=True)`)

**Package Manager:**
- `pip` 26.0.1 inside a local virtualenv (`.venv/`, created with `virtualenv 20.31.2`)
- Lockfile: missing — only a loose `requirements.txt` with `~=` compatible-release specifiers, no `requirements.lock`/`poetry.lock`/`pip freeze` snapshot

## Frameworks

**Core:**
- None (no web framework) — this is a batch/CLI ETL script, not a service. Entry point is `src/main.py` calling `wheeck.run()`.

**Testing:**
- None detected — no `pytest`/`unittest` config, no `tests/` directory, no test files anywhere in `src/`

**Build/Dev:**
- No build tool (no `setup.py`/`pyproject.toml`/`Makefile` in this repo). The project is run directly from source via PyCharm (`.idea/wheeck.iml` marks `src` as the sources root) or `python src/main.py`.
- JetBrains PyCharm project metadata under `.idea/` (interpreter bound to `.venv`)

## Key Dependencies

**Critical (from `requirements.txt`):**
- `openpyxl~=3.1.5` - reads/writes the Excel overview and trip-summary reports (`src/wheeck/overview_docs.py`, template files in `scheme/summary.xlsx`)
- `openrouteservice~=2.3.3` - geocoding and driving-distance calculation client (`src/geo/geo.py`)
- `pyodbc~=5.3.0` - ODBC database driver used to talk to PostgreSQL (via the sibling `core` package's `Querier`, see INTEGRATIONS.md)
- `pypdfium2~=5.5.0` - PDF parsing/text extraction and page-splitting for DDT documents (`src/wheeck/recording_docs.py`)

**Transitive (present in `.venv/Lib/site-packages`, pulled in by the above):**
- `requests` 2.32.5, `urllib3` 2.6.3, `certifi`, `idna`, `charset_normalizer` - HTTP stack used internally by `openrouteservice`
- `et_xmlfile` - XML writer used internally by `openpyxl`

**Local/workspace dependency (not on PyPI, not in `requirements.txt`):**
- `core` package — a sibling local project at `C:\source\core\src\core` (separate git repo), imported as `from core import Querier, decode_json, get_logger`. Provides:
  - `core/querier.py`: `Querier` (pyodbc-based DB wrapper) and `LowQuerier` (sqlite3-based variant)
  - `core/common.py`: `decode_json()` (JSON config reader with key/value filtering) and `get_logger()` (shared logging setup)
  - Not installed via pip/editable-install in this `.venv` (no matching `.dist-info`/`.egg-link`/`.pth`); must be made importable via `PYTHONPATH` or an IDE-configured additional source root (e.g. PyCharm run configuration adding `C:\source\core\src`) at run time — this is not captured anywhere in the wheeck repo itself.
- `src/geo` — an in-repo local package (not the `geo` PyPI package) wrapping `openrouteservice`; imported as `from geo import GeoMap`.

## Configuration

**Environment:**
- No `.env` file mechanism; configuration is via two JSON files in `config/` (present but excluded from documentation of contents — see INTEGRATIONS.md):
  - `config/wheeck.json` - application-level settings, read via `decode_json(PATH_CFG_PRJ)` in `src/wheeck/constants.py` / `recording_docs.py` / `overview_docs.py` / `geo.py`. Known keys used in code: `ors_api_key`, `departure_coords`, `members` (list of `{vehicle, driver}`), `backup_dir`.
  - `config/querier.json` - database connection profile(s), read by `core.Querier`. Known keys: `name`, `driver`, `server`, `port`, `database`, `user`, `password`. Supports multiple named connections (default profile name `main`); `Querier.__init__` caches decoded config per `(file, conn_name)` pair.
- All path/config constants centralized in `src/wheeck/constants.py` (`PATH_PRJ`, `PATH_CFG`, `PATH_LOG`, `PATH_RES`, `PATH_SCHEME`, `PATH_WORKING_DIR`, etc.) computed relative to the repo root via `Path(__file__).resolve().parents[2]`.
- `res` (used for generated Excel overviews) is a symlink in this checkout pointing outside the repo to `/c/Users/Alex/MEGA/AGM/consegne/overviews` — external, machine-specific, not portable.

**Build:**
- No dedicated build config. `.gitignore` excludes `/.idea/`, `/.venv/`, `__pycache__/`, `/DDTs/`, `/log/`, `/res/`, `*.dump`, `*.xlsx` from version control — these are all local/generated/private artifacts.

## Platform Requirements

**Development:**
- Windows with a PostgreSQL ODBC driver installed (pyodbc connects via a named ODBC `driver` string from `config/querier.json`)
- PostgreSQL client tools available on `PATH` for `pg_dump` (used by `CMD_BACKUP_DB` in `src/wheeck/constants.py` to back up the `wheeck` schema)
- Network access to the OpenRouteService public API (`openrouteservice.Client`) for geocoding/distance calls, rate-limited in code to ~1 call every 1.5s (`time.sleep(1.5)` in `src/wheeck/recording_docs.py`, matching the documented "40 calls/min" free-tier limit)
- The sibling `core` local package must be importable (see above)

**Production:**
- No containerization, no cloud deployment target detected — this runs as a locally-scheduled script (log filenames like `log/2026_07_05.log` suggest a recurring/scheduled batch job, likely daily) against a local/on-prem PostgreSQL instance and a local filesystem folder (`DDTs/`) of PDF documents to process.

---

*Stack analysis: 2026-08-09*
