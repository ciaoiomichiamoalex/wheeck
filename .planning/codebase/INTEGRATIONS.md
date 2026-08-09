# External Integrations

**Analysis Date:** 2026-08-09

## APIs & External Services

**Geocoding / Routing:**
- OpenRouteService (openrouteservice.org) - used to (1) geocode a delivery city into coordinates and (2) compute the driving distance in km between the fixed departure point and that city
  - SDK/Client: `openrouteservice` PyPI package, wrapped in `GeoMap` class at `src/geo/geo.py`
  - Calls used: `openrouteservice.geocode.pelias_search(...)` (`GeoMap.search`, `src/geo/geo.py:41`) and `openrouteservice.directions.directions(...)` (`GeoMap.get_distance_from_coords`, `src/geo/geo.py:92`), profile `driving-car`, format `geojson`
  - Auth: API key read from `config/wheeck.json` key `ors_api_key`, decoded via `core.decode_json` (`src/geo/geo.py:24`) — file present in repo, contents intentionally not reproduced here (see Environment Configuration below)
  - Rate limiting: caller (`src/wheeck/recording_docs.py:280`) sleeps 1.5s after each distance calculation to stay under the ~40 calls/min free-tier limit
  - Error handling: `openrouteservice.exceptions.ApiError`, `HTTPError`, `Timeout` are all caught and logged (never raised) in `GeoMap.search` and `GeoMap.get_distance_from_coords`, degrading to `None` distance/coordinates on failure

## Data Storage

**Databases:**
- PostgreSQL, schema `wheeck` (DDL in `scheme/wheeck.sql`)
  - Connection: via ODBC, configured in `config/querier.json` (keys: `name`, `driver`, `server`, `port`, `database`, `user`, `password` — values not reproduced here)
  - Client: `pyodbc` wrapped by `Querier` class in the sibling `core` package (`C:\source\core\src\core\querier.py`), instantiated throughout `src/wheeck/recording_docs.py` and `src/wheeck/overview_docs.py` as `Querier(cfg_in=PATH_CFG, save_changes=...)`
  - Connection default profile name is `main` (see `Querier.__init__` default `conn_name='main'`); config supports multiple named connections in one JSON file
  - Tables (from `scheme/wheeck.sql`):
    - `wheeck.delivery` - one row per successfully parsed DDT page (document number/genre/date, company, delivery city, quantity, delivery date, vehicle, driver, distance, source PDF, page number, recording timestamp). Unique on `(document_number, document_genre, document_date)` and on `(document_source, page_number)`.
    - `wheeck.delivery_history` - same shape as `delivery`, used to retain historical/superseded records (see `912ecb3 Update dependencies and add delivery history table with constraints and indexes` in git log)
    - `wheeck.delivery_discard` - failed/discarded page records plus `id_warning_message` FK-like link and `status` flag, queried via `QUERY_GET_DISCARD` / written via `QUERY_INSERT_DISCARD`
    - `wheeck.delivery_warning` - warning/message log (`message_genre`, `message_text`, `status`), written via `QUERY_INSERT_WARNING`, closed via `QUERY_UPDATE_WARNING`
  - Views: `wheeck.vw_discard_message`, `wheeck.vw_gap_message`, `wheeck.vw_delivery_gap` (`scheme/wheeck.sql:185,222,239`) - used to detect gaps in `document_number` sequences (`QUERY_CHECK_GAPS`, `QUERY_GET_GAP` in `src/wheeck/constants.py`) and to correlate delivery gaps with existing warning messages
  - All SQL is parametrized with `?` placeholders and passed as positional args to `pyodbc`/`Querier.run` — no string-interpolated SQL for user-derived values, except `QUERY_GET_SUMMARY` which interpolates a `%s`-style placeholder list for an `IN (...)` clause built from a fixed, config-derived driver list (`src/wheeck/overview_docs.py:79`), not from external/untrusted input
  - Backup: `pg_dump` invoked via `subprocess.run(shell=True)` using `CMD_BACKUP_DB` (`src/wheeck/constants.py:25`) to `scheme/wheeck.bak.dump`, promoted to `scheme/wheeck.dump` if size is non-decreasing (`src/wheeck/recording_docs.py:487-498`)

**File Storage:**
- Local filesystem only. No cloud storage/object store integration.
  - `DDTs/` (`PATH_WORKING_DIR`) - inbox of PDF documents to process; moved to `DDTs/recorded/*.recorded.pdf` on success
  - `DDTs/discarded/` (`PATH_DISCARDED_DIR`) - single-page PDFs extracted from documents whose page failed pattern matching (`discard_doc()` in `src/wheeck/recording_docs.py:329`)
  - `res` (`PATH_RES`) - symlinked outside the repo (`res -> /c/Users/Alex/MEGA/AGM/consegne/overviews`, a MEGA cloud-sync folder mounted locally) — generated Excel overview (`{year}_{month}.xlsx`) and trip-summary (`{year}_TRIPS.xlsx`) files are written here (`src/wheeck/overview_docs.py`)
  - `config.backup_dir` (from `config/wheeck.json`) - optional secondary copy destination for successfully processed source PDFs (`shutil.copy` in `src/wheeck/recording_docs.py:476`)

**Caching:**
- None — a lightweight in-process class-level cache exists only for decoded DB connection config (`Querier._cache_config` dict in `core/querier.py`), not a general application cache

## Authentication & Identity

**Auth Provider:**
- None — this is an unattended local batch script with no end users, no login flow, and no auth middleware. The only "authentication" is the OpenRouteService API key (see above) and the database username/password in `config/querier.json`.

## Monitoring & Observability

**Error Tracking:**
- None (no Sentry/error-tracking SaaS). Errors are caught, logged, and generally converted into "discard" or "warning" database rows so a human can review them later via `wheeck.delivery_discard` / `wheeck.delivery_warning`.

**Logs:**
- Local rotating-by-date log files under `log/` (`PATH_LOG`), one file per run day (e.g. `log/2026_07_05.log`), created via `core.get_logger(PATH_LOG, __name__)` (`src/wheeck/recording_docs.py:25`, `src/wheeck/overview_docs.py:12`, `src/geo/geo.py:9`)
  - Format: `dd/mm/YYYY HH:MM:SS.mmm [logger.name] LEVEL - message` (`core/common.py:67-70`)
  - File handler logs at `INFO`+; a separate console `StreamHandler` logs `ERROR`+ only (`core/common.py:73-81`)
  - Top-level unhandled exceptions are caught in `src/main.py` and logged with `logger.exception('unhandled exception')`

## CI/CD & Deployment

**Hosting:**
- None detected — no Dockerfile, no cloud deployment manifest. Runs as a local/on-prem scheduled script (evidence: irregular, roughly-daily log file dates under `log/`, suggesting a scheduled task rather than a long-running service).

**CI Pipeline:**
- None — no `.github/workflows`, no other CI config found in the repo.

## Environment Configuration

**Required config files (not env vars):**
- `config/wheeck.json` - keys: `ors_api_key` (OpenRouteService credential), `departure_coords` (`[lon, lat]` fixed origin for distance calc), `members` (vehicle/driver roster used for fuzzy-matching OCR'd vehicle plates and driver names), `backup_dir` (optional path)
- `config/querier.json` - keys: `name`, `driver`, `server`, `port`, `database`, `user`, `password` (PostgreSQL ODBC connection profile(s))

**Secrets location:**
- Both config files above live in `config/` inside the repository working tree and are **not** excluded by `.gitignore` (only `/.idea/`, `/.venv/`, `__pycache__/`, `/DDTs/`, `/log/`, `/res/`, `*.dump`, `*.xlsx` are ignored — see `.gitignore`). This means the OpenRouteService API key and the PostgreSQL username/password currently sit in a JSON file that git can track. Note whether these files are actually committed to the git history before treating them as safe to share.

## Webhooks & Callbacks

**Incoming:**
- None — no HTTP server, no listener of any kind in this codebase.

**Outgoing:**
- None beyond the OpenRouteService REST calls described above (no Slack/email/webhook notifications on job completion or failure — failures only surface via log files and DB warning rows).

---

*Integration audit: 2026-08-09*
