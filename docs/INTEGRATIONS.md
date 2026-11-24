<!-- generated-by: codebase-mapper -->
# External Integrations

**Analysis Date:** 2026-11-22

## APIs & External Services

**Geocoding and routing:**
- OpenRouteService (`https://api.openrouteservice.org`, the client's default base URL). It turns each delivery city into coordinates and then into a driving distance in km from the configured departure point.
  - SDK/Client: `openrouteservice` 2.3.3, wrapped by `GeoMap` in `src/geo/geo.py`.
  - Auth: API key `ors_api_key` in `config/wheeck.local.json`, passed to `openrouteservice.Client(key=...)`. No environment variable is used.
  - Endpoints used:
    - `GeoMap.search` calls `geocode.pelias_search` (`GET /geocode/search`) with default country `IT`.
    - `GeoMap.get_distance_from_coords` calls `directions.directions` (`POST /v2/directions/driving-car/geojson`) with `radiuses=(500,)`. The result is `segments[0].distance / 1000`.
  - Callers in `src/wheeck/recording_docs.py`:
    - `city_distance()` builds a new `GeoMap` and re-reads `departure_coords` on every call.
    - `resolve_distances()` resolves each distinct city of a document before the DB transaction opens. It reuses a distance already stored in `wheeck.delivery` (`QUERY_GET_DISTANCE`), so the API is only called for cities it has not seen before.
  - Rate limiting: a fixed `time.sleep(1.5)` after each newly calculated city (`src/wheeck/recording_docs.py`, line 153). The code comment there states a "40 call/min" quota. The client keeps its defaults: `timeout=60`, `retry_timeout=60`, `retry_over_query_limit=True`.
  - Error handling: `GeoMap` catches `exceptions.ApiError`, `exceptions.HTTPError` and `exceptions.Timeout`, logs them and returns `None`. The delivery is then stored with a NULL `distance`. Other exceptions are not caught, for example `requests.exceptions.ConnectionError` (the client only converts `requests` timeouts) or a `KeyError` on an unexpected payload. They escape `resolve_distances()` before the transaction opens, `doc_scanner()` aborts, and `run()` puts the PDF back in the queue.

## Data Storage

**Databases:**
- PostgreSQL, schema `wheeck`. Server version: Not detected.
  - Connection: the `main` entry of `config/querier.local.json`, with keys `driver`, `server`, `port`, `database`, `user`, `password`. The template `config/querier.json` points at `127.0.0.1:5432`, database `postgres`, user `wheeck`.
  - Client: `pyodbc` 5.5.0 over the psqlODBC driver (template driver name `PostgreSQL ANSI`), wrapped by `Querier` in `src/core/querier.py`. There is no ORM. Raw SQL constants live in `src/wheeck/constants.py`.
  - Transactions:
    - `Querier(save_changes=False)` turns autocommit off. `doc_scanner()` in `src/wheeck/recording_docs.py` runs one transaction per PDF and commits or rolls it back through `Querier.save_changes()`.
    - Only `save_warning()` opens an autocommit connection: with no `querier` passed it opens one with `save_changes=True`. `check_gaps()`, `check_duplicate()` with no `querier` passed and the overview generators (`generate_overview()`, `generate_summary()`, `generate_current()`) open their own connections with the default `save_changes=False`, so autocommit is off.
  - Connections are opened per function call and closed in `Querier.__del__`. There is no pool.
  - Objects, installed in this order with the psql `\ir` meta-command (sequence in the README):
    - Schema: `sql/schema/wheeck.sql` runs `DROP SCHEMA IF EXISTS wheeck CASCADE`, then recreates the schema. Re-running the install deletes all existing data.
    - Tables:
      - `wheeck.delivery` (`sql/tables/delivery.sql`)
      - `wheeck.delivery_history` (`sql/tables/delivery_history.sql`). It has the same columns as `wheeck.delivery`, and `sql/views/vw_delivery_gap.sql` unions the two. Nothing in `src/` writes to it.
      - `wheeck.delivery_warning` (`sql/tables/delivery_warning.sql`)
      - `wheeck.delivery_discard` (`sql/tables/delivery_discard.sql`)
    - Function and trigger: `wheeck.sync_discard_status()` and `trg_sync_discard_status`, which copy `delivery_warning.status` to `delivery_discard.status` for `DISCARD` messages (`sql/functions/sync_discard_status.sql`, `sql/triggers/trg_sync_discard_status.sql`).
    - Role: login role `wheeck` with `SELECT, INSERT, UPDATE` and execute/sequence grants on the schema (`sql/roles/wheeck.sql`). The script contains a plaintext role password. Replace the literal before install, put the new value in `config/querier.local.json`, and never copy it into docs.
    - Views:
      - `wheeck.vw_discard_message` (`sql/views/vw_discard_message.sql`)
      - `wheeck.vw_gap_message` (`sql/views/vw_gap_message.sql`)
      - `wheeck.vw_delivery_gap` (`sql/views/vw_delivery_gap.sql`)
      - The first two parse the warning `message_text` written by `MessageGenre` in `src/wheeck/recording_docs.py`. That text format is a contract between the Python code and the SQL views.
  - Backup: at the end of every run in which at least one file name matched `PATTERN_WORKING_DOC`, even if that document rolled back, `run()` executes `CMD_BACKUP_DB` from `src/wheeck/constants.py` through `subprocess.run(..., shell=True)`.
    - The command is `pg_dump -U postgres -h 127.0.0.1 -p 5432 -d postgres -n wheeck -w -c -F c` and writes `scheme/wheeck.bak.dump`.
    - The new dump replaces `scheme/wheeck.dump` only if it is not smaller than the old one. Otherwise it is deleted.
    - The superuser login, host and port are hardcoded and ignore `config/querier.local.json`.
    - `-w` never prompts, so authentication must work without one: a pgpass file, `PGPASSWORD`, or a `pg_hba.conf` rule that needs no password.
- SQLite: `LowQuerier` in `src/core/querier.py` (stdlib `sqlite3`) is exported but the app never uses it.

**File Storage:**
- Local filesystem only. All paths are defined in `src/wheeck/constants.py`.
  - Input: `DDTs/*.pdf`, matching `PATTERN_WORKING_DOC` (`YYYY_MM_DDT_NNNN_NNNN[_PNNN...].pdf`).
    - While a document is processed it is renamed to `*.recording.pdf`.
    - On success it moves to `DDTs/recorded/*.recorded.pdf`.
    - On failure it gets its original name back, or `*.failed.pdf` if even that rename fails.
  - Discards: single-page PDFs written to `DDTs/discarded/<doc>_PNNN.pdf` by `discard_doc()`. To re-process one, drop it back into `DDTs/`.
  - Optional backup copy: each recorded non-discard PDF is copied to `backup_dir` from `config/wheeck.local.json`.
  - Excel output, written by `src/wheeck/overview_docs.py` from templates in `scheme/`:
    - `res/YYYY_MM.xlsx` (monthly overview, template `scheme/overview.xlsx`)
    - `res/YYYY_TRIPS.xlsx` (yearly trip summary, template `scheme/summary.xlsx`)
    - Locally, `res/` and `scheme/overview.xlsx` are symlinks to a folder outside the repo.
  - DB dumps: `scheme/wheeck.dump` and `scheme/wheeck.bak.dump`, both gitignored as `*.dump`.

**Caching:**
- None external.
- In-process cache: `Querier._cache_config` holds the parsed DB config per file and connection name (`src/core/querier.py`).
- Database-backed cache: the `wheeck.delivery.distance` column acts as a persistent geocoding cache. It is looked up by `delivery_city` through `QUERY_GET_DISTANCE`.

## Authentication & Identity

**Auth Provider:**
- None. This is a single-user local batch tool with no end-user authentication.
  - Implementation, service credentials only:
    - The ORS API key comes from `config/wheeck.local.json`.
    - The PostgreSQL user and password for the `wheeck` role come from `config/querier.local.json`.
    - `pg_dump` connects as the `postgres` superuser without a prompt, through pgpass, `PGPASSWORD` or a passwordless `pg_hba.conf` rule.

## Monitoring & Observability

**Error Tracking:**
- None. There is no Sentry or any other external service.
- Unhandled exceptions are caught in `src/main.py` and written with `logger.exception('unhandled exception')`.

**Logs:**
- Python `logging`, configured by `get_logger()` in `src/core/common.py`.
  - The file handler writes INFO and above to `log/YYYY_MM_DD.log`, one file per day. Format: `%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s - %(message)s`.
  - The console handler writes ERROR and above.
  - `src/geo/geo.py`, `src/wheeck/overview_docs.py` and `src/wheeck/recording_docs.py` create their logger with `get_logger(PATH_LOG, __name__)`. `src/main.py` calls `get_logger(wheeck.PATH_LOG)` with no name, so its logger uses the default name `main`.
- There is no rotation or retention. Daily files of about 0.5-0.8 MB pile up in `log/` (gitignored).

## CI/CD & Deployment

**Hosting:**
- A local Windows workstation, run by hand or from an external scheduler. No scheduler or task definition is committed.

**CI Pipeline:**
- None. There is no `.github/`, other CI config or pre-commit hook.

## Environment Configuration

**Required env vars:**
- None for the Python app.
- Optional: `PGPASSWORD` for the `pg_dump` backup, unless a pgpass file or a passwordless `pg_hba.conf` rule is used.

**Required config files (gitignored, copied from the committed templates):**
- `config/wheeck.local.json`: keys `ors_api_key`, `departure_coords`, `members`, `backup_dir`. Template: `config/wheeck.json`.
- `config/querier.local.json`: the `main` connection entry. Template: `config/querier.json`.

**Secrets location:**
- In the `config/*.local.json` files, which `.gitignore` excludes (`/config/*.local.json`). The committed templates hold `null` placeholders.
- Exception: `sql/roles/wheeck.sql` is committed with a literal password for the `wheeck` role.
- `.idea/dataSources.local.xml` (gitignored) may hold IDE database credentials.

## Webhooks & Callbacks

**Incoming:**
- None. The app has no network listener. Input arrives as PDF files dropped into `DDTs/`.

**Outgoing:**
- None, apart from the synchronous OpenRouteService REST calls above.

---

*Integration audit: 2026-11-22*
