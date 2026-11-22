<!-- generated-by: gsd-doc-writer -->
# Configuration

wheeck takes its settings from two JSON files in `config/`. The Python code reads no environment variables. Everything else (paths, the backup command, API call tuning, Excel styling) is hardcoded in `src/wheeck/constants.py` and the modules that use it.

## Environment variables

The application reads no environment variables. There are no `os.environ` or `os.getenv` calls under `src/`.

The only environment-level setting involved belongs to the external `pg_dump` command that `run()` executes at the end of each run:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PGPASSWORD` | Optional | unset | Read by `pg_dump`, not by wheeck. The backup command runs with `-w` (never prompt), so the `postgres` login must authenticate without a prompt. That can come from `PGPASSWORD`, a pgpass file or a `pg_hba.conf` rule that needs no password. |

<!-- VERIFY: how the host running wheeck authenticates the postgres login for pg_dump (PGPASSWORD, pgpass file, or pg_hba.conf trust rule) -->

## Config file format

Two templates are committed with placeholder values. The application never reads the templates. It reads only their gitignored `*.local.json` copies, whose paths are fixed in `src/wheeck/constants.py`:

| Template (committed) | Real file (gitignored) | Constant |
|----------------------|------------------------|----------|
| `config/wheeck.json` | `config/wheeck.local.json` | `PATH_CFG_PRJ` |
| `config/querier.json` | `config/querier.local.json` | `PATH_CFG_DB` |

`.gitignore` excludes `/config/*.local.json`, so secrets stay out of git. To set up a checkout:

```bash
cp config/wheeck.json config/wheeck.local.json
cp config/querier.json config/querier.local.json
```

Then fill in the real values as described below. Both files are parsed by `decode_json` in `src/core/common.py` as UTF-8 JSON.

### `config/querier.local.json`: database connection

The file is a list of named connection entries. A single object also works, because `decode_json` wraps it in a list. The application always uses the entry whose `name` is `main` and ignores any others. Template:

```json
[
  {
    "name": "main",
    "driver": "PostgreSQL ANSI",
    "server": "127.0.0.1",
    "port": 5432,
    "database": "postgres",
    "user": "wheeck",
    "password": null
  }
]
```

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `name` | **Required** | none | Entry name. Must be `main`, because every `Querier(cfg_in=PATH_CFG_DB)` call uses the default `conn_name='main'`. |
| `driver` | **Required** | none | ODBC driver name, as registered on the machine. The template's `PostgreSQL ANSI` is the Windows psqlODBC driver. `Querier` wraps the name in braces before passing it to `pyodbc.connect`. |
| `server` | **Required** | none | PostgreSQL host. |
| `port` | **Required** | none | PostgreSQL port. |
| `database` | **Required** | none | Database that contains the `wheeck` schema. |
| `user` | **Required** | none | Login role. The schema scripts create the role `wheeck` with `SELECT, INSERT, UPDATE` on the schema (`db/roles/wheeck.sql`). |
| `password` | **Required** | none | Password of that role. It must match the password that `db/roles/wheeck.sql` sets when it creates the role. |

`Querier` keeps the parsed entry in the class attribute `Querier._cache_config`. Within one process the file is read only once per file path and connection name, so edits apply from the next run.

### `config/wheeck.local.json`: project settings

A single JSON object. Template:

```json
{
  "ors_api_key": null,
  "departure_coords": [0.0, 0.0],
  "members": [
    {
      "vehicle": null,
      "driver": null
    }
  ],
  "backup_dir": null
}
```

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `ors_api_key` | **Required** | none | OpenRouteService API key, passed to `openrouteservice.Client(key=...)` in `src/geo/geo.py`. It is used to geocode delivery cities and compute driving distances. |
| `departure_coords` | **Required** | none | Start point for every distance calculation, as a two-number list. It is passed to the ORS directions call next to the coordinates that `GeoMap.search` returns, which are GeoJSON `[longitude, latitude]`. Use the same `[longitude, latitude]` order. The `GeoMap.search` docstring says "latitude and longitude", but the API returns longitude first. |
| `members` | **Required** | none | List of `{"vehicle": ..., "driver": ...}` entries. They define the known vehicle plates and driver names. See [The `members` list](#the-members-list). |
| `backup_dir` | Optional | `null` (no backup copy) | Directory that receives a copy of every successfully recorded PDF. See [Defaults](#defaults). |

#### The `members` list

`members` is read in two places:

- `doc_scanner` in `src/wheeck/recording_docs.py` resolves the vehicle and driver of each DDT page.
- `generate_summary` in `src/wheeck/overview_docs.py` builds the yearly trip summary.

Rules that follow from the code:

- **Upper case.** The plate and driver extracted by `PATTERN_VEHICLE` are upper-cased before the comparison. `members` values must therefore be upper case to match exactly.
- **Plates.** `PATTERN_VEHICLE` captures the plate as exactly 7 word characters.
- **Fuzzy matching.** A value that does not match exactly is replaced by the most similar configured value when the `difflib` ratio is above 0.5 (`check_similarity`). Otherwise a warning is saved to `wheeck.delivery_warning`.
- **Default driver.** If the extracted driver cannot be resolved, the page gets the `driver` of the entry whose `vehicle` matches the page's vehicle. Pair each vehicle with its usual driver.
- **Drivers without a vehicle.** An entry with `"vehicle": null` declares a driver with no own vehicle. Deliveries by these drivers are excluded from the yearly summary (`vehicle_driver NOT IN (...)` in `QUERY_GET_SUMMARY`).
- **Summary shape.** `QUERY_GET_SUMMARY` has exactly two `vehicle = ?` slots, and the vehicles are sorted alphabetically. The first vehicle fills the `_0` columns and the second fills the `_1` columns of `res/{year}_TRIPS.xlsx`. The summary only works with:
  - exactly two entries that have a `vehicle`. Any other count gives a parameter-count error.
  - at least one entry with `"vehicle": null`. With none, the query contains `NOT IN ()`, which is a PostgreSQL syntax error.

Example shape, with placeholder values:

```json
"members": [
  { "vehicle": "AB123CD", "driver": "MARIO ROSSI" },
  { "vehicle": "EF456GH", "driver": "LUCA BIANCHI" },
  { "vehicle": null, "driver": "PAOLO VERDI" }
]
```

## Required vs optional settings

There is no startup validation. Neither file is loaded when the process starts. Each value is read the first time the code needs it, so a missing or wrong setting fails at its first use, not at launch. If no file in `DDTs/` matches `PATTERN_WORKING_DOC`, a run reads neither config file.

| Setting | First use | Failure when missing or invalid |
|---------|-----------|---------------------------------|
| `config/querier.local.json` file | First `Querier(cfg_in=PATH_CFG_DB)`, while scanning the first document | `IOError: Querier: no config <path> found!` |
| `main` entry in `querier.local.json` | Same | `IOError: Querier: no config <main> found in <path>!` |
| Any `querier.local.json` key except `name` | Same | `KeyError` on the missing key. `driver`, `server`, `port`, `database`, `user` and `password` are read with `config['...']`. A missing `name` gives the `no config <main> found` error above, because `name` only filters entries in `decode_json`. |
| `config/wheeck.local.json` file | First new delivery city, or first page with a vehicle match | `FileNotFoundError` from `decode_json` |
| `ors_api_key` | First delivery city that has no stored distance in `wheeck.delivery` | A `null` key makes the OpenRouteService client raise `ValueError: No API key was specified...` |
| `departure_coords` | Same | `KeyError` if absent |
| `members` | First page where `PATTERN_VEHICLE` matches. Later, the yearly summary. | `KeyError` if absent. If there is no vehicle or no driver, `check_similarity` raises `IndexError`. This is the case for the unedited template, where both are `null`. |
| `backup_dir` | After each recorded document | Optional, read with `.get()` |

What a failure does depends on where it happens:

- **While a document is scanned.** This covers the DB config, `ors_api_key`, `departure_coords` and the `members` lookups. Nothing is saved for that document. If its transaction is already open, `doc_scanner` rolls it back. `run()` logs `JOB FAILED` and renames the PDF back to its original name in `DDTs/`, so it is retried on the next run.
- **After the document loop.** This covers `check_gaps()`, `generate_current()` and the `members` lookup in `generate_summary`. These steps are outside the per-document `try`, so an error ends the run. `src/main.py` logs it as `unhandled exception`, and the database backup step is skipped.

All errors go to the log file in `log/`. Only `ERROR` and higher also print to the console.

## Defaults

The only optional config key is `backup_dir`:

| Key | Default | Where it is read | Behaviour |
|-----|---------|------------------|-----------|
| `backup_dir` | `null` | `run()` in `src/wheeck/recording_docs.py` | When it is set, each recorded document is copied with `shutil.copy` to `{backup_dir}/{original file name}`. Discard-page files (names that match `PATTERN_DISCARD_DOC`) are not copied. |

`backup_dir` must point to an existing, writable directory. The copy runs after the document is committed and outside the per-document error handling. A bad path stops the run with an unhandled exception and leaves the committed PDF in `DDTs/` as `*.recording.pdf`. Later runs skip that name, because it does not match `PATTERN_WORKING_DOC`.

### Settings fixed in source

These settings are not configurable. Changing them means editing the code.

| Setting | Value | Location |
|---------|-------|----------|
| Config file paths | `config/wheeck.local.json`, `config/querier.local.json` | `PATH_CFG_PRJ`, `PATH_CFG_DB` in `src/wheeck/constants.py` |
| Project folders | `DDTs/`, `DDTs/discarded/`, `DDTs/recorded/`, `log/`, `res/`, `scheme/`, all relative to the repository root | `PATH_*` in `src/wheeck/constants.py` |
| Database backup | `pg_dump -U postgres -h 127.0.0.1 -p 5432 -d postgres -n wheeck -w -c -F c`, written to `scheme/wheeck.bak.dump` and then kept as `scheme/wheeck.dump` | `CMD_BACKUP_DB` in `src/wheeck/constants.py`, `run()` |
| Geocoding country | `IT` | `GeoMap.search` default in `src/geo/geo.py` |
| Routing | profile `driving-car`, snapping radius `500` m | `GeoMap.get_distance_from_coords` in `src/geo/geo.py` |
| ORS throttle | 1.5 s pause after each new city (the code comment cites a 40 calls/min limit) | `resolve_distances` in `src/wheeck/recording_docs.py` |
| Logging | one file per day `log/YYYY_MM_DD.log` at `INFO`. Console output at `ERROR` and above. | `get_logger` in `src/core/common.py` |
| Excel output | font `Arial`. Files `res/{year}_{MM}.xlsx` and `res/{year}_TRIPS.xlsx`, built from the templates `scheme/overview.xlsx` and `scheme/summary.xlsx`. | `OVERVIEW_*` in `src/wheeck/constants.py`, `src/wheeck/overview_docs.py` |

<!-- VERIFY: 40 calls/min is the OpenRouteService rate limit of the account behind ors_api_key -->

## Per-environment overrides

There is no per-environment mechanism:

- There are no `.env` files, no `NODE_ENV`-style switch and no development, staging or production config sets.
- Each checkout has exactly one `config/wheeck.local.json` and one `config/querier.local.json`. To point a checkout at another database or API key, edit those files.
- `config/querier.local.json` can hold several named entries, but the application always connects with `main`. Switching databases therefore means changing the `main` entry.
- The backup command ignores `config/querier.local.json`. It always connects as `postgres` to `127.0.0.1:5432`, database `postgres`. If the database is elsewhere, `pg_dump` fails, the run logs `error during backup database schema...` at `CRITICAL` and carries on. To fix this, edit `CMD_BACKUP_DB` in `src/wheeck/constants.py`.

<!-- VERIFY: PostgreSQL runs on the same machine as wheeck (127.0.0.1:5432), as both the template and CMD_BACKUP_DB assume -->
