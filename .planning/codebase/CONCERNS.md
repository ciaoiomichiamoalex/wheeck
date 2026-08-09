# Codebase Concerns

**Analysis Date:** 2026-08-09

## Tech Debt

**External `core` package linked via filesystem symlink, not a managed dependency:**
- Issue: `src/wheeck/recording_docs.py`, `src/wheeck/overview_docs.py`, and `src/geo/geo.py` all do `from core import ...`, but there is no `core` package inside this repository or in `requirements.txt`. `src/core` is a symlink (`src/core -> /c/source/core/src/core`) pointing at a sibling, independently-versioned sister project on the local disk.
- Files: `src/core` (symlink), `src/wheeck/recording_docs.py:16`, `src/wheeck/overview_docs.py:8`, `src/geo/geo.py:6`
- Impact: The project cannot run, be packaged, or be cloned/built on another machine without also cloning `C:\source\core` at the exact same relative path. `requirements.txt` (`openpyxl`, `openrouteservice`, `pyodbc`, `pypdfium2`) omits `pyodbc`'s consumer (`core.querier.Querier`) dependency chain entirely from the documented contract, and any change made to the sibling `core` project silently changes `wheeck` behavior with no version pinning.
- Fix approach: Vendor `core` as a proper installable package (e.g. publish to a private index or use a pinned git dependency in `requirements.txt`), or inline the small amount of functionality (`decode_json`, `get_logger`, `Querier`) directly into `wheeck`.

**README.md is out of date with the actual module layout:**
- Issue: `README.md` describes `common.py` and `querier.py` as if they live in this project ("shared functions across the project" / "wrapper for pyodbc functions"), but those files actually live in the external `core` sibling project (`C:\source\core\src\core\common.py`, `C:\source\core\src\core\querier.py`), not in `wheeck`.
- Files: `README.md`
- Impact: New contributors will look for `common.py`/`querier.py` inside `wheeck` and not find them, and won't realize a second repository must be present on disk.
- Fix approach: Update `README.md` to explicitly document the dependency on the sibling `core` project and its location, or remove the description once `core` is vendored/packaged properly.

**New database connection opened on almost every function call instead of being reused/pooled:**
- Files: `src/wheeck/recording_docs.py` (`doc_scanner` line 107, `check_duplicate` line 367, `check_gaps` line 387, `save_warning` line 434), `src/wheeck/overview_docs.py` (`generate_overview` line 25, `generate_summary` line 69, `generate_current` line 102)
- Issue: Each of these functions independently constructs its own `Querier(cfg_in=PATH_CFG, ...)`, which opens a brand-new `pyodbc.connect(...)` (see `C:\source\core\src\core\querier.py:74`). During `doc_scanner`'s per-page loop, `check_duplicate` and `save_warning` each open (and rely on `del` to close) a fresh connection on top of the already-open `doc_scanner` connection.
- Impact: Unnecessary connection/auth overhead per page scanned (hundreds of pages per document, see `DDTs/recorded/*.pdf` naming ranges of ~300 pages), and increased risk of exhausting the DB's max-connections limit under load.
- Fix approach: Pass a single shared `Querier` instance into `check_duplicate`/`check_gaps`/`save_warning` rather than constructing new ones, or introduce connection pooling in `core.querier.Querier`.

**Connection lifecycle relies on `__del__` and explicit `del querier` rather than context managers:**
- Files: `C:\source\core\src\core\querier.py:87-89` (`__del__` closes cursor/connection), `src/wheeck/recording_docs.py:325,376,395,441` (`del querier`), `src/wheeck/overview_docs.py:59,95,110` (`del querier`)
- Issue: `Querier` does not implement `__enter__`/`__exit__`; cleanup depends on CPython's refcount-based garbage collection triggering `__del__` when the local variable is deleted or goes out of scope. If an exception is raised before the `del querier` statement (as it was — see Known Bugs below), the connection may remain open longer than intended, and the object model gives no guarantee of *when* `__del__` runs on other Python implementations.
- Fix approach: Add `__enter__`/`__exit__` to `Querier` and use `with Querier(...) as querier:` at each call site.

**`generate_summary` hardcodes exactly two vehicle columns:**
- Files: `src/wheeck/constants.py:162-214` (`QUERY_GET_SUMMARY`, two `vehicle = ?` placeholders producing `summary0`/`summary1`), `src/wheeck/overview_docs.py:62-95` (`generate_summary`)
- Issue: `generate_summary` builds `vehicles = sorted([e['vehicle'] for e in members if e['vehicle']])` from `config/wheeck.json` and always calls `querier.run(QUERY_GET_SUMMARY % ..., year, *drivers, *vehicles)`. `QUERY_GET_SUMMARY` is only written to accept exactly two vehicle parameters (`summary0`, `summary1` CTEs). The current fleet configuration in `config/wheeck.json` happens to have exactly two vehicles (`ES745WH`, `FC065ZW`), so this currently works, but adding or removing a vehicle from `members` in `config/wheeck.json` will break the query (too few/many `?` placeholders bound) without any validation or clear error message.
- Fix approach: Generalize `QUERY_GET_SUMMARY` to `UNPIVOT`/dynamically join over an arbitrary vehicle list, or add an explicit guard in `generate_summary` that raises a clear error when `len(vehicles) != 2`.

**Mutable-looking default argument evaluated once at import time:**
- Files: `src/wheeck/recording_docs.py:96` — `def doc_scanner(working_doc: str | Path, job_begin: datetime = datetime.now()) -> tuple[int, int]:`
- Issue: `datetime.now()` as a default parameter value is evaluated exactly once, when the module is imported, not on every call. Every call to `doc_scanner(doc)` without an explicit `job_begin` argument will reuse the same "start of process" timestamp rather than the true current time when the page was scanned.
- Impact: Currently not triggered because `run()` (`src/wheeck/recording_docs.py:460`) always passes an explicit `job_begin = datetime.now()`, but any future caller (tests, REPL usage, a new entry point) that omits the argument will silently get a stale `recording_date` on the `Delivery` records it inserts.
- Fix approach: Change the default to `None` and set `job_begin = job_begin or datetime.now()` inside the function body.

**Regex-based PDF text extraction is tightly coupled to one supplier's DDT layout:**
- Files: `src/wheeck/constants.py:19-23` (`PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`, `PATTERN_CITY_SX`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`)
- Issue: All extraction is driven by five large, layout-specific regular expressions matching literal Italian labels (`Luogo di consegna`, `Luogo di partenza`, `Peso soggetto accisa`, `Targa automezzo`) and exact whitespace/`\r\n` sequences produced by one PDF generator. Commit `3cde393` ("Fix regex for CITY_SX pattern to handle additional characters like commas") shows this has already broken in production once due to an unexpected character in an address field.
- Impact: Any change to the source system's PDF template (spacing, added fields, punctuation in addresses) will cause `PATTERN_CITY_DX`/`PATTERN_CITY_SX`/etc. to stop matching, silently discarding valid delivery pages into `DDTs/discarded/` until someone notices and patches the regex.
- Fix approach: Add a small fixture-based regression test suite (see Test Coverage Gaps) that runs the current patterns against a corpus of representative PDF text extracts, so layout drift is caught before it reaches production discards.

## Known Bugs

**Unhandled `pyodbc.Error` crashes the whole batch job when a page's city pattern fails to match:**
- Symptoms: The entire `run()` job aborts with an unhandled exception (`ERROR - unhandled exception`, caught only by the top-level `try/except` in `src/main.py:7-8`, which just logs and exits) instead of gracefully discarding the single bad page and continuing.
- Files: `src/wheeck/recording_docs.py:270-278` (the `elif not delivery.distance:` branch), `C:\source\core\src\core\querier.py:107-125` (`Querier.run`)
- Trigger: Confirmed in `log/2026_05_17.log` (line 1354 onward) on `2026_04_DDT_0955_1291.pdf`, page 93: `PATTERN_CITY` failed to match (both `PATTERN_CITY_DX` and `PATTERN_CITY_SX`), leaving `delivery.delivery_city = None` and `discarded_pages.on_error = 'PATTERN_CITY'`. Execution still falls into `elif not delivery.distance:` (line 270) because that branch is only gated on `not delivery.distance`, **not** on `discarded_pages.on_error`, so it calls `querier.run(QUERY_GET_DISTANCE, delivery.delivery_city)` with `delivery.delivery_city = None`. Inside `Querier.run` (`C:\source\core\src\core\querier.py:120-124`), the condition `args and any(val is not None for val in args)` evaluates `False` when the only argument is `None`, so the code falls through to `self._cursor.execute(query)` **without** binding any parameter — but `QUERY_GET_DISTANCE` still contains a `?` placeholder (`src/wheeck/constants.py:68-73`). The ODBC driver then raises `pyodbc.Error: ('07002', "[07002] Parameters exist but IPD isn't set. Please call SQLDescribeParam() (32) (SQLExecDirectW)")`, which is never caught, killing the whole `run()` loop for every remaining page/document in that batch.
- Workaround: None automated; requires manually re-running the job after the offending page/document has been fixed or manually discarded, and any documents queued after the crashed one in that batch are left unprocessed until the next run.
- Fix approach: Gate the `elif not delivery.distance:` distance-lookup branch on `discarded_pages.on_error` (skip entirely when a page is already discarded), and/or fix `Querier.run` so it never silently drops a bound parameter that the query text still expects — e.g. only skip parameter binding when `args` is empty, not when values happen to be `None`.

**Interrupted job leaves a document stuck in `.recording.pdf` state, invisible to the next run:**
- Symptoms: When `run()` crashes mid-document (as in the bug above), the file has already been renamed from `<name>.pdf` to `<name>.recording.pdf` (`src/wheeck/recording_docs.py:463-464`) before scanning starts, and is only renamed back to `<name>.recorded.pdf` (line 477) after the whole document finishes without a fatal error. A crash midway leaves the file as `<name>.recording.pdf` in `DDTs/`.
- Files: `src/wheeck/recording_docs.py:449-477` (`run`), `src/wheeck/constants.py:16` (`PATTERN_WORKING_DOC`)
- Trigger: Any exception raised inside `doc_scanner` for a page in the middle of a multi-page document.
- Workaround: Operator must manually rename the `.recording.pdf` file back to its original name (or move it to `DDTs/recorded/`) before the next run, since `PATTERN_WORKING_DOC` (`^\d{4}_\d{2}_DDT_\d{4}_\d{4}(?:_P\d{3})*\.pdf$`) does not match filenames containing `.recording`, so `run()` silently skips (`logger.warning('error on PATTERN_WORKING_DOC ...')`) any such file on subsequent runs rather than resuming it.
- Fix approach: Wrap the per-document work in `run()` in a `try/finally` that always restores the original filename on failure, or make `doc_scanner` resumable by tracking last-successful-page in the database.

## Security Considerations

**Live third-party API key committed to the tracked configuration file:**
- Risk: `config/wheeck.json` is tracked by git (not covered by `.gitignore`, which only excludes `/DDTs/`, `/log/`, `/res/`, `*.dump`, `*.xlsx`, `/.venv/`, `/.idea/`, `__pycache__/`) and currently contains a live OpenRouteService API key value in its `ors_api_key` field, along with real GPS departure coordinates and real vehicle plate/driver names in `members`.
- Files: `config/wheeck.json`
- Current mitigation: None — the file is committed as-is; `git status` at analysis time shows it as locally modified (uncommitted change), meaning the previously committed version(s) already contain historical secret/PII values in git history.
- Recommendations: Move `ors_api_key` (and any other secret) out of the versioned `config/wheeck.json` into an untracked file or environment variable, add `config/wheeck.json` (or a `*.local.json` variant) to `.gitignore`, commit only a `config/wheeck.example.json` template with placeholder values, and rotate/revoke the currently-exposed OpenRouteService key since it has been committed to git history.

**Database credentials committed to the tracked configuration file:**
- Risk: `config/querier.json` is tracked by git and contains connection fields `name`, `driver`, `server`, `port`, `database`, `user`, `password` (keys confirmed via structural inspection; values not read/reproduced here per policy) consumed directly by `pyodbc.connect(...)` in `C:\source\core\src\core\querier.py:74-82`.
- Files: `config/querier.json`
- Current mitigation: None — file has been tracked since commit `61bdf0c` ("wheeck v1.0.0") and is not excluded by `.gitignore`.
- Recommendations: Treat `config/querier.json` as a secret: remove it from version control (`git rm --cached`, add to `.gitignore`), rotate the database password, and load credentials from environment variables or an untracked local file at runtime instead.

**Local shell command execution with `shell=True` for database backup:**
- Risk: `CMD_BACKUP_DB` (`src/wheeck/constants.py:25`) is an f-string building a `pg_dump` command line that gets executed via `subprocess.run(CMD_BACKUP_DB, shell=True, ...)` in `src/wheeck/recording_docs.py:489`. The path `PATH_SCHEME` is interpolated directly into the shell command string without escaping.
- Files: `src/wheeck/constants.py:25`, `src/wheeck/recording_docs.py:487-500`
- Current mitigation: `PATH_SCHEME` is derived from a fixed, code-controlled path (`PATH_PRJ / 'scheme'`), not user input, so exploitability is low in the current single-operator usage, but the pattern (dynamic string + `shell=True`) is fragile if the project path or scheme directory name ever becomes configurable.
- Recommendations: Use `subprocess.run([...], shell=False)` with an argument list instead of a shell string, and avoid embedding filesystem paths into a shell-interpreted command.

## Performance Bottlenecks

**Synchronous 1.5s sleep per page requiring a new geocoding lookup:**
- Problem: `time.sleep(1.5)` in `src/wheeck/recording_docs.py:280` throttles every OpenRouteService distance calculation to respect the "40 calls/min" comment on the same line, executed serially once per page whose delivery city isn't already cached in the database (`QUERY_GET_DISTANCE` check at line 273).
- Files: `src/wheeck/recording_docs.py:270-280`
- Cause: `calculate_distance()` (`src/wheeck/recording_docs.py:62-72`) makes two blocking HTTP calls per new city (`GeoMap.search` for geocoding + `GeoMap.get_distance_from_coords` for routing, `src/geo/geo.py:27-103`), and the whole `doc_scanner` loop is fully sequential — one page at a time, one document at a time (`src/wheeck/recording_docs.py:455` `for doc in docs:`).
- Improvement path: Since distances are cached per `delivery_city` in the database (`QUERY_GET_DISTANCE`), pre-resolve and batch all distinct new cities in a document before the page loop, or process the geocoding lookups for a batch of pages in parallel while respecting the API rate limit with a token-bucket rather than a flat per-page `sleep`.

**Excel overview files fully reloaded and rewritten with `openpyxl` on every recording batch:**
- Problem: `generate_overview` (`src/wheeck/overview_docs.py:15-59`) and `generate_summary` (`src/wheeck/overview_docs.py:62-95`) reload the entire `.xlsx` workbook via `openpyxl.load_workbook(...)`, rewrite the affected rows, and re-save the entire file for every month/year touched, called once per `run()` invocation via `generate_current()` (`src/wheeck/overview_docs.py:98-110`).
- Files: `src/wheeck/overview_docs.py`
- Cause: `openpyxl` has no partial-write mode; the whole workbook (potentially years of rows in `res/*_TRIPS.xlsx`, per `scheme/summary.xlsx`) is parsed and re-serialized on disk each run.
- Improvement path: Acceptable at current single-operator scale (monthly-ish batches), but if run frequency or workbook size grows, consider writing to a lightweight CSV/DB-backed store and generating the Excel presentation layer on demand instead of on every ingest.

## Fragile Areas

**`recording_docs.py` `doc_scanner` function (500-line module, ~230-line function):**
- Files: `src/wheeck/recording_docs.py:95-326`
- Why fragile: A single function handles regex extraction for five distinct fields, vehicle/driver fuzzy-matching, discard-record reconciliation, duplicate detection, distance calculation/caching, delivery persistence, and warning-message bookkeeping, all inside one nested branching flow with shared mutable state (`discarded_pages`, `delivery`). The known bug above (distance lookup running on discarded pages) is a direct symptom of this branching complexity — a code path that "shouldn't" run given the surrounding `if` did in fact run.
- Safe modification: Any change to the extraction/branching order should be manually traced against all four discard reasons (`PATTERN_DOC_NUMBER`, `PATTERN_CITY`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`, `CHECK_DUPLICATE`, `INSERT_DELIVERY`) to confirm the "discarded page" path is fully short-circuited from all further per-page side effects (distance calc, DB insert, warning creation).
- Test coverage: None — see Test Coverage Gaps below; there are no automated tests for this function at all, so behavior changes are currently validated only by production log-watching.

**Vehicle/driver fuzzy-matching via `difflib.SequenceMatcher` with a fixed 50% threshold:**
- Files: `src/wheeck/recording_docs.py:399-420` (`check_similarity`), called from `doc_scanner` lines 206 and 220
- Why fragile: `check_similarity` picks the "most similar" known vehicle/driver string using plain character-sequence similarity ratio with a hardcoded `> 0.5` acceptance threshold and no logging of the runner-up candidates. Two similarly-named drivers (or OCR/regex noise producing a garbled name) could silently resolve to the wrong person, and the only signal that something went wrong is a `save_warning(MessageGenre.WARNING, ...)` database row, not a hard failure.
- Safe modification: Adding a new vehicle or driver with a name similar to an existing one should be manually checked against `check_similarity` for potential collisions before being added to `config/wheeck.json`'s `members` list.
- Test coverage: None — no unit tests exercise this function's edge cases (empty string, tie scores, near-threshold scores).

## Scaling Limits

**Fleet size hardcoded to two vehicles in the yearly summary query:**
- Current capacity: Exactly 2 entries in `config/wheeck.json`'s `members` list with a non-null `vehicle` field (`ES745WH`, `FC065ZW`); a third entry has `"vehicle": null` (driver-only, e.g. `FIANDINO DOMENICO`).
- Limit: `QUERY_GET_SUMMARY` (`src/wheeck/constants.py:162-214`) is structurally built around exactly two `vehicle = ?` CTEs (`summary0`, `summary1`) side-by-side in the output columns; adding a third vehicle to `config/wheeck.json` breaks `generate_summary` (see Tech Debt above) because the parameter count generated by `*vehicles` (3) will not match the two positional placeholders the query actually has.
- Scaling path: Rewrite `QUERY_GET_SUMMARY` to pivot dynamically over an arbitrary number of vehicles (e.g. one row per vehicle rather than paired columns), and update `generate_summary`/the `scheme/summary.xlsx` template's `viaggi` sheet layout accordingly.

## Dependencies at Risk

**`core` package is an unversioned, unpublished local dependency:**
- Risk: `Querier`, `decode_json`, and `get_logger` (used throughout `src/wheeck/*.py` and `src/geo/geo.py`) come from `C:\source\core\src\core`, a separate git repository (`C:\source\core`) linked only via an OS-level symlink at `src/core`, with no version constraint, changelog, or compatibility contract with `wheeck`.
- Impact: Any breaking change made in the `core` sibling repository (e.g. a `Querier.run` signature change) immediately and silently affects `wheeck` with no dependency-resolution warning, and the two repositories' git histories are not linked, so there is no way to know from `wheeck`'s history alone which `core` version a given `wheeck` commit was built/tested against.
- Migration plan: Package `core` (e.g. `pip install -e ../core` recorded in `requirements.txt`, or a private package index entry with an explicit version pin) so the dependency is explicit, reproducible, and versioned.

## Missing Critical Features

**No automatic recovery/resume logic after a mid-batch crash:**
- Problem: As shown in the Known Bugs section, an unhandled exception during `run()` aborts the entire batch, leaves the in-progress document in an inconsistent `.recording.pdf` filename state, and does not resume from where it left off — the operator must intervene manually and rerun.
- Blocks: Unattended/scheduled execution of `wheeck.run()` (e.g. via a cron/Task Scheduler job) is not safe without an external monitoring/retry layer, since any single malformed PDF page can halt processing of every subsequent document in that run.

## Test Coverage Gaps

**No automated tests exist anywhere in the repository:**
- What's not tested: There is no `tests/` directory, no `pytest`/`unittest` files, and no test runner configuration (`pytest.ini`, `tox.ini`, CI workflow) in `wheeck` or in the sibling `core` project. All five files under `src/` (`src/main.py`, `src/wheeck/constants.py`, `src/wheeck/overview_docs.py`, `src/wheeck/recording_docs.py`, `src/geo/geo.py`) plus `core.querier.Querier`/`core.common.decode_json`/`core.common.get_logger` have zero test coverage.
- Files: entire `src/` tree; confirmed absence via directory listing of the repository root and `src/`.
- Risk: Regex changes (`PATTERN_CITY_DX`, `PATTERN_CITY_SX`, etc.), the branching logic in `doc_scanner`, and the summary/overview Excel generation can all regress silently; the only current detection mechanism is production log inspection after the fact (as seen with the `2026_05_17.log` crash and the `3cde393` regex hotfix).
- Priority: High — particularly for `doc_scanner`'s discard/duplicate/distance branching (`src/wheeck/recording_docs.py:95-326`) and the five regex patterns in `src/wheeck/constants.py:19-23`, since both have already caused a production incident and a hotfix respectively.

---

*Concerns audit: 2026-08-09*
