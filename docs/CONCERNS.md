<!-- generated-by: codebase-mapper -->
# Codebase Concerns

**Analysis Date:** 2026-11-22

Each section is ordered by severity, most severe first. Line numbers refer to the files at HEAD. Statements about PostgreSQL behaviour come from the SQL text and were not checked against the live database.

## Tech Debt

**`Querier.run` drops every parameter when all of them are `None`:**
- Issue: `run()` calls `execute(query)` with no parameters when `args` is empty or when every value is `None`. A query with `?` placeholders and only `None` values therefore runs without bound parameters and pyodbc raises `07002 Parameters exist but IPD isn't set`.
- Files: `src/core/querier.py:120-124`
- Impact: Any caller that passes only `None` values crashes. Inside `doc_scanner` that crash rolls back the whole document, and `run()` then requeues it on every run. Today the callers guard against it: `resolve_distances` (`src/wheeck/recording_docs.py:133-138`) queries only regex-matched cities, and the gap lookup is guarded by `recording_docs.py:409`. The tracebacks in `log/2026_05_17.log` and `log/2026_07_05.log` show this crash on a distance lookup with a `None` city. Their line numbers do not match the code at HEAD.
- Fix approach: Choose the branch on `if args`, not on `any(val is not None ...)`. No caller passes `None` as a "no arguments" signal, so the change is safe.

**Connection lifecycle relies on `__del__`:**
- Issue: `Querier` has no `close()` method and no context manager. Connections close in `__del__` (`src/core/querier.py:87-89`), and callers release them with `del querier`. If `pyodbc.connect` fails inside `__init__`, `__del__` raises `AttributeError` on `self._cursor`, which prints an "Exception ignored" message.
- Files: `src/core/querier.py:74-89`, `src/wheeck/recording_docs.py:155, 440, 514, 533, 589`, `src/wheeck/overview_docs.py:59, 95, 110`
- Impact: The connection lifetime depends on CPython reference counting and traceback lifetimes. The comment at `recording_docs.py:437-439` documents that a failed document keeps its connection open until `run()` releases the exception.
- Fix approach: Add `close()`, `__enter__` and `__exit__` to `Querier`, use `with Querier(...) as querier:` at every call site, and make `__del__` a guarded fallback.

**Structured data stored as free text and parsed back in SQL:**
- Issue: `MessageGenre` (`src/wheeck/recording_docs.py:81-93`) formats document number, genre, date, page and source into `delivery_warning.message_text`. The views `sql/views/vw_discard_message.sql:4-33` and `sql/views/vw_gap_message.sql:4-13` then recover those fields with `STRPOS`/`SUBSTRING` and `::INTEGER`/`::DATE` casts.
- Files: `src/wheeck/recording_docs.py:81-93`, `sql/views/vw_discard_message.sql`, `sql/views/vw_gap_message.sql`, `sql/views/vw_delivery_gap.sql:37-39`, `src/wheeck/constants.py:138-156`
- Impact: Any change to a message template, or a value that contains a delimiter such as `, genre: `, breaks the parsing. A failed cast breaks the whole view, which breaks `QUERY_CHECK_GAPS` and the gap check in `run()`. `delivery_discard` already stores the same fields in typed columns, linked through `id_warning_message`.
- Fix approach: Build `vw_discard_message` as a join of `delivery_warning` with `delivery_discard`. For GAP rows, add nullable `document_number` and `document_year` columns to `delivery_warning`. Keep `message_text` for display only.

**`doc_scanner` is one function of about 280 lines:**
- Issue: One function does the pattern extraction, the fuzzy vehicle and driver resolution, discard recovery, duplicate detection, inserts, discard-PDF writing, gap closing and transaction compensation. Per-page error state lives in an anonymous class built with `type('DiscardedPages', ...)` (`recording_docs.py:194-202`). The `on_error` string is built with four copies of the same concatenation pattern (`recording_docs.py:226-227, 251-252, 263-264, 315-316`).
- Files: `src/wheeck/recording_docs.py:159-441`
- Impact: The function is hard to reason about and impossible to unit-test in parts. Every change risks the commit and rollback invariants documented in its comments.
- Fix approach: Extract a pure `extract_delivery(text, members) -> (Delivery, list[str])` with no I/O. Keep `doc_scanner` as the transactional shell. Replace the anonymous class with a plain list of error names.

**Positional coupling between `Delivery` and SQL:**
- Issue: `QUERY_INSERT_DELIVERY` receives `astuple(delivery)[:-1]` (`recording_docs.py:359`), and `QUERY_INSERT_DISCARD` receives the full `astuple(delivery)` (`recording_docs.py:391`). This only works while the `Delivery` field order (`recording_docs.py:34-47`) matches the column lists in `src/wheeck/constants.py:37-54, 103-121`. `vw_delivery_gap` unions `SELECT * FROM public.delivery` with `SELECT * FROM public.delivery_history` (`sql/views/vw_delivery_gap.sql:3-6`).
- Impact: Reordering or adding a `Delivery` field, or letting the two table definitions drift apart, silently shifts values into the wrong columns.
- Fix approach: Pass explicit field tuples, or name the columns in the view's `UNION ALL`.

**`delivery_history` is ignored by the duplicate and distance checks:**
- Issue: `delivery_history` (`sql/tables/delivery_history.sql`) is filled manually, and only `vw_delivery_gap` reads it. `QUERY_CHECK_DUPLICATE` and `QUERY_GET_DISTANCE` (`src/wheeck/constants.py:56-83`) query only `public.delivery`.
- Impact: After rows are archived, a re-dropped archived DDT is inserted into `delivery` again, and the cities of archived rows are geocoded again.
- Fix approach: Add `UNION ALL` over `delivery_history` to both queries, or document that archived DDTs must never be re-dropped.

**Configuration is read repeatedly and never validated:**
- Issue: `decode_json(PATH_CFG_PRJ)` runs once per page with a vehicle match (`recording_docs.py:277`), once per new city (`recording_docs.py:112-113`, which also builds a new `GeoMap` client each time), once per summary and once per backup. Missing keys raise `KeyError` deep in the pipeline.
- Files: `src/wheeck/recording_docs.py:112-113, 277, 635`, `src/wheeck/overview_docs.py:75`, `src/geo/geo.py:24`
- Fix approach: Load and validate `config/wheeck.local.json` once at the start of `run()`: check that `members` has at least one vehicle and at least one driver, and that `departure_coords` has two numbers. Pass the result down.

**Backup command is hardcoded:**
- Issue: `CMD_BACKUP_DB` (`src/wheeck/constants.py:27`) hardcodes the user `postgres`, host `127.0.0.1`, port `5432` and database `wheeck`. It ignores `config/querier.local.json`.
- Impact: A change of database host or port moves the application but not the backup, and nothing reports the mismatch.
- Fix approach: Build the `pg_dump` argument list from the querier config and call `subprocess.run([...])` without `shell=True`.

**No schema migrations, and the install is destructive:**
- Issue: The install sequence starts with `sql/database/wheeck.sql`, whose first line is `DROP DATABASE IF EXISTS wheeck;`. The `CREATE TABLE IF NOT EXISTS` statements suggest idempotence, but the database is always dropped first. `CREATE INDEX` statements have no `IF NOT EXISTS`. `CREATE USER wheeck` fails when the role exists, and `psql` continues past errors unless `ON_ERROR_STOP` is set.
- Files: `sql/database/wheeck.sql:1`, `sql/roles/wheeck.sql:9`, `sql/tables/*.sql`
- Impact: Running the install against the production database deletes every delivery, warning and history row. The only way to change the schema is a manual `ALTER` or a reinstall followed by a restore from `scheme/wheeck.dump`.
- Fix approach: Move the `DROP DATABASE` into a separate, explicitly named reset script. Run `\set ON_ERROR_STOP on` before the `\ir` lines. Keep numbered migration scripts for later changes.

**Dead code:**
- Files: `LowQuerier` (`src/core/querier.py:211-293`, exported from `src/core/__init__.py:2`, never used), `Querier.save_excel` (`src/core/querier.py:174-208`), `Querier.cursor` property (`src/core/querier.py:97-105`), `GeoMap.get_distance` (`src/geo/geo.py:54-76`), `Delivery.calculate_distance` (`src/wheeck/recording_docs.py:63-67`, referenced only in a comment), and the `!= -1` check at `recording_docs.py:391`, which its own comment calls dead code.
- Fix approach: Delete them. `LowQuerier` is the only code that requires Python 3.12 or later (`sqlite3.connect(autocommit=...)`).

**Packaging and import structure:**
- Issue: There is no `pyproject.toml`, and versions are duplicated by hand (`src/core/__init__.py:4`, `src/geo/__init__.py:3`, `src/wheeck/__init__.py:6`). `src/geo/geo.py:7` imports `PATH_LOG` from `wheeck`, and `wheeck` imports `geo`, which is a circular import.
- Impact: `import geo` on its own fails with `ImportError: cannot import name 'GeoMap' from 'geo'`. This was reproduced with `.venv` from `src/`. The import works only when `wheeck` is imported first, as `src/main.py` does.
- Fix approach: Pass the log path into `GeoMap`, or move `PATH_LOG` into `core`, so that `geo` no longer depends on `wheeck`.

**Required runtime assets are not under version control:**
- Issue: `*.xlsx` in `.gitignore:10` excludes the Excel templates `scheme/overview.xlsx` and `scheme/summary.xlsx`, which `src/wheeck/overview_docs.py:29, 73` require. The code also hardcodes their sheet names (`consegne`, `cifre`, `litri`, `cifre manuale`, `litri manuale`, `viaggi`).
- Impact: A fresh clone cannot generate overviews or summaries, and template changes have no history.
- Fix approach: Add `!/scheme/*.xlsx` to `.gitignore` and commit the templates.

**Constraint name does not match its definition:**
- Issue: `uq_delivery_document_number_genre_year` is `UNIQUE (document_number, document_genre, document_date)` (`sql/tables/delivery.sql:18-19`, same in `delivery_history.sql:18-19`). It constrains the full date, while `QUERY_CHECK_DUPLICATE` (`src/wheeck/constants.py:63-65`) checks the year.
- Impact: The database accepts two documents with the same number and genre in the same year if their dates differ. Only the Python-side check catches this case.
- Fix approach: Use a unique index on `(document_number, document_genre, EXTRACT(YEAR FROM document_date))`, or rename the constraint.

## Known Bugs

**A failed geocode or route stores a NULL distance for the city permanently:**
- Symptoms: Every later delivery to that city gets `distance = NULL`, and the city is never geocoded again.
- Files: `src/wheeck/recording_docs.py:144-153`, `src/wheeck/constants.py:78-83`, `src/geo/geo.py:44-52, 95-103`
- Trigger: `GeoMap.search` or `get_distance_from_coords` returns `None` on an ORS error, on a timeout, or on a 429 or 503 that the client keeps retrying past its 60 s `retry_timeout`. The delivery is then inserted with a NULL distance. On the next run, `SELECT DISTINCT distance ... WHERE delivery_city = ?` returns one row, `NULL`, and `rows == 1` is true, so the NULL is reused. If a city has both a NULL row and a non-NULL row, `rows == 2`, so the city is geocoded again on every document. `log/2025_01_12.log` records one such failure: ORS 404, error code 2010, "Could not find routable point within a radius of 500.0 meters".
- Workaround: Update `public.delivery.distance` for the city by hand.
- Fix: Add `AND distance IS NOT NULL` to `QUERY_GET_DISTANCE` and use `rows >= 1`. Consider a larger `radiuses` value at `src/geo/geo.py:93`.

**The trip summary puts NULL-distance deliveries first:**
- Symptoms: In `{year}_TRIPS.xlsx`, the trip chosen for a day and vehicle is a delivery with unknown distance, not the farthest one.
- Files: `src/wheeck/constants.py:179-182`
- Trigger: `ROW_NUMBER() OVER (PARTITION BY delivery_date, vehicle ORDER BY distance DESC)`. PostgreSQL sorts NULLs first under `DESC` by default, so a NULL distance (see the previous bug) gets `rnk = 1`.
- Fix: Use `ORDER BY distance DESC NULLS LAST`.

**The trip summary depends on the exact shape of `members`:**
- Symptoms: The summary is wrong, or summary generation crashes.
- Files: `src/wheeck/overview_docs.py:75-79`, `src/wheeck/constants.py:185, 197, 209`
- Triggers:
  - If no member has `vehicle: null`, `drivers` is empty. The query then contains `vehicle_driver NOT IN ()`, which is a PostgreSQL syntax error.
  - The SQL has exactly two `vehicle = ?` placeholders, but `vehicles` holds every configured vehicle. Any number of vehicles other than two causes a parameter-count error.
  - `vehicle_driver NOT IN (...)` evaluates to NULL for rows whose `vehicle_driver` is NULL. Deliveries with an unresolved driver are therefore silently left out of the summary.
- Fix: Skip the filter when `drivers` is empty, use `(vehicle_driver IS NULL OR vehicle_driver NOT IN (...))`, and either generate one column pair per vehicle or assert that there are exactly two vehicles.

**A discard can only be promoted if every column is non-NULL, including nullable ones:**
- Symptoms: The user fixes a `delivery_discard` row and puts its page PDF back into `DDTs/`, but the page is discarded again with "still found NULL cell". A new discard PDF named `..._P001_P001.pdf` is written.
- Files: `src/wheeck/recording_docs.py:330-339`, `src/wheeck/constants.py:85-102`
- Trigger: `if None not in discard` requires all 11 selected columns to be non-NULL. In `public.delivery`, `vehicle_driver` and `distance` are nullable (`sql/tables/delivery.sql:11-12`). A discard whose driver could not be resolved, or whose city cannot be geocoded, cannot be promoted until the user also invents those values.
- Fix: Check only the columns that are `NOT NULL` in `public.delivery`.

**A rescanned discard page that now parses cleanly leaves its discard open:**
- Symptoms: The delivery is recorded, but the original DISCARD warning and its `delivery_discard` row stay at `status = TRUE`. They keep appearing in `vw_discard_message`.
- Files: `src/wheeck/recording_docs.py:318-339, 363`
- Trigger: The discard row is consulted only when `discarded_pages.on_error` is set (`recording_docs.py:330`). A clean rescan therefore never loads `id_warning_message`, and `QUERY_UPDATE_WARNING` at line 363 never runs.
- Fix: When the working document matches `PATTERN_DISCARD_DOC`, always fetch the discard `id_warning_message` and close it after a successful insert.

**A failure after commit leaves a recorded document stuck as `.recording.pdf`:**
- Symptoms: The database contains the document, but the file stays in `DDTs/` as `*.recording.pdf`. `run()` skips it on every later run with only a `PATTERN_WORKING_DOC` warning. The remaining documents in the batch are not processed.
- Files: `src/wheeck/recording_docs.py:612, 633-639`
- Trigger: After `doc_scanner` commits, `shutil.copy` to `backup_dir` fails (directory missing or offline) or `shutil.move` to `DDTs/recorded/` fails. Neither call is inside a `try`. The rename at line 612 is also outside the `try`, so a PDF held open by a viewer (Windows sharing violation) aborts the whole run.
- Workaround: Rename the file into `DDTs/recorded/` by hand.
- Fix: Wrap the post-commit file operations in their own `try`, log CRITICAL, and continue with the next document. Wrap the initial rename in a `try` and skip the document on error.

**An interrupted run orphans the document:**
- Symptoms: The document stays as `*.recording.pdf`, and discard PDFs written before the interruption stay in `DDTs/discarded/`.
- Files: `src/wheeck/recording_docs.py:423-435, 616-630`
- Trigger: A `KeyboardInterrupt` (Ctrl+C), a killed process or a power loss during `doc_scanner`. Both handlers catch `Exception`, which does not include `KeyboardInterrupt`, so neither the rename-back nor the discard cleanup runs. The server rolls back the open transaction when the connection drops.
- Workaround: Rename `X.recording.pdf` back to `X.pdf` and delete its discard PDFs.
- Fix: At the start of `run()`, check each leftover `*.recording.pdf` against `public.delivery.document_source`. Rename it back to its original name only when no row exists, and move it to `DDTs/recorded/` when rows exist. A blind rename is not safe: a leftover from the post-commit failure above is already committed. On reprocess every page hits `CHECK_DUPLICATE` (`recording_docs.py:342-346`), and lines 395-402 then write one discard PDF per page with no database row.

**The backup is silently replaced only when the new dump is at least as large:**
- Symptoms: `scheme/wheeck.dump` stops updating without any log entry.
- Files: `src/wheeck/recording_docs.py:653-660`
- Trigger: A new dump smaller than the previous one is deleted (`os.remove(backup_copy)`) without logging. Compression differences, or rows removed or moved by hand, can shrink the dump. From then on the only backup copy is stale.
- Fix: Always keep the newest successful dump, keep the last N dumps rotated by date, and log the size comparison.

**A failure after the run loop loses overview regeneration:**
- Symptoms: Monthly overviews and trip summaries miss the deliveries recorded in a run, and later runs do not fix them.
- Files: `src/wheeck/recording_docs.py:641-662`, `src/wheeck/constants.py:225-233`, `src/wheeck/overview_docs.py:57, 93`
- Trigger: `check_gaps()` or `generate_current()` raises, for example a `PermissionError` because the xlsx file is open in Excel, or because the `res/` link target is unavailable. Neither call is guarded. The exception skips the overviews and the database backup. `QUERY_GET_LAST_OVERVIEWS` looks only at `recording_date >= NOW() - INTERVAL '1' DAY`, so a run more than 24 h later does not regenerate those months.
- Fix: Guard each post-processing step separately. Derive the months to regenerate from the run's own `job_begin` values, not from a 24 h window. Give `generate_overview` a manual rerun entry point.

**`check_similarity` crashes when the member list is empty:**
- Symptoms: An `IndexError` rolls back every document on every run.
- Files: `src/wheeck/recording_docs.py:550-555, 277-300`
- Trigger: `members` has no vehicles or no drivers, for example the committed template `config/wheeck.json` copied without edits. `sorted(enum, ...)[0]` is then evaluated on an empty dict.
- Fix: Return `source_record` when `source_enum` is empty, and validate the config at startup.

**On a fresh checkout without `log/`, the logger writes to a file named `log`:**
- Symptoms: Logs go to a file called `log` at the project root. A later `mkdir log` then fails.
- Files: `src/core/common.py:60-65`, `src/wheeck/constants.py:6`
- Trigger: `log/` is gitignored, so it is missing after a clone. Then `fou.is_dir()` is false, and `logging.FileHandler(fou)` creates a file at that path.
- Fix: Use `fou.mkdir(parents=True, exist_ok=True)` when the path has no suffix. Create `DDTs/discarded/` and `DDTs/recorded/` at startup in the same way.

**Rewriting an overview leaves stale rows:**
- Symptoms: An existing `res/YYYY_MM.xlsx` keeps rows from an earlier generation when the month now has fewer deliveries.
- Files: `src/wheeck/overview_docs.py:33-41`
- Trigger: The code loads the existing file and overwrites rows from row 2 onward, but never clears rows below the new last row.
- Fix: Clear `ws.max_row` down to the new row count before saving.

**Annotations and default arguments do not match behaviour:**
- `doc_scanner`, `discard_doc` and `discarded_doc_path` are annotated `str | Path`, but they call `.stem` (`src/wheeck/recording_docs.py:173, 487`), so a `str` argument raises `AttributeError`. `doc_scanner` and `discard_doc` are exported from `src/wheeck/__init__.py:3-4`.
- `doc_scanner(job_begin=datetime.now())` (`recording_docs.py:160`) and `generate_overview(year=date.today().year, month=date.today().month)` / `generate_summary(year=date.today().year)` (`src/wheeck/overview_docs.py:15-16, 62`) evaluate their defaults once, at import time.
- Fix: Convert with `Path(working_doc)` at the top of each function. Default to `None` and resolve inside the function.

## Security Considerations

**A database password is committed to the repository:**
- Risk: `sql/roles/wheeck.sql:9` contains `CREATE USER wheeck WITH PASSWORD` followed by a plaintext literal. Git history contains the same string in `config/querier.json` in every commit from `61bdf0c` up to the parent of `a8bc080`, the commit that sets it to `null`. All of these commits are on `origin/main` (GitHub).
- Files: `sql/roles/wheeck.sql:9`; git history of `config/querier.json`
- Current mitigation: Live configuration is read from the gitignored `config/querier.local.json` (`.gitignore:12`, `src/wheeck/constants.py:10-12`).
- Recommendations: Rotate the `wheeck` role password. Pass the new password as a psql variable (start `psql` with `-v wheeck_password=...` and write `PASSWORD :'wheeck_password'` in `sql/roles/wheeck.sql`). If the repository is or ever becomes public, treat the old value as leaked. Do not quote the value in documentation.

**Backups run with superuser credentials:**
- Risk: `CMD_BACKUP_DB` (`src/wheeck/constants.py:27`) runs `pg_dump -U postgres -w` through `subprocess.run(..., shell=True)` (`src/wheeck/recording_docs.py:651`). It connects as the `postgres` superuser, and `-w` means it never prompts for a password. Where those credentials come from was not inspected, but the account that runs wheeck can authenticate as the superuser without interaction.
- Current mitigation: The command is static. The only interpolated value is `PATH_SCHEME`, which is quoted.
- Recommendations: Create a dedicated read-only backup role with only the privileges needed to dump database `wheeck`, and call `pg_dump` without `shell=True`.

**Plaintext secrets on disk:**
- Risk: `config/querier.local.json` (database credentials) and `config/wheeck.local.json` (`ors_api_key`) exist as plaintext files. Their contents were not inspected.
- Current mitigation: Both files are gitignored (`.gitignore:12`). The committed templates `config/querier.json` and `config/wheeck.json` contain `null` placeholders.
- Recommendations: Restrict NTFS permissions on these files to the account that runs wheeck.

**Business and personal data is kept without a retention policy:**
- Risk: `Delivery.__str__` is logged at INFO for every saved page (`src/wheeck/recording_docs.py:358`), together with similarity warnings. Each entry contains the customer company name, delivery city, vehicle plate and driver name. One log file per day accumulates in `log/` with no rotation. `scheme/wheeck.dump` and `DDTs/recorded/` hold the full dataset in the project directory.
- Current mitigation: `log/`, `DDTs/`, `*.dump` and `res/` are gitignored.
- Recommendations: Rotate or prune `log/`, for example with `TimedRotatingFileHandler` and a `backupCount`. Store dumps outside the working tree.

**SQL injection surface (no issue found):**
- Every query uses `?` placeholders. `QUERY_GET_SUMMARY % ', '.join('?' * len(drivers))` (`src/wheeck/overview_docs.py:79`) inserts only placeholders. Keep this pattern and never format values into SQL strings.

## Performance Bottlenecks

Current volumes are small: monthly DDT batches named `YYYY_MM_DDT_<first>_<last>.pdf`. None of the items below hurt at this scale, but they decide how the system degrades.

**The gap view is recomputed in full on every run:**
- Problem: `QUERY_CHECK_GAPS` reads `vw_delivery_gap`. That view generates every number between MIN and MAX per year with `GENERATE_SERIES`, runs a correlated subquery for `document_month` on each gap row (`document_month` is not used by the Python code), and joins on fields parsed out of text from `vw_discard_message`.
- Files: `sql/views/vw_delivery_gap.sql:7-41`, `src/wheeck/recording_docs.py:518-534`
- Cause: A single misparsed document number (`chk_delivery_document_number` only enforces `> 0`) widens the series. That produces one false GAP warning per missing number, and each is inserted on its own connection (see the next item).
- Improvement path: Drop `document_month` or move it into a separate view. Add an upper-bound sanity check on `document_number`, relative to the batch range in the filename, before insert.

**One database connection per gap warning:**
- Problem: `check_gaps()` calls `save_warning()` without a `querier`, so every warning opens and closes its own auto-commit connection (`src/wheeck/recording_docs.py:528-532, 578-579`).
- Improvement path: Open one auto-commit `Querier` in `check_gaps` and pass it with `querier=`.

**Distance resolution is serial and throttled:**
- Problem: Each new city costs two ORS calls (`pelias_search` and `directions`) plus `time.sleep(1.5)` (`src/wheeck/recording_docs.py:150-153`). With the defaults in `openrouteservice.Client` (`timeout=60`, `retry_timeout=60`, retry on 429 and 503), one call can block for up to about 60 s before it returns `None`.
- Improvement path: Fix the NULL caching bug first, since it is what causes repeated lookups. If more speed is needed, pass `retry_timeout` explicitly to `openrouteservice.Client` in `src/geo/geo.py:25`.

**The source PDF is reopened for every discarded page:**
- Problem: `discard_doc()` opens the full source PDF again for each discarded page (`src/wheeck/recording_docs.py:459`).
- Improvement path: Keep one `PdfDocument` open for the discard phase, or batch the discards after the loop. The handle must still be released before the rollback rename (see `recording_docs.py:175-177`).

**Date filters that cannot use an index:**
- Problem: `EXTRACT(YEAR FROM ...)` and `EXTRACT(MONTH FROM ...)` filters (`src/wheeck/constants.py:65, 167-168, 184`) cannot use an index, and there is no index on `delivery_date` or `document_date`. `idx_delivery_warning_message_text` (`sql/tables/delivery_warning.sql:12-14`) is not used by any query.
- Improvement path: Use range predicates (`delivery_date >= make_date(?, ?, 1) AND delivery_date < ...`), add an index on `delivery_date`, and drop the unused index.

## Fragile Areas

**Regex extraction patterns:**
- Files: `src/wheeck/constants.py:18-25`, `src/wheeck/recording_docs.py:133-138, 213-316`
- Why fragile:
  - Every pattern hardcodes the supplier's DDT layout, including Italian labels such as `Luogo di consegna`, `Quantità Prezzo` and `Targa automezzo`. `PATTERN_CITY_DX`, `PATTERN_CITY_SX` and `PATTERN_VEHICLE` also depend on the `\r\n` line endings that `pypdfium2`'s `get_text_bounded()` produces. `PATTERN_QUANTITY` has `\r\n` only inside an optional group, and `PATTERN_DOC_NUMBER` has none.
  - A layout change by the supplier, or a `pypdfium2` upgrade that changes text output, discards every page. The run does not fail, so the problem shows up only as a flood of discards.
  - `PATTERN_QUANTITY` matches only quantities with a `,000` decimal part, and `re.search` takes only the first product line of a page.
  - `PATTERN_VEHICLE` requires a 7-character plate.
  - `PATTERN_CITY_DX` and `PATTERN_CITY_SX` match the province code but do not capture it. Geocoding (`src/geo/geo.py:41`) and the distance cache (`constants.py:78-83`) therefore key on the city name alone, and Italian towns with the same name collide.
- Safe modification: Collect anonymized page-text fixtures (`page.get_textpage().get_text_bounded()` output) for every known layout variant before changing a pattern. Add a test per pattern. Run a pattern change against `DDTs/recorded/` with a dry run before deploying.
- Test coverage: None.

**`doc_scanner` transaction and compensation logic:**
- Files: `src/wheeck/recording_docs.py:159-441, 593-630`
- Why fragile:
  - Correctness depends on invariants documented only in comments. The PDF handle must be released before the transaction (`recording_docs.py:175-182`), the commit must be the last statement of the `try` (`recording_docs.py:418-420`), and the re-raise is required (`recording_docs.py:434`).
  - Discard PDFs are registered before they are written (`recording_docs.py:399-400`).
  - `MessageGenre.DISCARD` must use `%(document_number)s` (`recording_docs.py:85-91`).
  - `QUERY_UPDATE_WARNING` failures are logged CRITICAL but do not raise (`recording_docs.py:363-365, 414-416`). This contradicts the `RecordingError` policy used for the inserts.
- Safe modification: Read every comment in the function before editing. Keep all database writes on the shared `querier`. Raise `RecordingError` on any write that does not affect exactly one row.
- Test coverage: None.

**Message templates and SQL views:**
- Files: `src/wheeck/recording_docs.py:91-93`, `sql/views/vw_discard_message.sql`, `sql/views/vw_gap_message.sql`
- Why fragile: The views slice `message_text` on literal substrings such as `'[number: '`, `', genre: '`, `'of doc '` and `' of year '`. Editing a template string in Python silently breaks gap detection and discard tracking.
- Safe modification: Change the Python template and the matching view in the same commit, and re-run `QUERY_CHECK_GAPS` against existing rows. Removing the coupling is the better fix (see Tech Debt).

**Reliance on `rowcount` for SELECT statements:**
- Files: `src/core/querier.py:120-124`, `src/wheeck/recording_docs.py:147, 328, 410, 526`, `src/wheeck/overview_docs.py:31, 79, 103`
- Why fragile: Control flow branches on `cursor.rowcount` after a SELECT. The DB-API allows `-1` for SELECT statements, so the current behaviour depends on the psqlODBC driver settings. With `rows == -1`, `if querier.run(QUERY_GET_GAP, ...).rows:` would pass on an empty result. `fetch` would return `None`, and `QUERY_UPDATE_WARNING` with `None` goes through the all-`None` branch of `Querier.run` (see Tech Debt). That raises 07002 and rolls back the document.
- Safe modification: Branch on the fetched value (`fetch(Querier.FETCH_VAL) is not None`), not on `rows`, for SELECT statements.

**Fuzzy vehicle and driver matching:**
- Files: `src/wheeck/recording_docs.py:272-312, 537-558`
- Why fragile: `check_similarity` replaces a value whenever the `SequenceMatcher` ratio exceeds 0.5. With a short roster, a badly read plate can be silently mapped to a real vehicle. The delivery is inserted anyway, and only a WARNING message is saved. Nothing ever closes WARNING-genre messages.
- Safe modification: Raise the threshold, or require a unique best match. Consider discarding the page when the vehicle is not in `members`, not just warning.

**ORS error handlers:**
- Files: `src/geo/geo.py:44-46, 95-97`
- Why fragile:
  - The handlers read `e.message['error']['code']` and `['message']`, which assumes the error body shape seen for the directions endpoint (`log/2025_01_12.log`). The shape for geocode errors and for 401/403 responses was not verified. If the body differs, the handler raises `TypeError` and the document fails.
  - The handlers catch only `ApiError`, `HTTPError` and `Timeout`. `requests.ConnectionError` (no network) passes through `openrouteservice.Client` unwrapped. Both failures happen before the transaction opens, so the document is requeued safely.
- Safe modification: Log `str(e)`, not the indexed fields, and catch `requests.RequestException` as well.

**Excel templates:**
- Files: `src/wheeck/overview_docs.py:33-53, 81-90`, `src/wheeck/constants.py:29-35`
- Why fragile: Sheet names are hardcoded, and the templates are not versioned. `OVERVIEW_FORMATS[type(cell)]` raises `KeyError` for any column type outside `None`, `str`, `int` and `date`, for example `Decimal` if `distance` is ever added to the overview query.
- Safe modification: Use `OVERVIEW_FORMATS.get(type(cell), 'General')`. `Querier._EXCEL_FORMATS.get(type(cell))` at `src/core/querier.py:197, 204` avoids the `KeyError` but has no default, so unknown types get `None`.

## Scaling Limits

**Single host, single process:**
- Current capacity: One manual run at a time. The run logs in `log/` are dated irregularly, roughly monthly. The database and the backup command both assume `127.0.0.1:5432`.
- Limit: There is no run lock. Two overlapping runs list the same `DDTs/` contents. The second `shutil.move` (`src/wheeck/recording_docs.py:612`) then fails outside any `try` and aborts that run.
- Scaling path: Take an exclusive lock file in `DDTs/` at the start of `run()`.

**OpenRouteService throughput:**
- Current capacity: At most one new city every 1.5 s or more, per the "40 call/min" comment at `src/wheeck/recording_docs.py:152`. Known cities are served from `public.delivery`.
- Limit: A first run over a large backlog of unseen cities is bounded by the throttle and by the client's 60 s retry window per call.
- Scaling path: Add a dedicated `city_distance` cache table keyed on city and province, so the cache no longer depends on delivery rows.

**Warning table growth:**
- Current capacity: Unbounded.
- Limit: WARNING-genre rows are never closed. GAP rows multiply when a document number is misparsed.
- Scaling path: Add a resolution workflow and a periodic cleanup of closed warnings.

## Dependencies at Risk

**`openrouteservice` 2.3.3 (`requirements.txt:2`):**
- Risk: The client still carries Python 2 compatibility shims (`.venv/Lib/site-packages/openrouteservice/client.py:37`). It wraps only `requests` timeouts in its own exception types, and its retry behaviour is fixed unless constructor arguments are passed.
- Impact: Error handling in `src/geo/geo.py` depends on its exception classes and body shapes.
- Migration plan: Call the ORS HTTP endpoints directly with `requests`, which is already installed as a transitive dependency. Only two endpoints are used: `/geocode/search` and `/v2/directions/driving-car/geojson` (see `.venv/Lib/site-packages/openrouteservice/geocode.py:132` and `directions.py:281`).

**`pyodbc` with the system psqlODBC driver (`requirements.txt:3`, `config/querier.json:4`):**
- Risk: The ODBC driver (`PostgreSQL ANSI`) is a system install and is not captured in `requirements.txt`. The behaviour of `rowcount` and of parameter binding depends on it (see Fragile Areas).
- Impact: A driver change or a new machine can change control flow without any code change.
- Migration plan: Document the required driver and version in `README.md`, or move to `psycopg`, whose behaviour is defined in the package.

**`pypdfium2` (`requirements.txt:4`):**
- Risk: The `~=5.5.0` pin allows patch updates. Any change in text extraction output, such as line endings or whitespace, breaks every regex.
- Migration plan: Pin the exact version, and gate upgrades on the regex fixture tests (see Test Coverage Gaps).

**Unpinned interpreter and transitive dependencies:**
- Risk: There is no lockfile and no `python_requires`. The venv runs Python 3.15.1 (`.venv/pyvenv.cfg`), and `__pycache__` also holds 3.14 bytecode. `requests`, `urllib3` and `et_xmlfile` float. `pg_dump` must be on `PATH`, and nothing checks for it.
- Migration plan: Add `pyproject.toml` with `requires-python` and generate a lockfile.

## Missing Critical Features

**Automated tests, CI and linting:**
- Problem: The repository has no test files, no test runner configuration, no CI workflow and no linter configuration.
- Blocks: Safe changes to the regex patterns, the transaction logic or the SQL views.

**Recovery of stuck documents at startup:**
- Problem: Leftover `*.recording.pdf` and `*.failed.pdf` files in `DDTs/` are only logged as `PATTERN_WORKING_DOC` warnings (`src/wheeck/recording_docs.py:604-606`).
- Blocks: Unattended operation. Every stuck document needs a manual rename.

**Schema migrations:**
- Problem: The only provisioning path is the destructive full install with psql `\ir`.
- Blocks: Evolving the schema of a populated database safely.

**Startup validation and bootstrap:**
- Problem: Nothing checks the config keys and the `members` shape, nothing creates `log/`, `DDTs/discarded/` and `DDTs/recorded/`, and nothing checks for the `scheme/*.xlsx` templates, `res/` or `pg_dump`.
- Blocks: Setting up the tool on a new machine from a clean clone.

**Resolution of WARNING messages:**
- Problem: Similarity WARNING rows are never closed, and nothing links them to the delivery row they concern.
- Blocks: Auditing which recorded deliveries carry an auto-corrected vehicle or driver.

**Separate delivery date:**
- Problem: `delivery_date` is always copied from `document_date` (`src/wheeck/recording_docs.py:266-268`).
- Blocks: Correct reporting for deliveries made on a different day from the DDT date.

## Test Coverage Gaps

**Extraction patterns and page parsing:**
- What's not tested: `PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`, `PATTERN_CITY_SX`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`, `PATTERN_WORKING_DOC` and `PATTERN_DISCARD_DOC`, and the left/right city fallback.
- Files: `src/wheeck/constants.py:18-25`, `src/wheeck/recording_docs.py:213-316`
- Risk: A layout or library change discards every page without any visible failure.
- Priority: High

**Transaction rollback and compensation:**
- What's not tested: Rollback on `RecordingError`, cleanup of discard PDFs, rename-back to the original filename, the `.failed.pdf` fallback, and skipping an existing discard record.
- Files: `src/wheeck/recording_docs.py:204-441, 593-630`
- Risk: Partial writes, or documents requeued forever.
- Priority: High

**Summary and overview SQL:**
- What's not tested: NULL distance ordering, the NULL driver exclusion, the two-vehicle assumption, an empty `drivers` list, and overwriting an existing overview.
- Files: `src/wheeck/constants.py:158-233`, `src/wheeck/overview_docs.py`
- Risk: Wrong figures in the delivered Excel reports.
- Priority: High

**Discard promotion and gap closing:**
- What's not tested: Promoting a corrected `delivery_discard` row, closing warnings through `trg_sync_discard_status`, and closing gap warnings.
- Files: `src/wheeck/recording_docs.py:318-339, 407-416`, `sql/functions/sync_discard_status.sql`, `sql/triggers/trg_sync_discard_status.sql`
- Risk: Warnings stay open, or corrected discards are never recorded.
- Priority: Medium

**SQL views:**
- What's not tested: Parsing of `message_text` in `vw_discard_message` and `vw_gap_message`, including the `None` document-number case, and the gap series in `vw_delivery_gap`.
- Files: `sql/views/*.sql`
- Risk: A template edit breaks gap detection without any error until a cast fails.
- Priority: Medium

**`Querier` parameter handling and `check_similarity`:**
- What's not tested: `run()` with all-`None` arguments, with a single iterable argument and with no arguments; `check_similarity` with an empty roster, ties and the threshold edge.
- Files: `src/core/querier.py:107-125`, `src/wheeck/recording_docs.py:537-558`
- Risk: Latent crashes that turn into permanent requeue loops.
- Priority: Medium

**Geocoding error handling:**
- What's not tested: The ORS error body shapes, timeouts and connection errors in `GeoMap.search` and `get_distance_from_coords`.
- Files: `src/geo/geo.py`
- Risk: NULL distances are stored, or documents fail because a handler raises.
- Priority: Low

---

*Concerns audit: 2026-11-22*
