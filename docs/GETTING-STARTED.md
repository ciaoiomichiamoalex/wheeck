<!-- generated-by: gsd-doc-writer -->
# Getting Started

This guide takes a fresh clone of wheeck to a first recorded batch of DDTs. wheeck runs on a Windows workstation, and the commands below use Git Bash.

## Prerequisites

- **Windows.** The template ODBC driver name `PostgreSQL ANSI` is the Windows psqlODBC name, and the file handling in `src/wheeck/recording_docs.py` is written around Windows file locks.
- **Python >= 3.12.** Nothing in the repository pins the version. The floor comes from `sqlite3.connect(..., autocommit=...)` in `src/core/querier.py` (3.12) and `pyodbc` 5.5 (3.11).
- **PostgreSQL server**, with access as the `postgres` superuser for the schema install. The config template targets `127.0.0.1:5432`, database `postgres`, and the backup command in `src/wheeck/constants.py` is hardcoded to the same host, port and database.
- **psqlODBC driver**, registered under the name you put in `driver` (the template uses `PostgreSQL ANSI`). Its bitness must match your Python (64-bit driver for 64-bit Python).
- **PostgreSQL client tools on `PATH`:** `psql` for the schema install and `pg_dump` for the backup at the end of each run.
- **OpenRouteService API key**, and outbound HTTPS access to OpenRouteService. Each new delivery city costs one geocoding call and one directions call.
- **Files that are not in the repository:**
  - the two Excel templates `scheme/overview.xlsx` and `scheme/summary.xlsx` (gitignored as `*.xlsx`)
  - at least one DDT PDF with a text layer, to process

## Installation steps

1. Clone the repository, then create the virtualenv and install the dependencies from `requirements.txt` (`openpyxl`, `openrouteservice`, `pyodbc`, `pypdfium2`):

   ```bash
   git clone https://github.com/ciaoiomichiamoalex/wheeck.git
   cd wheeck
   python -m venv .venv
   source .venv/Scripts/activate
   pip install -r requirements.txt
   ```

   Keep the virtualenv active for every command that follows.

2. Install the database schema:
   1. Open `db/roles/wheeck.sql` and change the password in the `CREATE USER` line. This sets the password of the `wheeck` role that the application connects with.
   2. Run the install as a superuser, against the database that the config points to:

      ```bash
      psql -U postgres -d postgres -f db/install.sql
      ```

   `db/install.sql` chains the files under `db/` in order: schema, tables, function, trigger, role, views. The first file, `db/schema/wheeck.sql`, runs `DROP SCHEMA IF EXISTS wheeck CASCADE`, so running the install again deletes every recorded delivery.

3. Create the real config files from the committed templates. The application reads only the gitignored `*.local.json` copies:

   ```bash
   cp config/wheeck.json config/wheeck.local.json
   cp config/querier.json config/querier.local.json
   ```

   Fill in these values:
   - `config/querier.local.json`: set `password` in the `"name": "main"` entry to the password from step 2.
   - `config/wheeck.local.json`: set `ors_api_key`, `departure_coords` (`[longitude, latitude]`), the `members` list of vehicles and drivers, and optionally `backup_dir`.

   [CONFIGURATION.md](CONFIGURATION.md) describes every key, including the shape that `members` must have.

4. Create the runtime folders. They are gitignored or empty, so a fresh clone does not have them:

   ```bash
   mkdir -p DDTs/discarded DDTs/recorded log res scheme
   ```

5. Put the two Excel templates in `scheme/`:
   - `scheme/overview.xlsx`, with the sheets `consegne`, `cifre`, `litri`, `cifre manuale` and `litri manuale`
   - `scheme/summary.xlsx`, with the sheet `viaggi`

   <!-- VERIFY: the run references scheme/overview.xlsx and scheme/summary.xlsx, which are not in the repository -->

6. Let `pg_dump` log in as `postgres` without a password prompt. The backup command runs with `-w` (never prompt), so it fails if the login needs a typed password. A password file or `PGPASSWORD` works. See [CONFIGURATION.md](CONFIGURATION.md#environment-variables).

## First run

1. Check that every module compiles, from the repository root:

   ```bash
   python -m py_compile src/main.py src/core/*.py src/geo/*.py src/wheeck/*.py
   ```

2. Check the database connection from `src/`. This uses the same `Querier` class and config file as the application:

   ```bash
   cd src
   python -c "from core import Querier; print(Querier('../config/querier.local.json').run('SELECT COUNT(*) FROM wheeck.delivery').fetch(Querier.FETCH_VAL))"
   ```

   On a new install it prints `0`. That confirms the ODBC driver, the `wheeck` role password, the schema and the grants.

3. Optional: run the batch with an empty `DDTs/` folder, still from `src/`:

   ```bash
   python main.py
   ```

   With no matching PDF, the run does not contact the database or OpenRouteService. It prints nothing to the console and writes one `DDTs dir content []` line to `log/YYYY_MM_DD.log`. That confirms the imports and the log folder.

4. Copy a DDT PDF into `DDTs/`, named `YYYY_MM_DDT_NNNN_NNNN.pdf` (for example `2024_01_DDT_0001_0100.pdf`). Files with other names are skipped.

5. Run the batch again from `src/`:

   ```bash
   python main.py
   ```

   Only errors are printed to the console, so a clean run prints nothing.

6. Check the result:
   - `log/YYYY_MM_DD.log` contains a `JOB END: worked N pages in <file> [M discarded pages]` line for the document.
   - The PDF has moved to `DDTs/recorded/<name>.recorded.pdf`.
   - `res/YYYY_MM.xlsx` (monthly overview) and `res/YYYY_TRIPS.xlsx` (yearly trip summary) exist for the delivery dates of the document.
   - `scheme/wheeck.dump` holds the schema backup.
   - Pages that could not be read are in `DDTs/discarded/` as `<name>_PNNN.pdf`. Their warnings are listed in the view `wheeck.vw_discard_message`.

The [README](../README.md#recovering-a-discarded-page) explains how to recover a discarded page.

## Common setup issues

**A file named `log` appears at the project root instead of a `log/` folder.**
The logger writes into `log/` only when that folder exists. Otherwise it creates a plain file named `log`. Delete the file and run `mkdir log`.

**`Querier: no config ... found!`**
`config/querier.local.json` is missing. Copy it from `config/querier.json` (installation step 3). A missing `config/wheeck.local.json` shows up as a `FileNotFoundError` instead.

**The connection check fails with a `pyodbc` error.**
Check that the `driver` value in `config/querier.local.json` matches the name of an installed psqlODBC driver, and that the driver and Python are both 64-bit or both 32-bit. Then check the `server`, `port`, `database` and `password` values.

**A PDF in `DDTs/` is ignored.**
Its name does not match `YYYY_MM_DDT_NNNN_NNNN.pdf`. The log shows `error on PATTERN_WORKING_DOC for <file>... skipping doc!`. Rename the file.

**The log shows `JOB FAILED` and the PDF stays in `DDTs/`.**
The whole document was rolled back and keeps its original name, so the next run processes it again. The `JOB FAILED` log entry includes the traceback. Common causes:
- A missing `DDTs/discarded/` folder, as soon as a page needs to be discarded. Create the folder and run again.
- `ors_api_key` still `null` in `config/wheeck.local.json`, as soon as a document has a new delivery city. The traceback ends with `ValueError: No API key was specified`. Set the key and run again.

**The run ends with `unhandled exception` and the PDF stays in `DDTs/` as `*.recording.pdf`.**
The document was committed, but a file step after the commit failed. Two causes:
- `DDTs/recorded/` does not exist, so the final move failed.
- `backup_dir` in `config/wheeck.local.json` points to a folder that does not exist, so the backup copy failed.

Fix the folder, then move the file into `DDTs/recorded/` as `<name>.recorded.pdf`. Do not rename it back to its original name: its pages are already recorded, so a new run would only discard them as duplicates.

The run stopped at that document. The PDFs after it stay in `DDTs/` and are processed by the next run. The workbooks and the schema backup were skipped: rebuild the workbooks by hand as shown in the next issue.

**The run ends with `unhandled exception` while building the workbooks.**
This happens after the documents are committed and moved, and it skips the schema backup. Common causes:
- A missing Excel template in `scheme/` (installation step 5).
- A `members` list with a shape that the yearly summary query cannot use. It needs exactly two entries with a `vehicle`, plus at least one entry with a `null` vehicle and a driver name. See [CONFIGURATION.md](CONFIGURATION.md).

A later run does not rebuild these workbooks, because it has no new documents. After the fix, rebuild them by hand from `src/`. Replace `2024` and `1` with the year and month of the affected deliveries:

```bash
python -c "from wheeck import generate_overview, generate_summary; generate_overview(2024, 1); generate_summary(2024)"
```

**The log shows `error during backup database schema...`.**
`pg_dump` is not on `PATH`, or the `postgres` login asks for a password (installation step 6). The deliveries are already saved. Only the backup is missing.

**The log shows `API error on searching` or `HTTP error on searching`.**
OpenRouteService rejected the geocoding call, for example because of a wrong key. Check `ors_api_key` in `config/wheeck.local.json`.

**Running `db/install.sql` a second time.**
The install drops the `wheeck` schema and all its data. `CREATE USER` has no `IF NOT EXISTS`, so psql reports that the role already exists and continues. The role keeps its old password.

## Next steps

- [CONFIGURATION.md](CONFIGURATION.md): every config key, the hardcoded settings and the backup authentication.
- [DEVELOPMENT.md](DEVELOPMENT.md): local development workflow.
- [TESTING.md](TESTING.md): the checks you can run today, and how to add tests.
- [ARCHITECTURE.md](ARCHITECTURE.md): how a batch run flows through the modules and the database.
- [README](../README.md): usage, discard recovery and manual workbook rebuilds.
