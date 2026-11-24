<!-- generated-by: gsd-doc-writer -->
# wheeck

Python batch tool which extracts delivery information from the text of PDF DDTs (transport documents) using regular expressions, records it in PostgreSQL and builds Excel overviews of the deliveries.

Each run scans the PDFs in `DDTs/`, stores one delivery per page in the `wheeck` database, splits the pages it cannot read into single-page PDFs for review, and refreshes the monthly and yearly workbooks in `res/`.

## Requirements

- Python 3.12 or later. Nothing in the repository pins the version: `sqlite3.connect(..., autocommit=...)` in `src/core/querier.py` needs 3.12, and `pyodbc` 5.5 needs 3.11.
- A PostgreSQL server. The template config targets `127.0.0.1:5432`, database `wheeck`.
- The psqlODBC driver, registered under the name set in `driver` (the template uses `PostgreSQL ANSI`).
- The PostgreSQL client tools `psql` (database install) and `pg_dump` (automatic backup) on `PATH`.
- An OpenRouteService API key, and outbound HTTPS access to OpenRouteService for every new delivery city.

## Installation

The project runs on a Windows workstation. The commands below use Git Bash.

1. Clone the repository, then create and activate a virtualenv and install the Python dependencies:

   ```bash
   git clone <repository-url> wheeck
   cd wheeck
   python -m venv .venv
   source .venv/Scripts/activate
   pip install -r requirements.txt
   ```

2. Set the password of the `wheeck` database role in `sql/roles/wheeck.sql`, then install the database as a superuser. Start `psql` from the repository root:

   ```bash
   psql -U postgres -d postgres
   ```

   At the `psql` prompt, run the object scripts in dependency order with the `\ir` meta-command:

   ```
   \ir sql/database/wheeck.sql
   \c wheeck
   \ir sql/tables/delivery.sql
   \ir sql/tables/delivery_history.sql
   \ir sql/tables/delivery_warning.sql
   \ir sql/tables/delivery_discard.sql
   \ir sql/functions/sync_discard_status.sql
   \ir sql/triggers/trg_sync_discard_status.sql
   \ir sql/roles/wheeck.sql
   \ir sql/views/vw_discard_message.sql
   \ir sql/views/vw_gap_message.sql
   \ir sql/views/vw_delivery_gap.sql
   ```

   The repository does not track an install script. To reuse these lines, save them in a local file at the repository root and run it with `psql -f`. `sql/database/wheeck.sql` starts with `DROP DATABASE IF EXISTS wheeck`, so running the install again deletes all recorded data. `\c wheeck` connects to the new database, so the other scripts create their objects in its `public` schema.

3. Copy each committed config template to its gitignored `*.local.json` sibling and fill in the real values (see [Configuration](#configuration)):

   ```bash
   cp config/wheeck.json config/wheeck.local.json
   cp config/querier.json config/querier.local.json
   ```

4. Create the runtime folders, which are missing from a fresh clone (`DDTs/`, `log/` and `res/` are gitignored, and `scheme/` has no tracked files):

   ```bash
   mkdir -p DDTs/discarded DDTs/recorded log res scheme
   ```

   `log/` must be a directory: if it is missing, the logger writes to a plain file named `log` instead.

5. Put the two Excel templates in `scheme/`. They are gitignored (`*.xlsx`), so they do not come with the repository:
   - `scheme/overview.xlsx`, with the sheets `consegne`, `cifre`, `litri`, `cifre manuale` and `litri manuale`
   - `scheme/summary.xlsx`, with the sheet `viaggi`

   <!-- VERIFY: the run references scheme/overview.xlsx and scheme/summary.xlsx, which are not in the repository -->

## Configuration

`config/wheeck.local.json`:

| Key | Description |
|-----|-------------|
| `ors_api_key` | OpenRouteService API key, used to geocode delivery cities and compute driving distances. |
| `departure_coords` | Departure point as `[longitude, latitude]` (OpenRouteService order). Distances are driving distances in km from here. |
| `members` | List of `{"vehicle": ..., "driver": ...}` entries. Vehicle plates and driver names read from the DDTs are matched against this list, exactly or by more than 50% similarity. A plate that does not match is saved as a warning. A driver name that does not match, or is missing, is replaced by the driver configured for the page's vehicle; a warning is saved only if no configured driver can be assigned. |
| `backup_dir` | Optional folder. When set, every successfully recorded source PDF is copied there. Use `null` to disable. |

The yearly trip summary query lays out two vehicles side by side, and it excludes the drivers of entries whose `vehicle` is `null` (`vehicle_driver NOT IN (...)`). It works as intended when `members` lists exactly two entries with a `vehicle`, and at least one entry with a `null` vehicle and a driver name.

`config/querier.local.json` is a list of ODBC connections. The application uses the entry with `"name": "main"`. Set its `password` to the one set for the `wheeck` role in `sql/roles/wheeck.sql`.

The automatic backup runs `pg_dump -U postgres -h 127.0.0.1 -p 5432 -d wheeck` with the flags `-w -c -F c` (`CMD_BACKUP_DB`, hardcoded in `src/wheeck/constants.py`). Because of `-w`, the `postgres` user must be able to connect without a password prompt, for example through a password file.

## Quick start

1. Copy the DDT PDFs into `DDTs/`, named `YYYY_MM_DDT_NNNN_NNNN.pdf` (for example `2024_01_DDT_0001_0100.pdf`). Files with other names are skipped.
2. With the virtualenv active, run the batch from the `src/` directory, because the packages are imported by their top-level names:

   ```bash
   cd src
   python main.py
   ```

3. Check the log for the day in `log/YYYY_MM_DD.log`. Only errors are also printed to the console.

## Usage

### Recording a batch of DDTs

For each matching PDF in `DDTs/`, `python main.py` does the following:

- It renames the PDF to `*.recording.pdf` while working on it.
- It reads the text of every page and extracts the document number, genre and date, the company and delivery city, the quantity, and the vehicle and driver.
- It resolves the distance of each new delivery city through OpenRouteService before the database transaction opens. Cities already in `public.delivery` reuse their stored distance.
- It saves every page in one transaction per document:
  - Valid pages go to `public.delivery`.
  - A page with a missing field is saved as `DDTs/discarded/<name>_PNNN.pdf`, and a row is added to `public.delivery_discard` with a warning in `public.delivery_warning`.
  - A duplicate page is saved as a discard PDF only, with no database record.
- On success, it moves the PDF to `DDTs/recorded/<name>.recorded.pdf`.
- If reading or saving the document fails, it rolls back the whole document and deletes the discard PDFs that this run created. It then restores the original file name, so the next run processes the document again. If the restore also fails, it renames the file to `*.failed.pdf`. This does not cover the backup copy or the move to `DDTs/recorded/`, which run after the commit: if one of them fails, the data stays recorded, the file stays named `*.recording.pdf`, and the whole run stops, skipping the remaining documents, the gap check, the workbooks and the dump.

Once at least one file name has matched, the run also does the following:

- It checks for gaps in the document numbering.
- It rebuilds the workbooks for the deliveries recorded in the last day: `res/YYYY_MM.xlsx` per month and `res/YYYY_TRIPS.xlsx` per year.
- It dumps the `wheeck` database to `scheme/wheeck.dump`. A new dump replaces the previous one only if it is not smaller.

Open discard and gap warnings can be reviewed in the views `public.vw_discard_message` and `public.vw_gap_message`.

### Recovering a discarded page

This applies to pages discarded for a missing field. Duplicate pages have no `public.delivery_discard` row.

1. Fill in the missing (`NULL`) columns of the page's row in `public.delivery_discard`.
2. Move its PDF from `DDTs/discarded/` back into `DDTs/`.
3. Run the batch again.

The page is then recorded from the database values, and its warning is closed.

### Rebuilding a workbook manually

From the `src/` directory, call the generators exported by the `wheeck` package:

```python
from wheeck import generate_overview, generate_summary

generate_overview(2024, 1)  # writes res/2024_01.xlsx, updating it if it already exists
generate_summary(2024)      # writes res/2024_TRIPS.xlsx from scheme/summary.xlsx
```

## Project layout

- `src/main.py`: entry point. It runs `wheeck.run()` and logs any unhandled exception.
- `src/core/common.py`: shared helpers (JSON config decoding, daily file logger).
- `src/core/querier.py`: wrapper for pyodbc functions (`Querier`), plus a SQLite variant (`LowQuerier`).
- `src/geo/geo.py`: wrapper for OpenRouteService API calls (`GeoMap`).
- `src/wheeck/recording_docs.py`: text extraction from the PDFs, with one database transaction per document, discard handling and the end-of-run backup.
- `src/wheeck/overview_docs.py`: generation of the Excel overviews and summaries of the extracted information.
- `src/wheeck/constants.py`: paths, file name and text patterns, SQL queries.
- `sql/`: PostgreSQL DDL of the `wheeck` database, one file per object, installed in dependency order with the psql `\ir` meta-command (see [Installation](#installation)).
- `config/`: committed config templates.

Further documentation is in `docs/`: [ARCHITECTURE](docs/ARCHITECTURE.md), [STRUCTURE](docs/STRUCTURE.md), [STACK](docs/STACK.md), [INTEGRATIONS](docs/INTEGRATIONS.md), [CONVENTIONS](docs/CONVENTIONS.md), [TESTING](docs/TESTING.md) and [CONCERNS](docs/CONCERNS.md).

## License

The repository contains no license file.
