<!-- generated-by: gsd-doc-writer -->
# Development

This guide covers working on the wheeck source code. It explains how to set up a checkout for development, which commands are available, the code style to follow and how changes reach `main`. For a first installation and a first run, see [GETTING-STARTED.md](GETTING-STARTED.md) and the [README](../README.md).

## Local setup

wheeck is developed and run on Windows. The commands below use Git Bash.

1. Clone the repository, then create the virtualenv and install the dependencies. Use Python 3.12 or later. Nothing in the repository pins the version, so the minimum comes from the code (see [STACK.md](STACK.md)).

   ```bash
   git clone <repository-url> wheeck
   cd wheeck
   python -m venv .venv
   source .venv/Scripts/activate
   pip install -r requirements.txt
   ```

2. Copy the committed config templates to their gitignored `*.local.json` siblings and fill in the real values. [CONFIGURATION.md](CONFIGURATION.md) describes every key.

   ```bash
   cp config/wheeck.json config/wheeck.local.json
   cp config/querier.json config/querier.local.json
   ```

   In `config/`, only the `*.local.json` files are gitignored. Never put real values in `config/wheeck.json` or `config/querier.json`, because those templates are committed.

3. Create the runtime folders. Git tracks none of them, so a fresh clone does not have them. `DDTs/`, `log/` and `res/` are gitignored, and `scheme/` holds only ignored `*.xlsx` and `*.dump` files.

   ```bash
   mkdir -p DDTs/discarded DDTs/recorded log res scheme
   ```

4. Put the Excel templates `scheme/overview.xlsx` and `scheme/summary.xlsx` in `scheme/`. The README lists the sheets each template must contain. Both files match the `*.xlsx` ignore pattern, so they do not come with the repository.

   <!-- VERIFY: the run references scheme/overview.xlsx and scheme/summary.xlsx, which are not in the repository -->

5. Install the database schema as described in the README: run its `\ir` lines at a `psql` prompt started from the repository root. The first script drops the `wheeck` database, so it deletes all recorded data.

There is no `.env` file, no build step and no compile step. The application runs straight from `src/`.

### Running from source

The packages import each other by their top-level names (`import wheeck`, `from core import ...`, `from geo import ...`), so `src/` must be on `sys.path`. Run the batch from the `src/` directory:

```bash
cd src
python main.py
```

In PyCharm, mark `src/` as the sources root. In the run configuration for `src/main.py`, set the working directory to `src/`. The `.idea/` folder is gitignored, so a fresh clone has no IDE settings.

Two import details matter when you write scripts or tests against the packages:

- **Import `wheeck` before `geo`.** `src/geo/geo.py` imports `PATH_LOG` from `wheeck`, and `wheeck` imports `geo`. If you run `import geo` first, it fails with `ImportError: cannot import name 'GeoMap' from 'geo'`. Keep `from .constants import ...` as the first line of `src/wheeck/__init__.py`. [ARCHITECTURE.md](ARCHITECTURE.md) explains the chain.
- **Importing `wheeck` or `geo` touches the log folder.** The modules `src/wheeck/recording_docs.py`, `src/wheeck/overview_docs.py` and `src/geo/geo.py` call `get_logger(PATH_LOG, __name__)` at import time. This opens today's file in `log/`, and creates it if it does not exist. Only `core` imports without side effects.

### No development mode

wheeck has no dry-run or development mode. A run in which at least one file name in `DDTs/` matches the DDT name pattern can do all of the following:

- It renames the PDFs in `DDTs/` and moves them to `DDTs/recorded/`. Pages it cannot read are saved to `DDTs/discarded/`.
- It writes to the database set in the `main` entry of `config/querier.local.json`.
- It calls OpenRouteService for every delivery city that has no stored distance, using the quota of `ors_api_key`.
- It rebuilds the affected workbooks in `res/`.
- It copies each recorded PDF to `backup_dir`, if that key is set.
- It runs `pg_dump` against `127.0.0.1:5432`, database `wheeck`. The new dump replaces `scheme/wheeck.dump` unless it is smaller than the previous one. This command is hardcoded as `CMD_BACKUP_DB` in `src/wheeck/constants.py` and ignores `config/querier.local.json`. If the database is elsewhere, the backup fails, the failure is logged at `CRITICAL` and the run carries on.

Before you run a change, check which database `config/querier.local.json` points to and which PDFs are in `DDTs/`.

## Build commands

The project has no build step, no task runner and no `Makefile`. Run these commands from the repository root unless the command changes directory itself.

| Command | Description |
|---------|-------------|
| `pip install -r requirements.txt` | Installs the four runtime dependencies (`openpyxl`, `openrouteservice`, `pyodbc`, `pypdfium2`), pinned with `~=`. There are no development dependencies and no lockfile. |
| `cd src && python main.py` | Runs the batch on the PDFs in `DDTs/` (see [No development mode](#no-development-mode)). |
| `cd src && python -c "from wheeck import generate_overview; generate_overview(2024, 1)"` | Rebuilds one monthly workbook (`res/2024_01.xlsx`) from the database. `generate_summary(year)` does the same for the yearly trip summary. |
| `psql -U postgres -d postgres`, then the `\ir` lines from the README | Installs the database by running the scripts in `sql/` in dependency order. **It drops the `wheeck` database and all its data first.** |
| `.venv/Scripts/python.exe -m py_compile src/main.py src/core/*.py src/geo/*.py src/wheeck/*.py` | Checks the syntax of every module. It needs no database. Run it in Git Bash, because PowerShell does not expand the `*.py` globs. |
| `cd src && ../.venv/Scripts/python.exe -c "import core; print(core.__version__)"` | Import smoke test of the `core` package. It needs no database and writes no log file. |

The repository has no automated tests. [TESTING.md](TESTING.md) describes the checks that exist and a recommended layout for adding tests.

There is no packaging metadata (`pyproject.toml`, `setup.py`). Each package keeps its own `__version__` by hand in its `__init__.py`: `src/core/__init__.py`, `src/geo/__init__.py` and `src/wheeck/__init__.py`.

## Code style

No linter, formatter or type checker is configured. There is no `pyproject.toml`, `setup.cfg`, `.editorconfig`, ruff, black, flake8, pylint or mypy configuration, and no CI or Git hook enforces style. Follow the house style in [CONVENTIONS.md](CONVENTIONS.md). The main rules are:

- Indent with 4 spaces, leave 2 blank lines between top-level definitions, and keep lines within 120 columns. The limit is not enforced.
- Use single-quoted strings. Switch to double quotes only when the string contains a single quote.
- Write a short compound statement on one line when its body is a single short statement, for example `try: wheeck.run()`.
- Keep paths, regex patterns and SQL queries as module-level constants in `src/wheeck/constants.py` (`PATH_*`, `PATTERN_*`, `QUERY_*`). SQL constants use UPPERCASE keywords, `?` placeholders and schema-qualified names such as `public.delivery`.
- Write type hints, but keep them accurate by hand, because nothing checks them.
- Put each database object in its own file under `sql/<kind>/`, and add its `\ir` line to the install sequence in `README.md`, after every object it depends on.

## Branch conventions

No branch convention is documented. `main` is the default branch and the only branch, both locally and on `origin`. The history has no merge commits, so changes are committed directly to `main`.

Recent commits use the Conventional Commits format `type(scope): subject`, for example:

- `chore(deps): bump pyodbc to 5.5 to run on Python 3.15`
- `refactor(db): split schema script into one file per object`
- `chore(config): move real configs to gitignored *.local.json files`

Older commits use plain sentences. Use the Conventional Commits format for new commits.

## PR process

No pull request process is defined. The repository has no `.github/` directory, no pull request template, no `CONTRIBUTING.md`, no CI pipeline and no pre-commit hook. Nothing checks a change automatically, so check it yourself before you push:

- Run the syntax check and the import smoke test from [Build commands](#build-commands).
- Run the batch on sample DDT PDFs against a database you can change. Then read today's `log/YYYY_MM_DD.log` and the affected tables, or the views `public.vw_discard_message` and `public.vw_gap_message`.
- If you add a column to the delivery data, change `sql/tables/delivery.sql`, `sql/tables/delivery_discard.sql` and `sql/tables/delivery_history.sql` together. [STRUCTURE.md](STRUCTURE.md) lists every place a new field must reach.
- If you add a config key, add it with a `null` value to the committed template `config/wheeck.json`, and document it in [CONFIGURATION.md](CONFIGURATION.md).
- Keep secrets out of commits. `sql/roles/wheeck.sql` is tracked and sets the `wheeck` role password in plain text. If you edit it to set your own password, do not commit that change. [CONCERNS.md](CONCERNS.md) covers the password that is already in the history.
