<!-- generated-by: gsd-doc-writer -->
# Testing Patterns

**Analysis Date:** 2026-11-22

**Summary:**
- The repository has **no automated tests**. There is no test directory, no test file, no test runner config, and no test dependency in `requirements.txt` or `.venv`.
- **Current verification:** nothing is scripted. The compile and import checks under Running Tests can be run today, but nothing in the repo runs them. The only other check is a manual end-to-end run against a local PostgreSQL.
- **What this document covers:**
  - The checks that exist today.
  - The test seams and hazards found in the code.
  - A recommended pattern for adding tests. Every test example in this document was run against the current sources in the recommended `tests/` layout (Python 3.15.1, stdlib `unittest`, the discovery command below) and passes. The examples live in no file in the repo.

## Test Framework and Setup

**Runner:**
- Not detected. There is no `pytest.ini`, `pyproject.toml`, `setup.cfg`, `tox.ini` or `conftest.py`. In the local, gitignored `.idea/wheeck.iml`, `TestRunnerService` is empty.
- Recommended: stdlib `unittest` (the local `.venv/pyvenv.cfg` records Python 3.15.1). It needs no new dependency, and `requirements.txt` holds runtime packages only.

**Assertion Library:**
- Not detected. Recommended: the `unittest.TestCase` assertions, plus `unittest.mock` for doubles.

**Setup:**
- No test-only install is needed. `unittest` and `unittest.mock` ship with Python.
- The runtime dependencies must be installed, because importing `wheeck` loads `pyodbc`, `openpyxl`, `pypdfium2` and `openrouteservice`. Use the same virtualenv as the app:
  ```bash
  python -m venv .venv
  source .venv/Scripts/activate
  pip install -r requirements.txt
  ```
- Tests that follow this document need no database, no `config/*.local.json` and no network.

## Running Tests

```bash
# runnable today: syntax check of every module (no DB; writes only the gitignored __pycache__/)
.venv/Scripts/python.exe -m py_compile src/main.py src/core/*.py src/geo/*.py src/wheeck/*.py

# runnable today: import smoke test of the side-effect-free package (no DB, no log file)
cd src && ../.venv/Scripts/python.exe -c "import core; print(core.__version__)"

# recommended once tests/ exists (run from the repo root)
.venv/Scripts/python.exe -m unittest discover -s tests -t . -v
```
- Keep `-t .` in the discovery command. It makes `tests` a package, so `tests/__init__.py` runs before any test module.
- Without `-t .`, discovery imports the test modules as top-level names and skips `tests/__init__.py`. The imports of `wheeck` then fail. With `PYTHONPATH=src` added they succeed, but the log guard is skipped, so the modules attach their real `FileHandler`s.
- Watch mode: Not available.
- Coverage: Not available, because `coverage` is not installed.

## Writing New Tests

The short version:
1. Create `tests/__init__.py` with the `sys.path` and logging setup shown under Test Structure.
2. Add one `tests/test_<module>.py` per source module, with `unittest.TestCase` classes.
3. Replace every `Querier`, network, PDF and filesystem dependency with a fake or a patch (see Mocking).
4. Run the discovery command under Running Tests.

Read Common Patterns before writing the first test: importing the application has side effects.

### Test File Organization

**Location:**
- Not detected. Recommended: a top-level `tests/` directory next to `src/`. Do not put tests inside `src/`: `src/` is the import root, and every package there is importable application code.

**Naming:**
- Recommended: `tests/test_<module>.py`, one file per source module: `tests/test_recording_docs.py`, `tests/test_querier.py`, `tests/test_common.py`, `tests/test_constants.py`. Name each test method `test_<behaviour>`.

**Structure:**
```
tests/
├── __init__.py              # sets up sys.path and logging before any app import (see Test Structure)
├── test_common.py           # decode_json, get_logger
├── test_constants.py        # PATTERN_* regexes against sample DDT text
├── test_recording_docs.py   # check_similarity, discarded_doc_path, save_warning, resolve_distances, doc_scanner
└── fixtures/                # sample text layers / small PDFs (never real DDTs, they hold customer data)
```

### Test Structure

**Suite Organization (recommended; verified to pass):**

`tests/__init__.py` runs once, before any test module:
```python
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
# must run BEFORE importing wheeck/geo: get_logger() adds its file handler only when no handler exists up the tree
logging.getLogger().addHandler(logging.NullHandler())
```

`tests/test_recording_docs.py`:
```python
import unittest
from pathlib import Path

from wheeck.recording_docs import check_similarity, discarded_doc_path


class CheckSimilarityTest(unittest.TestCase):
    def test_close_match_returns_enum_value(self):
        self.assertEqual(check_similarity('CN123AB', ['CN123AC', 'TO999ZZ']), 'CN123AC')


class DiscardedDocPathTest(unittest.TestCase):
    def test_strips_recording_suffix_and_pads_page(self):
        path = discarded_doc_path(Path('2026_01_DDT_0001_0100.recording.pdf'), 7)
        self.assertEqual(path.name, '2026_01_DDT_0001_0100_P007.pdf')
```

**Patterns:**
- **Setup:** in `tests/__init__.py`, add `src/` to `sys.path` and give the root logger a `NullHandler`. Both must happen before the first application import. `get_logger` (`src/core/common.py`) checks `logger.hasHandlers()`, which walks up to the root logger. With a root handler in place, it skips the `FileHandler` on `log/<today>.log` and the console handler.
- **Teardown:** nothing is needed for pure functions. Code that touches the filesystem must run against a temp dir (`tempfile.TemporaryDirectory`) with the `PATH_*` names patched (see Mocking).
- **Assertions:** assert return values and the arguments recorded by the fake `Querier`. To check database behaviour, assert which `QUERY_*` constant ran and with which positional args. Do not assert on log text.

### Mocking

**Framework:** `unittest.mock` (stdlib) for recommended use. No mocking library exists in the repo today.

**Patterns (recommended; verified to pass):**
```python
from unittest import mock

from wheeck.recording_docs import MessageGenre, RecordingError, resolve_distances, save_warning


class FakeQuerier:
    """Stands in for core.Querier: run() records the call and chains, fetch() returns a canned value."""
    def __init__(self, rows=1, value=None):
        self.rows, self._value, self.calls = rows, value, []

    def run(self, query, *args):
        self.calls.append((query, args))
        return self

    def fetch(self, genre=None, size=200):
        return self._value


class SaveWarningTest(unittest.TestCase):
    def test_shared_querier_failure_raises(self):
        with self.assertRaises(RecordingError):
            save_warning(MessageGenre.GAP, querier=FakeQuerier(rows=0), document_number=1, document_year=2026)

    def test_returns_inserted_id(self):
        querier = FakeQuerier(rows=1, value=42)
        self.assertEqual(save_warning(MessageGenre.GAP, querier=querier, document_number=1, document_year=2026), 42)
        self.assertEqual(querier.calls[0][1], ('GAP', 'Found gap for doc number 1 of year 2026'))


class ResolveDistancesTest(unittest.TestCase):
    TEXT = 'Luogo di consegna\r\nACME SRL\r\nVIA ROMA 1\r\n12100 Cuneo (CN)\r\nTelefono'

    def test_cached_city_skips_api(self):
        with mock.patch('wheeck.recording_docs.Querier', return_value=FakeQuerier(rows=1, value=12.3)), \
                mock.patch('wheeck.recording_docs.city_distance') as city_distance:
            self.assertEqual(resolve_distances([self.TEXT]), {'CUNEO': 12.3})
        city_distance.assert_not_called()

    def test_new_city_calls_api_and_throttles(self):
        with mock.patch('wheeck.recording_docs.Querier', return_value=FakeQuerier(rows=0)), \
                mock.patch('wheeck.recording_docs.city_distance', return_value=None), \
                mock.patch('wheeck.recording_docs.time.sleep') as sleep:
            self.assertEqual(resolve_distances([self.TEXT]), {'CUNEO': None})
        sleep.assert_called_once_with(1.5)
```

**Patch where the name is looked up.** `src/wheeck/recording_docs.py` and `src/wheeck/overview_docs.py` bind their imports into their own module namespace, so patch the consuming module:

| Dependency | Patch target | Why |
|---|---|---|
| `Querier` | `wheeck.recording_docs.Querier`, `wheeck.overview_docs.Querier` | `Querier.__init__` opens a pyodbc connection immediately; `PATH_CFG_DB` points at the gitignored `config/querier.local.json` |
| OpenRouteService | `wheeck.recording_docs.city_distance` (or `geo.geo.openrouteservice.Client`) | network calls, API key in `config/wheeck.local.json` |
| Throttle | `wheeck.recording_docs.time.sleep` | `resolve_distances` sleeps 1.5 s per new city. This target is the global `time` module, so the patch applies process-wide while it is active |
| PDF I/O | `wheeck.recording_docs.pypdfium2.PdfDocument`, `wheeck.recording_docs.discard_doc` | native handles; on Windows an open handle blocks renames |
| Paths | `wheeck.recording_docs.PATH_WORKING_DIR`, `PATH_DISCARDED_DIR`, `PATH_RECORDED_DIR`, `PATH_SCHEME`; `wheeck.overview_docs.PATH_RES`, `PATH_SCHEME` | they resolve to the real repo dirs `DDTs/`, `res/`, `scheme/` |
| Config reads | `wheeck.recording_docs.decode_json` / `wheeck.overview_docs.decode_json` | `PATH_CFG_PRJ` is the gitignored `config/wheeck.local.json` |
| DB backup | `wheeck.recording_docs.subprocess.run` | `run()` shells out to `pg_dump` (`CMD_BACKUP_DB`) with `shell=True` |

- The `pypdfium2.PdfDocument` and `subprocess.run` targets also patch the shared module, like `time.sleep`. They apply process-wide while active.
- Patching `geo.geo.*` in a process that has not imported `wheeck` raises the circular `ImportError` described under Common Patterns. The `wheeck.*` targets resolve on their own.

**Fake `Querier` contract.** Production code relies on these members:
- `run(query, *args)`: returns `self` and sets `.rows`.
- `fetch(genre, size)`: returns the canned value.
- `row_header()`: used on the re-discard path in `doc_scanner`.
- `save_changes(bool)`: True commits, False rolls back.
- `__iter__`: used by `check_gaps`, `generate_overview` and `generate_summary`. `generate_current` uses `fetch(Querier.FETCH_ALL)` instead.

Rows returned by pyodbc support attribute access (`row.year`, `discard.id_warning_message`) and iteration (`None not in discard`). Build fake rows with `collections.namedtuple`, which supports both.

**`LowQuerier` is not a drop-in fake.** This was checked against `src/core/querier.py`:
- After a SELECT, `.rows` is `-1`.
- `save_changes()` raises `AttributeError: 'sqlite3.Cursor' object has no attribute 'commit'`.
- `fetch` returns `dict`s, not attribute rows.
- Every `QUERY_*` in `src/wheeck/constants.py` is PostgreSQL-specific: the `public.` schema prefix, `RETURNING`, `::INT`, `INTERVAL '1' DAY`.

Use a fake object or `mock`, not SQLite.

**What to Mock:**
- The `Querier` constructor, OpenRouteService, `time.sleep`, `pypdfium2`, `subprocess.run`, and every `PATH_*` constant that writes to disk.
- Pass the fake through the existing injection seam where one exists: `check_duplicate(delivery, querier=fake)` and `save_warning(genre, querier=fake, **kwargs)`.

**What NOT to Mock:**
- Pure logic:
  - The `PATTERN_*` regexes.
  - `check_similarity`, `discarded_doc_path` and `decode_json` (give it a temp JSON file).
  - `Delivery.__str__` and `Delivery.charge`.
  - The `MessageGenre` templates.
- The `Delivery` dataclass. Build real instances: `Delivery('2026_01_DDT_0001_0100.pdf', 1, datetime(2026, 1, 5))`.

### Fixtures and Factories

**Test Data:**
- Not detected. Recommended: inline text-layer strings that copy what pypdfium2 returns. The text uses `\r\n` line endings, and the `PATTERN_*` regexes in `src/wheeck/constants.py` depend on them:
```python
DOC_NUMBER_LINE = 'Num. D.D.T. 1.234/AB Data D.D.T. 05/01/2026 Pag. 1'
CITY_BLOCK = 'Luogo di consegna\r\nACME SRL\r\nVIA ROMA 1\r\n12100 Cuneo (CN)\r\nTelefono'

self.assertEqual(re.search(PATTERN_DOC_NUMBER, DOC_NUMBER_LINE).groups(), ('1.234', 'AB', '05/01/2026'))
self.assertEqual(re.search(PATTERN_CITY_DX, CITY_BLOCK).group(2), 'Cuneo')
self.assertTrue(re.search(PATTERN_WORKING_DOC, '2026_01_DDT_0001_0100.pdf'))
```
- File-name fixtures must follow `PATTERN_WORKING_DOC` (`YYYY_MM_DDT_NNNN_NNNN.pdf`). Discard files add `_PNNN` (`PATTERN_DISCARD_DOC`).

**Location:**
- Not detected. Recommended: `tests/fixtures/`.
- Do not use files from `DDTs/`, `res/` or `scheme/`. They hold real business data and none of them is tracked: `.gitignore` excludes `/DDTs/` and `/res/`, and the `scheme/` files by their `*.xlsx` and `*.dump` patterns. `res/` and `scheme/overview.xlsx` are symlinks outside the repo.

## Coverage Requirements

**Requirements:** No coverage threshold configured. No coverage tool is installed or configured.

**View Coverage:**
```bash
# Not available; would require adding coverage as a dev-only dependency (not in requirements.txt)
```

**Coverage hot spots** (highest risk, currently 0% automated):
- `doc_scanner`, `src/wheeck/recording_docs.py:159-441`:
  - Per-page extraction.
  - The discard / re-discard / duplicate branches.
  - Commit-last and rollback-plus-cleanup-plus-re-raise.
- `run`, `src/wheeck/recording_docs.py:593-662`: requeue on failure, the `.failed.pdf` fallback, and backup rotation.
- `save_warning`: the shared vs. own querier behaviour. With its own querier a failed insert returns the `-1` sentinel; with a shared one it raises `RecordingError`.
- `check_duplicate`: opens and closes its own `Querier` only when no shared one is passed.
- `MessageGenre` templates: `DISCARD` must render with `document_number=None`. The views `public.vw_discard_message` and `public.vw_gap_message` parse the rendered `DISCARD` and `GAP` text back, so any wording change breaks them.

## Test Types

**Unit Tests:**
- Not present.
- Scope when added:
  - The pure helpers and regexes.
  - The `Querier`-dependent functions, driven through a fake `Querier`.
  - `doc_scanner`, with `pypdfium2.PdfDocument` patched to yield fake pages whose `get_textpage().get_text_bounded()` returns fixture text, plus `resolve_distances` and `discard_doc` patched.

**Integration Tests:**
- Not present.
- Manual path:
  1. Install the database on a **disposable** PostgreSQL server with the `\ir` sequence from the README.
  2. Point a `main` entry in `config/querier.local.json` at that database.
- Warning: `sql/database/wheeck.sql` runs `DROP DATABASE IF EXISTS wheeck`. Never run the install on a server whose `wheeck` database holds data you need.
- `sql/roles/wheeck.sql` runs `CREATE USER wheeck`. Roles are cluster-wide, so on a cluster that already has the role this statement fails.
- `Querier` also accepts `conn_str=` (a dict) and `conn_name=`. The `wheeck` functions never pass these and always use `Querier(cfg_in=PATH_CFG_DB)`; `save_warning` (`src/wheeck/recording_docs.py`) also passes `save_changes=True`.

**E2E Tests:**
- Not used. The manual end-to-end check:
  1. Put a correctly named PDF in `DDTs/`.
  2. Run `.venv/Scripts/python.exe src/main.py`.
  3. Inspect `log/<today>.log` for the `JOB START:` / `JOB END:` / `JOB FAILED:` markers.
  4. Check `DDTs/recorded/` (`*.recorded.pdf`), `DDTs/discarded/` (`*_PNNN.pdf`) and the `public.delivery`, `public.delivery_discard` and `public.delivery_warning` tables.
- Caveat: a disposable database does not isolate the run. When `run()` picks up at least one correctly named PDF, even one that is rolled back, it then does three more steps:
  - Runs `check_gaps`.
  - Runs `generate_current`, which rewrites the Excel files in `res/` (a symlink outside the repo) for every month with deliveries recorded in the last day.
  - Runs `pg_dump` into `scheme/`.
- `CMD_BACKUP_DB` (`src/wheeck/constants.py`) is fixed to database `wheeck` on `127.0.0.1:5432` as user `postgres`. It ignores `config/querier.local.json`. The size-based rotation can then replace `scheme/wheeck.dump`. Back up `res/` and `scheme/wheeck.dump` before a manual run.

## Common Patterns

**Hazards to account for in any test:**
- **Import side effects.** These modules call `get_logger` at import: `src/main.py` (`get_logger(wheeck.PATH_LOG)`, logger name `main`), and `src/geo/geo.py`, `src/wheeck/overview_docs.py`, `src/wheeck/recording_docs.py` (`get_logger(PATH_LOG, __name__)`). Without the root `NullHandler` set up first, importing `wheeck` or `geo` creates or appends `log/<today>.log` in the real repo.
  - `import wheeck.constants` also runs `src/wheeck/__init__.py`, which imports every module.
  - Only `core` is free of side effects.
- **Import `wheeck` before `geo`.** `src/geo/geo.py` imports `PATH_LOG` from `wheeck`, and `wheeck.recording_docs` imports `GeoMap` from `geo`. A process whose first import is `geo` fails with `ImportError: cannot import name 'GeoMap' from 'geo'`. A `tests/test_geo.py` must run `import wheeck` before `import geo`.
- **Defaults frozen at import.** `doc_scanner(job_begin=datetime.now())`, `generate_overview(year=date.today().year, month=date.today().month)` and `generate_summary(year=date.today().year)` are evaluated once. Always pass these arguments explicitly in tests.
- **`Path` required.** `discarded_doc_path` and `doc_scanner` call `.stem` on `working_doc`. Passing a `str` raises `AttributeError`, despite the `str | Path` annotation.
- **Connections open eagerly.** Each `Querier(...)` connects in `__init__` and closes only in `__del__`. A test that forgets to patch `Querier` fails with `OSError: Querier: no config ... found!` when `config/querier.local.json` is absent, or connects to a real database when it is present. The failed constructor also prints `Exception ignored while calling deallocator`, because `__del__` runs on an object that has no `_cursor`.

**Async Testing:**
- Not applicable. The code is fully synchronous. The only timing behaviour is `time.sleep(1.5)` in `resolve_distances`; patch it.

**Error Testing (recommended; verified to pass):**
```python
def test_shared_querier_failure_raises(self):
    with self.assertRaises(RecordingError):
        save_warning(MessageGenre.GAP, querier=FakeQuerier(rows=0), document_number=1, document_year=2026)

def test_discard_template_accepts_missing_document_number(self):
    MessageGenre.DISCARD.value % dict(page=1, doc='x.pdf', pattern='PATTERN_DOC_NUMBER',
                                      document_number=None, document_genre=None, document_date=None)

def test_discarded_doc_path_rejects_str(self):
    with self.assertRaises(AttributeError):
        discarded_doc_path('2026_01_DDT_0001_0100.recording.pdf', 7)
```

## CI Integration

No CI pipeline detected:
- There is no `.github/workflows/` directory and no other CI configuration in the repository.
- The repository ships no hook configuration, and the local clone has no active git hooks (`.git/hooks` holds only `*.sample` files).

Nothing runs the checks automatically. Run the commands under Running Tests by hand before committing.

---

*Testing analysis: 2026-11-22*
