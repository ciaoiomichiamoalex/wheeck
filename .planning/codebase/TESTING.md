# Testing Patterns

**Analysis Date:** 2026-08-09

## Test Framework

**Runner:**
- Not detected. No `pytest`, `unittest`, `nose`, or any other test runner is declared in `requirements.txt`, and no test configuration file (`pytest.ini`, `pyproject.toml`, `setup.cfg`, `tox.ini`) exists anywhere in the repository.

**Assertion Library:**
- Not applicable (no tests exist).

**Run Commands:**
```bash
# Not applicable — no test command is defined anywhere (no Makefile, no CI workflow, no package script).
```

## Test File Organization

**Location:**
- Not applicable. No `tests/` directory, no `test_*.py` / `*_test.py` files, and no `conftest.py` exist anywhere under `src/` or the repo root.

**Naming:**
- Not applicable.

**Structure:**
```
Not applicable — repository currently contains only `src/wheeck/` and `src/geo/` implementation packages, no test tree.
```

## Test Structure

Not applicable — no existing suite to model conventions from.

## Mocking

**Framework:** Not detected (no `unittest.mock`, `pytest-mock`, or `responses`/`vcrpy` usage found anywhere in `src/`).

**What would need mocking if tests are added:**
- `core.Querier` (external DB wrapper imported in `src/wheeck/recording_docs.py` and `src/wheeck/overview_docs.py`) — all DB access goes through this single class, so it is the natural seam to mock/stub for unit tests of `doc_scanner`, `check_duplicate`, `check_gaps`, `save_warning`, `generate_overview`, `generate_summary`, `generate_current`.
- `openrouteservice.Client` / `directions.directions` / `geocode.pelias_search` (used only inside `src/geo/geo.py`) — network calls that must not run in unit tests; `GeoMap.search` and `GeoMap.get_distance_from_coords` are the two call sites to intercept.
- `pypdfium2.PdfDocument` (used in `src/wheeck/recording_docs.py` for `doc_scanner` and `discard_doc`) — real PDF parsing should be exercised with small fixture PDFs rather than mocked, since the regex patterns in `src/wheeck/constants.py` (`PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`, `PATTERN_CITY_SX`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`) are the actual thing under test and only produce realistic input from real extracted PDF text layout.
- `subprocess.run(CMD_BACKUP_DB, ...)` in `wheeck.run()` (`src/wheeck/recording_docs.py`) — shells out to `pg_dump`; must be mocked/stubbed in any test of `run()`.
- `time.sleep(1.5)` in `doc_scanner` (rate-limiting for the openrouteservice API) — should be patched out in tests to avoid slow test runs.

## Fixtures and Factories

Not applicable — no fixtures exist. If added, natural fixture candidates are:
- Sample DDT PDF pages covering each regex branch (right-side city match, left-side fallback, missing vehicle driver, already-a-discard-document filename pattern) — the pattern constants live in `src/wheeck/constants.py` and the filename patterns are `PATTERN_WORKING_DOC` / `PATTERN_DISCARD_DOC`.
- A minimal `config/wheeck.json`-shaped dict fixture with `members`, `departure_coords`, `ors_api_key`, `backup_dir` keys (see `src/wheeck/recording_docs.py` usage of `decode_json(PATH_CFG_PRJ)['members']` and `['departure_coords']`) — note the real `config/wheeck.json` and `config/querier.json` exist on disk but must never be read into test fixtures verbatim (they contain live API keys / DB connection info); use synthetic placeholder values instead.

## Coverage

**Requirements:** None enforced — no coverage tool (`coverage.py`, `pytest-cov`) is configured or present in `requirements.txt`.

**View Coverage:**
```bash
# Not applicable — no coverage tooling configured.
```

## Test Types

**Unit Tests:** None exist. Highest-value first targets, in priority order:
1. `check_similarity()` (`src/wheeck/recording_docs.py`) — pure function (string similarity via `difflib.SequenceMatcher`), no I/O, trivial to unit test with a table of (source_record, source_enum, expected) cases including the >50% threshold boundary.
2. Regex constants in `src/wheeck/constants.py` (`PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`, `PATTERN_CITY_SX`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`, `PATTERN_WORKING_DOC`, `PATTERN_DISCARD_DOC`) — can be tested directly against captured sample text strings without needing a real PDF or DB, and are the most failure-prone part of the system (already patched once per `git log`, e.g. commit `3cde393 Fix regex for CITY_SX pattern to handle additional characters like commas`).
3. `Delivery.__str__()` and `Delivery.charge()` (`src/wheeck/recording_docs.py`) — pure dataclass logic, easy to isolate.
4. `GeoMap.search()` / `GeoMap.get_distance()` / `GeoMap.get_distance_from_coords()` (`src/geo/geo.py`) — with `openrouteservice.Client` mocked, verify the three distinct exception-handling branches (`ApiError`, `HTTPError`, `Timeout`) each log and return `None` without raising.

**Integration Tests:** None exist. `doc_scanner()` (`src/wheeck/recording_docs.py`) is the primary integration surface — it combines PDF text extraction, regex matching, DB reads/writes via `core.Querier`, similarity fallback, distance calculation, and discard-file generation in one function. An integration test would need a real or containerized Postgres with the `wheeck` schema (see `scheme/wheeck.sql`) plus fixture PDFs, and would mock only the external network dependency (`GeoMap`).

**E2E Tests:** Not used. `run()` (`src/wheeck/recording_docs.py`) drives the full pipeline (scan `DDTs/` directory → process each doc → generate Excel overviews via `src/wheeck/overview_docs.py` → back up the DB via `pg_dump`), and would need a fully provisioned environment (DB, filesystem dirs `DDTs/discarded`, `DDTs/recorded`, `log/`, `res/`, `scheme/`, `config/wheeck.json` with valid `ors_api_key`) to exercise end-to-end. No such harness exists.

## Common Patterns

**Async Testing:** Not applicable — codebase is fully synchronous (no `async`/`await` anywhere in `src/`).

**Error Testing:**
```python
# No existing pattern. If added, the natural approach given the codebase's
# "log and return None" error-handling convention (see CONVENTIONS.md) is to
# assert the return value and assert on caplog/logger output rather than
# asserting a raised exception, e.g. for GeoMap.search():
#
#   def test_search_returns_none_on_api_error(mocker, caplog):
#       mocker.patch("geo.geo.geocode.pelias_search", side_effect=exceptions.ApiError(...))
#       geo_map = GeoMap(cfg_in=fixture_cfg_path)
#       assert geo_map.search("Via Roma 1, Torino") is None
#       assert "API error on searching" in caplog.text
```

## Recommended Setup (Not Currently Present)

- Add `pytest` (and `pytest-mock` for the `mocker` fixture) to `requirements.txt` as a dev-only dependency (e.g. split into `requirements-dev.txt` or a `[test]` extra) since `requirements.txt` currently lists only runtime dependencies (`openpyxl`, `openrouteservice`, `pyodbc`, `pypdfium2`).
- Place tests under a new top-level `tests/` directory mirroring the `src/` package layout (`tests/wheeck/test_recording_docs.py`, `tests/wheeck/test_overview_docs.py`, `tests/geo/test_geo.py`), matching the existing `src/wheeck/`, `src/geo/` split.
- Because `core` (providing `Querier`, `decode_json`, `get_logger`) is an external package not present in this repo, any test suite must either install/stub it or fully mock its imports at the module boundary — there is no in-repo fake to reuse yet.

---

*Testing analysis: 2026-08-09*
