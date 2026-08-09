# Coding Conventions

**Analysis Date:** 2026-08-09

## Naming Patterns

**Files:**
- Lowercase with underscores, matching Python module convention: `src/wheeck/recording_docs.py`, `src/wheeck/overview_docs.py`, `src/wheeck/constants.py`, `src/geo/geo.py`
- Package `__init__.py` files act as the public API surface for a package, re-exporting selected names and declaring `__version__` (`src/wheeck/__init__.py`, `src/geo/__init__.py`)

**Functions:**
- `snake_case` verbs describing the action: `doc_scanner`, `discard_doc`, `check_duplicate`, `check_gaps`, `check_similarity`, `save_warning`, `run` (`src/wheeck/recording_docs.py`)
- Generator/factory-style functions prefixed `generate_`: `generate_overview`, `generate_summary`, `generate_current` (`src/wheeck/overview_docs.py`)

**Variables:**
- `snake_case`, often named after the domain concept with an inline comment giving an example value, e.g. `working_doc_name`, `discarded_pages`, `discard_doc_source` in `src/wheeck/recording_docs.py`
- Comment style: `# variable_name: description (es. example_value)` placed directly above or beside the assignment — see lines 108-125 of `src/wheeck/recording_docs.py`

**Types/Classes:**
- `PascalCase` for classes and enums: `Delivery`, `MessageGenre`, `GeoMap` (`src/wheeck/recording_docs.py`, `src/geo/geo.py`)
- Private/internal instance attributes prefixed with a single underscore: `self._client` in `src/geo/geo.py`

**Constants:**
- `UPPER_SNAKE_CASE`, grouped by prefix and all centralized in `src/wheeck/constants.py`:
  - `PATH_*` for filesystem locations (`PATH_CFG`, `PATH_LOG`, `PATH_WORKING_DIR`)
  - `PATTERN_*` for compiled-at-use regular expressions (`PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`, `PATTERN_CITY_SX`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`, `PATTERN_WORKING_DOC`, `PATTERN_DISCARD_DOC`)
  - `QUERY_*` for raw SQL strings (`QUERY_INSERT_DELIVERY`, `QUERY_CHECK_DUPLICATE`, `QUERY_GET_SUMMARY`, etc.)
  - `CMD_*` for shell command templates (`CMD_BACKUP_DB`)
  - `OVERVIEW_*` for Excel-formatting lookup tables (`OVERVIEW_DEFAULT_FONT`, `OVERVIEW_FORMATS`)

## Code Style

**Formatting:**
- No formatter config detected (no `.prettierrc`, `black` config, or `pyproject.toml` `[tool.black]` section). Formatting is manual, guided only by IDE project settings in `.idea/codeStyles/codeStyleConfig.xml` (per-project settings enabled, no explicit overrides committed).
- 4-space indentation throughout `src/`.
- Function signatures with multiple parameters are wrapped one-per-line with the closing `) -> ReturnType:` aligned to the opening paren, consistently in `src/geo/geo.py` and `src/wheeck/recording_docs.py`:
  ```python
  def get_distance(self,
                   departure: str,
                   destination: str,
                   departure_country: str = 'IT',
                   destination_country: str = 'IT') -> float | None:
  ```
- Single-line compressed control flow is used for short, self-contained statements instead of a multi-line block — this is a deliberate, repeated style choice, not an inconsistency:
  ```python
  # src/main.py
  try: wheeck.run()
  except Exception: logger.exception('unhandled exception')

  # src/wheeck/recording_docs.py
  if hasattr(self, key): setattr(self, key, value)
  if driver in drivers_enum: delivery.vehicle_driver = driver
  else: logger.info('no delivery found...')
  ```
- Ternary/conditional expressions are preferred over multi-line if/else for value assignment:
  ```python
  delivery.vehicle = (vehicle if vehicle in vehicles_enum
                      else check_similarity(vehicle, vehicles_enum))
  ```

**Linting:**
- No linter config detected (no `.flake8`, `ruff.toml`, `pyproject.toml` lint section, or `setup.cfg`). No CI pipeline enforces style.

## Import Organization

**Order** (observed consistently in `src/wheeck/recording_docs.py`, `src/wheeck/overview_docs.py`, `src/geo/geo.py`):
1. Standard library imports (`import decimal`, `import json`, `import os`, `import re`, `from pathlib import Path`)
2. Third-party package imports (`import pypdfium2`, `import openpyxl`, `import openrouteservice`)
3. Local/internal absolute imports for sibling top-level packages (`from core import Querier, decode_json, get_logger`, `from geo import GeoMap`)
4. Relative imports for same-package modules (`from .constants import (...)`, `from .overview_docs import generate_current`)

Each group is separated by a blank line. Multi-name imports from the same module use parenthesized multi-line lists, wrapped to fit a line-length target of roughly 110-120 characters:
```python
from .constants import (CMD_BACKUP_DB, PATH_CFG, PATH_CFG_PRJ, PATH_DISCARDED_DIR, PATH_LOG, PATH_RECORDED_DIR,
                        PATH_SCHEME, PATH_WORKING_DIR, PATTERN_CITY_DX, PATTERN_CITY_SX, PATTERN_DISCARD_DOC, ...)
```

**Path Aliases:**
- None (Python has no bundler-style aliases). Project-relative paths are resolved once in `src/wheeck/constants.py` via `Path(__file__).resolve().parents[2]` and re-exported as `PATH_*` constants, then imported everywhere else instead of recomputing paths.

**Notable external dependency:**
- `core` (imported as `from core import Querier, decode_json, get_logger` in `src/geo/geo.py`, `src/wheeck/recording_docs.py`, `src/wheeck/overview_docs.py`, `src/main.py`) is NOT part of this repository — it is an external/private package providing the DB access wrapper (`Querier`), JSON config decoding (`decode_json`), and logger factory (`get_logger`). Any convention analysis of logging/DB access patterns must treat `core`'s public API as a fixed contract to call, not code to modify here.

## Error Handling

**Patterns:**
- External API calls are wrapped in narrow, library-specific `except` clauses rather than a bare `except Exception`, each logged and falling back to `None`:
  ```python
  # src/geo/geo.py
  try:
      response = geocode.pelias_search(self._client, address, country=country)
      return (...)
  except exceptions.ApiError as e:
      logger.error('API error on searching (%s, %s)! [status code: %s, error code: %s, error message: %s]',
                   address, country, e.status, e.message['error']['code'], e.message['error']['message'])
  except exceptions.HTTPError as e:
      logger.error('HTTP error on searching (%s, %s)! [status code: %s]', address, country, e.status_code)
  except exceptions.Timeout:
      logger.error('timeout error on searching (%s, %s)!', address, country)
  return None
  ```
- The top-level entry point catches broadly and logs the traceback, since nothing above `src/main.py` can recover:
  ```python
  try: wheeck.run()
  except Exception: logger.exception('unhandled exception')
  ```
- Domain-level "soft failures" (a regex pattern not matching, a DB write returning an unexpected row count) are not raised as exceptions — they are recorded as state on a mutable tracker object (`discarded_pages.on_error` in `src/wheeck/recording_docs.py`) and logged with `logger.error`/`logger.critical`, then handled by discarding the page (`discard_doc`) and writing a warning record (`save_warning`). This keeps `doc_scanner` running across an entire multi-page PDF instead of aborting on the first bad page.
- Database availability problems are distinguished by log level: `logger.critical` is used specifically when a DB write does not affect exactly one row (`querier.run(...).rows != 1`), signaling "check the database connection" in the message text — treat any new DB-write call site the same way for consistency.
- Subprocess failures (`subprocess.run(CMD_BACKUP_DB, ...)`) are caught narrowly with `except subprocess.CalledProcessError as error` and logged via `logger.critical` with `error.stderr`.

## Logging

**Framework:** `core.get_logger` (external package), instantiated once per module at import time, not per call:
```python
logger = get_logger(PATH_LOG, __name__)
```
- `src/main.py` calls `get_logger(wheeck.PATH_LOG)` without a module name (root/app logger); every other module passes `__name__` as the second argument.

**Patterns:**
- Always use the `%s`/`%d` lazy-formatting style (never f-strings) so logging can skip string interpolation when the level is disabled: `logger.info('searching for %s on page %d of %s...', PATTERN_DOC_NUMBER, working_page, working_doc_name)`.
- Log level conveys severity of the discovered condition, not just verbosity:
  - `logger.info` — normal step-by-step progress narration (heavily used; nearly every branch in `doc_scanner` logs entry and success)
  - `logger.warning` — a fallback path is being taken (e.g. right-side city pattern failed, falling back to left-side pattern)
  - `logger.error` — a page/record is being discarded or a query condition failed
  - `logger.critical` — a database write did not behave as expected (row count mismatch), i.e. "check the database connection"
  - `logger.exception` — only used at the outermost boundary (`src/main.py`) to capture an unhandled traceback
- Log messages consistently end with `...` when narrating an in-progress action and `!` when reporting a completed/failed outcome, e.g. `'searching for PATTERN_DOC_NUMBER on page %d of %s...'` then `'searching ok on page %d of %s! [...]'`.

## Comments

**When to Comment:**
- Every non-obvious local variable gets a one-line comment directly above its first use explaining its meaning and, where helpful, a concrete example value in parentheses prefixed `es.` (Italian abbreviation for "esempio", i.e. "example") — see `src/wheeck/recording_docs.py` lines 108-135 and `src/wheeck/overview_docs.py` lines 26-29.
- Comments describe *why*/*what this variable represents*, not a restatement of the code.

**Docstrings:**
- reStructuredText (Sphinx-style) docstrings are used on every public function and class, with `:param name:`, `:type name:`, `:return:`, `:rtype:` fields — used consistently across `src/geo/geo.py`, `src/wheeck/recording_docs.py`, `src/wheeck/overview_docs.py`:
  ```python
  def get_distance(self,
                   departure: str,
                   destination: str,
                   departure_country: str = 'IT',
                   destination_country: str = 'IT') -> float | None:
      """
      Return the distance in kilometers between two addresses, by passing address names.

      :param departure: The departure address.
      :type departure: str
      :param destination: The destination address.
      :type destination: str
      :param departure_country: The country of the departure address in iso-2 or iso-3, defaults to 'IT'.
      :type departure_country: str
      :param destination_country: The country of the destination address in iso-2 or iso-3, defaults to 'IT'.
      :type destination_country: str
      :return: The distance in kilometers.
      :rtype: float | None
      """
  ```
- New public functions/classes/methods must follow this exact docstring shape (summary line, blank line, `:param`/`:type` pairs per argument, `:return:`/`:rtype:`).

## Function Design

**Size:** Most functions are short and single-purpose (`check_similarity`, `check_duplicate`, `save_warning` in `src/wheeck/recording_docs.py` are each under 20 lines). The exception is `doc_scanner` (`src/wheeck/recording_docs.py`, ~230 lines), which intentionally inlines a long sequential pipeline (extract fields → validate → check duplicate/discard → compute distance → persist) — this is treated as an acceptable "orchestration" function, not something to be broken up piecemeal, given it mirrors a strictly ordered document-processing workflow.

**Parameters:**
- Type-hinted with modern PEP 604 union syntax (`str | Path`, `float | None`) rather than `typing.Optional`/`typing.Union`.
- Optional parameters use sensible domain defaults evaluated at call time when appropriate, e.g. `job_begin: datetime = datetime.now()` in `doc_scanner`, `year: int = date.today().year` in `generate_overview`.
- Variadic keyword arguments (`**kwargs`) are used when a function needs to forward named template values, e.g. `save_warning(message_genre: MessageGenre, **kwargs)` which interpolates `kwargs` into an `Enum` value string with `%`-formatting.

**Return Values:**
- Functions that can fail return `None`/a sentinel (`-1` for "message not saved") rather than raising, to keep the calling document-processing loop resilient — see `GeoMap.search`, `GeoMap.get_distance`, `save_warning`.
- Functions performing DB actions consistently return an `int` (row/record count or ID), never the raw DB cursor/connection.

## Module Design

**Exports:**
- Each package's `__init__.py` is the curated public API: `src/wheeck/__init__.py` re-exports specific names from its submodules (`from .overview_docs import generate_current, generate_overview, generate_summary`) rather than `import *`, and declares `__version__` as a plain string (semantic versioning, e.g. `'1.0.3'`).
- Internal submodules (`constants.py`, `recording_docs.py`, `overview_docs.py`) are implementation detail; other packages should import from the package root (`from wheeck import PATH_LOG`) when possible, matching the existing usage in `src/geo/geo.py`.

**Dataclasses:**
- Use `@dataclass` for structured records with `field(init=False, default=None)` for attributes that are populated after construction rather than passed in, keeping the constructor signature limited to values known up front (`document_source`, `page_number`, `recording_date` in `Delivery`, `src/wheeck/recording_docs.py`).
- Provide a `__str__` override for JSON-serializable logging/persistence when a dataclass holds `date`/`datetime`/`Decimal` fields that `json.dumps` cannot handle natively — convert them explicitly before serializing (see `Delivery.__str__`).

**Enums:**
- `Enum` members hold `%`-style string templates as their value (`MessageGenre` in `src/wheeck/recording_docs.py`), interpolated later via `message_genre.value % kwargs`. Follow this pattern for any new categorized/templated message type instead of building strings ad hoc at each call site.

## Language & Locale Notes

- All log messages, docstrings, and inline comments are in English; comment example markers use the Italian abbreviation `es.` (example) — this is the one intentional Italian convention embedded in comments (matches the domain being Italian delivery documents/DDT). Keep new comments' example markers consistent with `es.` rather than switching to `e.g.`.
- SQL query constants use double-quoted schema-qualified table names (`wheeck.delivery`, `wheeck.delivery_discard`, `wheeck.delivery_warning`) and `?` positional placeholders (pyodbc/ODBC style, not `%s`/named parameters) — follow this placeholder style for any new query added to `src/wheeck/constants.py`.

---

*Convention analysis: 2026-08-09*
