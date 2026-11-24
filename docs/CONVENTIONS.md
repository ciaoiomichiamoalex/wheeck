<!-- generated-by: codebase-mapper -->
# Coding Conventions

**Analysis Date:** 2026-11-22

Scope: Python sources under `src/` (`src/main.py`, `src/core/`, `src/geo/`, `src/wheeck/`) and the PostgreSQL scripts under `sql/`. No linter or formatter enforces anything, so the rules below are the house style the code already follows. Match them when you change code.

## Naming Patterns

**Files:**
- Modules use `snake_case.py` and are named after their job: `src/wheeck/recording_docs.py` (records documents), `src/wheeck/overview_docs.py` (writes Excel overviews), `src/core/querier.py` (wraps the database), `src/geo/geo.py` (wraps OpenRouteService).
- Packages use one lowercase word: `core`, `geo`, `wheeck`.
- Every module-level `PATH_*`, `PATTERN_*`, `QUERY_*`, `CMD_*` and `OVERVIEW_*` constant, every regex and every SQL string live in `src/wheeck/constants.py`. Class constants (`Querier.FETCH_*`, `_EXCEL_FORMATS`) and the `MessageGenre` templates stay in their class, and some file names are built inline, such as `'overview.xlsx'`, `'summary.xlsx'`, `'wheeck.dump'` and `'querier.json'`.
- SQL scripts use one file per database object, named after the object: `sql/tables/delivery_discard.sql`, `sql/views/vw_gap_message.sql`, `sql/triggers/trg_sync_discard_status.sql`, `sql/functions/sync_discard_status.sql`.

**Functions:**
- Use `snake_case`. Start with a verb prefix that shows what the function does:
  - `check_*`: runs a check and returns its result: a bool, a count or a chosen value. Examples: `check_duplicate`, `check_gaps` and `check_similarity` in `src/wheeck/recording_docs.py`. Most are read-only, but `check_gaps` also writes one GAP warning per gap through `save_warning`. If a new `check_*` function writes, say so in its docstring.
  - `save_*`: writes to the database or a file. Examples: `save_warning` in `src/wheeck/recording_docs.py`; `save_changes` and `save_excel` in `src/core/querier.py`.
  - `generate_*`: writes an Excel output. Examples: `generate_overview`, `generate_summary` and `generate_current` in `src/wheeck/overview_docs.py`.
  - `get_*` / `resolve_*`: computes or looks up a value. Examples: `get_logger` in `src/core/common.py`, `GeoMap.get_distance`, `resolve_distances`.
- Path builders are named after the path they return: `discarded_doc_path(working_doc, working_page)`.

**Variables:**
- Use `snake_case` with domain words: `working_doc`, `working_doc_name`, `working_page`, `discarded_docs`, `job_begin`.
- Name input paths with an `_in` suffix: `cfg_in`, `json_in`, `conn_in`. Name output files `fou` ("file out"), as in `get_logger(fou, ...)` and `Querier.save_excel(fou, ...)`.
- Start boolean locals with `is_`: `is_duplicate`, `is_discard`, `is_discarded`, `is_new_discard`. The one exception in `src/wheeck/` is the `own_querier` flag (see Function Design). Boolean parameters in `src/core/` have no prefix: `single`, `console`, `save`, `save_changes`.
- For openpyxl objects, use `wb` for the workbook and `ws` for the worksheet (`src/wheeck/overview_docs.py`, `src/core/querier.py`).
- Use the word **genre** for "kind/type" everywhere: `document_genre`, `message_genre`, `MessageGenre`, `Querier.fetch(genre=...)`. Do not add `type`/`kind` synonyms, because the database columns use `genre`.

**Types / Classes:**
- Classes use `PascalCase`: `Querier`, `LowQuerier`, `GeoMap`, `Delivery`, `MessageGenre`, `RecordingError`.
- Exception classes end in `Error`. They subclass a built-in exception and contain only a docstring:
  ```python
  class RecordingError(RuntimeError):
      """
      Raised when a database write does not affect exactly one row, in order to abort and roll back the whole
      document currently being recorded.
      """
  ```
- Name class constants in `UPPER_SNAKE` (`Querier.FETCH_VAL`, `FETCH_ONE`, `FETCH_ALL`, `FETCH_MANY`). Prefix private class and instance state with `_`: `_cache_config`, `_EXCEL_FORMATS`, `_connection`, `_cursor`, `_client`.

**Module constants (`src/wheeck/constants.py`):**
- Use `UPPER_SNAKE` and start the name with a category prefix:
  - `PATH_*`: `pathlib.Path` objects derived from `PATH_PRJ = Path(__file__).resolve().parents[2]`.
  - `PATTERN_*`: raw-string regexes.
  - `QUERY_*`: SQL text.
  - `CMD_*`: shell commands.
  - `OVERVIEW_*`: Excel formatting.

**SQL (`sql/` and `QUERY_*` strings):**
- Identifiers are `snake_case` and always schema-qualified (`public.delivery`).
- Name constraints and objects with these prefixes:
  - Primary key: `pk_<table>_id`.
  - Unique constraint: `uq_<table>_<columns>`.
  - Check constraint: `chk_<table>_<column>`.
  - Foreign key: `fk_<referenced_table>_id`.
  - Index: `idx_<table>_<columns>`.
  - View: `vw_<name>`. Trigger: `trg_<name>`.
- Foreign-key columns are named `id_<entity>` (`id_warning_message`).
- Primary keys are always `id INTEGER GENERATED ALWAYS AS IDENTITY`.

## Code Style

**Formatting:**
- No formatter is configured: there is no `pyproject.toml`, `setup.cfg`, `.editorconfig`, black or ruff. The project is edited in PyCharm (`.idea/`), with `src/` marked as the source root in `.idea/wheeck.iml`.
- Indent with 4 spaces. Leave 2 blank lines between top-level definitions.
- Keep lines to **120 columns**. The limit is not enforced: 22 lines go over it, almost all of them log messages or `# name: ... (es. ...)` comments in `src/wheeck/recording_docs.py`. Do not add more overruns in code statements.
- Strings use single quotes: `'utf-8'`, `'IT'`, `'consegne'`. Switch to double quotes only when the string itself contains single quotes:
  ```python
  working_doc = PATH_WORKING_DIR / f"{doc.split('.')[0]}.recording.pdf"
  PATTERN_CITY_DX = r"Luogo di consegna\r\n([\w\t \.\&\-'\/\(\)]+)\r\n..."
  ```
- **Write short compound statements on one line.** The codebase has 39 of them. Use this form when the body is one short statement:
  ```python
  if isinstance(res, dict): res = [res]
  try: pages_text = [page.get_textpage().get_text_bounded() for page in doc]
  finally: doc.close()
  except Exception: logger.exception('unhandled exception')
  else: logger.info('no delivery found in year %d... skipping summary!', year)
  ```
- Put a multi-line conditional expression inside parentheses, with the `else` part on its own line:
  ```python
  return (self.get_distance_from_coords(departure_coords, destination_coords)
          if departure_coords and destination_coords else None)
  ```
- SQL constants are triple-double-quoted strings that start with `"""\`, so they do not begin with a newline. Indent the body by 4 spaces, write keywords in UPPERCASE and put the closing `;` on its own line:
  ```python
  QUERY_UPDATE_WARNING = """\
      UPDATE public.delivery_warning
      SET status = FALSE
      WHERE id = ?
      ;
  """
  ```
- `.sql` files in `sql/` use UPPERCASE keywords and a 4-space indent. `CREATE TABLE/VIEW/INDEX/TRIGGER/FUNCTION` statements put `;` on its own line. One-line statements keep `;` inline: every `DROP ...`, `sql/roles/wheeck.sql`, and `RETURN NEW;` / `END;` inside function bodies.
  - Views and functions: `DROP ... IF EXISTS`, then `CREATE OR REPLACE ...`.
  - Triggers: `DROP TRIGGER IF EXISTS ... ON <table>`, then a plain `CREATE TRIGGER` (`sql/triggers/trg_sync_discard_status.sql`).
  - Tables: `CREATE TABLE IF NOT EXISTS`, with `CREATE INDEX` statements after the table.

**Linting:**
- Not detected. There is no ruff, flake8, pylint or mypy config, and none of these tools is installed in `.venv`. `.idea/inspectionProfiles/profiles_settings.xml` uses the IDE default profile (`USE_PROJECT_PROFILE=false`).
- Type hints are written but never checked. Keep them accurate by hand.

## Import Organization

**Order** (one blank line between groups, alphabetical inside each group):
1. Standard library. Plain `import x` lines come first, then `from x import y` lines (see `src/wheeck/recording_docs.py:1-12`).
2. Third-party packages: `openpyxl`, `pyodbc`, `pypdfium2`, `openrouteservice`.
3. First-party imports, in one block with no blank line inside it:
   - First the absolute imports across packages: `from core import Querier, decode_json, get_logger`, `from geo import GeoMap`.
   - Then the relative imports inside the same package: `from .constants import (...)`, `from .overview_docs import generate_current`.
   - See `src/wheeck/recording_docs.py:16-24` and `src/wheeck/overview_docs.py:8-10`.

**Wrapping:** list long multi-name imports alphabetically inside parentheses. Continuation lines align with the opening parenthesis and fill up to 120 columns:
```python
from .constants import (OVERVIEW_DEFAULT_FONT, OVERVIEW_FORMATS, PATH_CFG_DB, PATH_CFG_PRJ, PATH_LOG, PATH_RES,
                        PATH_SCHEME, QUERY_GET_LAST_OVERVIEWS, QUERY_GET_OVERVIEW, QUERY_GET_SUMMARY)
```

**Path Aliases:**
- None. The import root is `src/`. `python src/main.py` puts `src/` on `sys.path`, so `core`, `geo` and `wheeck` are top-level packages.
- Import across packages through the package (`from core import Querier`), not through the submodule (`from core.querier import Querier`).

**Circular import, order matters:** `src/geo/geo.py` runs `from wheeck import PATH_LOG` while `wheeck` is still being initialised. The chain is `wheeck/__init__` → `recording_docs` → `geo` → `wheeck`. This works only because `src/wheeck/__init__.py` imports `.constants` first. Keep `from .constants import ...` as the first line of `src/wheeck/__init__.py`.

## Error Handling

**Patterns:**
- **One transaction per document.** Follow the structure of `doc_scanner` in `src/wheeck/recording_docs.py`:
  - Make the commit the last database statement of the `try` body. Only the `committed ...` log line follows it.
  - In `except Exception`, roll back inside its own nested `try`, so that a dead connection cannot replace the original exception.
  - Clean up the non-transactional side effects (discard PDFs).
  - End with a bare `raise`. The re-raise is required: without it `run()` archives a failed document.
  ```python
      querier.save_changes(True)
      logger.info('committed %d pages of %s! [%d discarded pages]',
                  doc_pages, working_doc_name, discarded_pages.number)
  except Exception:
      logger.critical('rolling back the whole doc %s... nothing will be saved!', working_doc_name)
      try: querier.save_changes(False)
      except Exception: logger.exception('error on rollback of %s... check the database connection!', working_doc_name)
      ...
      raise
  ```
- **Check every write's row count.** After an INSERT inside the document transaction, check `.rows`. If it is not 1, log at `critical` and raise `RecordingError` with the query's short name:
  ```python
  if querier.run(QUERY_INSERT_DELIVERY, *astuple(delivery)[:-1]).rows != 1:
      logger.critical('error on saving delivery record from page %d of %s... check the database connection!',
                      working_page, working_doc_name)
      raise RecordingError('INSERT_DELIVERY')
  ```
- **Shared querier vs. own querier.** Helpers that write take an optional `querier: Querier = None` (keyword-only in `save_warning`):
  - **No querier passed:** the helper opens its own auto-commit connection and returns the `-1` sentinel on failure.
  - **Querier passed (shared transaction):** the helper raises `RecordingError` instead, so the whole document rolls back.
  - Reuse this shape (`own_querier = querier is None`) for any new helper that writes during `doc_scanner`.
- **External service wrappers return `None`.** `src/geo/geo.py` catches the specific `openrouteservice.exceptions.ApiError`, `HTTPError` and `Timeout` and logs them at `error`, then returns `None`. Callers treat `None` as "unknown" (a NULL `distance`). Do not catch bare `Exception` in wrappers.
- **Per-document isolation in `run()`.**
  - Catch `Exception` around each `doc_scanner` call, log it with `logger.exception`, and restore the original file name so the document is reprocessed.
  - If the restore fails, rename the document to `.failed.pdf`. Then `continue`.
  - One bad document must never stop the batch.
- **Filesystem cleanup errors** are caught as `except OSError as error:` and logged at `critical` with the error in brackets: `'error on removing discard doc %s! [%s]'`.
- **Native handles** (pypdfium2 documents) are closed in `try/finally`. On Windows an open handle blocks later renames (`discard_doc`, `doc_scanner` in `src/wheeck/recording_docs.py`).
- **Missing configuration** raises `IOError` with a `'Querier: ...'` prefix in `src/core/querier.py:62`, `:70` and `:72`.
- **Top level:** `src/main.py` wraps `wheeck.run()` in `try` / `except Exception: logger.exception('unhandled exception')`.

## Logging

**Framework:** standard `logging`, set up through `core.get_logger` (`src/core/common.py`).

**Patterns:**
- Every module that logs creates its logger at import time: `logger = get_logger(PATH_LOG, __name__)` (`src/geo/geo.py`, `src/wheeck/overview_docs.py`, `src/wheeck/recording_docs.py`). `src/main.py` uses the default name `'main'`.
- Handlers:
  - A file handler at `INFO` writes to `log/YYYY_MM_DD.log`.
  - A console handler at `ERROR` writes to the terminal.
  - Format for both: `'%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s - %(message)s'`.
- Always pass values as lazy `%` arguments. Never use f-strings in logger calls (the codebase has 0) or `print` (also 0).
- Message style:
  - Lowercase, apart from acronyms (`API`, `HTTP`, `NULL`, `DDTs`), constant names (`PATTERN_QUANTITY`, `CHECK_DUPLICATE`) and the `JOB` markers.
  - Messages that announce an action start with a verb in `-ing` form (`searching ...`, `saving ...`, `discarding ...`). Outcome and error messages do not (`committed ...`, `found ...`, `error on ...`).
  - `...` ends a step in progress; `!` ends an outcome.
  - Details go in square brackets at the end.
  ```python
  logger.info('searching for PATTERN_QUANTITY on page %d of %s...', working_page, working_doc_name)
  logger.info('searching ok on page %d of %s! [%d]', working_page, working_doc_name, delivery.quantity)
  logger.error('discarding page %d of %s for error on PATTERN_QUANTITY...', working_page, working_doc_name)
  ```
- Level meaning:
  - `info`: every progress step.
  - `warning`: a fallback or a skip (`PATTERN_CITY_DX` falls back to `PATTERN_CITY_SX`; a file name does not match).
  - `error`: a page discard or an external API error.
  - `critical`: a failed database write, a rollback, or a failed file restore.
  - `exception`: an unexpected exception (logs the traceback).
- Use the batch markers `JOB START:`, `JOB END:` and `JOB FAILED:` in `run()` for per-document boundaries.

## Comments

**When to Comment:**
- **Variable-intent comments.** Write them just above the assignment, in the form `# name: meaning (es. example)`. `es.` is Italian for "e.g.". This is the dominant comment style:
  ```python
  # discarded_doc: path to the discard PDF of this page (es. c:/source/wheeck/DDTs/discarded/2024_01_DDT_0001_0100_P001.pdf)
  discarded_doc = discarded_doc_path(working_doc, working_page)
  ```
- **"Why" blocks.** Use multi-line comments before code that is load-bearing or non-obvious: ordering constraints, transaction safety, Windows file locking, compatibility with the SQL views. Examples: the `MessageGenre.DISCARD` comment (`src/wheeck/recording_docs.py:85-90`) and the commit and rollback comments (`:418-440`). Cross-reference other code in brackets: `[see resolve_distances]`.
- Do not add comments that repeat the code.
- Comments, docstrings and log messages are in English. Domain strings stay in Italian, because they match the source documents and Excel templates (`'Luogo di consegna'`, sheet names `'consegne'`, `'viaggi'`).

**Docstrings (reST / Sphinx field lists):**
- Write a docstring for every public class, method and function.
- Use triple double quotes, with the opening and closing quotes on their own lines.
- Start with a one-sentence summary that ends with a period. Then add `:param x:` and `:type x:` pairs, `:return:` and `:rtype:`, and `:raise X:` when the function raises.
- State the default in prose ("defaults to 'IT'") and give examples with `(es. ...)`.
- Start class docstrings with "The X object ..." (`Querier`, `LowQuerier`, `GeoMap`, `Delivery`). The enum uses "The MessageGenre enum ...", and exception classes start with "Raised when ...".
```python
def city_distance(delivery_city: str) -> float | None:
    """
    Calculate the driving distance between the departure position in wheeck.json and a delivery city.

    :param delivery_city: The destination city (es. 'CUNEO').
    :type delivery_city: str
    :return: The distance or None when the city cannot be geocoded.
    :rtype: float | None
    """
```

## Function Design

**Signatures:**
- Annotate every parameter and every return value.
  - Use PEP 604 unions (`str | Path`, `dict | list | None`) and built-in generics (`list[str]`, `tuple[int, int]`, `dict[str, float | None]`).
  - Use `typing.Self` for chainable methods.
  - Optional parameters are written `name: T = None`, without `| None`.
- With two or more parameters, put the first one on the `def` line and each further parameter on its own line, aligned with the opening parenthesis. Methods put `self` alone on the first line:
  ```python
  def discard_doc(working_doc: str | Path,
                  working_page: int) -> Path:

  def charge(self,
             attributes: dict) -> None:
  ```
  Functions with zero or one parameter stay on one line: `def check_gaps() -> int:`, `def city_distance(delivery_city: str) -> float | None:`.
- Annotate locals when the type helps the reader: `querier: Querier = Querier(cfg_in=PATH_CFG_DB)`, `discarded_docs: list[Path] = []`.

**Parameters:**
- Path parameters are annotated `str | Path`. Turn them into `Path` with `Path(x).resolve()` before calling `Path` methods, as `get_logger` and `Querier.__init__` do. `discarded_doc_path` and `doc_scanner` call `.stem` directly, so they need a real `Path`.
- Make new optional parameters on existing functions **keyword-only** (`*`). Then a new parameter cannot shift a positional argument of existing callers: `save_warning(message_genre, *, querier=None, **kwargs)`.
- Do not use call-time values as defaults. Python evaluates defaults once, at import: `job_begin: datetime = datetime.now()` and `year: int = date.today().year` keep the value from import time. Pass these values explicitly.

**Return Values:**
- Return `None` for "not found" or "unavailable": `decode_json`, `GeoMap.search`, `GeoMap.get_distance_from_coords`, `Querier.row_header`.
- Return a tuple for multiple counters: `doc_scanner(...) -> tuple[int, int]` gives (worked pages, discarded pages).
- `Querier.run` returns `self`, so calls chain:
  ```python
  is_duplicate = bool(querier.run(QUERY_CHECK_DUPLICATE, ...).fetch(Querier.FETCH_VAL))
  if querier.run(QUERY_GET_DISTANCE, city).rows == 1: ...
  ```

**Database access:**
- Create a `Querier(cfg_in=PATH_CFG_DB)` for each function and drop it at the end with `del querier`. `Querier` has no `close()`; `__del__` closes the cursor and the connection. Pass the shared `querier` into helpers instead of opening a second connection inside a document transaction.
- Use `?` placeholders (pyodbc qmark style) for all values. For a variable-length `IN (...)` list, put only placeholders into the SQL text, never values: `QUERY_GET_SUMMARY % ', '.join('?' * len(drivers))`.
- Keep SQL out of logic modules. Add new queries as `QUERY_*` constants in `src/wheeck/constants.py`.

**Data objects:**
- `Delivery` (`src/wheeck/recording_docs.py:29`) is a `@dataclass`. Fields filled by extraction use `field(init=False, default=None)`.
- The field order is part of the SQL contract. `astuple(delivery)` feeds `QUERY_INSERT_DISCARD` positionally, and `astuple(delivery)[:-1]` feeds `QUERY_INSERT_DELIVERY` (it drops `id_warning_message`). When you add or move a field, update both INSERT column lists in `src/wheeck/constants.py`.
- The values of the `MessageGenre` enum are `%`-format templates. The SQL views `sql/views/vw_gap_message.sql` and `sql/views/vw_discard_message.sql` parse the rendered text, so changing a template string means changing the view. Use `%(name)s`, not `%(name)d`, for any field that can be `None`.

## Module Design

**Exports:**
- Each package `__init__.py` re-exports its public names explicitly and sets `__version__`:
  ```python
  from .constants import PATH_CFG, PATH_LOG, PATH_RES, PATH_SCHEME
  from .overview_docs import generate_current, generate_overview, generate_summary
  from .recording_docs import (RecordingError, check_duplicate, check_gaps, check_similarity, discard_doc, doc_scanner,
                              run, save_warning)

  __version__ = '1.0.3'
  ```
- No `__all__` is used. When you add a public function, add it to the package `__init__.py`.

**Barrel Files:**
- `src/core/__init__.py`, `src/geo/__init__.py` and `src/wheeck/__init__.py` serve as barrels. Consumers import from the package.

**Configuration access:**
- Read the runtime config with `decode_json(PATH_CFG_PRJ)['key']` for `config/wheeck.local.json`. Database connections go through `Querier(cfg_in=PATH_CFG_DB)`, which reads `config/querier.local.json`.
- The committed `config/*.json` files are templates. Add any new config key to the template too.
- Never commit real values. The `*.local.json` files are gitignored.

**Database scripts:**
- Add a new database object as a new file under `sql/<kind>/`, then add its `\ir` line to the install sequence in `README.md`, in dependency order.

**Commits:**
- Commit messages use Conventional Commits, usually with a scope: `chore(deps): ...`, `refactor(db): ...`, `chore(config): ...`, `fix(...)`, `feat(...)`, `docs(...)`. Some omit it (`docs: map existing codebase`, `chore: remove .planning docs`). Most commits before `cd893bf` use plain subjects (`Fix error on managing path variable`).

---

*Convention analysis: 2026-11-22*
