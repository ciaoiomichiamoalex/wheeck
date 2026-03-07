# Coding Conventions

**Analysis Date:** 2026-03-07

## Naming Patterns

**Files:**
- Lowercase with underscores: `geo.py`, `recording_docs.py`, `overview_docs.py`, `constants.py`
- Module directories follow same pattern: `wheeck/`, `geo/`
- Main entry point: `main.py`

**Functions:**
- Lowercase with underscores: `doc_scanner()`, `discard_doc()`, `check_duplicate()`, `generate_overview()`, `get_distance()`, `save_warning()`
- Utility functions prefixed with action verbs: `check_*`, `generate_*`, `get_*`, `save_*`

**Variables:**
- Lowercase with underscores for local variables: `working_doc`, `document_number`, `discarded_pages`, `delivery_city`
- Type-prefixed for clarity in complex functions: `working_doc_name`, `overview_name`, `scheme_name`, `querier`
- Private attributes use leading underscore: `_client` (as in `GeoMap._client`)

**Constants:**
- All uppercase with underscores: `PATH_PRJ`, `PATH_CFG`, `PATTERN_DOC_NUMBER`, `OVERVIEW_FORMATS`, `QUERY_INSERT_DELIVERY`
- Grouped by purpose in `constants.py`
- Long query strings defined as module-level constants

**Types:**
- Class names PascalCase: `Delivery`, `GeoMap`, `MessageGenre`
- Enums inherit from `Enum`: `MessageGenre(Enum)`
- Dataclasses use `@dataclass` decorator

## Code Style

**Formatting:**
- 4-space indentation throughout (Python standard)
- Line length not explicitly enforced, but generally follows readability
- Blank lines separate logical sections within functions

**Imports:**
1. Standard library imports: `from datetime import date`, `import json`
2. Third-party imports: `import openpyxl`, `import pypdfium2`
3. Local imports: `from core import`, `from geo import`
- Imports grouped with blank lines between groups
- Both `from X import Y` and `import X` styles used contextually

**Linting:**
- No explicit linter configuration found (no `.flake8`, `.pylintrc`, or `pyproject.toml`)
- Code follows PEP 8 conventions implicitly

## Error Handling

**Patterns:**

1. **Try-except with specific exception types:**
```python
try:
    response = geocode.pelias_search(self._client, address, country=country)
    return (tuple(response['features'][0]['geometry']['coordinates'])
            if response['features'] else None)
except exceptions.ApiError as e:
    logger.error('API error on searching (%s, %s)! [status code: %s]', ...)
except exceptions.HTTPError as e:
    logger.error('HTTP error on searching (%s, %s)! [status code: %s]', ...)
except exceptions.Timeout:
    logger.error('timeout error on searching (%s, %s)!', ...)
return None
```

2. **Conditional checks before operations:**
```python
if not discarded_pages.on_error:
    logger.info('saving delivery...')
    if querier.run(QUERY_INSERT_DELIVERY, *astuple(delivery)[:-1]).rows != 1:
        logger.critical('error on saving...')
```

3. **None returns for recoverable errors:**
- API failures return `None` rather than raising exceptions
- Allows graceful degradation and continue processing

4. **Logging at different levels:**
- `logger.error()` for validation failures
- `logger.warning()` for fallback patterns
- `logger.critical()` for database connection issues
- `logger.info()` for normal operations and state changes

## Logging

**Framework:** Built-in `logging` module via `get_logger()` from `core`

**Patterns:**
```python
logger = get_logger(PATH_LOG, __name__)
```

**Usage:**
- Info level for operation steps and results
- Warning level for recoverable issues (e.g., pattern not found on first try, fallback used)
- Error level for failed extractions and discarded pages
- Critical level for database errors
- Always include contextual parameters: `logger.info('message', param1, param2, param3)`

**Examples:**
```python
logger.info('scanning on page %d of %s...', working_page, working_doc_name)
logger.error('discarding page %d of %s for error on %s...', working_page, working_doc_name, pattern)
logger.critical('error on saving delivery record... check the database connection!')
```

## Comments

**When to Comment:**
- Comments explain WHY, not WHAT (code reads naturally)
- Inline comments for non-obvious logic or business rules
- Comments in string format above variable assignments explain purpose: `# overview_name: path to result overview (es. c:/...)`

**Docstrings:**
- All public functions have docstrings
- Google/NumPy style with parameters and return values
- Format: description, then `:param name:`, `:type name:`, `:return:`, `:rtype:`

**Examples:**
```python
def doc_scanner(working_doc: str | Path,
                job_begin: datetime = datetime.now()) -> tuple[int, int]:
    """
    Scan a DDT PDF document to extract delivery information.

    :param working_doc: The path to the document.
    :type working_doc: str | Path
    :param job_begin: The timestamp of the job starting.
    :type job_begin: datetime
    :return: The number of worked pages and discarded pages.
    :rtype: tuple[int, int]
    """
```

## Function Design

**Size:**
- Most functions 20-100 lines
- `doc_scanner()` at 230 lines is the largest, justified by sequential pattern matching
- Functions focus on single responsibility

**Parameters:**
- Positional parameters for required inputs
- Default parameters for optional values (e.g., `year=date.today().year`)
- Type hints used throughout: `working_doc: str | Path`, `delivery_city: str`
- Union types for flexibility: `str | Path`

**Return Values:**
- Single return per function (no multiple returns with different types)
- Return `None` for errors/not found cases
- Return tuples for multiple values: `tuple[int, int]`
- Return converted types from database: `dict`, `float`, `int`

## Module Design

**Exports:**
- `wheeck/__init__.py` exports main functions: `generate_current`, `generate_overview`, `generate_summary`, `doc_scanner`, `check_duplicate`, `check_gaps`, `discard_doc`, `run`, `save_warning`
- `geo/__init__.py` exports main class: `GeoMap`

**Barrel Files:**
- Used minimally - only export core classes/functions in `__init__.py`
- Constants module (`constants.py`) imports but not re-exported

**Module Dependencies:**
- `wheeck` imports from `geo` (distance calculation)
- `geo` imports from `wheeck` only for `PATH_LOG` (loggers)
- No circular imports
- Clear separation: `geo` handles geolocation, `wheeck` handles PDF extraction and database

## Type Hints

**Usage:**
- Consistent use of Python 3.10+ union syntax: `str | Path`, `tuple[float, float] | None`
- Type hints on function parameters and returns
- Type hints on dataclass fields

**Examples:**
```python
def search(self, address: str, country: str = 'IT') -> tuple[float, float] | None:
def check_similarity(source_record: str, source_enum: list[str]) -> str:
```

## Dataclass Patterns

**Usage in `Delivery` class:**
```python
@dataclass
class Delivery:
    document_number: int = field(init=False, default=None)
    document_source: str  # init=True
    page_number: int      # init=True
    recording_date: datetime  # init=True
```

- Init-disabled fields populated via `charge()` method
- Init-enabled fields set at construction
- Default `None` for extracted fields until populated
- Uses `asdict()` and `astuple()` for conversion

---

*Convention analysis: 2026-03-07*
