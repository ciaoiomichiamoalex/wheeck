# Testing Patterns

**Analysis Date:** 2026-03-07

## Test Framework

**Status:** Not detected

The codebase currently has no automated testing infrastructure. No test files, test runners (pytest, unittest), or test configuration files are present.

**Recommended Setup:**
- Use `pytest` for Python testing (integrates well with typing)
- Configuration via `pyproject.toml` or `pytest.ini`
- Place tests in `tests/` directory or co-locate with source using `test_*.py` pattern

## Test File Organization

**Current State:**
- No test directory structure exists
- No test files found in codebase

**Recommended Pattern:**
```
wheeck/
├── src/
│   ├── wheeck/
│   │   ├── recording_docs.py
│   │   ├── overview_docs.py
│   │   └── constants.py
│   └── geo/
│       └── geo.py
├── tests/
│   ├── __init__.py
│   ├── test_recording_docs.py
│   ├── test_overview_docs.py
│   ├── test_geo.py
│   └── fixtures/
│       ├── sample_pdfs/
│       └── sample_configs/
└── pyproject.toml
```

## Priority Test Areas

**Critical (No Tests Exist):**

1. **`doc_scanner()` in `src/wheeck/recording_docs.py`** (230 lines)
   - Core function responsible for PDF text extraction and data population
   - Multiple regex patterns to validate: `PATTERN_DOC_NUMBER`, `PATTERN_CITY_DX`, `PATTERN_CITY_SX`, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`
   - Database operations: duplicate checking, warning saving, delivery insertion
   - Complex state management with `discarded_pages` object
   - Fallback logic (e.g., DX pattern fails → tries SX pattern)
   - **Test needs:** Sample PDF pages, mock database, regex validation, state transitions

2. **`Delivery` dataclass in `src/wheeck/recording_docs.py`**
   - `__str__()` method requires date/decimal serialization
   - `calculate_distance()` external API call to OpenRouteService
   - `charge()` method for attribute population
   - **Test needs:** Object initialization, serialization, distance calculation with mocked GeoMap

3. **`check_similarity()` in `src/wheeck/recording_docs.py`**
   - String matching algorithm (SequenceMatcher with 50% threshold)
   - Returns original if no match above threshold
   - **Test needs:** Edge cases (exact match, partial match, no match, null input)

4. **`GeoMap` class in `src/geo/geo.py`**
   - Three public methods make external API calls
   - Error handling with multiple exception types: `ApiError`, `HTTPError`, `Timeout`
   - Graceful degradation (returns `None` on error)
   - **Test needs:** Mocked `openrouteservice` client, exception scenarios

5. **`generate_overview()` and `generate_summary()` in `src/wheeck/overview_docs.py`**
   - Complex Excel workbook manipulation with `openpyxl`
   - Date formatting and calendar iteration
   - Database query execution and result iteration
   - **Test needs:** Mock database results, temporary Excel files, format validation

6. **Database operations:**
   - Duplicate detection logic: `check_duplicate()`
   - Gap detection logic: `check_gaps()`
   - Warning message storage: `save_warning()`
   - All use hardcoded SQL queries from `constants.py`
   - **Test needs:** In-memory database fixture or mocked `Querier`, query validation

**High Priority (Complex Logic):**

1. **`run()` in `src/wheeck/recording_docs.py`**
   - Orchestrates document processing pipeline
   - File system operations: move, copy, shutil
   - Database backup via subprocess
   - **Test needs:** Temporary file system, mocked subprocess, file operations

2. **Pattern matching fallbacks** in `doc_scanner()`
   - PATTERN_CITY_DX fails → fallback to PATTERN_CITY_SX
   - Vehicle/driver matching with similarity checks
   - **Test needs:** Regex pattern validation, fallback scenarios

3. **Message genre formatting** in `MessageGenre.Enum`
   - Template strings with kwargs substitution
   - Three message types with different parameter sets
   - **Test needs:** Enum access, string formatting with various kwargs

**Medium Priority (Data Handling):**

1. Configuration loading: `decode_json()` from `core`
   - Tests should validate config structure expectations

2. Excel generation:
   - Font and format application
   - Cell value type mapping (str → '@', int → '#,##0', date → 'dd/mm')

## Testing Patterns to Implement

### Unit Test Structure (Recommended)

**Example test for `check_similarity()`:**
```python
import pytest
from src.wheeck.recording_docs import check_similarity

class TestCheckSimilarity:
    """Test string similarity matching with 50% threshold."""

    def test_exact_match(self):
        """Return matching value when exact match in enum."""
        result = check_similarity('VEHICLE1', ['VEHICLE1', 'VEHICLE2'])
        assert result == 'VEHICLE1'

    def test_high_similarity_match(self):
        """Return best match when similarity > 50%."""
        result = check_similarity('VEHICL1', ['VEHICLE1', 'OTHER'])
        assert result == 'VEHICLE1'

    def test_low_similarity_returns_original(self):
        """Return original value when no match > 50%."""
        result = check_similarity('XXX', ['VEHICLE1', 'VEHICLE2'])
        assert result == 'XXX'

    def test_empty_enum_returns_original(self):
        """Return original when enum is empty."""
        result = check_similarity('VALUE', [])
        assert result == 'VALUE'
```

### Mocking Pattern (Recommended)

**For external dependencies:**
```python
from unittest.mock import Mock, patch, MagicMock
import pytest

@pytest.fixture
def mock_querier():
    """Provide mocked Querier for database operations."""
    querier = Mock()
    querier.run.return_value = querier  # Method chaining
    querier.rows = 0
    querier.fetch.return_value = None
    return querier

@pytest.fixture
def mock_geomap(mocker):
    """Provide mocked GeoMap for API calls."""
    mock = mocker.patch('src.geo.geo.GeoMap')
    mock_instance = Mock()
    mock_instance.search.return_value = (12.5, 45.5)
    mock_instance.get_distance_from_coords.return_value = 150.5
    mock.return_value = mock_instance
    return mock_instance

def test_delivery_calculate_distance(mock_geomap):
    """Test distance calculation uses mocked GeoMap."""
    delivery = Delivery('doc.pdf', 1, datetime.now())
    delivery.delivery_city = 'MILAN'
    delivery.calculate_distance()
    assert delivery.distance == 150.5
    mock_geomap.search.assert_called_once_with('MILAN')
```

### Fixture Pattern (Recommended)

**For test data:**
```python
import pytest
from datetime import date, datetime
from src.wheeck.recording_docs import Delivery

@pytest.fixture
def sample_delivery():
    """Provide a populated Delivery object for testing."""
    delivery = Delivery('2024_01_DDT_0001_0100.pdf', 1, datetime(2024, 1, 15, 10, 30))
    delivery.charge({
        'document_number': 1234,
        'document_genre': 'DDT',
        'document_date': date(2024, 1, 15),
        'company_name': 'ACME INC',
        'delivery_city': 'MILAN',
        'quantity': 500,
        'delivery_date': date(2024, 1, 15),
        'vehicle': 'AB123CD',
        'vehicle_driver': 'JOHN DOE',
        'distance': 150.5
    })
    return delivery

@pytest.fixture
def sample_excel_template(tmp_path):
    """Provide a temporary copy of the Excel template."""
    template_path = Path(__file__).parent / 'fixtures' / 'overview_template.xlsx'
    temp_xlsx = tmp_path / 'test_overview.xlsx'
    shutil.copy(template_path, temp_xlsx)
    return temp_xlsx
```

## Run Commands

**Recommended pytest setup:**
```bash
pytest                    # Run all tests
pytest tests/            # Run tests in tests directory
pytest -v                # Verbose output
pytest --cov=src         # Generate coverage report
pytest -k test_scanner   # Run specific test by name
pytest tests/test_geo.py::TestGeoMap::test_search  # Run single test
```

## Test Coverage Gaps

| Area | Current Coverage | Recommended Coverage |
|------|------------------|----------------------|
| `doc_scanner()` | 0% | 80%+ (core function) |
| `Delivery` class | 0% | 90%+ (data class) |
| `GeoMap` class | 0% | 85%+ (external API) |
| `check_similarity()` | 0% | 95%+ (matching logic) |
| `generate_overview()` | 0% | 70%+ (Excel generation) |
| `generate_summary()` | 0% | 70%+ (Excel generation) |
| Regex patterns | 0% | 100% (pattern validation) |
| Database operations | 0% | 70%+ (mocked) |
| Error handling | 0% | 80%+ (exception paths) |

## What NOT to Test

- External API behavior (mock `openrouteservice` and `openpyxl`)
- Database schema (provide fixed SQL, mock `Querier`)
- File system existence (use `tmp_path` fixture for temporary files)
- Logging output (mock logger or assert log calls)

## What TO Test

- Regex pattern matching accuracy
- String similarity algorithm behavior
- Data class serialization
- Fallback logic (e.g., DX pattern fails → SX pattern)
- Error handling paths (API timeouts, HTTP errors)
- State transitions in `doc_scanner()` loop
- Complex query results processing
- Excel cell formatting application

---

*Testing analysis: 2026-03-07*
