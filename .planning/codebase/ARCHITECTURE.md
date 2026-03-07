# Architecture

**Analysis Date:** 2026-03-07

## Pattern Overview

**Overall:** Document Processing Pipeline with Data Validation and Reporting

**Key Characteristics:**
- Modular separation of concerns (PDF parsing, geo-location, data persistence, reporting)
- ETL-style pipeline: Extract (PDF) → Transform (regex patterns, validation) → Load (database)
- Configuration-driven pattern matching and delivery information extraction
- External service integration (OpenRouteService for distance calculation)
- Asynchronous warning/message logging system for data quality tracking

## Layers

**Presentation/Reporting Layer:**
- Purpose: Generate Excel summaries and overviews of delivery data
- Location: `src/wheeck/overview_docs.py`
- Contains: Excel generation functions, spreadsheet formatting
- Depends on: Database queries, OpenPyXL library
- Used by: Main orchestration flow after data recording

**Business Logic/Processing Layer:**
- Purpose: Extract delivery information from PDFs, validate data, manage discards
- Location: `src/wheeck/recording_docs.py`
- Contains: `Delivery` dataclass, pattern matching, duplicate checking, similarity analysis
- Depends on: pypdfium2 (PDF processing), regex patterns, geographic service, database
- Used by: Main entry point

**Data Access/Persistence Layer:**
- Purpose: Abstract database operations for delivery records, warnings, and discarded pages
- Location: Core module `Querier` class (external dependency)
- Contains: SQL query execution, connection management
- Depends on: pyodbc (database driver), PostgreSQL database
- Used by: Recording and overview modules

**Geo-Location Service Layer:**
- Purpose: Resolve delivery city names to coordinates and calculate distances
- Location: `src/geo/geo.py`
- Contains: `GeoMap` class wrapping OpenRouteService API
- Depends on: openrouteservice (external API client)
- Used by: Delivery processing during record validation

**Configuration & Constants Layer:**
- Purpose: Centralize configuration values, regex patterns, SQL queries
- Location: `src/wheeck/constants.py`
- Contains: File paths, regex patterns (PATTERN_*), SQL queries, formatting rules
- Depends on: Pathlib
- Used by: All modules for consistent pattern definitions and database schema

## Data Flow

**Primary Processing Flow (PDF → Database):**

1. Main entry point calls `wheeck.run()` from `src/main.py`
2. `run()` scans `DDTs/` directory for matching PDF files
3. For each PDF:
   - Rename with `.recording` suffix to prevent concurrent processing
   - Call `doc_scanner()` to process all pages
4. For each PDF page:
   - Extract raw text using pypdfium2
   - Apply regex patterns to extract: document number, city, quantity, vehicle, driver
   - Call `calculate_distance()` using GeoMap for delivery city
   - Check for duplicates using database query
   - Insert delivery record or create discard record with warning
   - If pattern matching fails, generate discard PDF page and warning message
5. After all PDFs processed:
   - Call `check_gaps()` to find missing document numbers
   - Generate/update Excel overviews and summaries
   - Backup PostgreSQL database schema

**State Management:**

- **Processing State**: Tracked via filename suffix (`.recording` during processing, `.recorded` after completion)
- **Data State**: Stored in PostgreSQL `wheeck.delivery` table with `recording_date` timestamp
- **Quality State**: Tracked via `wheeck.delivery_warning` and `wheeck.delivery_discard` tables
- **Configuration State**: Loaded from `config/wheeck.json` (API keys, vehicle/driver mappings, departure coordinates)

## Key Abstractions

**Delivery Dataclass:**
- Purpose: Encapsulate all extracted information from a single DDT page
- Location: `src/wheeck/recording_docs.py` lines 28-46
- Pattern: Python dataclass with field defaults, custom methods (`charge()`, `calculate_distance()`)
- Used for: Type-safe data structure passing through validation pipeline

**MessageGenre Enum:**
- Purpose: Standardize warning message templates for different error conditions
- Location: `src/wheeck/recording_docs.py` lines 86-92
- Pattern: Enum with formatted string templates for database logging
- Values: DISCARD (page extraction failed), GAP (missing document number), WARNING (data similarity issue)

**GeoMap Service:**
- Purpose: Encapsulate geolocation API interactions
- Location: `src/geo/geo.py` lines 12-103
- Pattern: Class wrapper around OpenRouteService client with three public methods
- Methods: `search()` (address → coordinates), `get_distance()` (address → distance), `get_distance_from_coords()` (coordinates → distance)

**DiscardedPages Anonymous Class:**
- Purpose: Track error state during multi-page processing
- Location: `src/wheeck/recording_docs.py` lines 117-125
- Pattern: Inline class instance with state tracking (number count, current error, restart method)
- Usage: Maintains error context across page iteration loop

## Entry Points

**src/main.py:**
- Location: `/c/source/wheeck/src/main.py`
- Triggers: Command-line execution (`python src/main.py`)
- Responsibilities:
  - Import wheeck package and logging utility
  - Call `wheeck.run()` with exception handling
  - Log any unhandled exceptions to file

**wheeck.run():**
- Location: `src/wheeck/recording_docs.py` lines 445-501
- Triggers: Called from main entry point
- Responsibilities:
  - Discover PDF files in `PATH_WORKING_DIR` matching naming pattern
  - Orchestrate `doc_scanner()` for each document
  - Manage file transitions (.recording → .recorded suffixes)
  - Trigger post-processing (gaps check, Excel generation, database backup)

**wheeck.doc_scanner():**
- Location: `src/wheeck/recording_docs.py` lines 95-326
- Triggers: Called per-document by `run()`
- Responsibilities:
  - Page-by-page PDF text extraction
  - Pattern matching against six regex patterns
  - Distance calculation and duplicate detection
  - Error handling with discard record creation
  - Return statistics (total pages, discarded pages)

## Error Handling

**Strategy:** Multi-layered approach with graceful degradation

**Patterns:**

- **Pattern Matching Failures**: If regex pattern fails to match, mark page for discard but continue processing remaining patterns
- **Similarity-Based Recovery**: If extracted vehicle/driver don't match known values, use sequence matching (>50% threshold) to suggest correction; log warning but continue
- **Database Errors**: Catch and log critical errors on INSERT/UPDATE; mark record for potential retry
- **API Errors**: OpenRouteService API errors (ApiError, HTTPError, Timeout) caught explicitly and logged; distance calculation returns None and process continues
- **Duplicate Detection**: Pre-INSERT check prevents duplicate recording; duplicates logged and page moved to discarded directory
- **Gap Detection**: Post-processing identifies missing document numbers and logs separately

**Error Logging:**
- All errors logged to `PATH_LOG` via `get_logger()` utility
- Four severity levels used: `info()`, `warning()`, `error()`, `critical()`
- Error context includes page number, document name, and pattern name

## Cross-Cutting Concerns

**Logging:**
- Centralized via `get_logger(PATH_LOG, __name__)` utility
- Each module logs to same directory with module name context
- Levels: INFO (progress tracking), WARNING (recoverable issues), ERROR (page-level failures), CRITICAL (database/API failures)

**Validation:**
- Regex pattern matching (6 patterns applied per page)
- Duplicate detection via SQL query
- Similarity analysis for vehicle/driver matching (SequenceMatcher, 50% threshold)
- Required field checks (None values trigger discard)

**Authentication:**
- Database: pyodbc connection using credentials from `config/querier.json`
- OpenRouteService: API key from `config/wheeck.json`
- No in-app user authentication (batch processing system)

**Data Integrity:**
- Transaction-based database updates (`save_changes=True` in Querier)
- Post-processing database backup (pg_dump) stored in `scheme/` directory
- Warning/message system tracks problematic records for audit trail

---

*Architecture analysis: 2026-03-07*
