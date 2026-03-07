# Codebase Structure

**Analysis Date:** 2026-03-07

## Directory Layout

```
wheeck/
├── .planning/                  # GSD planning artifacts
│   └── codebase/              # Architecture and conventions docs
├── DDTs/                       # Working directory for PDF documents
│   ├── discarded/             # Pages extracted from failed parsing (subdir)
│   └── recorded/              # Completed documents moved here after processing (subdir)
├── config/                     # Configuration files
│   ├── querier.json           # Database connection credentials
│   └── wheeck.json            # Application settings, API keys, vehicle mappings
├── log/                        # Application logs directory (created at runtime)
├── res/                        # Generated Excel reports symlinked from external storage
├── scheme/                     # Database schema and backup files
│   ├── wheeck.sql            # Database schema definition
│   └── wheeck.dump            # Latest PostgreSQL backup (binary format)
├── src/                        # Python source code
│   ├── main.py               # Entry point script
│   ├── geo/                  # Geolocation service module
│   │   ├── __init__.py       # Package initialization (exports GeoMap)
│   │   └── geo.py            # GeoMap class for address→coordinates and distance calculation
│   └── wheeck/               # Core business logic package
│       ├── __init__.py       # Package initialization (exports public functions)
│       ├── constants.py      # Paths, regex patterns, SQL queries, formatting rules
│       ├── recording_docs.py # PDF scanning, delivery extraction, data validation
│       └── overview_docs.py  # Excel report generation
├── .venv/                      # Virtual environment (excluded from git)
└── README.md                   # Project description and module overview
```

## Directory Purposes

**DDTs/:**
- Purpose: Working directory for inbound PDF documents (Documento Di Trasporto - transport documents in Italian)
- Contains: Original PDF files, processed files, discarded pages
- Key files: `*.pdf` (original), `*.recording.pdf` (in-progress), `*.recorded.pdf` (completed)
- Lifecycle: Files move from root → `discarded/` (on error) or `recorded/` (on success)

**.planning/codebase/:**
- Purpose: GSD (Get Stuff Done) planning documents for codebase analysis
- Contains: Architecture and coding convention documentation
- Key files: `ARCHITECTURE.md`, `STRUCTURE.md`, `CONVENTIONS.md` (when generated)

**config/:**
- Purpose: Configuration and credentials
- Contains: Database connection strings, API keys, application settings
- Key files:
  - `querier.json`: pyodbc connection to PostgreSQL (server, port, user, password)
  - `wheeck.json`: OpenRouteService API key, vehicle/driver mapping, departure coordinates, backup directory

**log/:**
- Purpose: Application runtime logs
- Contains: Text log files generated during PDF processing
- Key files: `wheeck.log` and module-specific logs (created as needed)

**scheme/:**
- Purpose: Database schema management and backups
- Contains: SQL definitions and binary backup dumps
- Key files:
  - `wheeck.sql`: PostgreSQL schema definition (tables, views, constraints, indexes)
  - `wheeck.dump`: Latest successful database backup in pg_dump binary format
  - `overview.xlsx` (symlink): Template for monthly delivery overviews

**res/:**
- Purpose: Output directory for generated Excel reports
- Contains: Monthly overviews and annual trip summaries
- Key files:
  - `YYYY_MM.xlsx`: Monthly delivery overview for specific month
  - `YYYY_TRIPS.xlsx`: Annual trip summary comparing vehicle routes
- Note: Symlinked to external MEGA storage at `/c/Users/Alex/MEGA/AGM/consegne/overviews`

**src/:**
- Purpose: Python application source code
- Contains: Entry point and two domain packages (geo, wheeck)
- Structure: Two-level package hierarchy

**src/main.py:**
- Purpose: Command-line entry point
- Contents: Exception handling wrapper around `wheeck.run()`
- Dependencies: wheeck package, logging utility (from core module)

**src/geo/:**
- Purpose: Geolocation service abstractions
- Contains: Single GeoMap class wrapping OpenRouteService API
- Key files: `geo.py` (implementation), `__init__.py` (package export)

**src/wheeck/:**
- Purpose: Core business logic for delivery document processing
- Contains: Data models, processing logic, report generation, configuration
- Key files:
  - `constants.py`: 200+ lines of centralized configuration
  - `recording_docs.py`: 500+ lines of PDF processing logic
  - `overview_docs.py`: 100+ lines of Excel generation
  - `__init__.py`: Package initialization

## Key File Locations

**Entry Points:**
- `src/main.py`: Command-line entry point for batch processing

**Configuration:**
- `config/wheeck.json`: Application settings (API keys, vehicle mappings, backup location)
- `config/querier.json`: Database credentials for PostgreSQL connection

**Core Logic:**
- `src/wheeck/recording_docs.py`: PDF text extraction, pattern matching, delivery record creation
- `src/wheeck/overview_docs.py`: Excel report generation and formatting
- `src/geo/geo.py`: OpenRouteService API integration for coordinate lookup and distance calculation

**Constants & Patterns:**
- `src/wheeck/constants.py`: All regex patterns, SQL queries, file paths, formatting rules

**Testing:**
- No test files present in codebase

## Naming Conventions

**Files:**
- Module files: lowercase with underscores (e.g., `recording_docs.py`, `overview_docs.py`)
- Configuration: lowercase with underscores (e.g., `wheeck.json`, `querier.json`)
- Constants: uppercase with underscores in code (e.g., `PATTERN_DOC_NUMBER`, `PATH_WORKING_DIR`)

**Directories:**
- Source packages: lowercase matching functionality (e.g., `wheeck`, `geo`)
- Working directories: descriptive (e.g., `DDTs`, `discarded`, `recorded`)
- Output directories: short codes (e.g., `config`, `log`, `res`, `scheme`)

**Python Classes:**
- PascalCase for user-defined classes (e.g., `Delivery`, `GeoMap`, `MessageGenre`)
- Dataclass suffix not used (naming relies on purpose clarity)

**Python Functions:**
- snake_case for functions (e.g., `doc_scanner()`, `generate_overview()`, `check_duplicate()`)
- Verb-prefixed for clarity (e.g., `generate_*`, `check_*`, `calculate_*`, `save_*`)

**Python Variables:**
- snake_case for all variables
- Prefixed with type indicator for clarity (e.g., `path_*` for Path objects, `query_*` for SQL queries)
- Constants in all caps (e.g., `PATTERN_*`, `QUERY_*`, `PATH_*`)

**Pattern/Regex Names:**
- All-caps prefix `PATTERN_` followed by target (e.g., `PATTERN_DOC_NUMBER`, `PATTERN_VEHICLE`)

**Query Names:**
- All-caps prefix `QUERY_` followed by action (e.g., `QUERY_INSERT_DELIVERY`, `QUERY_CHECK_DUPLICATE`)

## Where to Add New Code

**New Feature (Document Processing):**
- Primary code: `src/wheeck/recording_docs.py` (add function or method to existing classes)
- Constants: `src/wheeck/constants.py` (add regex pattern if pattern-matching based)
- Database query: `src/wheeck/constants.py` (add QUERY_* constant if database operation)

**New Report/Overview:**
- Implementation: `src/wheeck/overview_docs.py` (add new `generate_*()` function)
- SQL query: `src/wheeck/constants.py` (add QUERY_* for data retrieval)
- Template: `scheme/` (add new Excel template file)

**New Geolocation Feature:**
- Implementation: `src/geo/geo.py` (add method to GeoMap class)
- Export: `src/geo/__init__.py` (update __version__ if changed)

**Utilities/Helpers:**
- Shared validation: Add to `src/wheeck/recording_docs.py` as standalone functions
- Shared patterns: Add to `src/wheeck/constants.py` as PATTERN_* or PATH_* constants

**Configuration:**
- Application settings: `config/wheeck.json` (JSON key-value pairs)
- Database connection: `config/querier.json` (connection string details)
- Runtime logging: `log/` directory (created automatically, check via PATH_LOG)

## Special Directories

**DDTs/ and subdirectories:**
- Purpose: Working directory for document processing with automatic state transitions
- Generated: Yes (DDTs/discarded/*.pdf and DDTs/recorded/*.pdf created at runtime)
- Committed: No (gitignored, contains user data)
- Filename patterns:
  - Input: `YYYY_MM_DDT_NNNN_NNNN.pdf` (matches PATTERN_WORKING_DOC)
  - Processing: `YYYY_MM_DDT_NNNN_NNNN.recording.pdf` (prevents concurrent processing)
  - Completed: `YYYY_MM_DDT_NNNN_NNNN.recorded.pdf` (moved to `recorded/`)
  - Failed pages: `YYYY_MM_DDT_NNNN_NNNN_PNNN.pdf` (moved to `discarded/`)

**log/:**
- Purpose: Runtime application logs
- Generated: Yes (created on first run)
- Committed: No (gitignored, contains runtime data)

**.venv/:**
- Purpose: Python virtual environment
- Generated: Yes (created via `python -m venv .venv`)
- Committed: No (gitignored, contains dependencies)

**res/:**
- Purpose: Generated Excel reports (symlinked to external storage)
- Generated: Yes (created by `generate_overview()` and `generate_summary()`)
- Committed: Symlink exists in git, but files stored externally
- Naming: `YYYY_MM.xlsx` (monthly overview), `YYYY_TRIPS.xlsx` (annual summary)

**scheme/:**
- Purpose: Database schema and backup management
- Generated: Partially (wheeck.dump backup created at runtime, schema is committed)
- Committed: `wheeck.sql` (schema definition) committed, `.dump` backup not committed

---

*Structure analysis: 2026-03-07*
