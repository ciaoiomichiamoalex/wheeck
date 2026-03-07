# External Integrations

**Analysis Date:** 2026-03-07

## APIs & External Services

**Geolocation & Routing:**
- OpenRouteService (ORS) - Geocoding and route distance calculation
  - SDK/Client: `openrouteservice` package v2.3.3
  - Auth: API key stored in `config/wheeck.json` as `ors_api_key`
  - Usage: Convert delivery city names to coordinates, calculate distances from departure point
  - Implementation: `src/geo/geo.py` contains `GeoMap` class
  - Methods:
    - `search(address, country='IT')` - Pelias geocoding API
    - `get_distance(departure, destination)` - Distance calculation via directions API
    - `get_distance_from_coords(departure, destination)` - Distance from coordinates

## Data Storage

**Databases:**
- PostgreSQL 9.6+ (ODBC via pyodbc)
  - Connection: ODBC DSN in `config/querier.json`
  - Server: `127.0.0.1:5432`
  - Database: `postgres`
  - User: `wheeck`
  - Password: Stored in `config/querier.json` (plaintext)
  - Client: `pyodbc` v5.3.0
  - Schema: `wheeck` with 4 tables + views

**Database Schema:**
- `wheeck.delivery` - Extracted delivery records (13 columns, ~30 constraints/indexes)
- `wheeck.delivery_history` - Historical record tracking
- `wheeck.delivery_warning` - Warning/error messages
- `wheeck.delivery_discard` - Discarded document records
- Views: `vw_delivery_gap`, `vw_gap_message` - Gap detection queries

**File Storage:**
- Local filesystem only
  - Input: `DDTs/` directory for PDF documents
  - Discarded: `DDTs/discarded/` for rejected documents
  - Recorded: `DDTs/recorded/` for processed documents
  - Logs: `log/` directory
  - Backups: `config/` directory or specified in `backup_dir` from config
  - Resources: `res/` symlink to external delivery data directory
  - Scheme: `scheme/` with database backup dumps and templates

**Caching:**
- None - Application is stateless, caches only within process memory during execution

## Authentication & Identity

**Auth Provider:**
- None for external APIs (IP-based access to PostgreSQL)
- OpenRouteService: API key authentication
- PostgreSQL: Username/password in config file (no environment variables)

**Database Credentials:**
- Stored: `config/querier.json` (JSON plaintext)
  - User: `wheeck`
  - Password: `Wheeck!2024` (hardcoded in config)

## Monitoring & Observability

**Error Tracking:**
- None - No external error tracking service (Sentry, Rollbar, etc.)
- Errors logged locally

**Logs:**
- File-based logging via `core.get_logger()` function
- Log directory: `log/` (path from `wheeck.PATH_LOG`)
- Log format: Python logging module (standard)
- Modules log with `__name__` parameter for file identification
  - `recording_docs.py` logs via `core` logger
  - `overview_docs.py` logs via `core` logger
  - `geo.py` logs via `core` logger

## CI/CD & Deployment

**Hosting:**
- Local/on-premises only
- No cloud deployment detected

**CI Pipeline:**
- None detected - No GitHub Actions, GitLab CI, Jenkins, or similar

**Backup & Recovery:**
- PostgreSQL `pg_dump` command defined in `wheeck.constants.py`
- Backup command: `pg_dump -U postgres -h 127.0.0.1 -p 5432 -d postgres -n wheeck -w -c -F c`
- Backup location: `scheme/wheeck.bak.dump`

## Environment Configuration

**Required env vars:**
- None - Configuration via JSON files only

**Configuration Files:**
- `config/wheeck.json` (required):
  - `ors_api_key` - OpenRouteService API key
  - `departure_coords` - [latitude, longitude] of delivery origin
  - `members` - Array of drivers/vehicles (vehicle, driver name)
  - `backup_dir` - Backup directory path

- `config/querier.json` (required):
  - Database connection parameters (server, port, database, user, password)
  - Supports multiple named connections (array format)

**Secrets location:**
- Stored in JSON config files (plaintext)
- `.gitignore` exists but config files may not be excluded

## Webhooks & Callbacks

**Incoming:**
- None - Batch processing only (no API endpoints)

**Outgoing:**
- None - No external callbacks or notifications

## Data Flow Summary

1. **PDF Processing:**
   - Input: PDF documents in `DDTs/` directory
   - Processing: `doc_scanner()` extracts text using pypdfium2
   - Pattern matching: Regular expressions in `constants.py` parse delivery data
   - Delivery object: Dataclass populated with parsed fields

2. **Geolocation Enrichment:**
   - City name extracted from PDF
   - OpenRouteService `search()` converts to coordinates
   - Distance calculated via routing API
   - Results stored in `Delivery.distance`

3. **Database Persistence:**
   - Validated delivery records inserted via pyodbc
   - Duplicate detection using existing records
   - Discarded records tracked with warning messages
   - Gap analysis via SQL views on recorded data

4. **Excel Report Generation:**
   - Query PostgreSQL for monthly deliveries
   - openpyxl reads template from `scheme/overview.xlsx`
   - Populates data into workbook sheets
   - Saves to `res/YYYY_MM.xlsx`

---

*Integration audit: 2026-03-07*
