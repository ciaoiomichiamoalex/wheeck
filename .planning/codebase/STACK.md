# Technology Stack

**Analysis Date:** 2026-03-07

## Languages

**Primary:**
- Python 3.14.3 - Core application logic, data extraction, and document processing

## Runtime

**Environment:**
- Python 3.14.3 (CPython)
- Virtual environment: `.venv` directory present (venv 20.31.2)

**Package Manager:**
- pip 24.0+ (via venv)
- Lockfile: Not present (using `requirements.txt` pinned versions)

## Frameworks

**Core:**
- None - Standalone Python application

**PDF Processing:**
- pypdfium2 ~5.5.0 - PDF text extraction from DDT (Delivery Documents)

**Data Handling:**
- openpyxl ~3.1.5 - Excel file generation and manipulation

**Database:**
- pyodbc ~5.3.0 - ODBC database client for PostgreSQL connectivity

**Geolocation:**
- openrouteservice ~2.3.3 - Route calculation and geocoding API client

## Key Dependencies

**Critical:**
- pypdfium2 - Extracts text from PDF DDT documents to parse delivery information
- openpyxl - Generates Excel reports (overviews and summaries) from extracted data
- pyodbc - Connects to PostgreSQL database via ODBC for persistence
- openrouteservice - Geocodes delivery cities to coordinates and calculates delivery distances

**Infrastructure:**
- core (local package) - Shared utilities from `/c/source/core/src/core` symlink
  - Provides: `Querier`, `decode_json`, `get_logger`
- geo (local package) - Geolocation wrapper module at `src/geo/`
  - Provides: `GeoMap` class for address geocoding and distance calculation

## Configuration

**Environment:**
- Configuration via JSON files in `config/` directory
- `config/wheeck.json` - Application settings (API keys, driver data, backup directory)
- `config/querier.json` - Database connection parameters (PostgreSQL ODBC via pyodbc)
- Database credentials stored in config files (username/password for `wheeck` user)

**Build:**
- No build system - Direct Python execution
- Entry point: `src/main.py`
  - Imports from `wheeck` package (v1.0.3)
  - Exception handling with centralized logging

## Platform Requirements

**Development:**
- Python 3.14.3
- Virtual environment: `.venv/`
- PostgreSQL database server running on `127.0.0.1:5432`
- ODBC driver for PostgreSQL required for pyodbc
- OpenRouteService API key required in config
- Write access to `DDTs/` directory for document processing
- Write access to `log/` directory for logging

**Production:**
- PostgreSQL 9.6+ database with `wheeck` schema
- OpenRouteService API endpoint reachable
- File system for PDF input (`DDTs/`), backups, and logging
- `pg_dump` tool available for database backups
- Bash shell for executing backup commands

---

*Stack analysis: 2026-03-07*
