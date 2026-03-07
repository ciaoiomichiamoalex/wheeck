# Codebase Concerns

**Analysis Date:** 2026-03-07

## Tech Debt

**Hardcoded API Key and Configuration Secrets:**
- Issue: API key (`ors_api_key`) and sensitive configuration stored in plain-text JSON file committed to version control
- Files: `/c/source/wheeck/config/wheeck.json`
- Impact: API credentials exposed to anyone with repository access; key compromise requires repository history rewrite
- Fix approach: Implement environment variable loading for secrets; keep `.json` config template only; add `.gitignore` exclusion for actual config files; use `python-dotenv` or similar for local development

**Bare Except Clause with Silent Failure:**
- Issue: `except Exception: logger.exception('unhandled exception')` catches and logs everything without re-raising or proper handling
- Files: `/c/source/wheeck/src/main.py:8`
- Impact: Critical errors are logged but the application exits silently; caller cannot determine if job succeeded or failed; no proper exit codes
- Fix approach: Replace with specific exception handlers; use proper exit codes; consider re-raising critical exceptions; log at appropriate severity levels

**Anonymous Class Pattern for State Management:**
- Issue: `discarded_pages = type('DiscardedPages', (object,), {...})()` creates unnamed dynamic class for state tracking
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:117-125`
- Impact: Difficult to debug, test, and maintain; type hints impossible; IDE support compromised; no validation
- Fix approach: Replace with proper dataclass or NamedTuple; make state management explicit; enable type checking

**SQL String Concatenation for Query Building:**
- Issue: `QUERY_GET_SUMMARY` uses string formatting with `%s` placeholders and `', '.join('?' * len(drivers))` for parameterized queries
- Files: `/c/source/wheeck/src/wheeck/constants.py:162-213`, `/c/source/wheeck/src/wheeck/overview_docs.py:79`
- Impact: Fragile query construction; difficult to validate; potential for SQL injection if not carefully handled
- Fix approach: Build WHERE/IN clauses using list comprehension; use proper SQL generation utilities; validate parameter counts at compile time

**Magic Value in Discard Warning:**
- Issue: `delivery.id_warning_message = -1` used as a sentinel value to mean "don't save discard record" (no actual warning saved)
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:269`
- Impact: Semantically incorrect; difficult to understand intent; sentinel values are error-prone; mixing None, -1, and valid IDs
- Fix approach: Use explicit enum or boolean flag like `should_save_discard: bool`; separate concerns

## Known Bugs

**Missing Null Check in Delivery Dataclass Tuple Unpacking:**
- Symptoms: Potential IndexError if dataclass field order changes; `astuple(delivery)[:-1]` relies on field order to exclude last field
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:285` (INSERT_DELIVERY), `/c/source/wheeck/src/wheeck/recording_docs.py:307` (INSERT_DISCARD)
- Trigger: When inserting delivery or discard records; fragile if dataclass fields are reordered
- Workaround: Ensure field order in Delivery dataclass never changes; add comment explaining slice rationale

**Incomplete None Check in Discard Record Loading:**
- Symptoms: Line 257 checks `if None not in discard:` but then line 258 could fail if `discard` is None or doesn't have expected fields
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:257-259`
- Trigger: When loading existing discard records from database with `QUERY_GET_DISCARD`
- Workaround: Check returns from `querier.fetch()` are not None before using

**Potential Driver Similarity Match Returns None:**
- Symptoms: `check_similarity(None, drivers_enum)` can be called if driver is None or unmapped
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:220`
- Trigger: When PDF contains unrecognized driver name that doesn't match vehicles
- Workaround: Check the function returns a valid driver from `drivers_enum`; currently relies on 50% similarity threshold

**Missing Null Guard on calculate_distance:**
- Symptoms: `delivery.calculate_distance()` sets distance to None if destination cannot be found, but code assumes numeric result
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:278`, called from `calculate_distance()` in recording_docs.py:62-72
- Trigger: When OpenRouteService API fails to geocode delivery_city
- Workaround: Check `delivery.distance` is not None before database insertion; current database constraint allows NULL

## Security Considerations

**Exposed OpenRouteService API Key:**
- Risk: API key visible in `/c/source/wheeck/config/wheeck.json` committed to git; anyone with repo access can use ORS service and consume quota
- Files: `/c/source/wheeck/config/wheeck.json`, `/c/source/wheeck/src/geo/geo.py:24`
- Current mitigation: None (key is committed and exposed)
- Recommendations: Immediately rotate the exposed key; move to environment variables; implement key rotation policy; use OAuth or API tokens with TTL

**SQL Injection via String Formatting in Summary Query:**
- Risk: While using parameterized queries, the dynamic query building with `% ', '.join('?' * len(drivers))` could be vulnerable if list construction is compromised
- Files: `/c/source/wheeck/src/wheeck/constants.py:175`, `/c/source/wheeck/src/wheeck/overview_docs.py:79`
- Current mitigation: Parameters are passed separately via `*drivers, *vehicles`
- Recommendations: Use SQLAlchemy or similar ORM to build dynamic queries safely; add validation for list lengths match placeholders

**Database Backup Command Injection:**
- Risk: `CMD_BACKUP_DB` uses `subprocess.run(..., shell=True)` which executes through shell; path contains user-controlled data
- Files: `/c/source/wheeck/src/wheeck/constants.py:25`, `/c/source/wheeck/src/wheeck/recording_docs.py:489`
- Current mitigation: Paths are from constants, not user input; but shell=True is dangerous pattern
- Recommendations: Use `shell=False` with list arguments; validate paths before use; consider using pg_dump Python library instead

**Configuration File Contains Departure Coordinates:**
- Risk: Departure coordinates in JSON could be sensitive business location information
- Files: `/c/source/wheeck/config/wheeck.json:3-6`
- Current mitigation: Only accessible to those with repo access
- Recommendations: Store location separately from VCS; use environment variables for sensitive coordinates

## Performance Bottlenecks

**Hard Sleep Between OpenRouteService Calls:**
- Problem: `time.sleep(1.5)` for every distance calculation (40 calls/min limit = 1.5s per call)
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:280`
- Cause: Fixed sleep regardless of actual API response time; no retry backoff strategy for rate limits
- Improvement path: Implement exponential backoff on 429 errors; use async requests to queue parallel calls; cache distance results by city; batch requests if ORS API supports it

**N+1 Query Pattern for Distance Caching:**
- Problem: Checks `QUERY_GET_DISTANCE` for every delivery to see if distance cached; could trigger multiple lookups for same city
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:273-278`
- Cause: Linear scan through document pages without distance pre-fetch
- Improvement path: Batch-fetch all distances at start of document; use in-memory cache with city → distance mapping; refactor to separate concern

**Configuration Loaded Multiple Times:**
- Problem: `decode_json(PATH_CFG_PRJ)` called repeatedly throughout execution (members, departure_coords, backup_dir, ors_api_key)
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:67,200,473`, `/c/source/wheeck/src/geo/geo.py:24`, `/c/source/wheeck/src/wheeck/overview_docs.py:75`
- Cause: No caching of configuration; every call re-parses JSON file
- Improvement path: Load config once at startup; pass as context to functions; use singleton pattern or dependency injection

**Regular Expression Pattern Compilation at Runtime:**
- Problem: Patterns like `PATTERN_WORKING_DOC`, `PATTERN_DISCARD_DOC` compiled on every `re.search()` call
- Files: `/c/source/wheeck/src/wheeck/constants.py` (patterns defined as strings), `/c/source/wheeck/src/wheeck/recording_docs.py:138,154,165,178,195,242,456,472`
- Cause: Patterns stored as raw strings, not compiled regex objects
- Improvement path: Pre-compile all patterns at module load time; use `re.compile()` in constants; cache compiled patterns

## Fragile Areas

**Delivery Dataclass Initialization and Tuple Unpacking:**
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:28-46, 285, 307`
- Why fragile: Field order critical for `astuple()` slicing; `init=False` fields with defaults can be missed; depends on exact tuple structure matching SQL INSERT order
- Safe modification: Add explicit field ordering documentation; replace `astuple()[:-1]` with named tuple unpacking or dictionary construction; consider NamedTuple instead of dataclass
- Test coverage: No visible tests; manually verify field order matches INSERT statement; integration tests would catch reordering

**Discard Document Workflow State Management:**
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:241-313`
- Why fragile: Complex state transitions (check if discard exists → load record → update error → save new record); multiple database calls depend on prior state
- Safe modification: Document state machine explicitly; add validation at each state transition; consider separating discard logic into dedicated function
- Test coverage: No visible tests; difficult to test error paths

**PDF Text Extraction Regex Patterns:**
- Files: `/c/source/wheeck/src/wheeck/constants.py:16-23`
- Why fragile: Hardcoded patterns match specific PDF format; any document layout change breaks extraction; patterns use specific whitespace sequences `\r\n`, `\t`
- Safe modification: Add fallback patterns; implement fuzzy matching for layout changes; add error recovery if pattern fails; test with real PDF samples
- Test coverage: No visible tests; relies on manual testing with PDF documents

**GeoMap Error Handling:**
- Files: `/c/source/wheeck/src/geo/geo.py:40-52, 91-103`
- Why fragile: Returns None on any error; no distinction between "location not found" vs "API down"; calling code must handle None
- Safe modification: Create custom exceptions for different error types; add retry logic; implement fallback distance values for known cities
- Test coverage: No visible tests; API error paths untested

## Scaling Limits

**Single-Threaded PDF Processing:**
- Current capacity: Processes one PDF document sequentially; each page sleeps 1.5s for API calls
- Limit: Processing 100 pages per document with 1.5s sleep = 150+ seconds per document; cannot process multiple documents in parallel
- Scaling path: Implement async PDF processing; use thread pool or asyncio; separate API calls to background task queue; implement producer-consumer pattern

**Database Connection Per Function Call:**
- Current capacity: Each operation creates new Querier instance (connection)
- Limit: 100+ separate connections per large document; connection overhead dominates actual work
- Scaling path: Implement connection pooling; pass single connection through call stack; use context manager for connection lifecycle

**In-Memory Configuration Loading:**
- Current capacity: Config file small (< 1KB); acceptable for current load
- Limit: If member list grows to 10k+ entries, repeated loading becomes problematic
- Scaling path: Cache configuration in process; implement file watcher for config changes; use lazy-loading for large datasets

## Dependencies at Risk

**OpenRouteService API Dependency (openrouteservice~=2.3.3):**
- Risk: Distance calculation failure stops document processing; no fallback if API is down
- Impact: Cannot process deliveries without distance; entire batch fails if ORS service down
- Migration plan: Implement fallback to haversine distance formula; cache historical distances; allow distance to be manually specified; consider PostGIS for distance calculation

**pyodbc Database Driver (pyodbc~=5.3.0):**
- Risk: Windows-specific dependency; ODBC configuration required; hardcoded schema structure
- Impact: Database schema changes break application; no schema versioning or migrations
- Migration plan: Implement database migration framework (Alembic); create schema versioning table; validate schema on startup

**pypdfium2 PDF Library (pypdfium2~=5.5.0):**
- Risk: PDF layout changes break regex patterns; library updates could change text extraction
- Impact: PDFs with different formats cannot be processed; document format changes require pattern updates
- Migration plan: Implement layout detection; create pattern registry; add version detection for document formats; consider OCR fallback

## Missing Critical Features

**No Test Suite:**
- Problem: Zero visible unit or integration tests; no test framework configured
- Blocks: Cannot safely refactor; cannot validate changes; no regression detection; brittle delivery mechanism
- Priority: HIGH - Implement unit tests for regex patterns, GeoMap, Querier interactions; integration tests for full workflow

**No Database Migration System:**
- Problem: Schema changes (like recent delivery_history table) require manual SQL execution
- Blocks: Cannot track schema version; hard to rollback changes; environment sync issues
- Priority: HIGH - Add Alembic or similar; track all schema changes; auto-apply migrations on startup

**No Configuration Validation:**
- Problem: Config file loaded without validation; missing fields cause AttributeError at runtime
- Blocks: Cannot catch config errors early; unclear which configs are required
- Priority: MEDIUM - Add Pydantic models for config validation; fail fast on startup if incomplete

**No Error Recovery Mechanism:**
- Problem: Document processing failure stops entire batch; no resume capability
- Blocks: Cannot retry failed pages; must reprocess entire document; no partial completion tracking
- Priority: MEDIUM - Add checkpoint/resume tracking; implement page-level error recovery; track failed pages separately

**No Monitoring or Observability:**
- Problem: Only file-based logging; no metrics, alerts, or health checks
- Blocks: Cannot monitor in production; no visibility into performance; hard to diagnose issues
- Priority: MEDIUM - Add structured logging; implement health check endpoint; add metrics (pages processed, errors, API latency)

## Test Coverage Gaps

**Regex Pattern Extraction:**
- What's not tested: `PATTERN_*` extraction from PDF text; fallback from PATTERN_CITY_DX to PATTERN_CITY_SX
- Files: `/c/source/wheeck/src/wheeck/constants.py:16-23`, `/c/source/wheeck/src/wheeck/recording_docs.py:138-175`
- Risk: Document format changes break silently; pattern updates not validated against real PDFs
- Priority: HIGH - Create test fixtures with real PDF text; validate each pattern with sample data

**GeoMap Service Integration:**
- What's not tested: `search()` and `get_distance_from_coords()` with actual ORS API calls
- Files: `/c/source/wheeck/src/geo/geo.py:27-103`
- Risk: API changes, network errors, rate limiting not validated; None returns not handled consistently
- Priority: HIGH - Add mock API responses; test error paths (Timeout, HTTPError, ApiError); test caching behavior

**Database Operations:**
- What's not tested: SQL query execution, duplicate detection, discard record loading, gap detection
- Files: `/c/source/wheeck/src/wheeck/recording_docs.py:358-396`
- Risk: Data corruption, constraint violations, loss of records not caught
- Priority: HIGH - Add integration tests with test database; validate constraint enforcement

**Configuration Loading:**
- What's not tested: Config file parsing, missing fields, invalid JSON
- Files: `/c/source/wheeck/src/geo/geo.py:24`, `/c/source/wheeck/src/wheeck/recording_docs.py:67,200,473`
- Risk: Missing config causes runtime crashes; no validation
- Priority: MEDIUM - Add config validation tests; test with malformed JSON; test missing fields

**Excel Generation:**
- What's not tested: `generate_overview()` and `generate_summary()` XLSX file creation and formatting
- Files: `/c/source/wheeck/src/wheeck/overview_docs.py:15-95`
- Risk: File corruption, incorrect formulas, formatting loss not detected
- Priority: MEDIUM - Verify generated XLSX structure; validate cell values and types; test with edge cases (empty data, large datasets)

---

*Concerns audit: 2026-03-07*
