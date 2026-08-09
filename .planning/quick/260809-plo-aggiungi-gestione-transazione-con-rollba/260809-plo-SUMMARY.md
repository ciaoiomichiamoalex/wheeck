---
phase: quick-260809-plo
plan: 01
subsystem: recording pipeline
tags: [transaction, rollback, pyodbc, filesystem-compensation]
requires: [core.Querier.save_changes]
provides: [RecordingError, per-document transaction, automatic re-processing of failed docs]
affects: [src/wheeck/recording_docs.py, src/wheeck/__init__.py, README.md]
tech-stack:
  added: []
  patterns: [one implicit transaction per document, shared Querier threading, filesystem compensation on rollback]
key-files:
  created: []
  modified:
    - src/wheeck/recording_docs.py
    - src/wheeck/__init__.py
    - README.md
decisions:
  - "The exception handler is the real rollback path; the rows != 1 checks converge on it via RecordingError"
  - "Both QUERY_UPDATE_WARNING rows != 1 checks stay log-and-continue, they are not rollback triggers"
  - "A regex discard must never roll back: DISCARD template made None-safe, gap and distance lookups guarded"
  - "A failed document is renamed back to <name>.pdf in DDTs/ so it is re-processed automatically"
metrics:
  tasks: 3
  commits: 3
  duration: ~25 min
  completed: 2026-08-09
---

# Quick Task 260809-plo: Transaction management with rollback Summary

One implicit database transaction per PDF in `doc_scanner()`: every page of a document is committed
together or nothing is, with the discard PDFs written by the failed run removed and the working file
renamed back into the `DDTs/` queue for automatic re-processing.

## What Changed

### Task 1 — `RecordingError`, None-safe DISCARD template, shared `Querier` threading (`cd893bf`)

- `MessageGenre.DISCARD` now uses `%(document_number)s` instead of `%(document_number)d`. With the `%d`
  form, every page discarded on `PATTERN_DOC_NUMBER` raised `TypeError` during `message_genre.value % kwargs`
  **before** any INSERT, which under the new broad handler would have turned the most common discard path
  into a rollback trigger. `wheeck.vw_discard_message` already parses the `%s` form back with
  `NULLIF(..., 'None')`. `MessageGenre.GAP` untouched.
- Added `class RecordingError(RuntimeError)` right after the enum, re-exported from `src/wheeck/__init__.py`
  (the import line became a parenthesised multi-line import, `RecordingError` first per case-sensitive sort).
- `check_duplicate(delivery, querier=None)` and `save_warning(message_genre, querier=None, **kwargs)` accept
  an optional shared querier via the `own_querier = querier is None` idiom; only a self-opened connection is
  torn down with `del`. `save_warning` raises `RecordingError('INSERT_WARNING')` on `rows != 1` when running
  inside a shared transaction and keeps the `-1` sentinel only on the standalone auto-commit path.
- `check_gaps()` untouched — it iterates `for row in querier:` while calling `save_warning` inside the loop,
  and the single shared cursor would silently truncate that resultset if a querier were passed down.

### Task 2 — `doc_scanner` becomes one transaction per document (`86a4a9f`)

- `Querier(cfg_in=PATH_CFG)` (auto-commit off; the implicit transaction opens on the first statement).
- The whole page loop is wrapped in a single `try / except Exception / finally`:
  - `querier.save_changes(True)` + the commit log are the last two statements of the `try` body, after the
    loop and never inside it. The commit lives inside the `try` on purpose, so a failing commit also triggers
    the rollback attempt and the discard cleanup.
  - the handler logs `critical`, rolls back inside its own nested `try/except`, unlinks the collected discard
    PDFs inside `try/except OSError`, then ends with a bare `raise`.
  - `finally: doc.close()` (previously after the loop and skipped on any exception, leaving a pypdfium2
    handle that would block the Task 3 rename on Windows) plus `del querier`.
  - `return doc_pages, discarded_pages.number` after the whole construct.
- Discard PDF compensation: `discarded_doc_path(working_doc, working_page)` extracted as a module-level
  helper; `is_new_discard = not discarded_doc.exists()` registers the path in `discarded_docs` **before**
  `discard_doc()` writes it, so only files created by this run are unlinked and a partial write is still
  compensated.
- `discard_doc()` now wraps `import_pages` / `save` in `try / finally: discard.close(); doc.close()`.
- Shared querier passed to the two `save_warning(MessageGenre.WARNING, querier, ...)` calls, to
  `check_duplicate(delivery, querier)` and to `save_warning(MessageGenre.DISCARD, querier, ...)`.
- `raise RecordingError('INSERT_DELIVERY')` replaces the now-unreachable discard fallback; `raise
  RecordingError('INSERT_DISCARD')` added after its `logger.critical`. The `!= -1` guard is kept as
  documented defensive dead code. Both `QUERY_UPDATE_WARNING` `rows != 1` checks left as log-and-continue.
- Two regex-discard crash guards, written exactly as planned:
  `if delivery.document_number is not None and delivery.document_date is not None:` around the whole
  `QUERY_GET_GAP` block, and `elif delivery.delivery_city and not delivery.distance:` on the distance
  branch (with `delivery_city = None` the external `Querier` collapses `args = (None,)` to a
  no-parameter execute and psqlODBC raises `07001`).
- No `SAVEPOINT`, no explicit connection close, no `querier.run` / `querier.fetch` pair reordered.

### Task 3 — `run()` restores the filename, README documents the semantics (`0c232bc`)

- `try: worked_pages, discarded_pages = doc_scanner(...)` / `except Exception:` →
  `logger.exception(...)` (deliberate small extension of the convention that reserves it for `src/main.py`:
  this loop is now an error boundary, and the message states that the doc will be retried on every run),
  then `shutil.move(working_doc, PATH_WORKING_DIR / doc)` inside its own `try/except OSError` logging
  `critical`, then `continue` — which skips both the `backup_dir` copy and the move to `DDTs/recorded/`.
- `README.md`: the `recording_docs.py` bullet now documents the per-document transaction, the rollback with
  discard-PDF deletion and the automatic re-processing.

## Three-path reasoning (verification point 6)

- **Happy path.** N pages are inserted on the shared transaction; after the loop a single
  `save_changes(True)` commits everything, the commit is logged, `finally` closes the PDF and drops the
  querier, `run()` copies the file to `backup_dir` and moves it to `DDTs/recorded/`.
- **Discard path (any `PATTERN_*` failure, including `PATTERN_DOC_NUMBER`).** The `%s` template interpolates
  a `None` document number without raising; the distance branch is skipped when `delivery_city` is `None`;
  `delivery_warning` + `delivery_discard` are written on the shared transaction (all three nullable columns
  pass, `chk_delivery_discard_quantity` and `chk_delivery_discard_distance` do not fire on NULL); the discard
  PDF is produced and registered; the gap lookup is skipped by the `None` guard; the loop continues and the
  page is **committed** with the rest of the document. No rollback.
- **Failure path.** Any exception (`pyodbc.Error`, `RecordingError`, `TypeError`, `AttributeError`) →
  `save_changes(False)` → only the discard PDFs created by this run are unlinked → bare re-raise →
  `finally` closes the pypdfium2 handle → `run()` logs the traceback, renames `<name>.recording.pdf` back to
  `<name>.pdf` in `DDTs/` and `continue`s. Documents committed earlier in the same batch are untouched, and
  the failed one is re-processed from scratch on the next run.

## Deviations from Plan

None — the plan was executed exactly as written. The only judgement calls, both explicitly permitted:

- the explanatory comment about the `%s` / `%d` specifiers was placed inside `MessageGenre`, immediately
  above its members, rather than above the `class` line — same intent, better fit with the file's style;
- the `src/wheeck/__init__.py` re-export line exceeded the line width once `RecordingError` was added, so it
  became a parenthesised two-line import in the style already used for `.constants`.

`discard_doc()`'s pre-existing `:rtype: str` docstring inaccuracy (the signature says `-> Path`) was left
alone: out of scope, and the plan required the extraction pipeline to stay otherwise byte-for-byte identical.

## Verification Results

All gates run with the Bash tool from `C:/source/wheeck`.

| Gate | Result |
|------|--------|
| Task 1 automated gate (AST + `inspect` signatures + template interpolation with `None`) | `ok` |
| Task 2 automated gate (single try/except/finally, commit placement, rollback + unlink + bare raise, guards, helpers, no SAVEPOINT, no explicit close) | `ok` |
| Task 3 automated gate (guarded `doc_scanner` call, `logger.exception`, defensive rename, `continue`, README wording) | `ok` |
| `py_compile src/wheeck/recording_docs.py src/wheeck/__init__.py` | pass |
| `import wheeck` → `1.0.3` | pass (no DB connection opened at import) |
| `git diff -- src/wheeck/constants.py src/core src/main.py scheme/wheeck.sql` | empty |
| `git diff -w` review of `doc_scanner` | only the planned edits; the extraction pipeline is otherwise unchanged (the rest of the diff is pure re-indentation into the `try`) |

No test suite exists in this repo and none was created. No database write was performed during execution.

## Follow-ups / accepted trade-offs

1. **`src/main.py`'s bare `except Exception` still swallows the re-raised failure.** Anything escaping `run()`
   is logged and the process exits without signalling the scheduler. Out of scope by CONTEXT.
2. **A deterministic failure now retries — and logs — on every run**, because the working file is renamed back
   into the `DDTs/` queue. Accepted per the amended CONTEXT decision; this is why the `logger.exception`
   message is self-explanatory about the repetition.
3. **The `QUERY_GET_DISTANCE` cache warm-up is lost on rollback**, so the OpenRouteService calls (and the
   1.5 s sleeps) for those cities are repeated on re-processing.
4. **Rollbacks leave gaps in the `delivery.id` / `delivery_warning.id` identity sequences** — identity
   sequences are not transactional. Harmless for surrogate keys, but expect non-contiguous ids.
5. **A discard PDF that already existed before this run and was overwritten by it is not restored** on
   rollback. The content is regenerated from the same source page, so it is equivalent.
6. **Connection teardown on the re-raise path is deferred.** `del querier` in `finally` only drops
   `doc_scanner`'s reference; the traceback still holds the same object in the `check_duplicate` /
   `save_warning` frames, so `Querier.__del__` — and the socket close — fires once `run()`'s `except` block
   releases the exception. The ROLLBACK itself is deterministic; the teardown is not. `core.Querier` exposes
   no public `close()` and calling `connection.close()` would make `__del__` raise.

## Known Stubs

None.

## Self-Check: PASSED

- `src/wheeck/recording_docs.py` — FOUND
- `src/wheeck/__init__.py` — FOUND
- `README.md` — FOUND
- commit `cd893bf` — FOUND
- commit `86a4a9f` — FOUND
- commit `0c232bc` — FOUND
