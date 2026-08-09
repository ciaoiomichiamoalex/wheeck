---
phase: quick-260809-plo
reviewed: 2026-08-09T00:00:00Z
depth: quick
files_reviewed: 3
files_reviewed_list:
  - src/wheeck/recording_docs.py
  - src/wheeck/__init__.py
  - README.md
findings:
  critical: 2
  warning: 9
  info: 5
  total: 16
status: issues_found
---

# Quick task 260809-plo: Code Review Report

**Reviewed:** 2026-08-09
**Depth:** quick (extended with targeted per-file reading of the diff and its call graph)
**Files Reviewed:** 3
**Status:** issues_found

## Summary

The change converts `doc_scanner()` into one database transaction per PDF. The core transactional
mechanics are sound and I could not break them:

**Verified negatives (checked, no defect found):**

- **No write escapes the transaction.** Every `save_warning(...)` call inside `doc_scanner`
  (lines 227, 245, 318) passes the shared `querier`; `check_duplicate` (line 282) does too.
  The only remaining standalone `save_warning` caller is `check_gaps()` (line 467), which by design
  keeps its own auto-commit connection and only ever runs *outside* a document transaction.
  `Delivery.calculate_distance()` / `GeoMap` touch no database.
- **No commit happens mid-document.** `save_changes(True)` appears exactly once (line 356), after the
  page loop, and `Querier(cfg_in=PATH_CFG)` correctly drops `save_changes=True`.
  `check_duplicate`/`save_warning` never call `save_changes` and correctly guard `del querier` behind
  `own_querier`, so the shared connection is never torn down by a helper.
- **No unconsumed-resultset / silent-`None` interleave exists today.** I traced every
  `querier.run(...)` → `querier.fetch(...)` pair on the shared cursor
  (lines 268/271, 293/294, 305, 309, 327, 346/347, 350, `check_duplicate` 443-449,
  `save_warning` 516/522). Each `fetch` is the statement immediately following its `run` with no
  intervening query. See WR-05 for the latent fragility that remains.
- **The page-discard path no longer raises.** The `%(document_number)s` fix, the
  `delivery.delivery_city and not delivery.distance` guard (line 290) and the
  `document_number is not None and document_date is not None` guard (line 345) together close the
  three known crash sites. `delivery_discard`'s nullable columns and `CHECK (quantity > 0)` accept
  the resulting NULLs, and `id_warning_message INTEGER NOT NULL` is satisfied because `save_warning`
  now raises instead of returning `-1` under a shared querier.
- **The discard-PDF cleanup cannot delete a foreign file.** `discarded_doc_path()` derives the name
  deterministically from `working_doc.stem`, `PATTERN_WORKING_DOC` forbids dots in the stem, and
  `is_new_discard` correctly excludes pre-existing files.

What the change does **not** get right is the *failure-of-the-failure-path*: every route by which the
compensation itself is skipped or fails ends with the working file stranded as
`<name>.recording.pdf`, which `PATTERN_WORKING_DOC` (`^\d{4}_\d{2}_DDT_\d{4}_\d{4}(?:_P\d{3})*\.pdf$`)
never matches again. That is silent, permanent loss of a document with nothing but a log line, and it
directly contradicts the locked design goal ("the filesystem must be left re-processable"). Two
BLOCKERs below are instances of that. The rest are robustness and API-hygiene defects.

## Critical Issues

### CR-01: A `BaseException` bypasses the whole compensation and strands the document forever

**File:** `src/wheeck/recording_docs.py:359` and `src/wheeck/recording_docs.py:552`
**Issue:**
Both handlers are `except Exception`. `KeyboardInterrupt`, `SystemExit`, `MemoryError` and
`GeneratorExit` derive from `BaseException`, not `Exception`, so on any of them:

- `querier.save_changes(False)` (line 362) never runs — the rollback is left to the implicit
  `connection.close()` in `Querier.__del__`, which is not deterministic;
- the `discarded_docs` cleanup loop (lines 366-369) never runs, so `DDTs/discarded/*.pdf` files written
  during the aborted document survive with **no matching `delivery_discard` row** — precisely the
  filesystem inconsistency this task exists to prevent;
- `run()`'s handler (line 552) does not fire either, so `shutil.move(working_doc, PATH_WORKING_DIR / doc)`
  is skipped and the file stays `<name>.recording.pdf`. `PATTERN_WORKING_DOC` will never match it, so
  the document is dropped from **every** future run, silently.

This is a realistic trigger: the pipeline is a batch job (Ctrl-C, a killed scheduled task, a
`SystemExit` from anywhere in `geo`/`openrouteservice`). Before this change the same interrupt left
partial rows committed but at least *visible*; now the whole document vanishes with the DB clean and
the file invisible.

**Fix:** compensate on `BaseException`, or better, move the filesystem compensation out of the
`except` clause so it cannot be skipped:

```python
    # committed: True only once save_changes(True) returned, drives the compensation in finally
    committed = False
    try:
        for working_page, page in enumerate(doc, start=1):
            ...
        querier.save_changes(True)
        committed = True
        logger.info('committed %d pages of %s! [%d discarded pages]', ...)
    except BaseException:
        logger.critical('rolling back the whole doc %s... nothing will be saved!', working_doc_name)
        try: querier.save_changes(False)
        except Exception: logger.exception('error on rollback of %s... check the database connection!', working_doc_name)
        raise
    finally:
        if not committed:
            # the filesystem is not transactional and must be compensated on every abort path
            for discarded in discarded_docs:
                logger.info('removing discard doc %s after rollback...', discarded.name)
                try: discarded.unlink(missing_ok=True)
                except OSError as error: logger.critical('error on removing discard doc %s! [%s]', discarded.name, error)
        doc.close()
        del querier
```

and widen `run()`'s handler to `except BaseException:` so the filename is restored before the
interrupt propagates (re-`raise` after the restore rather than `continue` for non-`Exception`).

### CR-02: A failed restore in `run()` silently and permanently drops the document

**File:** `src/wheeck/recording_docs.py:557-560`
**Issue:**

```python
try: shutil.move(working_doc, PATH_WORKING_DIR / doc)
except OSError as error: logger.critical('error on restoring doc %s for reprocessing! [%s]', doc, error)
# skipping both the backup copy and the move in the recorded dir, the previous docs stay recorded
continue
```

If the rename fails (antivirus/indexer holding the freshly written PDF, a network share, a residual
handle — see WR-01/WR-02), the code logs one `critical` and `continue`s. The file stays
`<name>.recording.pdf`, which `PATTERN_WORKING_DOC` never matches, so `run()` skips it on every
subsequent run. The database was rolled back, so the document is now **nowhere**: not in the DB, not
in `DDTs/recorded/`, not in the queue. The whole locked decision ("restore its original name so the
document is automatically re-processed") is defeated by a swallowed `OSError`, and nothing downstream
(`main.py` swallows everything) surfaces it.

**Fix:** make the restore best-effort-with-retry and, on definitive failure, escalate instead of
continuing silently — e.g. quarantine into a name that is still visible, and count failures so the
batch reports a non-zero outcome:

```python
            # restoring the original name makes PATTERN_WORKING_DOC match the doc again
            try:
                shutil.move(working_doc, PATH_WORKING_DIR / doc)
            except OSError as error:
                logger.critical('error on restoring doc %s for reprocessing! [%s]', doc, error)
                # last resort: the doc must never stay invisible as '<name>.recording.pdf'
                try: shutil.move(working_doc, PATH_WORKING_DIR / f"{working_doc.stem.removesuffix('.recording')}.failed.pdf")
                except OSError as error: logger.critical('doc %s is stuck as %s and will be skipped forever! [%s]', doc, working_doc.name, error)
            continue
```

(a `*.failed.pdf` name is at least greppable/alertable; alternatively retry the original rename after
forcing a `gc.collect()` so any lingering pypdfium2/pyodbc reference is released first.)

## Warnings

### WR-01: The pypdfium2 handle is opened outside the `try`, so `finally: doc.close()` does not protect it

**File:** `src/wheeck/recording_docs.py:121-141`
**Issue:** `querier` (121), `doc = pypdfium2.PdfDocument(working_doc)` (127) and `doc_pages = len(doc)`
(129) all execute **before** `try:` (143). If anything between line 127 and line 143 raises — most
plausibly `len(doc)` on a damaged PDF, or the `type('DiscardedPages', ...)` construction — the
`finally` block never runs, `doc.close()` is never called, and the file handle on
`<name>.recording.pdf` survives until GC. On Windows that is exactly the condition that makes
`run()`'s restore (`shutil.move`) fail, feeding CR-02. `querier` is also left un-`del`'d on that path.
**Fix:** move `doc = pypdfium2.PdfDocument(working_doc)` and `doc_pages = len(doc)` inside the `try`,
and guard the `finally` with `if doc is not None: doc.close()` (initialise `doc = None` before the
`try`).

### WR-02: `finally: doc.close()` can replace the in-flight exception, and swallows `del querier`

**File:** `src/wheeck/recording_docs.py:372-377`
**Issue:** `finally` runs while the original exception is propagating. If `doc.close()` raises — and it
can, pypdfium2 objects are closed here while the page object from `enumerate(doc)` is still alive in
the traceback frame — the new exception **replaces** the original. `run()` then calls
`logger.exception(...)` and records the wrong root cause, making the mandatory retry loop
undiagnosable. The same failure also skips `del querier`.
**Fix:**

```python
    finally:
        # a failure while releasing the pypdfium2 handle must never replace the original exception
        try: doc.close()
        except Exception as error: logger.critical('error on closing doc %s! [%s]', working_doc_name, error)
        del querier
```

### WR-03: OpenRouteService calls and `time.sleep(1.5)` now run inside the open transaction

**File:** `src/wheeck/recording_docs.py:296-300`
**Issue:** With auto-commit removed, the transaction opens on the first statement of page 1 and stays
open until the commit after the last page. `delivery.calculate_distance()` performs two network calls
(geocode + directions) and is followed by an unconditional `time.sleep(1.5)`, **per new city, per
page**. A 100-page document with 40 new cities keeps the connection `idle in transaction` for minutes,
holding the `wheeck.delivery` / `delivery_warning` row locks and the `vw_delivery_gap` read snapshot
the whole time. If the server has `idle_in_transaction_session_timeout` set (or the ODBC/TCP keepalive
drops), the session is terminated mid-document → rollback → `run()` restores the file → the document
retries and times out again on the *next* run. That is a self-sustaining failure loop, not the
"deterministic failure is retried" trade-off that CONTEXT accepted.
**Fix:** resolve distances before opening the transaction (pre-pass over the pages collecting the
distinct cities), or cache the geocoding result outside the DB session and drop the in-transaction
`sleep`. At minimum, document the required
`idle_in_transaction_session_timeout = 0` in the README.

### WR-04: `uq_delivery_discard_document_source_page` has no `check_duplicate` equivalent, and now aborts the whole document

**File:** `src/wheeck/recording_docs.py:327-329` (constraint at `scheme/wheeck.sql:122`)
**Issue:** `check_duplicate()` only queries `wheeck.delivery`. `wheeck.delivery_discard` carries its own
`UNIQUE (document_source, page_number)`. Re-processing a document that already has *committed* discard
rows (an operator re-drops a corrected `A.pdf`, a re-scan, a partially recovered batch) makes
`QUERY_INSERT_DISCARD` raise `IntegrityError 23505`. Before this change that propagated out and the
batch aborted with the earlier pages committed; now it rolls the document back **and** `run()` restores
the filename, so the very same violation recurs on every subsequent run — an unbounded retry loop that
grows the log and re-spends the OpenRouteService quota each time, and never converges.
**Fix:** extend the duplicate check to the discard table before inserting, e.g. reuse
`QUERY_GET_DISCARD` (dropping the `status IS TRUE` filter) as a pre-condition on line 327, or make the
insert idempotent with `ON CONFLICT (document_source, page_number) DO UPDATE`.

### WR-05: The shared cursor has no documented contract and one resultset is already left pending

**File:** `src/wheeck/recording_docs.py:268-282`, `427-452`, `498-525`
**Issue:** `querier.run(QUERY_GET_DISCARD, ...)` (line 268) leaves its resultset unconsumed whenever
`discarded_pages.on_error` is falsy, and `check_duplicate(delivery, querier)` (line 282) then issues a
new `execute()` on that same cursor, silently discarding it. Today nothing reads it afterwards, so
there is no live bug — but `check_duplicate` and `save_warning` are now public helpers that mutate the
caller's cursor state with nothing in their docstrings saying so. Any future reordering (or a third
helper) will produce a `fetch()` returning `None` instead of an error, which is unattributable at
runtime.
**Fix:** state the precondition in both docstrings, e.g.
`:param querier: The shared document transaction querier; the caller MUST have consumed any pending
resultset, this helper re-executes on the same single cursor.` and consume the `QUERY_GET_DISCARD`
result unconditionally (or restructure it as `if is_discard:` / inner `if on_error:`).

### WR-06: `save_warning` gained a *positional* second parameter in a publicly re-exported API

**File:** `src/wheeck/recording_docs.py:498-500`, `src/wheeck/__init__.py:3-4`
**Issue:** `save_warning(message_genre, querier=None, **kwargs)` is re-exported from the `wheeck`
package. Any external caller passing a second positional argument now silently binds it to `querier`,
and the first `querier.run(...)` fails with an attribute error deep inside the helper rather than at the
call site. `check_duplicate` has the same shape but there the second positional was previously
impossible, so only `save_warning` regresses. Nothing in the repo does this today, but the change is a
silent breaking change on a published surface, and `querier` is an implementation detail of the
transaction, not a caller-facing option.
**Fix:** make it keyword-only, which costs nothing at the three internal call sites:

```python
def save_warning(message_genre: MessageGenre,
                 *,
                 querier: Querier = None,
                 **kwargs) -> int:
```

and update lines 227, 245, 318 to `save_warning(MessageGenre.WARNING, querier=querier, ...)`.

### WR-07: `shutil.move` in the restore path silently overwrites an existing `DDTs/<name>.pdf`

**File:** `src/wheeck/recording_docs.py:557`
**Issue:** When the destination is an existing *file*, `shutil.move`'s `os.rename` fails on Windows and
it falls back to `copy_function(src, real_dst)` + `os.unlink(src)` — i.e. it **overwrites** without
warning. `docs` is snapshotted by `os.walk` at the top of `run()`, so a `<name>.pdf` dropped into
`DDTs/` while the batch is running is not in `docs` and will be destroyed by the restore of the failing
copy. The same latent hazard exists on line 547 and 569, but the restore is the new one.
**Fix:** check first and log rather than clobber:

```python
            # restore_doc: original filename to make PATTERN_WORKING_DOC match the doc again
            restore_doc = PATH_WORKING_DIR / doc
            if restore_doc.exists():
                logger.critical('doc %s already back in DDTs dir... not overwriting it!', doc)
            else:
                try: shutil.move(working_doc, restore_doc)
                except OSError as error: logger.critical('error on restoring doc %s for reprocessing! [%s]', doc, error)
```

### WR-08: The nested rollback handler loses the rollback failure's traceback

**File:** `src/wheeck/recording_docs.py:362-363`
**Issue:**

```python
try: querier.save_changes(False)
except Exception: logger.critical('error on rollback of %s... check the database connection!', working_doc_name)
```

A bare `except Exception` with a message-only `logger.critical` is indistinguishable between "the
connection died", "psqlODBC rejected the ROLLBACK" and a bug in `Querier`. Since this is the single
point where atomicity can actually fail, the diagnosis matters more here than anywhere else in the
file. It is also inconsistent with the rest of the handler, which does capture `as error` for the
`unlink` failures (line 369).
**Fix:** `except Exception as error: logger.critical('error on rollback of %s... check the database
connection! [%s]', working_doc_name, error)` — or `logger.exception(...)`, which `run()` already
establishes as acceptable at an error boundary.

### WR-09: The discard-PDF cleanup loop is not protected against a non-`OSError`

**File:** `src/wheeck/recording_docs.py:366-371`
**Issue:** Only `discarded.unlink(...)` is wrapped, and only against `OSError`. Anything else raised
inside the loop (including by the `logger.info` call if a handler is misconfigured, or a `TypeError`
from a malformed entry in `discarded_docs`) escapes before the bare `raise` on line 371, replacing the
original exception with a cleanup artefact. That is exactly the masking the design set out to avoid,
and unlike WR-02 it also aborts the remaining unlinks.
**Fix:** widen the inner handler to `except Exception as error:` and keep `raise` reachable
unconditionally (the restructuring in CR-01 also fixes this).

## Info

### IN-01: Type-annotation and docstring drift copied into the new helper

**File:** `src/wheeck/recording_docs.py:412-424`, `381-392`
**Issue:** `discarded_doc_path(working_doc: str | Path, ...)` immediately calls `working_doc.stem`, so a
`str` raises `AttributeError`; the annotation is wrong. It was copied verbatim from `discard_doc`, which
carries the same wrong annotation plus `:return: The path to the new document. / :rtype: str` while
actually returning a `Path`. The diff touched both functions without correcting either.
**Fix:** annotate `working_doc: Path` in `discarded_doc_path` (and in `discard_doc`/`doc_scanner` while
touching them), and fix `discard_doc`'s `:rtype:` to `Path`.

### IN-02: The discard path is now computed twice per discarded page

**File:** `src/wheeck/recording_docs.py:332`, `338`, `401`
**Issue:** `doc_scanner` calls `discarded_doc_path(working_doc, working_page)` and then
`discard_doc(working_doc, working_page)`, which computes the very same path again; the return value of
`discard_doc` is now thrown away (it used to be assigned to `discarded_doc`). The helper extraction
removed the duplication of the *expression* but introduced duplication of the *call*.
**Fix:** keep the single computation and pass it down, or drop the local and keep using the return
value only for the log line — the `discarded_docs.append` needs the path *before* the write, so
`discard_doc(working_doc, working_page)` could accept the precomputed `discarded_doc`.

### IN-03: `is_new_discard` is a single-use local

**File:** `src/wheeck/recording_docs.py:334-336`
**Issue:** `is_new_discard` is assigned and consumed on the next line only. The file's own style
elsewhere compresses this kind of predicate. The explanatory comment is the valuable part and can live
on the `if`.
**Fix:** `if not discarded_doc.exists(): discarded_docs.append(discarded_doc)  # only files created by this run are removed on rollback`.

### IN-04: `job_begin: datetime = datetime.now()` is evaluated once, at import time

**File:** `src/wheeck/recording_docs.py:109`
**Issue:** Pre-existing, but `doc_scanner` was substantially rewritten by this change and the defect
sits in its signature. Any caller relying on the default (e.g. a direct `wheeck.doc_scanner(path)` via
the public re-export) stamps every delivery with the process start time, not the job time. `run()` always
passes an explicit value, so nothing in-tree is affected.
**Fix:** `job_begin: datetime = None` plus `if job_begin is None: job_begin = datetime.now()`.

### IN-05: Out-of-scope follow-ups observed while reviewing

**File:** `src/main.py:7`, `scheme/wheeck.sql:174`
**Issue:** (a) `main.py`'s bare `except Exception` still swallows everything escaping `run()`, so a
`critical`-only failure never reaches an exit code — this is what makes CR-02 unobservable in
production. (b) `scheme/wheeck.sql` hardcodes `CREATE USER wheeck WITH PASSWORD 'Wheeck!2024'` in a
version-controlled file. Both are outside this change's scope and are recorded as follow-ups, not as
defects of this task.
**Fix:** (a) let `run()`'s exceptions set a non-zero exit status; (b) move the role password to an
environment variable / `psql -v` variable.

---

_Reviewed: 2026-08-09_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: quick_
