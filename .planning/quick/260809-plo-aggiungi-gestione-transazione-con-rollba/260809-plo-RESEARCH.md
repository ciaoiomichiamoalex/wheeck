# Quick Task 260809-plo: Transaction management with rollback - Research

**Researched:** 2026-08-09
**Domain:** pyodbc + psqlODBC transaction semantics inside `src/wheeck/recording_docs.py`
**Confidence:** HIGH (all core claims empirically probed against the live `wheeck` database)

## Summary

The environment is pyodbc 5.3.0 with the `PostgreSQL ANSI` psqlODBC driver `[VERIFIED: .venv probe + config/querier.json]`.
Every transaction claim below was verified by running throwaway probes against the real database using
`TEMP` tables and read-only statements only — **no `wheeck.*` row was written**.

Two findings drive the whole implementation:

1. **A failed write raises a `pyodbc.Error`; it does *not* return `rows != 1`.** The locked rollback trigger
   "`querier.run(...).rows != 1` on the three INSERTs" is therefore *practically unreachable* for constraint
   or SQL errors — those arrive as exceptions. Keep the `rows != 1` checks (cheap, and the locked contract),
   but the real rollback path is the exception handler.
2. **psqlODBC wraps every statement in an internal SAVEPOINT by default** ("Level of rollback on errors" =
   Statement), so PostgreSQL's "current transaction is aborted" (25P02) does **not** bite on this DSN. Prior
   work in the transaction survives a failed statement and the transaction stays usable. Explicit SAVEPOINTs
   are **not** warranted — see §2.

**Primary recommendation:** `Querier(cfg_in=PATH_CFG)` (drop `save_changes=True`), wrap the page loop in
`try / except / else / finally`, `save_changes(True)` in `else`, `save_changes(False)` + delete the discarded
PDFs in `except`, `doc.close()` + `del querier` in `finally`, re-raise so `run()` skips the recorded/backup move.

## User Constraints (from CONTEXT.md)

All decisions in `260809-plo-CONTEXT.md` are LOCKED and none of this research contradicts them: one
transaction per document opened in `doc_scanner()`; rollback on unhandled exception or `rows != 1` on
`QUERY_INSERT_DELIVERY` / `QUERY_INSERT_DISCARD` / `QUERY_INSERT_WARNING`; regex-driven discards are *not* a
rollback trigger; discarded PDFs deleted on rollback and the working file left as `.recording.pdf`;
`check_duplicate` / `save_warning` accept an optional shared `Querier`; `check_gaps` keeps its own connection.
`core.Querier` is read-only — no `__enter__`/`__exit__` may be added there.

Project conventions that apply (`.planning/codebase/CONVENTIONS.md`, global `CLAUDE.md`): 4-space indent,
snake_case, Sphinx `:param:/:type:/:return:/:rtype:` docstrings on every public function, lazy `%`-style
logging, `# name: description (es. value)` variable comments, English text, `logger.critical` for DB writes
that did not affect exactly one row. No project-level `CLAUDE.md` exists in this repo `[VERIFIED: ls]`.

---

## 1. pyodbc + psqlODBC transaction semantics (all VERIFIED by probe)

| Question | Answer | Evidence |
|----------|--------|----------|
| When does the implicit transaction begin with `autocommit=False`? | On the **first executed statement**; `txid_current()` returned the same xid (2734) across separate `execute()` calls and stayed identical after 6 further statements. | probe A/E |
| Is `cursor.commit()` == `connection.commit()`? | **Yes** — `cursor.connection is conn` is `True`; pyodbc's `Cursor.commit()` delegates to the connection and therefore commits **every** cursor on it. `Querier` holds exactly one cursor, so no caveat here. | probe 5 / 1a |
| Close without commit? | **Implicit rollback.** An xid assigned on a connection closed without `commit()` reported `txid_status(...) = 'aborted'` from a second connection. `Querier.__del__` is therefore safe-by-default: a leaked `Querier` loses its work rather than half-committing it. | probe H |
| Does `rollback()` invalidate the cursor / require re-preparing? | **No.** `SQL_CURSOR_COMMIT_BEHAVIOR` and `SQL_CURSOR_ROLLBACK_BEHAVIOR` both report `SQL_CB_PRESERVE`; the same cursor executed fine after `rollback()`, in a new transaction. | probe 1/4/G |
| `rollback()` with no active transaction? | **OK, no error.** Safe to call unconditionally in an error handler. | probe 3 |
| PostgreSQL "aborted transaction" (25P02)? | **Does not occur on this DSN.** After `1/0`, a duplicate-PK INSERT, a CHECK violation and a syntax error, the next statement still ran and the row inserted *before* the failures was still visible (`count(*) = 1`, then `= 2` after a further insert). This is psqlODBC statement-level rollback (per-statement internal SAVEPOINT). | probe 2/3, C/D |
| Failed INSERT → exception or rowcount? | **Exception.** duplicate PK → `pyodbc.IntegrityError 23505`; CHECK violation → `IntegrityError 23514`; malformed SQL → `ProgrammingError 42601`. A *successful* single-row INSERT returns `rowcount == 1`. | probe B |
| `INSERT ... RETURNING id` (i.e. `QUERY_INSERT_WARNING`) inside a transaction? | Works unchanged: `rowcount == 1` **and** `fetchval()` returns the id. No transaction-related caveat. | probe F |
| `UPDATE` matching 0 rows? | Returns `rowcount == 0`, **no exception** — so the `rows != 1` checks on `QUERY_UPDATE_WARNING` (lines 291, 320) are genuinely reachable and must stay "log critical and continue". | probe |

**Caveat `[CITED: psqlODBC driver option]`:** statement-level rollback is a *driver DSN option*
("Level of rollback on errors": Nop / Transaction / Statement). It is not set explicitly in
`config/querier.json`, so the code is relying on the driver default. The recommended design is immune to this
(§2), but do **not** write code that depends on continuing after a caught `pyodbc.Error`.

## 2. The "aborted transaction" risk — and why SAVEPOINT is not needed

The locked trigger `rows != 1` on the three INSERTs is **near-dead code**: pyodbc raises before `rows` can be
inspected for any constraint/SQL/connection failure. `rows != 1` can only fire for a statement that *succeeded*
with an unexpected row count, which a single-row `INSERT ... VALUES (...)` cannot produce. Keep the checks
(they are the locked contract and cost nothing), but **the exception handler is the real rollback path**.

Recommended structure — make both triggers converge on one handler by raising on `rows != 1`:

```python
class RecordingError(RuntimeError):
    """
    Raised when a database write does not affect exactly one row, to abort and roll back the whole document.
    """
```

```python
        if querier.run(QUERY_INSERT_DELIVERY, *astuple(delivery)[:-1]).rows != 1:
            logger.critical('error on saving delivery record from page %d of %s... check the database connection!',
                            working_page, working_doc_name)
            raise RecordingError('INSERT_DELIVERY')
```

**Consequence to plan for:** the existing fallback at lines 288-290
(`discarded_pages.on_error = 'INSERT_DELIVERY'` → write a discard record instead) becomes **unreachable and
must be removed**; the locked decision replaces it with a rollback.

**SAVEPOINT: not warranted — over-engineering here.** Two independent reasons:
1. psqlODBC already issues a per-statement savepoint (probe C/D), so partial-failure recovery inside a
   statement is free.
2. More importantly, the locked design **never continues after a DB error** — every DB error aborts the whole
   document. A transaction that is never used after an error cannot be poisoned by 25P02, so the design is
   correct even if the DSN option were later switched to `Transaction` or `Nop`. Explicit `SAVEPOINT` /
   `ROLLBACK TO` would only be needed to keep *some* pages while dropping others, which directly contradicts
   "either all pages of a given PDF are persisted, or none".

## 3. Deterministic cleanup without `with` — concrete skeleton

`del x` only decrements a refcount. In CPython it *did* fire `__del__` promptly in both tested scenarios,
including when the object had been passed as a parameter to a helper that raised `[VERIFIED: probe 4a/4b]` —
because the traceback is released when the `except` block ends. But if the handler **re-raises** (which it
must, per this design), a helper frame that still holds the shared `Querier` as a parameter keeps it alive
until `src/main.py` finishes logging. That window ends in an implicit rollback anyway (probe H), so it is
harmless — but do not treat `del` as a close.

Do **not** call `querier.cursor.connection.close()` explicitly: `Querier.__del__` would then run
`self._cursor.close()` on a closed connection and raise `ProgrammingError: The cursor's connection has been
closed`, printed as a noisy "Exception ignored in __del__" `[VERIFIED: probe 1b/1c]`.

```python
def doc_scanner(working_doc: str | Path,
                job_begin: datetime = datetime.now()) -> tuple[int, int]:
    # save_changes=False keeps auto-commit OFF: an implicit transaction opens on the first executed statement
    querier: Querier = Querier(cfg_in=PATH_CFG)
    working_doc_name = working_doc.stem.removesuffix('.recording') + working_doc.suffix

    doc = pypdfium2.PdfDocument(working_doc)
    doc_pages = len(doc)
    # discarded_docs: PDFs written under DDTs/discarded for this document, to be removed on rollback
    discarded_docs: list[Path] = []
    discarded_pages = type('DiscardedPages', (object,), {...})()

    try:
        for working_page, page in enumerate(doc, start=1):
            ...
            # inside the discard branch, track the produced file for the compensation
            discarded_doc = discard_doc(working_doc, working_page)
            discarded_docs.append(discarded_doc)
            ...
    except Exception:
        logger.critical('rolling back the whole doc %s... nothing will be saved!', working_doc_name)
        try: querier.save_changes(False)
        except Exception: logger.critical('error on rollback of %s... check the database connection!', working_doc_name)

        # the filesystem is not transactional: remove the discard PDFs produced during this document
        for discarded in discarded_docs:
            logger.info('removing discard doc %s after rollback...', discarded.name)
            try: discarded.unlink(missing_ok=True)
            except OSError as error: logger.critical('error on removing discard doc %s! [%s]', discarded.name, error)
        raise
    else:
        querier.save_changes(True)
        logger.info('committed %d pages of %s! [%d discarded pages]',
                    doc_pages, working_doc_name, discarded_pages.number)
        return doc_pages, discarded_pages.number
    finally:
        doc.close()
        del querier
```

Notes on the skeleton:
- `doc.close()` **must** move into `finally`. Today it is at line 324 and is skipped on any exception, leaving a
  pypdfium2 handle open on `<name>.recording.pdf` — on Windows that would make a later `shutil.move` fail with
  `PermissionError` `[ASSUMED: standard Windows file-locking behaviour, not probed]`.
- `return` inside `else` still runs `finally` — correct and intended.
- The rollback itself is wrapped in its own `try` so a dead connection cannot replace the original exception.

Caller side, in `run()`:

```python
        try: worked_pages, discarded_pages = doc_scanner(working_doc, job_begin)
        except Exception:
            logger.exception('JOB FAILED: doc %s rolled back, left as %s for reprocessing!', doc, working_doc.name)
            continue
```

`continue` skips both the backup copy and the move to `DDTs/recorded/`, exactly as locked.
**Flag for the user:** the file stays `<name>.recording.pdf`, which does **not** match `PATTERN_WORKING_DOC`
(`^\d{4}_\d{2}_DDT_\d{4}_\d{4}(?:_P\d{3})*\.pdf$` rejects the extra `.recording` segment)
`[VERIFIED: constants.py:16]`, so a failed document needs a **manual rename** before it is retried. That is the
locked behaviour, not a defect — but it should be stated in the log message.

Minor convention note: `CONVENTIONS.md` reserves `logger.exception` for `src/main.py`. Using it in `run()` is a
deliberate, small extension (that loop becomes a boundary) and falls under "logging wording and levels" discretion.

## 4. Threading the shared `Querier` through the helpers

```python
def check_duplicate(delivery: Delivery,
                    querier: Querier = None) -> bool:

def save_warning(message_genre: MessageGenre,
                 querier: Querier = None,
                 **kwargs) -> int:
```

- Additive with a default → `src/wheeck/__init__.py` re-exports keep working unchanged, and `check_gaps()`
  calls `save_warning(MessageGenre.GAP, document_number=..., document_year=...)` with no edit at all.
- No name collision: `querier` is not one of the `MessageGenre` template fields (`page`, `doc`, `pattern`,
  `document_number`, `document_genre`, `document_date`, `genre`, `record`, `document_year`)
  `[VERIFIED: recording_docs.py:86-92]`. Placing it before `**kwargs` is therefore safe; an explicit `*`
  separator is unnecessary.
- Body pattern in both helpers, matching the codebase's low-abstraction style:

```python
    # own_querier: True when no shared transaction querier is given, so the connection is opened and closed here
    own_querier = querier is None
    if own_querier: querier = Querier(cfg_in=PATH_CFG, save_changes=True)
    ...
    if own_querier: del querier
    return ...
```

- In `save_warning`, only the standalone path may keep the `-1` sentinel; inside a transaction the failure must
  abort the document:

```python
    if querier.run(QUERY_INSERT_WARNING, message_genre.name, message_genre.value % kwargs).rows != 1:
        logger.critical('error on saving delivery warning message record... check the database connection!')
        # inside a document transaction the failure must roll back the whole document
        if not own_querier: raise RecordingError('INSERT_WARNING')
        id_message = -1
    else: id_message = querier.fetch(Querier.FETCH_VAL)
```

## 5. Filesystem compensation

`discard_doc()` already returns the created `Path`, and it closes both `pypdfium2` documents before returning
(`discard.close()`, `doc.close()`, lines 353-354) — so no handle remains on the produced file and deleting it
later is safe. **Do not** move the deletion inside the page loop; collect the paths in a
`discarded_docs: list[Path]` and unlink them only in the `except` branch.

Windows specifics:
- Use `Path.unlink(missing_ok=True)` and catch `OSError` (covers `PermissionError`) — log `critical` and keep
  going, so a locked file cannot mask the original exception `[ASSUMED: general Windows/AV file-locking risk]`.
- The **source** `working_doc` is opened by `doc_scanner` for the whole loop *and* re-opened by every
  `discard_doc()` call; only the `finally: doc.close()` above releases the outer handle. That file is never
  deleted, only left in place — but the handle must still be released before `run()` continues.

## 6. Pitfalls specific to this change

1. **One cursor per `Querier` — interleaving silently truncates result sets.** `Querier` holds a single
   `_cursor`; executing a new statement mid-iteration discarded the pending rows and the resumed `fetchone()`
   returned `None` **without raising** `[VERIFIED: probe]`. Concretely: `check_gaps()` iterates `for row in querier:`
   while calling `save_warning(...)` inside the loop — if anyone ever passes that querier down, only the first
   gap would be processed, silently. This is exactly why CONTEXT locks `check_gaps` to its own connection.
   In `doc_scanner` the current ordering is safe (every `run()` is followed immediately by its `fetch()` before
   any helper call), but any future reordering can break it invisibly.
2. **Duplicate detection across pages now depends on sharing the querier.** A transaction sees its own
   uncommitted rows `[VERIFIED: probe C/D]`, so `check_duplicate` on the shared `Querier` keeps working; on a
   separate connection it would silently stop catching same-document duplicates, and the subsequent
   `QUERY_INSERT_DELIVERY` would then hit `uq_delivery_document_source_page` /
   `uq_delivery_document_number_genre_year` and raise `IntegrityError 23505` — rolling back the whole document.
3. **`id_warning_message` FK ordering is fine inside one transaction.** `delivery_discard.id_warning_message`
   is `NOT NULL REFERENCES delivery_warning(id) ON DELETE RESTRICT` `[VERIFIED: wheeck.sql:105,122-125]`.
   INSERT warning → INSERT discard in the same transaction resolves against the uncommitted parent row. The
   danger is the *current* code: `save_warning` commits on its own connection, so a rollback of `doc_scanner`
   leaves an orphan `DISCARD` warning with `status = TRUE`, which pollutes `vw_discard_message` and therefore
   the `is_discard` flag of `vw_delivery_gap` — i.e. real gaps would be misclassified. Sharing the connection
   is a correctness fix, not a cleanliness one.
4. **`QUERY_INSERT_WARNING` uses `RETURNING id`** `[VERIFIED: constants.py:113-120]` — unaffected by the
   transaction: `rowcount == 1` and `fetchval()` both behave normally (probe F).
5. **`trg_sync_discard_status`** fires on `UPDATE ... status` of `delivery_warning` and cascades into
   `delivery_discard` `[VERIFIED: wheeck.sql:147-166]`. It runs inside the same transaction and rolls back with
   it — no special handling.
6. **Identity sequences are not transactional.** A rollback leaves gaps in `delivery.id` /
   `delivery_warning.id`. Harmless (surrogate keys), but worth knowing before someone reports "missing ids".
7. **`QUERY_GET_DISTANCE` caching becomes transaction-scoped.** A distance computed for page 1 is visible to
   page 5 of the same document (good), but a rollback discards it, so the OpenRouteService call (plus the
   1.5 s sleep) will be repeated on reprocessing. Acceptable; note it in the plan rather than caching in memory.
8. **The most likely real-world rollback trigger already exists as a latent crash.** When `PATTERN_DOC_NUMBER`
   fails, `delivery.document_number` / `document_date` stay `None`, and:
   - `MessageGenre.DISCARD.value % kwargs` raises `TypeError: %d format: a real number is required, not
     NoneType` (line 300, template uses `%(document_number)d`) `[VERIFIED: probe]`;
   - line 316 `delivery.document_date.year` raises `AttributeError` `[VERIFIED: probe]`.

   Today that kills the whole batch via `main.py`'s bare `except`, leaving previously written pages committed.
   After this change it becomes a clean per-document rollback — which is precisely the intended win. **Do not
   fix these two bugs in this task** (out of scope), but the `except Exception` handler must be broad enough to
   catch `TypeError`/`AttributeError`, not just `pyodbc.Error`. (`wheeck.vw_discard_message` NULLIFs the literal
   string `'None'` for `document_number`, which suggests the template was once `%s` — do not "restore" it here.)

## Sources

**Primary (HIGH — empirical, this session):** three probe scripts run against the live `wheeck` DB with
`.venv/Scripts/python.exe` (pyodbc 5.3.0, `PostgreSQL ANSI` driver), using `TEMP` tables and read-only
statements only. Kept at
`C:/Users/Alex/AppData/Local/Temp/claude/C--source-wheeck/af34e990-6296-46ac-a572-89efa5cf2aef/scratchpad/probe_tx*.py`.

**Primary (HIGH — source read):** `C:/source/core/src/core/querier.py`, `src/wheeck/recording_docs.py`,
`src/wheeck/constants.py`, `scheme/wheeck.sql`, `src/wheeck/__init__.py`, `src/main.py`,
`.planning/codebase/ARCHITECTURE.md`, `.planning/codebase/CONVENTIONS.md`.

**Assumptions log:** (A1) Windows file-lock behaviour on `shutil.move` / `unlink` of a still-open PDF is
standard-OS knowledge, not probed. (A2) The psqlODBC "Level of rollback on errors" default is inferred from
observed behaviour rather than from the DSN configuration; the recommended design does not depend on it.

**Research date:** 2026-08-09 — **Valid until:** stable (no external dependency drift expected).
