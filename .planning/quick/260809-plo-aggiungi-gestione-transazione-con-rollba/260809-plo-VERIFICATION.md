---
phase: quick-260809-plo
verified: 2026-08-09T17:21:44Z
status: human_needed
score: 8/8 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Run the real pipeline on a test schema with a document that fails mid-way (es. temporarily point PATH_CFG at a copy of the schema and revoke INSERT on wheeck.delivery after the first page, or feed a PDF whose page 2 triggers a constraint violation), then query SELECT COUNT(*) FROM wheeck.delivery / delivery_discard / delivery_warning for that document_source."
    expected: "Zero rows for that document_source in all three tables, no DDTs/discarded/*.pdf for it, and the file back in DDTs/ as <name>.pdf."
    why_human: "Requires a live psqlODBC connection and a database write; the verifier is read-only and the repo has no test suite. The rollback logic itself was proven with an in-process fake Querier, but the real pyodbc statement-level rollback across the single shared cursor can only be observed against Postgres."
  - test: "On Windows, let a real document fail inside doc_scanner (real pypdfium2 handle open on <name>.recording.pdf) and check the log for 'restoring it in DDTs dir' without the following 'error on restoring doc ... [WinError 32]' critical."
    expected: "The rename to <name>.pdf succeeds because finally: doc.close() released the pypdfium2 handle before run() moves the file."
    why_human: "File-locking behaviour of pypdfium2 on Windows cannot be observed with a stubbed PdfDocument."
---

# Quick Task 260809-plo: Transaction management with rollback - Verification Report

**Task Goal (user's words):** add transaction management so that a runtime error mid-flow rolls back and
writes nothing to the database, while a fully processed flow commits — no more half-saved documents.
**Verified:** 2026-08-09T17:21:44Z
**Status:** human_needed (8/8 truths verified in the codebase; 2 items need a live end-to-end run)
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| #   | Truth (PLAN must_haves) | Status | Evidence |
| --- | ----------------------- | ------ | -------- |
| 1 | A runtime error while scanning page N leaves ZERO rows of that document in `delivery`, `delivery_discard`, `delivery_warning` | VERIFIED | Every write in `doc_scanner` goes through the single non-autocommit `querier` (`recording_docs.py:121`, writes at 227, 245, 305, 309, 318, 327, 346-350; grep confirms no other `Querier(` inside the loop). `except Exception:` calls `querier.save_changes(False)` (362). Simulation B: injected `rows=0` and an injected driver exception on page 2 both give `commits=0 rollbacks=1 committed_rows=0`, with page-1 rows in the rolled-back buffer. Simulation B3: the two `save_warning(MessageGenre.WARNING, querier, ...)` similarity rows (the early-in-the-loop case) are on the SAME connection (`1 connection`, `2 rolled back rows`, `0 committed`) — the old separate autocommit connection is gone. |
| 2 | A document scanned without runtime errors commits exactly once, after the loop, with all its pages | VERIFIED | `querier.save_changes(True)` at 356 is the penultimate statement of the `try` body, after the `for` loop; AST gate proves `save_changes` appears in no loop body and `save_changes(True)` occurs exactly once. Simulation A (3 pages): `commits=1 rollbacks=0`, commit event fires at page 3 with no `WRITE_PENDING` after it, 9 rows across pages {1,2,3} in a single commit, 1 connection with `autocommit=False`. No mid-document commit is reachable: `save_warning`/`check_duplicate` never commit when a shared querier is passed (`own_querier` is False), and no other component (`geo`, `check_similarity`) touches the database. |
| 3 | Documents already committed earlier in the same `run()` batch are not undone by a later document failing | VERIFIED | Each `doc_scanner` call constructs its own `Querier` and commits before returning; `run()` catches per-document (551-560) and `continue`s. Batch simulation: doc1 `commits=1 committed_rows=6 rolled_back=0` is unaffected while doc2 `commits=0 rolled_back=5` re-raises; the two connections are distinct objects. Filesystem side confirmed by run-simulation D7/D8. |
| 4 | A page discarded by a failed `PATTERN_*` still writes discard + warning and commits — never a rollback trigger | VERIFIED | Simulation C over all four patterns (`PATTERN_DOC_NUMBER`, `PATTERN_CITY` with DX **and** SX failing, `PATTERN_QUANTITY`, `PATTERN_VEHICLE`): no exception, `commits=1 rollbacks=0`, return `(2, 1)`, discard warning + discard row committed for the discarded page. The three former crash sites are neutralised: `%(document_number)s` template (96, interpolated with `None` in gate 1), `elif delivery.delivery_city and not delivery.distance:` (290, avoids the `args=(None,)` collapse in `core/querier.py:120-124`), and `if delivery.document_number is not None and delivery.document_date is not None:` around the `QUERY_GET_GAP` block (345) — the simulation shows `gap_lookups=1` for the `PATTERN_DOC_NUMBER` case vs `2` for the others, i.e. the guard actually fires. `check_duplicate` is short-circuited by `not discarded_pages.on_error` (282), so `document_date.year` is never dereferenced on `None`. Schema confirms `delivery_discard.document_number/genre/date` nullable and `CHECK (quantity > 0)` / `CHECK (distance >= 0)` do not fire on NULL (`scheme/wheeck.sql:103-134`). |
| 5 | On rollback the working file is renamed back to `<name>.pdf`, not moved to `DDTs/recorded/`, not copied to `backup_dir` | VERIFIED | `run()` handler 552-560: `logger.exception` → `shutil.move(working_doc, PATH_WORKING_DIR / doc)` inside its own `except OSError` → `continue`. Run-simulation D2/D3/D5/D6: the failed doc is back in `DDTs/` as `2024_01_DDT_0101_0200.pdf`, no `.recording.pdf` residue, absent from `recorded/` and from `backup/`, while the surrounding docs are archived. D4: the restored name matches `PATTERN_WORKING_DOC = ^\d{4}_\d{2}_DDT_\d{4}_\d{4}(?:_P\d{3})*\.pdf$` (which `.recording.pdf` does not). |
| 6 | On rollback only the `DDTs/discarded/*.pdf` files THIS run created are deleted | VERIFIED | 332-336: `discarded_doc_path(...)` → `is_new_discard = not discarded_doc.exists()` → appended **before** `discard_doc()` writes, so a partial write is compensated too; handler unlinks with `missing_ok=True` inside `try/except OSError` (366-369). Simulation B4: the PDF created during the failed run is gone, a pre-existing PDF for another page of the same document survives. |
| 7 | The pypdfium2 handle is released on every exit path, including the exception path, before the rename | VERIFIED (with a noted edge case) | `finally: doc.close()` (372-373) replaces the old post-loop `doc.close()`; simulation B shows `closed=1` on both exception flavours. `discard_doc` now closes both handles in `try/finally` (402-408). Edge case (WARNING below): lines 127-141 run *before* the `try`, so a failure in `len(doc)` would leave the handle open — DB-safe (no statement executed yet) but the rename could hit WinError 32. |
| 8 | `check_gaps()` keeps its own connection and `save_warning()` still works without a shared Querier | VERIFIED | `check_gaps()` unchanged (455-471), signature `() -> int`, opens its own `Querier` and calls `save_warning` with keywords only — the `for row in querier:` / `save_warning` interleaving keeps two cursors as required (`core/querier.py:91` exposes the single `_cursor`). Spot-check: standalone `save_warning` opens `autocommit=True` and returns the id (77) on success and `-1` on `rows != 1` without raising; with a shared querier it raises `RecordingError('INSERT_WARNING')` and opens no new connection. |

**Score:** 8/8 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
| -------- | -------- | ------ | ------- |
| `src/wheeck/recording_docs.py` | `RecordingError`, None-safe DISCARD template, guarded gap/distance lookups, transactional `doc_scanner`, shared-Querier helpers, self-restoring `run()` | VERIFIED | 593 lines; all elements present and reachable (see truths). `class RecordingError(RuntimeError)` at 101. |
| `src/wheeck/__init__.py` | Public re-export of `RecordingError` | VERIFIED | Line 3-4, parenthesised import, `wheeck.RecordingError is recording_docs.RecordingError` asserted by gate 1. |
| `README.md` | Documented per-document transaction and automatic re-processing | VERIFIED | Lines 9-12 document the per-document transaction, the rollback with discard-PDF deletion and the re-processing. |

### Key Link Verification

| From | To | Via | Status | Details |
| ---- | -- | --- | ------ | ------- |
| `doc_scanner` | `Querier.save_changes` | commit last in `try`, rollback in `except` | WIRED | 356 / 362; AST gate proves placement and uniqueness. |
| `doc_scanner` | `check_duplicate` / `save_warning` | shared querier argument | WIRED | 227, 245, 282, 318 — all four call sites pass `querier`; helpers use the `own_querier` idiom (440-441, 513-514, 524). |
| `doc_scanner except` | `DDTs/discarded/*.pdf` | `unlink(missing_ok=True)` restricted to this run | WIRED | 366-369 iterating `discarded_docs` filled at 336 only when `is_new_discard`. |
| `run() except` | `DDTs/<name>.pdf` | `shutil.move` restoring the original name, then `continue` | WIRED | 557-560, confirmed behaviourally by the run simulation. |

### Data-Flow Trace (Level 4)

| Artifact | Data | Source | Real data flows | Status |
| -------- | ---- | ------ | --------------- | ------ |
| `doc_scanner` transaction | pending INSERTs of every page | shared `querier` cursor | Yes — all writes observed on one connection, discarded by `save_changes(False)` and persisted by `save_changes(True)` | FLOWING |
| `discarded_docs` | list of `Path` | `discarded_doc_path()` + `is_new_discard` filter | Yes — non-empty in the discard simulations, drives the unlink loop | FLOWING |
| `run()` restore | `PATH_WORKING_DIR / doc` | loop variable `doc` (original basename) | Yes — file physically renamed in the simulation | FLOWING |

### Behavioural Spot-Checks

| Behaviour | Command | Result | Status |
| --------- | ------- | ------ | ------ |
| Package compiles | `.venv/Scripts/python.exe -m py_compile src/wheeck/recording_docs.py src/wheeck/__init__.py` | no output | PASS |
| Package imports without a DB connection | `python -c "import wheeck; print(wheeck.__version__)"` | `1.0.3` | PASS |
| Plan gate 1 (Task 1) | full AST/inspect one-liner from the PLAN | `GATE1 ok` | PASS |
| Plan gate 2 (Task 2) | full AST one-liner from the PLAN | `GATE2 ok` | PASS |
| Plan gate 3 (Task 3) | full AST + README one-liner from the PLAN | `GATE3 ok` | PASS |
| `doc_scanner` happy / failure / discard paths | in-process simulation with a fake `Querier` + fake `pypdfium2` (scratchpad, no DB, no project files) | 23/24 checks pass; the single FAIL is a faulty assertion in the harness itself (it asserted 1 row per page while the code correctly commits 9 rows over 3 pages — the printed detail `committed pages=[1,2,3] rows=9` confirms correct behaviour) | PASS |
| `run()` failure path | in-process simulation with `PATH_WORKING_DIR` redirected to a scratch dir | 9/9 (D1-D9) | PASS |
| `save_warning` standalone vs shared | fake `Querier` spot-check | standalone: autocommit connection, `77` / `-1`; shared: `RecordingError`, no new connection | PASS |

### Out-of-Scope Files

| File | Status | Evidence |
| ---- | ------ | -------- |
| `src/wheeck/constants.py` | UNMODIFIED | `git diff HEAD~3 HEAD -- src/wheeck/constants.py` empty |
| `src/main.py` | UNMODIFIED | empty diff |
| `src/wheeck/overview_docs.py` | UNMODIFIED | empty diff |
| `scheme/wheeck.sql` | UNMODIFIED | empty diff |
| `src/core` (symlink to `C:/source/core/src/core`) | UNMODIFIED | empty diff; `git diff --stat HEAD~3 HEAD` lists only `README.md`, `src/wheeck/__init__.py`, `src/wheeck/recording_docs.py` |

(`config/wheeck.json` shows as modified in the working tree but was already dirty before this task and is untouched by the three commits.)

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| ---- | ---- | ------- | -------- | ------ |
| — | — | no `TODO`/`FIXME`/`XXX`/`HACK`/`TBD`/`PLACEHOLDER` in the three modified files | — | none |

### Warnings (non-blocking, outside the declared truths)

1. **The pre-loop region is outside the transactional guard.** `pypdfium2.PdfDocument(working_doc)` and
   `len(doc)` (127-129) execute before `try:` (143). This is DB-safe — no statement has run, so nothing can be
   half-committed, and `run()` still restores the filename — but if `len(doc)` raised, `doc` would never be
   closed and the `shutil.move` in `run()` could fail with WinError 32 (caught, logged `critical`, file left as
   `<name>.recording.pdf` and thereafter skipped by `PATTERN_WORKING_DOC`). Cheap hardening: move
   `doc = pypdfium2.PdfDocument(...)` / `doc_pages = len(doc)` inside the `try`.
2. **Post-commit archiving in `run()` is unguarded.** `shutil.copy` to `backup_dir` and `shutil.move` to
   `PATH_RECORDED_DIR` (564-569) sit outside the `try`. An `OSError` there happens *after* the commit, so it
   cannot corrupt the database, but it aborts the whole batch and leaves a committed document as
   `<name>.recording.pdf`, which `PATTERN_WORKING_DOC` will never pick up again. Pre-existing behaviour, not a
   regression of this task, and correctly out of the CONTEXT boundary — recorded as a follow-up.
3. **Long-lived transaction.** With auto-commit off, the read queries (`QUERY_GET_DISTANCE`,
   `QUERY_GET_GAP`, `QUERY_CHECK_DUPLICATE`) now also run inside the document transaction, which stays open for
   the whole PDF — including the 1.5 s OpenRouteService sleeps. Correct by design, but a large document holds
   its transaction (and its locks) for minutes. Worth knowing, not a defect.
4. **A re-processed document that already has committed `delivery_discard` rows will hit
   `uq_delivery_discard_document_source_page` and roll back on every run.** This only occurs if a document is
   manually re-queued after a successful run; the amended CONTEXT already accepts "a deterministic failure is
   retried and logged on every run".

### Gaps Summary

No gaps. All eight must-have truths hold in the actual code, and the behaviours the user asked for are
observable: a runtime error anywhere in the page loop rolls the whole document back (zero rows in
`delivery`, `delivery_discard` and `delivery_warning`, including the early similarity `delivery_warning`
rows that previously escaped on their own auto-commit connection), a clean document commits exactly once
after the loop, earlier documents of the batch keep their commits, and a `PATTERN_*` discard stays a
discard instead of becoming a rollback. The SUMMARY's claims were checked against the code and the three
plan gates were re-run by the verifier — they hold, including the "no deviations" claim (the `-w` diff of
`doc_scanner` contains only the planned hunks; the extraction pipeline is byte-identical apart from
indentation).

Status is `human_needed` rather than `passed` only because two effects cannot be observed without a live
run: the real Postgres rollback (the verifier is read-only and the repo has no test suite) and the Windows
file-handle release before the rename. Both were proven at the logic level with in-process fakes.

---

_Verified: 2026-08-09T17:21:44Z_
_Verifier: Claude (gsd-verifier)_
