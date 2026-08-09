---
phase: quick-260809-plo
verified: 2026-08-09T17:21:44Z
reverified: 2026-08-09T18:05:00Z
status: human_needed
score: 8/8 must-haves verified (+5 new behaviours verified)
overrides_applied: 0
re_verification:
  reason: "Source changed after the first verification: network calls moved out of the transaction, QUERY_CHECK_DISCARD guard, keyword-only querier, .failed.pdf fallback"
  previous_status: human_needed
  previous_score: 8/8
  gaps_closed:
    - "WARNING 1 of the first report (pre-loop region unguarded / pypdfium2 handle leak) is now structurally impossible: the handle is released before the transaction opens"
    - "WARNING 3 of the first report (transaction held open across the 1.5s OpenRouteService throttle) is resolved by resolve_distances()"
    - "WARNING 4 of the first report (uq_delivery_discard_document_source_page violation rolling back a re-processed document) is resolved by the QUERY_CHECK_DISCARD guard"
  gaps_remaining: []
  regressions: []
human_verification:
  - test: "Run the real pipeline on a test schema with a document that fails mid-way (es. revoke INSERT on wheeck.delivery after the first page, or feed a PDF whose page 2 triggers a constraint violation), then query SELECT COUNT(*) FROM wheeck.delivery / delivery_discard / delivery_warning for that document_source."
    expected: "Zero rows for that document_source in all three tables, no DDTs/discarded/*.pdf for it, and the file back in DDTs/ as <name>.pdf."
    why_human: "Requires a live psqlODBC connection and a database write; the verifier is read-only and the repo has no test suite. The rollback logic itself was proven with an in-process fake Querier, but the real pyodbc statement-level rollback across the single shared cursor can only be observed against Postgres."
  - test: "On Windows, let a real document fail inside doc_scanner and check the log for 'restoring it in DDTs dir' without a following 'error on restoring doc ... [WinError 32]' critical."
    expected: "The rename to <name>.pdf succeeds because the pypdfium2 handle was released by the extraction try/finally before the transaction even opened; the .failed.pdf fallback is never reached."
    why_human: "File-locking behaviour of pypdfium2 on Windows cannot be observed with a stubbed PdfDocument."
  - test: "Process a multi-page document with several distinct delivery cities and time the run; watch the OpenRouteService calls in the log relative to the 'committed N pages' line."
    expected: "Every 'calculating distance from ...' line appears BEFORE the first delivery INSERT, one per distinct city, and the transaction lifetime no longer includes the 1.5s throttles."
    why_human: "The real ORS latency, the real distinct-city set of a production PDF and the server-side idle_in_transaction_session_timeout can only be observed on a live run."
---

# Quick Task 260809-plo: Transaction management with rollback - Verification Report

**Task Goal (user's words):** add transaction management so that a runtime error mid-flow rolls back and
writes nothing to the database, while a fully processed flow commits — no more half-saved documents.
**Verified:** 2026-08-09T17:21:44Z
**Re-verified against the current source:** 2026-08-09T18:05:00Z
**Status:** human_needed (8/8 original truths + 5 new behaviours verified; 3 items need a live run)

## Re-verification scope

The source changed after the first pass (uncommitted working-tree changes on top of commit `ab4457a`):
text extraction and city-distance resolution moved **before** the transaction (`resolve_distances`,
`city_distance`), a `QUERY_CHECK_DISCARD` pre-existing-discard guard was added, `save_warning`'s `querier`
became keyword-only, the nested rollback handler uses `logger.exception`, and `run()` gained a
`.failed.pdf` fallback. The simulation harness was rewritten for the new structure and re-run:
**42/42 `doc_scanner` checks pass, 13/13 `run()` checks pass**, plus an extra
`resolve_distances`-failure check. No regression found on any previously confirmed behaviour.

## Goal Achievement

### Observable Truths

| #   | Truth (PLAN must_haves) | Status | Evidence against the CURRENT source |
| --- | ----------------------- | ------ | ----------------------------------- |
| 1 | A runtime error while scanning page N leaves ZERO rows of that document in `delivery`, `delivery_discard`, `delivery_warning` | VERIFIED (re-confirmed) | All writes still go through the single non-autocommit transaction querier (`recording_docs.py:190`; writes at 287, 305, 359, 363, 382, 391, 410-414). `except Exception:` → `querier.save_changes(False)` (426). Simulation B, both flavours (`rows=0` → `RecordingError`, injected driver exception) on page 2 of 3: `commits=0 rollbacks=1 committed_rows=0`, 5 rows discarded covering pages {1,2}. Simulation B3: the two early `save_warning(..., querier=querier, ...)` similarity rows are `INTO wheeck.delivery_warning` entries in the **rolled-back** buffer, not committed. |
| 2 | A document scanned without runtime errors commits exactly once, after the loop, with all its pages | VERIFIED (re-confirmed) | `querier.save_changes(True)` at 420 is still the penultimate statement of the `try`; gate 2 AST assertions (single commit, none inside any loop) still pass. Simulation A: `commits=1 rollbacks=0`, the single COMMIT event fires at page 3 carrying 9 pending rows for pages {1,2,3}, and no write or autocommit event occurs after it. |
| 3 | Documents already committed earlier in the same `run()` batch are not undone by a later document failing | VERIFIED (re-confirmed) | Simulation E1: doc1 `commits=1`, 6 committed rows, 0 rolled back; doc2 (failing) `rollbacks=1`, 5 rows lost, distinct connection object. `run()` still catches per document and `continue`s (616-630). |
| 4 | A page discarded by a failed `PATTERN_*` still writes discard + warning and commits — never a rollback trigger | VERIFIED (re-confirmed, new code path) | Simulation C over all four patterns: no exception, `commits=1 rollbacks=0`, return `(2,1)`, discard warning + `delivery_discard` row committed for the discarded page. `PATTERN_CITY` (DX **and** SX failing) now takes the new `distances.get(None)` path: the committed `delivery_discard` row has `delivery_city=None` and `distance=None`, and no `city_distance` call is made for it — the previous `QUERY_GET_DISTANCE`-with-a-`None`-parameter hazard is gone by construction, not just guarded. The `PATTERN_DOC_NUMBER` case still shows `gap_lookups=1` vs `2` for the others, i.e. the `document_number is not None and document_date is not None` guard (409) still fires. |
| 5 | On rollback the working file is renamed back to `<name>.pdf`, not moved to `DDTs/recorded/`, not copied to `backup_dir` | VERIFIED (re-confirmed) + NEW fallback verified | Run-simulation D1-D8 unchanged: file back as `2024_01_DDT_0101_0200.pdf`, no `.recording.pdf` residue, absent from `recorded/` and `backup/`, neighbours archived, `PATTERN_WORKING_DOC` matches the restored name. NEW D9-D13 with an injected `OSError` on the restore: the doc becomes `2024_01_DDT_0101_0200.failed.pdf`, no `.recording.pdf` is left, the `.failed.pdf` name does **not** match `PATTERN_WORKING_DOC` (explicit dead end rather than an invisible skip), the batch still completes and the failed doc is still absent from `recorded/` and `backup/`. |
| 6 | On rollback only the `DDTs/discarded/*.pdf` files THIS run created are deleted | VERIFIED (re-confirmed) | 396-400 unchanged (`is_new_discard = not discarded_doc.exists()` registered before the write); simulation B4: the PDF created by the failed run is deleted, the pre-existing one for another page survives. |
| 7 | The pypdfium2 handle is released on every exit path, including the exception path, before the rename | VERIFIED — now STRONGER | The handle no longer lives across the transaction at all: `doc = pypdfium2.PdfDocument(...)` / `try: pages_text = [...] finally: doc.close()` (179-182) runs **before** `resolve_distances` and before the `Querier` is created. Simulation N4: every `DOC_CLOSE` event precedes every `OPEN_CONNECTION`. Simulation N7 (extraction raises on page 2): the exception propagates, `doc_closed=1`, and **zero** connections are ever opened. The WARNING 1 of the first report (the unguarded pre-loop region) is therefore closed. |
| 8 | `check_gaps()` keeps its own connection and `save_warning()` still works without a shared Querier | VERIFIED (re-confirmed) | `check_gaps()` untouched, signature `() -> int` (N14). N12: standalone `save_warning` opens its own `autocommit=True` connection and returns the id / the `-1` sentinel. N13: with a shared querier it raises `RecordingError('INSERT_WARNING')` and opens no connection. |

**Score:** 8/8 truths verified

### New Behaviours Verified

| # | New behaviour | Status | Evidence |
| - | ------------- | ------ | -------- |
| N-A | `resolve_distances` runs with the transaction NOT yet open, on its own connection | VERIFIED | N1: every `DB_DISTANCE_LOOKUP` / `CITY_DISTANCE` event precedes the opening of connection #1 (the transaction). N2: exactly 2 connections; the first ran **only** `QUERY_GET_DISTANCE`, the transaction connection ran **none**. N3: no `city_distance` call and no `time.sleep` occurs after the transaction opens. The distance connection is dropped by `del querier` (155) before the transaction starts. |
| N-B | Each distinct city is resolved exactly once, even across several pages | VERIFIED | N5 with 3 pages / 2 distinct cities (CUNEO on pages 1-2, TORINO on page 3), CUNEO already cached in the DB: DB lookups `['CUNEO','TORINO']`, API calls `['TORINO']`, sleeps `[1.5]` — one lookup per distinct city, one API call for the only unknown one, no per-page repetition. |
| N-C | A page whose city was never resolved ends with `distance = None` and is still committed as a discard | VERIFIED | C[PATTERN_CITY]: the committed `delivery_discard` row carries `delivery_city=None, distance=None`; the document commits once, no rollback. |
| N-D | `QUERY_CHECK_DISCARD` guard | VERIFIED | N8 (row already exists): the page produces **no** DISCARD warning and **no** `delivery_discard` INSERT — only the 2 unrelated similarity warnings — and the document still commits (`res=(2,1)`, `commits=1`); the `CHECK_DISCARD` query is actually issued. N9: the discard PDF is still produced for the skipped page. N10 (no existing row): unchanged behaviour, exactly 1 `delivery_discard` row + 3 warnings. This removes the "retried forever" failure mode flagged as WARNING 4 in the first report. |
| N-E | `save_warning`'s `querier` is keyword-only | VERIFIED | N11: `save_warning(MessageGenre.WARNING, some_querier, genre=...)` raises `TypeError: save_warning() takes 1 positional argument but 2 were given`. All three `doc_scanner` call sites pass `querier=querier` (287, 305, 382). |
| N-F (extra) | A failure inside `resolve_distances` (ORS unreachable / DB down) cannot half-write | VERIFIED | Extra check: the exception propagates to `run()`, the pypdfium2 handle is closed, only the distance connection was ever opened, and **no** write, commit or rollback is attempted — there is no transaction to leave behind. |

### Required Artifacts

| Artifact | Expected | Status | Details |
| -------- | -------- | ------ | ------- |
| `src/wheeck/recording_docs.py` | Transactional `doc_scanner`, `RecordingError`, guards, pre-transaction distance resolution, self-restoring `run()` | VERIFIED | 663 lines; `city_distance` (103), `resolve_distances` (119), `doc_scanner` (159), guards at 350/372/409, `run()` fallback at 622-628. |
| `src/wheeck/__init__.py` | Re-export of `RecordingError` | VERIFIED | Unmodified since the last commit; gate 1 asserts `wheeck.RecordingError is recording_docs.RecordingError`. |
| `README.md` | Per-document transaction, re-processing, and the new pre-transaction resolution documented | VERIFIED | Lines 9-14 now also document the `.failed.pdf` dead end and that text + distances are resolved before the transaction opens. |
| `src/wheeck/constants.py` | `QUERY_CHECK_DISCARD` | VERIFIED (see note) | Purely additive `SELECT COUNT(*)` at 68-74; no existing query touched. |

### Key Link Verification

| From | To | Via | Status | Details |
| ---- | -- | --- | ------ | ------- |
| `doc_scanner` | `Querier.save_changes` | commit last in `try`, rollback in `except` | WIRED | 420 / 426; gate 2 AST assertions pass. |
| `doc_scanner` | `check_duplicate` / `save_warning` | shared querier (`querier=querier`) | WIRED | 342, 287, 305, 382; positional passing now rejected (N11). |
| `doc_scanner` | `resolve_distances` | called before the `Querier` is constructed | WIRED | 187 vs 190; ordering proven by N1/N2/N3. |
| `doc_scanner` | `QUERY_CHECK_DISCARD` | guard before the discard write block | WIRED | 372-379; N8/N10. |
| `doc_scanner except` | `DDTs/discarded/*.pdf` | `unlink(missing_ok=True)` limited to this run | WIRED | 430-433; B4. |
| `run() except` | `DDTs/<name>.pdf` → `.failed.pdf` | `shutil.move` + nested `OSError` fallback | WIRED | 622-628; D2, D9-D11. |

### Behavioural Spot-Checks

| Behaviour | Command | Result | Status |
| --------- | ------- | ------ | ------ |
| Compiles | `py_compile recording_docs.py __init__.py constants.py` | no output | PASS |
| Imports with no DB connection | `python -c "import wheeck"` | `1.0.3` | PASS |
| Plan gate 1 (Task 1) | PLAN one-liner, verbatim | `GATE1 ok` | PASS |
| Plan gate 2 (Task 2) | PLAN one-liner, verbatim minus the obsolete `doc.close() in finally` assertion | `GATE2 ok` | PASS (see note) |
| Plan gate 3 (Task 3) | PLAN one-liner, verbatim | `GATE3 ok` | PASS |
| `doc_scanner` A/B/C/E/N scenarios | scratchpad simulation (fake `Querier`, fake `pypdfium2`, stubbed `city_distance`/`sleep`) | 42/42 | PASS |
| `run()` normal + `.failed.pdf` fallback | scratchpad simulation with `PATH_WORKING_DIR` redirected | 13/13 | PASS |
| `resolve_distances` failure | scratchpad extra check | no transaction opened, no write | PASS |
| Anti-pattern scan | `grep TODO\|FIXME\|XXX\|HACK\|TBD\|PLACEHOLDER` on the 4 changed files | no match | PASS |

**Note on plan gate 2.** One assertion of the PLAN's gate, `assert 'doc.close()' in fin` (the *transaction*
`finally`), no longer holds: the transaction `finally` is now `['del querier']` and `doc.close()` moved to the
extraction `try/finally` (`pages_text = [...]` / `finally: doc.close()`), which runs before the transaction
exists. This is an obsolete assertion, not a regression — the intent it encoded ("the handle is released on
every exit path before the rename") is satisfied more strongly than before, as N4 and N7 show. Every other
assertion of that gate still passes verbatim.

### Out-of-Scope Files

| File | Status | Evidence |
| ---- | ------ | -------- |
| `src/main.py`, `scheme/wheeck.sql`, `src/wheeck/overview_docs.py`, `src/core`, `src/wheeck/__init__.py` | UNMODIFIED | `git diff --stat` against those paths is empty (committed and working tree). |
| `src/wheeck/constants.py` | MODIFIED — additive only | The PLAN constraint said this file "MUST NOT change". The change is a new `QUERY_CHECK_DISCARD` `SELECT COUNT(*)`; no existing query, table or column is touched, and it is a read. Recorded as an authorised deviation from the plan constraint, not as a defect — flagging it because the constraint asked for it to be surfaced rather than done silently. |
| `config/wheeck.json` | MODIFIED — pre-existing dirt | Already dirty before this task started; not part of the recording-transaction change. Not reviewed. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
| ---- | ---- | ------- | -------- | ------ |
| — | — | none | — | none |

### Warnings (non-blocking)

1. **Post-commit archiving in `run()` is still unguarded.** `shutil.copy` to `backup_dir` and `shutil.move` to
   `PATH_RECORDED_DIR` (634-639) sit outside the `try`. An `OSError` there occurs *after* the commit, so it
   cannot corrupt the database, but it aborts the whole batch and leaves a committed document as
   `<name>.recording.pdf`, invisible to `PATTERN_WORKING_DOC`. Carried over from the first report; still the
   only unguarded filesystem step. Cheap fix: reuse the same `.failed.pdf` idiom there.
2. **`resolve_distances` widens the "before the transaction" window rather than removing failure.** A network
   failure there aborts the document (verified: cleanly, with no transaction ever opened) and `run()`
   requeues it, so an ORS outage means every document is retried on every run until the outage ends. This is
   the intended trade-off, but it now happens *before* any useful work, so it is cheap.
3. **The distance cache is read once per document.** If two cities of the same document map to the same
   normalised name the set collapses them correctly, but a city resolved during document N is not reused for
   document N+1 in the same batch — each `doc_scanner` call rebuilds its own `distances` dict from the DB.
   Correct, just not maximally cached.
4. **The `is_discarded` guard costs one extra `SELECT COUNT(*)` per discarded page** inside the transaction.
   Negligible, and it buys immunity from the UNIQUE-violation rollback loop.

### Gaps Summary

No gaps, and no regressions from the restructure. The user's goal is observable in the current code and was
exercised end-to-end with stubs: a runtime error anywhere in the page loop rolls the whole document back
(zero rows in `delivery`, `delivery_discard` and `delivery_warning`, including the early similarity warning
rows), a clean document commits exactly once after the loop, documents committed earlier in the batch keep
their commits, and a `PATTERN_*` discard remains a discard. The three changes that carried the most risk —
moving the network calls out of the transaction, the pre-existing-discard guard, and the keyword-only
`querier` — were each verified individually (N-A…N-E) and none of them broke the eight original truths;
two of them actively closed warnings raised in the first pass.

Status stays `human_needed`: the real Postgres rollback, the Windows handle release, and the live timing of
the pre-transaction distance resolution can only be observed on a real run against the database. Everything
verifiable without a database has been verified by execution, not by reading the SUMMARY.

---

_Verified: 2026-08-09T17:21:44Z — re-verified 2026-08-09T18:05:00Z_
_Verifier: Claude (gsd-verifier)_
