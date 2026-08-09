# Quick Task 260809-plo: Add transaction management with rollback on error - Context

**Gathered:** 2026-08-09
**Status:** Ready for planning

<domain>
## Task Boundary

Add transaction management to the recording pipeline so that if a runtime error occurs while
processing a flow, a rollback is performed and nothing is written to the database; if all pages
are processed correctly, a commit is performed. A runtime error mid-flow must not leave some
pages saved to the database and others not.

Scope is limited to `src/wheeck/recording_docs.py` (and any helper it calls). Reporting
(`overview_docs.py`) is read-only and out of scope.

</domain>

<decisions>
## Implementation Decisions

### Transaction scope — LOCKED
- **One transaction per document**, opened and closed inside `doc_scanner()`.
- Either all pages of a given PDF are persisted, or none of them are.
- Documents already processed in the same `run()` batch stay committed — a failure on document
  N does not undo documents 1..N-1.

### Rollback triggers — LOCKED
Rollback fires on:
1. Any unhandled runtime exception raised while scanning the document.
2. Failed write operations currently logged at `critical` level — i.e. `querier.run(...).rows != 1`
   on `QUERY_INSERT_DELIVERY`, `QUERY_INSERT_DISCARD`, `QUERY_INSERT_WARNING`.

Explicitly NOT a rollback trigger: a page discarded because a `PATTERN_*` regex failed. Discarding
is the designed, expected error path — it must keep writing `delivery_discard` + `delivery_warning`
rows and must still commit with the rest of the document.

### Filesystem side effects — LOCKED
DB rollback alone is not enough; the filesystem must be left re-processable:
- On rollback, delete the `DDTs/discarded/*.pdf` files generated for that document during the
  failed run (`discard_doc()` output).
- On rollback, do NOT move the working file to `DDTs/recorded/` and do NOT copy it to `backup_dir`.
  Instead restore its original name `<name>.pdf` in `DDTs/`, so `PATTERN_WORKING_DOC` matches it again
  and the document is automatically re-processed from scratch on the next run. (Amended 2026-08-09
  after the plan check: leaving it as `<name>.recording.pdf` would make `run()` skip it forever,
  honouring the letter but not the purpose of this decision. Accepted trade-off: a deterministic
  failure will be retried — and logged — on every run.)
- Files produced for *previous*, successfully committed documents in the same batch are untouched.

### Connection sharing — LOCKED (technical requirement, not a preference)
Today `save_warning()` and `check_duplicate()` each open their **own** `Querier`, i.e. their own
pyodbc connection. Two consequences that MUST be handled:
- `save_warning()` writes on a separate autocommit connection, so its `delivery_warning` rows would
  survive a rollback of `doc_scanner`'s transaction — breaking atomicity and orphaning the
  `id_warning_message` foreign key. It must accept an optional shared `Querier` and use it when
  called from within a document transaction.
- `check_duplicate()` on a separate connection cannot see rows inserted-but-not-yet-committed by
  the current transaction. With autocommit removed, duplicate detection *across pages of the same
  document* would silently stop working. It must reuse the transaction's `Querier`.
- `check_gaps()` runs in `run()`, outside any document transaction — it keeps its own connection,
  and `save_warning()` must still work when called without a shared querier (default behaviour
  preserved).

### Claude's Discretion
- Exact mechanism for the transactional boundary (`try/except/else` around the page loop, a context
  manager, or a helper function) — as long as `Querier.save_changes(True/False)` is the commit/rollback
  primitive and the connection is closed deterministically on every path.
- Whether the rollback path re-raises to the caller or returns a failure signal to `run()`, provided
  `run()` skips the recorded/backup move for the failed document and continues (or aborts) coherently.
- Signature style for threading the shared `Querier` through helpers.
- Logging wording and levels for commit/rollback events.

</decisions>

<specifics>
## Specific Ideas

The commit/rollback primitives already exist in the external `core` package — no new dependency is
needed:

- `core.Querier.__init__(..., save_changes: bool = False)` sets `pyodbc.connect(autocommit=save_changes)`.
  Passing `save_changes=False` (the default) is what opens an implicit transaction.
- `core.Querier.save_changes(save: bool = True)` calls `cursor.commit()` when `save` is truthy and
  `cursor.rollback()` otherwise.

Current offenders (both construct with `save_changes=True`, i.e. autocommit ON):
- `src/wheeck/recording_docs.py:107` — `doc_scanner`'s main querier.
- `src/wheeck/recording_docs.py:434` — `save_warning`'s own querier.

Related current behaviour worth preserving:
- `src/wheeck/recording_docs.py:325` — `del querier` teardown idiom (documented as an anti-pattern in
  `.planning/codebase/ARCHITECTURE.md`; deterministic cleanup on the exception path is now required).
- `src/main.py:7` — swallows every exception via a bare `except Exception`. If `doc_scanner` re-raises,
  the process still exits silently; note this but do not expand scope beyond what the rollback needs.

</specifics>

<canonical_refs>
## Canonical References

- `C:/source/core/src/core/querier.py` — external, unvendored `Querier` implementation (source of the
  `save_changes` semantics above). Read-only reference; do NOT modify it as part of this task.
- `.planning/codebase/ARCHITECTURE.md` — documents the per-call-site `Querier` lifecycle and the
  log-and-continue error strategy this change interacts with.
- `scheme/wheeck.sql` — FK relationships between `delivery`, `delivery_discard` and `delivery_warning`
  that constrain what may be partially committed.

</canonical_refs>
