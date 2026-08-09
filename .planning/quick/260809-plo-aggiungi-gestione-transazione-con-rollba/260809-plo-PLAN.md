---
phase: quick-260809-plo
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/wheeck/recording_docs.py
  - src/wheeck/__init__.py
  - README.md
autonomous: true
requirements: [TX-01, TX-02, TX-03, TX-04]

must_haves:
  truths:
    - "A runtime error while scanning page N of a document leaves ZERO rows of that document in wheeck.delivery, wheeck.delivery_discard and wheeck.delivery_warning"
    - "A document scanned without runtime errors is committed once, at the end, with all its pages"
    - "Documents already committed earlier in the same run() batch are not undone by a later document failing"
    - "A page discarded by a failed PATTERN_* regex still writes delivery_discard + delivery_warning and still commits with the rest of the document — it is NEVER a rollback trigger"
    - "On rollback, only the DDTs/discarded/*.pdf files that THIS run created for that document are deleted"
    - "On rollback, the working file is renamed back to its original <name>.pdf in DDTs/ — not copied to backup_dir, not moved to DDTs/recorded/ — so PATTERN_WORKING_DOC matches it again and it is re-processed on the next run"
    - "The pypdfium2 handle on the working document is released on every exit path, including the exception path, before the rename"
    - "check_gaps() keeps its own connection and save_warning() still works when called without a shared Querier"
  artifacts:
    - path: "src/wheeck/recording_docs.py"
      provides: "RecordingError, None-safe DISCARD template, guarded gap lookup, transactional doc_scanner, shared-Querier check_duplicate/save_warning, self-restoring run() loop"
      contains: "class RecordingError"
    - path: "src/wheeck/__init__.py"
      provides: "Public re-export of RecordingError"
      contains: "RecordingError"
    - path: "README.md"
      provides: "Documented per-document transaction semantics and automatic re-processing"
      contains: "transaction"
  key_links:
    - from: "doc_scanner"
      to: "Querier.save_changes"
      via: "commit as the last statement of the try body, rollback inside the except handler"
      pattern: "save_changes\\((True|False)\\)"
    - from: "doc_scanner"
      to: "check_duplicate / save_warning"
      via: "shared querier passed as argument so uncommitted rows are visible and warnings roll back together"
      pattern: "(check_duplicate|save_warning)\\(.*querier"
    - from: "doc_scanner except handler"
      to: "DDTs/discarded/*.pdf"
      via: "unlink of every path collected in discarded_docs, restricted to files this run created"
      pattern: "unlink\\(missing_ok=True\\)"
    - from: "run() except handler"
      to: "DDTs/<name>.pdf"
      via: "shutil.move restoring the original filename, then continue"
      pattern: "PATH_WORKING_DIR / doc"
---

<objective>
Make the recording pipeline transactional at document granularity: either every page of a PDF is
persisted, or none of it is.

Purpose: today `doc_scanner()` runs with `autocommit=True`, so a runtime error on page 7 of a 20-page
document leaves pages 1-6 committed, the working file half-processed, and orphaned
`delivery_warning` rows written by `save_warning()` on its own connection.

Output: `doc_scanner()` opens one implicit transaction per document, commits on success, rolls back and
deletes the discard PDFs it created on failure, and re-raises so `run()` restores the original filename
and the document is re-processed automatically on the next run.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
</execution_context>

<context>
@.planning/quick/260809-plo-aggiungi-gestione-transazione-con-rollba/260809-plo-CONTEXT.md
@.planning/quick/260809-plo-aggiungi-gestione-transazione-con-rollba/260809-plo-RESEARCH.md
@.planning/codebase/CONVENTIONS.md
@src/wheeck/recording_docs.py
@src/wheeck/__init__.py
@README.md
</context>

<interfaces>
<!-- External, READ-ONLY contract from C:/source/core/src/core/querier.py. Do NOT modify that file. -->

```python
class Querier:
    FETCH_ONE: ...   # fetch a single row
    FETCH_VAL: ...   # fetch a single scalar value

    def __init__(self, cfg_in, save_changes: bool = False): ...
        # save_changes -> pyodbc.connect(autocommit=save_changes)
        # save_changes=False (the default) is what opens an implicit transaction

    def save_changes(self, save: bool = True) -> None: ...
        # cursor.commit() when save is truthy, cursor.rollback() otherwise

    def run(self, query: str, *params): ...   # returns self; .rows is the row count
    def fetch(self, mode): ...
    def row_header(self): ...
    def __del__(self): ...                    # closes the cursor and the connection; there is NO public close()
```

Facts already VERIFIED against the live database and the real schema — do not re-investigate, do not contradict:
- A failed INSERT raises `pyodbc.Error` (IntegrityError 23505 / 23514, ProgrammingError 42601). It does
  NOT come back as `rows != 1`. The exception handler is therefore the real rollback path.
- psqlODBC does statement-level rollback by default, so 25P02 "current transaction is aborted" does not
  bite on this DSN. Explicit `SAVEPOINT` / `ROLLBACK TO` is NOT warranted and MUST NOT be added.
- `Querier` holds a SINGLE cursor: running a query while a resultset is still being iterated silently
  destroys it. This is why `check_gaps()` must keep its own connection and must NOT be passed a shared one.
- Do NOT call `querier.cursor.connection.close()`; `Querier.__del__` handles teardown, and closing
  manually makes `__del__` raise `ProgrammingError: The cursor's connection has been closed`.
- `rollback()` with no active transaction is a no-op, and the cursor survives a rollback
  (`SQL_CB_PRESERVE`) — safe to call unconditionally in an error handler.
- Closing a connection without committing performs an implicit rollback, so a leaked `Querier` loses its
  work rather than half-committing it.
- `INSERT ... RETURNING id` (`QUERY_INSERT_WARNING`) behaves normally inside a transaction.
- `UPDATE` matching 0 rows returns `rows == 0` WITHOUT raising, so the `rows != 1` checks on
  `QUERY_UPDATE_WARNING` (current lines 291 and 320) are genuinely reachable and MUST stay
  "log critical and continue" — they are NOT rollback triggers.
- `wheeck.delivery_discard.document_number`, `document_genre` and `document_date` are all NULLABLE, and
  `chk_delivery_discard_quantity` is `CHECK (quantity > 0)` which passes on NULL. A page discarded because
  `PATTERN_DOC_NUMBER` failed therefore inserts cleanly once the two Python-side crashes in Task 1 / Task 2
  are fixed — there is no schema obstacle to committing it.
- `wheeck.vw_discard_message` parses the warning text back out and applies `NULLIF(..., 'None')` to
  `document_number`, `document_genre` and `document_date`. The schema was written expecting the `%s`
  string form of the template, which is what Task 1 restores.
</interfaces>

<constraints>
- Code, comments, docstrings and log messages in ENGLISH. 4-space indent. snake_case.
  Sphinx `:param:/:type:/:return:/:rtype:` docstrings. Lazy `%`-style logging (never f-strings).
  `# variable_name: description (es. value)` comments for new locals. Single-line `if x: y` compressed
  control flow where the codebase already uses it.
- `src/wheeck/constants.py` MUST NOT change. If the implementation concludes otherwise, STOP and report it
  rather than editing the SQL silently.
- `C:/source/core/src/core/querier.py` is external and READ-ONLY (`src/core` is a symlink to it).
  No `__enter__`/`__exit__` may be added there.
- OUT OF SCOPE, do NOT implement (note as follow-ups only):
  * `src/main.py`'s bare `except Exception`.
  * `src/wheeck/overview_docs.py` (read-only reporting).
- There is NO test suite and no test framework in this repo. Do NOT create one. Verification is static:
  `py_compile`, import check, AST gates, and reading the diff.
- Any command run against the real database MUST be strictly read-only. No writes to `wheeck.*`.
- **All `<verify>` commands must be run with the Bash tool, with the current working directory set to the
  repository root `C:/source/wheeck`** — they use relative paths in `open(...)` and
  `sys.path.insert(0, 'src')`. They contain no POSIX-only `VAR=value cmd` prefix, so they are portable.
</constraints>

<tasks>

<task type="auto">
  <name>Task 1: Add RecordingError, make the DISCARD template None-safe, and thread an optional shared Querier through check_duplicate and save_warning</name>
  <files>src/wheeck/recording_docs.py, src/wheeck/__init__.py</files>
  <action>
**1a. Make `MessageGenre.DISCARD` None-safe (line 90).** Change the conversion specifier
`%(document_number)d` to `%(document_number)s`. This is a correctness fix, not cosmetics: when
`PATTERN_DOC_NUMBER` fails, `delivery.document_number` stays `None` and the interpolation at line 300
(`message_genre.value % kwargs`) raises `TypeError: %d format: a real number is required, not NoneType`
BEFORE any INSERT runs. Without this fix the most common discard path would become a rollback trigger,
which directly contradicts the LOCKED decision that a regex-discarded page must still commit with the rest
of the document. Leave the remaining specifiers alone and state why in a comment above the enum: `%(page)d`
is always the `working_page` int; `%(doc)s`, `%(pattern)s`, `%(document_genre)s` and `%(document_date)s` are
already `%s` and render `None` harmlessly. Do NOT touch `MessageGenre.GAP` — its two `%d` fields come from
`vw_delivery_gap` rows and are never `None`.
Supporting evidence: `wheeck.vw_discard_message` (`scheme/wheeck.sql:185-201`) already applies
`NULLIF(..., 'None')::INTEGER` to `document_number`, i.e. the view was written for the `%s` form.

**1b. Add `RecordingError`.** Directly after the `MessageGenre` enum (before `doc_scanner`), define
`class RecordingError(RuntimeError)` with a Sphinx-style class docstring saying it is raised when a database
write does not affect exactly one row, in order to abort and roll back the whole document. This is what makes
the two locked rollback triggers (unhandled exception, and `rows != 1` on `QUERY_INSERT_DELIVERY` /
`QUERY_INSERT_DISCARD` / `QUERY_INSERT_WARNING`) converge on a single handler.

**1c. `check_duplicate`.** Change `check_duplicate(delivery: Delivery) -> bool` to
`check_duplicate(delivery: Delivery, querier: Querier = None) -> bool`. Add the `:param querier:` /
`:type querier: Querier` docstring entries describing it as the shared document-transaction querier,
defaulting to `None` to open a private one. Body pattern (matching the codebase's low-abstraction style),
with an `# own_querier: ...` comment: set `own_querier = querier is None`; when true build
`Querier(cfg_in=PATH_CFG)`; keep the existing query and `FETCH_VAL` logic unchanged; only `del querier` when
`own_querier` is true, then return.

**1d. `save_warning`.** Change the signature to
`save_warning(message_genre: MessageGenre, querier: Querier = None, **kwargs) -> int`. Placing `querier`
before `**kwargs` is safe: `querier` is not one of the `MessageGenre` template fields (`page`, `doc`,
`pattern`, `document_number`, `document_genre`, `document_date`, `genre`, `record`, `document_year`), so no
`*` separator is needed and every existing keyword call site keeps working. Add the matching
`:param querier:` / `:type querier: Querier` docstring entries. Body: same `own_querier` pattern, but the
private querier keeps `Querier(cfg_in=PATH_CFG, save_changes=True)` so standalone callers stay autocommit.
On `rows != 1`, keep the existing `logger.critical` line, then — with a comment explaining that inside a
document transaction the failure must roll back the whole document — `raise RecordingError('INSERT_WARNING')`
when `own_querier` is false, and keep the `id_message = -1` sentinel only on the standalone path. Update the
`:return:` docstring to state that `-1` is only returned when no shared querier was given. Only `del querier`
when `own_querier` is true.

**1e.** Do NOT touch `check_gaps()` — it must keep its own connection (single-cursor hazard: it iterates
`for row in querier:` while calling `save_warning` inside the loop) and it calls `save_warning` with keyword
arguments only, so it needs no edit at all.

**1f.** In `src/wheeck/__init__.py`, add `RecordingError` to the existing `from .recording_docs import ...`
re-export list, keeping the names alphabetically sorted as they are today.
  </action>
  <verify>
    <automated>.venv/Scripts/python.exe -c "import sys; sys.path.insert(0, 'src'); import ast, inspect, wheeck; from wheeck.recording_docs import MessageGenre, RecordingError, check_duplicate, check_gaps, save_warning; assert issubclass(RecordingError, RuntimeError), 'FAIL: RecordingError is not a RuntimeError'; assert wheeck.RecordingError is RecordingError, 'FAIL: RecordingError not re-exported'; assert '%(document_number)d' not in MessageGenre.DISCARD.value, 'FAIL: DISCARD template still uses the %d specifier for document_number'; assert '%(document_number)s' in MessageGenre.DISCARD.value, 'FAIL: DISCARD template lost its document_number field'; MessageGenre.DISCARD.value % {'page': 1, 'doc': 'x.pdf', 'pattern': 'PATTERN_DOC_NUMBER', 'document_number': None, 'document_genre': None, 'document_date': None}; assert MessageGenre.GAP.value.count('%') == 2, 'FAIL: GAP template must be left untouched'; p=inspect.signature(check_duplicate).parameters; assert 'querier' in p and p['querier'].default is None, 'FAIL: check_duplicate querier param'; q=inspect.signature(save_warning).parameters; assert list(q) == ['message_genre', 'querier', 'kwargs'], 'FAIL: save_warning signature or param order'; assert q['querier'].default is None, 'FAIL: save_warning querier default'; assert not inspect.signature(check_gaps).parameters, 'FAIL: check_gaps must keep its own connection'; g=ast.unparse(next(n for n in ast.parse(open('src/wheeck/recording_docs.py', encoding='utf-8').read()).body if isinstance(n, ast.FunctionDef) and n.name == 'save_warning')); assert 'RecordingError' in g, 'FAIL: save_warning does not raise inside a shared transaction'; assert 'id_message = -1' in g, 'FAIL: save_warning lost the -1 standalone sentinel'; print('ok')"</automated>
  </verify>
  <done>`MessageGenre.DISCARD` interpolates a `None` document number without raising; `RecordingError(RuntimeError)` exists and is re-exported from `wheeck`; `check_duplicate` and `save_warning` accept an optional `querier` defaulting to `None` and only tear down the connection they opened themselves; `save_warning` raises `RecordingError` on `rows != 1` when running inside a shared transaction and still returns `-1` standalone; `check_gaps()` and `MessageGenre.GAP` are unchanged.</done>
</task>

<task type="auto">
  <name>Task 2: Make doc_scanner one transaction per document, with a guarded gap lookup, rollback, and safe discard-PDF compensation</name>
  <files>src/wheeck/recording_docs.py</files>
  <action>
Keep the whole extraction pipeline logic byte-for-byte identical except where stated below — this is a
control-flow change, not a rewrite.

**2a. Guard the gap lookup (line 316) — mandatory, second half of the regex-discard fix.**
`querier.run(QUERY_GET_GAP, delivery.document_number, delivery.document_date.year)` raises
`AttributeError: 'NoneType' object has no attribute 'year'` on EVERY page where `PATTERN_DOC_NUMBER` failed.
Wrap the whole existing block (lines 316-322: the `QUERY_GET_GAP` lookup, the `fetch`, the log and the
`QUERY_UPDATE_WARNING` check) in an outer guard written EXACTLY as:
`if delivery.document_number is not None and delivery.document_date is not None:`
with a comment explaining that a page discarded on `PATTERN_DOC_NUMBER` has no document number to match a
gap against, and that a regex discard must never become a rollback trigger. Do not otherwise change the
inner block; in particular the `QUERY_UPDATE_WARNING` `rows != 1` check stays log-and-continue.

**2a2. Guard the distance lookup (line 270) — mandatory, third and last half of the regex-discard fix.**
Found by the second plan check; the root cause is an argument-collapse in the external `Querier`, not a
`None` dereference. When `PATTERN_CITY_DX` AND `PATTERN_CITY_SX` both fail (lines 153-175),
`delivery.delivery_city` stays `None` and `on_error = 'PATTERN_CITY'`. Line 265 short-circuits on
`on_error`, so control reaches `elif not delivery.distance:` at line 270, which is true (`distance` is
`None`), and line 273 calls `querier.run(QUERY_GET_DISTANCE, delivery.delivery_city)` with `None`.
In `core/querier.py:120-124` the guard is
`self._cursor.execute(query, *args) if args and any(val is not None for val in args) else self._cursor.execute(query)`:
with `args = (None,)` the tuple is truthy but `any(...)` is False, so `QUERY_GET_DISTANCE`
(`... WHERE delivery_city = ?`, `constants.py:68-72`) executes with NO parameters →
psqlODBC `07001 The # of binded parameters < the # of parameter markers` → `pyodbc.ProgrammingError`.
Under the new `except Exception:` that would roll back the whole document — exactly what the LOCKED
decision forbids for a regex discard. `core/querier.py` is read-only, so the fix goes here.

Change line 270 from `elif not delivery.distance:` to EXACTLY:
`elif delivery.delivery_city and not delivery.distance:`
with a comment noting this mirrors the guard `Delivery.calculate_distance()` already applies to itself at
lines 66-72. With the guard, `distance` stays `None` and the `delivery_discard` INSERT still passes
(`distance NUMERIC(7,2)` is nullable and `chk_delivery_discard_distance` does not fire on NULL).

**2b. Extract the discard path helper (enables 2f without duplicating path logic).**
Add a module-level `discarded_doc_path(working_doc: str | Path, working_page: int) -> Path` with a full
Sphinx docstring, containing the single expression currently inlined at line 350
(`PATH_DISCARDED_DIR / f"{working_doc.stem.removesuffix('.recording')}_P{working_page:0>3}.pdf"`), and make
`discard_doc()` call it instead of building the path itself. `discard_doc()` keeps its signature and its
`-> Path` return, so `src/wheeck/__init__.py` is unaffected.

**2c. Close `discard_doc`'s handles deterministically.** In `discard_doc()`, wrap the
`discard.import_pages(...)` / `discard.save(...)` body in `try: ... finally: discard.close(); doc.close()`
so a failure inside `save()` cannot leak the two pypdfium2 handles opened at lines 344/346 (there is no
try/finally there today). Residual gap accepted and recorded in the summary: a failure mid-`save()` can
still leave a partially written PDF on disk — it IS compensated, because 2f registers the path in
`discarded_docs` BEFORE `discard_doc()` is called and `unlink(missing_ok=True)` tolerates a file that was
never created.

**2d. Open the transaction.** Line 107: build the querier as `Querier(cfg_in=PATH_CFG)` — drop
`save_changes=True`. Comment:
`# save_changes=False keeps auto-commit OFF: an implicit transaction opens on the first executed statement`.

**2e. Track the compensation targets.** Before the page loop add
`# discarded_docs: PDFs written under DDTs/discarded by THIS run, to be removed on rollback (es. [c:/source/wheeck/DDTs/discarded/2024_01_DDT_0001_0100_P001.pdf])`
and `discarded_docs: list[Path] = []`.

**2f. Only ever delete discard PDFs this run created.** `discard_doc()` writes to a deterministic path and
`discard.save()` overwrites silently, so a re-processed document failing on a later page could otherwise
unlink a PDF left by an EARLIER, ALREADY-COMMITTED run and orphan its `delivery_discard` row. With the
automatic re-processing of Task 3 this scenario becomes materially more likely. Replace line 310 with:
compute `discarded_doc = discarded_doc_path(working_doc, working_page)`; capture
`# is_new_discard: True when no discard PDF for this page exists yet, so only files created by this run are removed on rollback`
`is_new_discard = not discarded_doc.exists()`; `if is_new_discard: discarded_docs.append(discarded_doc)`
(registered BEFORE writing, so a partial write is compensated too); then call
`discard_doc(working_doc, working_page)` and keep the existing `logger.info('saving discard doc ...')` and
`discarded_pages.restart()` unchanged. Accepted trade-off to note in the summary: a pre-existing discard PDF
that this run overwrote is NOT restored on rollback — the content is regenerated from the same source page,
so it is equivalent.

**2g. The transactional boundary.** Wrap the whole `for working_page, page in enumerate(doc, start=1):` loop
in `try:`, with exactly ONE `except Exception:` handler and one `finally:`.
- The last two statements of the `try` body must be, in this order, the commit and its log line — AFTER the
  loop and never inside it: `querier.save_changes(True)` followed by
  `logger.info('committed %d pages of %s! [%d discarded pages]', ...)`.
  Nothing else may follow them inside the `try`.
  Deliberate choice: the commit lives INSIDE the `try` (not in an `else`) so that a commit which itself fails
  also triggers the rollback attempt and the discard-PDF cleanup below.
- Return `doc_pages, discarded_pages.number` AFTER the whole `try/except/finally` construct.

**2h. The `except Exception:` handler** (broad on purpose — it must catch `pyodbc.Error`, `RecordingError`
and any unexpected `TypeError`/`AttributeError`):
- `logger.critical('rolling back the whole doc %s... nothing will be saved!', working_doc_name)`
- roll back inside its own nested `try: querier.save_changes(False)` /
  `except Exception: logger.critical('error on rollback of %s... check the database connection!', working_doc_name)`
  so a dead connection cannot replace the original exception.
- the filesystem is not transactional: iterate `discarded_docs`, log
  `logger.info('removing discard doc %s after rollback...', discarded.name)` and
  `discarded.unlink(missing_ok=True)` inside `try: ... except OSError as error:` logging
  `logger.critical('error on removing discard doc %s! [%s]', discarded.name, error)` — a locked file must not
  mask the original exception.
- end with a BARE `raise`. This is load-bearing: if the exception is swallowed, `doc_scanner` returns
  normally, `run()` treats the document as successful and copies it to `backup_dir` and moves it to
  `DDTs/recorded/` with the database already rolled back — silent data loss.

**2i. `finally:`** move the existing `doc.close()` here (it is currently at line 324, after the loop, and is
skipped on any exception — leaving a pypdfium2 handle on `<name>.recording.pdf` that would block the rename
performed by Task 3 on Windows), plus `del querier`. Do NOT close the connection explicitly.
Be honest in the comment about what `del querier` does and does not do: it drops `doc_scanner`'s reference,
but on the re-raise path the same object is still a live parameter in the `check_duplicate` /
`save_warning` frames retained by the traceback, so `Querier.__del__` — and therefore the actual connection
close — only fires once `run()`'s `except` block releases the exception. This is data-safe because the
explicit `save_changes(False)` has already run, and `core.Querier` exposes no public `close()`;
CONTEXT's "closed deterministically on every path" is satisfied in the sense that the ROLLBACK is
deterministic, not the socket teardown. Do not attempt to force it.

**2j. Share the querier with the helpers** so their writes join the transaction and duplicate detection can
see uncommitted rows of the same document:
- line 210 and line 228: `save_warning(MessageGenre.WARNING, querier, genre=..., record=..., page=..., doc=...)`
- line 265: `check_duplicate(delivery, querier)`
- line 300: `save_warning(MessageGenre.DISCARD, querier, page=..., doc=..., pattern=..., document_number=...,
  document_genre=..., document_date=...)`

**2k. Turn the two INSERT row-count checks into rollback triggers**, keeping the existing `logger.critical`
messages verbatim:
- lines 285-290 `QUERY_INSERT_DELIVERY`: after the `logger.critical`, `raise RecordingError('INSERT_DELIVERY')`.
  REMOVE the now-unreachable fallback `logger.error('discarding page ... INSERT_DELIVERY...')` +
  `discarded_pages.on_error = 'INSERT_DELIVERY'` — the locked decision replaces that discard path with a rollback.
- line 307 `QUERY_INSERT_DISCARD`: after the `logger.critical`, `raise RecordingError('INSERT_DISCARD')`.
  Keep the `delivery.id_warning_message != -1` guard as defensive dead code and say so in the comment:
  with a shared querier `save_warning` now RAISES instead of returning `-1`, and the `-1` assigned at line 269
  makes line 297's `if not delivery.id_warning_message:` false so line 307 is never reached with it. The guard
  costs nothing and keeps the standalone semantics of `save_warning` honoured if this block is ever reused.
- Leave BOTH `QUERY_UPDATE_WARNING` checks (lines 291 and 320) exactly as they are: `rows == 0` is a real,
  non-raising outcome there and stays "log critical and continue", NOT a rollback trigger.

Do NOT add `SAVEPOINT` / `ROLLBACK TO`, do NOT reorder any `querier.run(...)` / `querier.fetch(...)` pair (the
single shared cursor makes interleaving silently truncate resultsets), and do NOT change
`src/wheeck/constants.py`.
  </action>
  <verify>
    <automated>.venv/Scripts/python.exe -c "import sys; sys.path.insert(0, 'src'); import ast, wheeck; src=open('src/wheeck/recording_docs.py', encoding='utf-8').read(); mod=ast.parse(src); fn=next(n for n in mod.body if isinstance(n, ast.FunctionDef) and n.name == 'doc_scanner'); body=ast.unparse(fn); tries=[n for n in ast.walk(fn) if isinstance(n, ast.Try) and n.finalbody and n.handlers]; assert len(tries) == 1, 'FAIL: expected exactly one try/except/finally in doc_scanner'; tx=tries[0]; assert len(tx.handlers) == 1, 'FAIL: expected exactly one except handler'; loops=[n for n in ast.walk(tx) if isinstance(n, ast.For)]; assert loops, 'FAIL: the page loop must be inside the try'; assert all('save_changes' not in ast.unparse(l) for l in loops), 'FAIL: commit or rollback placed inside the page loop'; tail=' '.join(ast.unparse(s) for s in tx.body[-2:]); assert 'save_changes(True)' in tail, 'FAIL: commit is not the final statement of the try body'; assert body.count('save_changes(True)') == 1, 'FAIL: more than one commit'; h=' '.join(ast.unparse(s) for s in tx.handlers[0].body); assert 'save_changes(False)' in h, 'FAIL: rollback not inside the except handler'; assert 'unlink(missing_ok=True)' in h, 'FAIL: discard PDF cleanup not inside the except handler'; assert any(isinstance(n, ast.Raise) and n.exc is None for n in ast.walk(tx.handlers[0])), 'FAIL: except handler does not end with a bare raise'; fin=' '.join(ast.unparse(s) for s in tx.finalbody); assert 'doc.close()' in fin, 'FAIL: doc.close() not in finally'; assert 'del querier' in fin, 'FAIL: del querier not in finally'; assert 'save_changes=True' not in body, 'FAIL: autocommit still on in doc_scanner'; assert 'discarded_docs.append' in body, 'FAIL: discard PDFs not collected'; assert 'discarded_doc.exists()' in body, 'FAIL: discard PDF pre-existence not checked'; assert 'discarded_doc_path(working_doc, working_page)' in body, 'FAIL: doc_scanner does not use the shared discard path helper'; assert body.count('RecordingError') == 2, 'FAIL: expected exactly 2 RecordingError raises in doc_scanner'; assert 'check_duplicate(delivery, querier)' in body, 'FAIL: check_duplicate not sharing the querier'; assert body.count('save_warning(MessageGenre.WARNING, querier') == 2, 'FAIL: WARNING save_warning not sharing the querier'; assert 'save_warning(MessageGenre.DISCARD, querier' in body, 'FAIL: DISCARD save_warning not sharing the querier'; assert \"on_error = 'INSERT_DELIVERY'\" not in body, 'FAIL: unreachable INSERT_DELIVERY discard fallback still present'; guards=[n for n in ast.walk(fn) if isinstance(n, ast.If) and ast.unparse(n.test) == 'delivery.document_number is not None and delivery.document_date is not None']; assert guards, 'FAIL: missing None guard before the gap lookup'; assert 'QUERY_GET_GAP' in ast.unparse(guards[0]), 'FAIL: the None guard does not cover the QUERY_GET_GAP block'; dist=[n for n in ast.walk(fn) if isinstance(n, ast.If) and 'delivery.distance' in ast.unparse(n.test)]; assert dist, 'FAIL: distance lookup branch not found'; assert all('delivery.delivery_city' in ast.unparse(n.test) for n in dist), 'FAIL: missing delivery_city guard before the QUERY_GET_DISTANCE lookup'; assert any(isinstance(n, ast.FunctionDef) and n.name == 'discarded_doc_path' for n in mod.body), 'FAIL: discarded_doc_path helper missing'; dd=next(n for n in mod.body if isinstance(n, ast.FunctionDef) and n.name == 'discard_doc'); assert any(isinstance(n, ast.Try) and n.finalbody for n in ast.walk(dd)), 'FAIL: discard_doc must close both pypdfium2 handles in a finally'; assert 'SAVEPOINT' not in src.upper(), 'FAIL: SAVEPOINT must not be used'; assert 'connection.close' not in src, 'FAIL: the connection must not be closed explicitly'; print('ok')"</automated>
  </verify>
  <done>`doc_scanner` opens a non-autocommit `Querier`, commits exactly once as the last statement of the `try` body (never inside the page loop), rolls back + unlinks only the discard PDFs this run created + bare-`raise`s in a single `except Exception` handler, closes the pypdfium2 document and drops the querier in `finally`, shares the querier with `check_duplicate`/`save_warning`, raises `RecordingError` on the two INSERT row-count failures, guards the `QUERY_GET_GAP` block against a `None` document number/date, guards the `QUERY_GET_DISTANCE` branch against a `None` `delivery_city`, leaves both `QUERY_UPDATE_WARNING` checks as log-and-continue, `discard_doc` closes both handles in a `finally`, and `src/wheeck/constants.py` is unmodified.</done>
</task>

<task type="auto">
  <name>Task 3: Restore the original filename in run() on failure and document the transaction semantics</name>
  <files>src/wheeck/recording_docs.py, README.md</files>
  <action>
**3a.** In `run()`, wrap the `doc_scanner(working_doc, job_begin)` call (line 468) in
`try: worked_pages, discarded_pages = doc_scanner(working_doc, job_begin)` / `except Exception:`.

The handler must, in this order:
1. `logger.exception(...)` stating that the document was rolled back, nothing was saved, and the file is
   being restored for automatic re-processing. Using `logger.exception` here is a deliberate small extension
   of the convention that reserves it for `src/main.py`: this loop is now an error boundary.
2. Restore the original filename — this is the AMENDED locked filesystem decision (CONTEXT.md 2026-08-09):
   `shutil.move(working_doc, PATH_WORKING_DIR / doc)`, wrapped defensively in
   `try: ... except OSError as error:` logging
   `logger.critical('error on restoring doc %s for reprocessing! [%s]', doc, error)`. A failed rename must
   NOT abort the batch. `doc` (the original basename, es. `2024_01_DDT_0001_0100.pdf`) is already in scope in
   this loop, and the pypdfium2 handle was released by `doc_scanner`'s `finally: doc.close()`, so the rename
   is not blocked on Windows.
   Rationale to put in the comment: restoring `<name>.pdf` makes `PATTERN_WORKING_DOC` match the file again,
   so the next run re-processes it from scratch against an empty database state. Leaving it as
   `<name>.recording.pdf` would make `run()` skip it forever. Accepted trade-off, state it in the log
   message: a deterministic failure will be retried — and logged — on every run.
3. `continue`. This is what implements the rest of the locked decision: it skips BOTH the `backup_dir` copy
   and the `shutil.move` to `DDTs/recorded/`. Documents processed earlier in the batch stay committed and
   their files stay in `DDTs/recorded/` — do not add any batch-wide cleanup.

Everything after the loop (`check_gaps`, `generate_current`, the database backup) is unchanged and still runs.

**3b.** In `README.md`, extend the `recording_docs.py` bullet to document the new behaviour in English,
in one or two lines matching the file's terse bullet style: text extraction from PDFs with one database
transaction per document — all pages of a document are committed together, and any runtime error rolls the
document back, deletes the discard PDFs it created and restores the original filename in `DDTs/` so the
document is re-processed automatically on the next run.

**3c.** Record in the SUMMARY (NOT in the source) a `Follow-ups / accepted trade-offs` list:
(a) `src/main.py`'s bare `except Exception` still swallows anything escaping `run()`;
(b) a deterministically failing document is retried and logged on every run — accepted per the amended
CONTEXT, and the reason the `logger.exception` message must be self-explanatory;
(c) a rolled-back document loses its cached `QUERY_GET_DISTANCE` rows, so the OpenRouteService calls (and the
1.5 s sleeps) are repeated on reprocessing;
(d) rollbacks leave gaps in the `delivery.id` / `delivery_warning.id` identity sequences — harmless;
(e) a discard PDF that already existed before this run and was overwritten by it is not restored on rollback;
(f) the actual connection socket closes when `run()`'s `except` block releases the traceback, not at
`del querier` — the ROLLBACK itself is deterministic, the teardown is not.
  </action>
  <verify>
    <automated>.venv/Scripts/python.exe -c "import sys; sys.path.insert(0, 'src'); import ast, wheeck; fn=next(n for n in ast.parse(open('src/wheeck/recording_docs.py', encoding='utf-8').read()).body if isinstance(n, ast.FunctionDef) and n.name == 'run'); tries=[n for n in ast.walk(fn) if isinstance(n, ast.Try) and any('doc_scanner' in ast.unparse(s) for s in n.body)]; assert tries, 'FAIL: the doc_scanner call is not guarded'; t=tries[0]; guarded=' '.join(ast.unparse(s) for s in t.body); assert 'backup_dir' not in guarded, 'FAIL: the backup copy must stay outside the guarded call'; assert 'PATH_RECORDED_DIR' not in guarded, 'FAIL: the recorded move must stay outside the guarded call'; h=' '.join(ast.unparse(s) for s in t.handlers[0].body); assert 'logger.exception' in h, 'FAIL: the handler does not log the traceback'; assert 'continue' in h, 'FAIL: the handler does not continue to the next document'; assert 'shutil.move' in h, 'FAIL: the handler does not restore the original filename'; assert 'PATH_WORKING_DIR / doc' in h, 'FAIL: the handler does not restore into DDTs with the original name'; assert any(isinstance(n, ast.Try) and n.handlers for n in ast.walk(t.handlers[0])), 'FAIL: the restore rename is not defensive'; r=open('README.md', encoding='utf-8').read().lower(); assert 'transaction' in r, 'FAIL: README does not document the per-document transaction'; assert 'roll' in r, 'FAIL: README does not document the rollback'; assert 're-processed' in r or 'reprocessed' in r, 'FAIL: README does not document the automatic re-processing'; print('ok')"</automated>
  </verify>
  <done>`run()` catches any exception from `doc_scanner`, logs it with the traceback, renames the working file back to its original `<name>.pdf` in `DDTs/` inside its own defensive `try/except OSError`, and `continue`s so the failed document is neither copied to `backup_dir` nor moved to `DDTs/recorded/`; `check_gaps`, `generate_current` and the schema backup after the loop are unchanged; `README.md` documents the per-document transaction, the rollback and the automatic re-processing.</done>
</task>

</tasks>

<verification>
Static gates only — this repo has no test suite and none is to be created.
**Run every command below with the Bash tool, from the repository root `C:/source/wheeck`.**

1. `.venv/Scripts/python.exe -m py_compile src/wheeck/recording_docs.py src/wheeck/__init__.py`
2. `.venv/Scripts/python.exe -c "import sys; sys.path.insert(0, 'src'); import wheeck; print(wheeck.__version__)"`
   — the package imports cleanly (no database connection is opened at import time).
3. `git diff -- src/wheeck/constants.py` is EMPTY.
4. `git diff -- src/core src/main.py scheme/wheeck.sql` is EMPTY (external read-only symlink, out-of-scope
   entry point, and the schema which this task does not migrate).
5. Read the diff of `doc_scanner` and confirm the extraction pipeline itself is unchanged: the only edits are
   the querier construction, the `try/except/finally` wrapper, the `None` guard around the gap lookup, the
   four helper call sites gaining `querier`, the two `raise RecordingError(...)`, the removal of the
   unreachable `INSERT_DELIVERY` discard fallback, and the `discarded_doc_path` / `is_new_discard` /
   `discarded_docs.append` block.
6. Reason through the three paths explicitly and record the conclusion in the summary:
   - happy path: N pages -> one `save_changes(True)` -> `run()` copies to backup and moves to `DDTs/recorded/`;
   - discard path (regex failure, including `PATTERN_DOC_NUMBER`): the `%s` template interpolates a `None`
     document number, the gap lookup is skipped by the `None` guard, `delivery_warning` +
     `delivery_discard` are written (all three columns are nullable), the discard PDF is produced, the loop
     continues, and the page is COMMITTED with the rest of the document — no rollback;
   - failure path: exception -> rollback -> only the discard PDFs created by this run are unlinked ->
     bare re-raise -> `run()` logs, renames `<name>.recording.pdf` back to `<name>.pdf` in `DDTs/` and
     `continue`s -> earlier documents of the batch untouched, failed document re-processed next run.
</verification>

<success_criteria>
- `doc_scanner()` runs with auto-commit OFF and issues exactly one `save_changes(True)` per successful
  document, placed after the page loop and never inside it.
- Any exception inside the page loop (including `pyodbc.Error`, `RecordingError`, `TypeError`,
  `AttributeError`) produces `save_changes(False)`, deletes only the discard PDFs this run created, and
  bare-re-raises.
- A page discarded because `PATTERN_DOC_NUMBER` (or any other `PATTERN_*`) failed does NOT raise anywhere and
  is committed with the rest of the document.
- `check_duplicate` and `save_warning` operate on the shared transaction querier when called from
  `doc_scanner`, and keep their standalone behaviour (including the `-1` sentinel) when called without one.
- `check_gaps()` still opens its own connection.
- `doc.close()` runs on every exit path, and `discard_doc()` closes both handles in a `finally`.
- `run()` restores `<name>.pdf` in `DDTs/`, skips the backup copy and the move to `DDTs/recorded/` for a
  failed document, and keeps processing the remaining ones.
- No changes to `src/wheeck/constants.py`, `src/core/**`, `src/main.py` or `scheme/wheeck.sql`.
- All static gates in `<verification>` pass.
</success_criteria>

<output>
Create `.planning/quick/260809-plo-aggiungi-gestione-transazione-con-rollba/260809-plo-SUMMARY.md` when done,
including the "Follow-ups / accepted trade-offs" list from Task 3c and the three-path reasoning from
`<verification>` point 6.
</output>
