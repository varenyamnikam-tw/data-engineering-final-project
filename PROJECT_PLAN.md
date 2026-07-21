# 🗺️ Final Project Plan — MH-CET Merit Predictor (v2: reconciled with real pipeline)

**⚠️ This replaces the earlier version of this plan.** The first draft assumed
a solo, from-scratch rebuild in a brand-new notebook. Since then, the team
(Varenyam Nikam + 5 members, including us) has shipped a **real, working,
deployed pipeline** — Bronze CSVs → Silver Delta → Gold Delta → a live
Streamlit app on Databricks Apps. That changes the job: we are no longer
building a parallel project, we're **auditing what exists, standardizing it,
and layering in the course topics it's still missing** — without breaking
what already works or duplicating it.

**How this works (unchanged):** one phase at a time, you test it on
Databricks, report back, then we move on.

---

## 1. What Changed & Why This Rewrite

| Old assumption | Reality |
|---|---|
| Solo project | Real 6-person team (`README.md` §12), owner Varenyam Nikam |
| Build fresh in a new notebook | `pdf_to_csv.ipynb` → `bronze_to_silver_v2.ipynb` → `silver_to_gold_v2.ipynb` → `streamlit_app/` already exist, deployed, and work |
| Placeholder catalog `workspace.mhcet_cet` | Real catalog: `rankrangers_project_data`, schemas `pdf` (raw volume), `silver`, `gold` |
| Bronze = new Delta table (Phase 2) | Bronze today = **CSVs on a UC Volume**, not a Delta table — a deliberate simplification, but a real medallion-architecture gap |
| Silver/Gold designed from scratch | Real Silver (`silver.mhcet_allotments`, ~633K rows) and Gold (`gold.mhcet_cutoffs`, `gold.mhcet_cutoffs_by_pool`) already built, with genuine data-quality fixes (seat-pool parsing) already done in v2 |
| No consumer app planned until later | A live Streamlit app already exists and **is the demo centerpiece** — must be preserved/enhanced, not replaced |

**Bottom line:** Bronze/Silver/Gold + a consumer app already exist and work.
What's still missing, purely by comparing against the course topic list, is:
a **true Bronze Delta layer** (schema/ACID/provenance), a **formal DQ
framework** (today it's print statements, not persisted metrics), **security/
governance (column masking)**, **streaming (Auto Loader)**, **orchestration
(Jobs/Workflows)**, and a bit of **repo hygiene**. Those become our phases.

---

## 2. As-Built Architecture (verified against the real notebooks + README)

```
MahaCET PDFs (1,480 files, 652MB)
        │  manually downloaded
        ▼
Volume: rankrangers_project_data.pdf.cet_raw_pdfs
  ├── mahacet_cutoffs_2025/        (raw PDFs)
  └── mahacet_cutoffs_2025_csv/    (1 CSV per PDF)  ◄── pdf_to_csv.ipynb
        │  spark.read.csv (glob, every run, no Delta table in between)
        ▼
Silver Delta: rankrangers_project_data.silver.mhcet_allotments (~633K rows)
        ◄── bronze_to_silver_v2.ipynb
        │  groupBy + agg
        ▼
Gold Delta: rankrangers_project_data.gold.mhcet_cutoffs
            rankrangers_project_data.gold.mhcet_cutoffs_by_pool
        ◄── silver_to_gold_v2.ipynb
        │  Databricks SDK Statement Execution API
        ▼
Streamlit app (Databricks Apps, OAuth M2M) — live demo
```

**Confirmed real PII columns that flow untouched from Bronze CSVs all the way
into Silver:** `candidate_name`, `application_id`, `gender` (README §6.1–6.2).
No masking/access control exists on them today — this is a genuine,
not-yet-covered course topic (Security & Privacy), not a hypothetical.

**Confirmed real gaps** (not opinions — verified by reading the notebooks):
- No Bronze **Delta table** — `bronze_to_silver_v2.ipynb` Cell 2 reads the CSV
  glob directly every run; there's no intermediate table with schema
  enforcement, ACID guarantees, or ingestion provenance columns.
- No persisted DQ metrics — Silver's Cell 8 "Data Quality Report" only
  `print()`s null counts; nothing is written to a table, so quality isn't
  measurable run-over-run.
- No column masking / UC governance grants beyond the app's own
  service-principal `SELECT` grant (README §8).
- No streaming/Auto Loader — ingestion is a one-shot batch notebook.
- No Databricks Job/Workflow — the three notebooks are run manually in
  sequence (README §10).
- No `gold_seat_movement`-style table — nothing currently tracks a single
  candidate's status across CAP rounds (window-function/lag() topic is
  unused so far).

---

## 3. Repo Hygiene Findings (fix regardless of which phase we do next)

| Issue | Where | Fix |
|---|---|---|
| `requirements.txt` lists `databricks-sql-connector` (unused — verified it isn't even a dependency of `databricks-sdk`) but `app.py` actually imports `databricks.sdk.WorkspaceClient` | `streamlit_app/requirements.txt` | Replace with `databricks-sdk`. It likely "works" today only because Databricks Apps' base image happens to preinstall the SDK — don't rely on that accident. |
| README's own "Repository Structure" (§3) and Step 1 instructions reference `SETUP.md`, but the file doesn't exist in the repo (renamed away, then never restored — verified via `git log`) | `README.md` §3 | Either restore a short `SETUP.md` (local/Colab/Kaggle setup, since the KT README doesn't cover local setup at all) or remove the dangling references. |
| `bronze_to_silver.ipynb` / `silver_to_gold.ipynb` (v1) are superseded by `_v2`, and v1 has a live bug (`CSV_ROOT` typo: `rankrankers_project_data` vs `rankrangers_project_data`) — README's own runbook (§10) never references v1 at all | repo root | ✅ **Done** — moved to `archive/` (with `archive/README.md` explaining why), not deleted, so history stays discoverable. |
| `data_engineering_final_project/` is an untouched `databricks bundle init` scaffold (sample taxi job, same workspace host as the real project but otherwise generic) | repo root | **Not yet decided** — unclear if it's already being used for something outside this repo's visible scope. Leaving untouched for now; revisit before Phase 6 (Orchestration) rather than assuming it's free to repurpose. |
| `.claude/settings.local.json` tracked in git | repo root | Personal tool config, shouldn't be shared — add to `.gitignore` and remove from tracking. |

These are small, low-risk, and don't require touching the owner's active
notebooks — good candidates to do first, in one short PR, to reduce noise
before the bigger topic phases below.

---

## 4. Coverage Matrix (updated against real state)

| Capability | Status | Plan |
|---|---|---|
| PDF → CSV ingestion | ✅ Done (`pdf_to_csv.ipynb`) | Keep as-is |
| Medallion Silver/Gold | ✅ Done (`_v2` notebooks) | Keep as-is, extend only |
| Consumer-facing app | ✅ Done (Streamlit on Databricks Apps) | Keep, enhance (viz phase) |
| Data-quality fixes (seat pools) | ✅ Done (v2's whole point) | Keep as-is |
| **Bronze as a Delta table** (schema, ACID, provenance) | ❌ Missing | Phase 2 |
| **Formal DQ framework** (persisted metrics, not prints) | ❌ Missing | Phase 3 |
| **Security & privacy** (UC column masking on PII) | ❌ Missing | Phase 4 |
| **Streaming / Auto Loader** | ❌ Missing | Phase 5 |
| **Orchestration** (Jobs/Workflows) | ❌ Missing | Phase 6 |
| **Visualization & accessibility** | Partial (app shows a results table, no charts) | Phase 7 (enhance the app, don't fork it) |
| **UC governance wrap-up** (grants, comments, lineage) | Partial (only the app's SP grant exists) | Phase 8 |
| Candidate cross-round movement (`lag()`/window functions) | ❌ Missing | Optional stretch (Section 9) |

Module 10 ("Exploring Databricks") and Module 13 ("Post-Course") remain out
of scope — non-technical/exploratory.

---

## 5. Team Coordination (new — this wasn't relevant when the plan assumed solo work)

Since this is a shared, actively-developed repo with an owner:

- **New work goes in new files** wherever possible (new notebooks, a new
  `.py` module, a new schema) rather than editing `bronze_to_silver_v2.ipynb`
  / `silver_to_gold_v2.ipynb` / `app.py` in place — minimizes merge conflicts
  with the owner's ongoing work.
- Where an existing file *must* change (e.g. `bronze_to_silver_v2.ipynb`'s
  Cell 2 source, once Bronze becomes a real Delta table; `app.py` for the
  viz enhancement; `requirements.txt`), keep the diff small and open it as
  its own PR so it's easy for Varenyam to review independently of everything
  else.
- Confirm with the team before deleting/archiving anything (v1 notebooks,
  the DAB scaffold) — flagged in Section 3, not yet actioned.

---

## 6. Naming Conventions (real, not placeholders)

- Catalog: `rankrangers_project_data`
- Existing schemas: `pdf` (Volume), `silver`, `gold`
- **New schemas we'll add:** `bronze` (real Delta table, Phase 2), `dq`
  (metrics table, Phase 3)
- New tables: `rankrangers_project_data.bronze.mhcet_allotments_raw`,
  `rankrangers_project_data.dq.dq_metrics`
- New files (additive, not replacing anything): `parser_utils.py` (Phase 2,
  needed for streaming's `foreachBatch`), `bronze_delta_ingest.ipynb`,
  `dq_checks.ipynb`, `security_masking.sql` (or notebook), a new streaming
  notebook, and a Databricks Asset Bundle for orchestration (reusing
  `data_engineering_final_project/` if confirmed free, otherwise new)

---

## Phase 1 — Repo Hygiene & Standardization
**Pseudocode:** skipped (housekeeping)

**What we do:** the fixes from Section 3 — fix `requirements.txt`, resolve
the dangling `SETUP.md` reference, propose archiving v1 notebooks, flag
`.claude/settings.local.json`. Low-risk, fast, no dependency on anything
else, and removes noise before the topic-focused phases below.

**Test on Databricks/locally:** redeploy the Streamlit app after the
`requirements.txt` fix and confirm it still starts cleanly (this is the only
item with runtime risk — the rest are doc/file-organization only).

---

## Phase 2 — True Bronze Layer (Delta table with schema + provenance)
**Modules:** 02 (Spark primer), 03 (Ingestion), 05 (Medallion: Bronze) · **Pseudocode:** included

**Concept primer — why CSVs-on-a-Volume isn't quite "Bronze":** the medallion
pattern's Bronze layer is specifically a **queryable, schema-enforced,
ACID table** that captures raw data plus provenance (where/when it came
from) — so you can always replay Silver/Gold from a known-good snapshot,
concurrent writers don't corrupt it, and you get Delta's time-travel for
free. Today, `bronze_to_silver_v2.ipynb` re-reads the raw CSV glob from
scratch every time it runs — that's a legitimate simplification the team
made deliberately, but it means there's no single frozen, versioned Bronze
artifact; if the CSV files on the Volume ever change or get partially
re-generated, Silver's next run silently sees different Bronze data with no
record of what changed.

**What we build:**
- `parser_utils.py` (Phase 1 idea from the old plan, still valid) is **not**
  required for this phase — Bronze Delta ingestion reads the *already-parsed*
  CSVs (no re-parsing, so the local-vs-Databricks `pdfplumber` layout
  discrepancy documented in the README doesn't apply here — this step is
  pure CSV→Delta, testable locally in our venv against the local `data/`
  folder before running on Databricks).
- A new notebook, `bronze_delta_ingest.ipynb`, that reads the CSV glob
  (same source `bronze_to_silver_v2.ipynb` uses today) and writes it as
  managed Delta table `rankrangers_project_data.bronze.mhcet_allotments_raw`,
  adding `_source_file`, `_ingest_ts`, `_batch_id` columns.
- A **one-line change** to `bronze_to_silver_v2.ipynb` Cell 2: swap
  `spark.read.csv(f'{CSV_ROOT}/**/*.csv')` for
  `spark.table('rankrangers_project_data.bronze.mhcet_allotments_raw')` —
  everything downstream (Cells 3–10) is column-compatible and untouched.

**Pseudocode (illustrative only):**
```python
df = (spark.read
      .option('header', True).option('inferSchema', True)
      .csv(f'{CSV_ROOT}/**/*.csv')
      .withColumn('_source_file', F.input_file_name())
      .withColumn('_ingest_ts', F.current_timestamp())
      .withColumn('_batch_id', F.lit(batch_id)))

df.write.format('delta').mode('append') \
  .saveAsTable('rankrangers_project_data.bronze.mhcet_allotments_raw')
```

**Test on Databricks:** row count of the new Bronze table matches the
735,136 raw rows the v2 Silver notebook's own markdown cites; re-point Silver
at it and confirm Silver's row counts (~633K) are unchanged.

**Status: ✅ implemented, verified locally, needs Databricks confirmation.**
- Built `bronze_delta_ingest.ipynb` and made the one-line source swap in
  `bronze_to_silver_v2.ipynb` Cells 2 & 4 (points at
  `rankrangers_project_data.bronze.mhcet_allotments_raw` now, keeps
  `CSV_ROOT` commented for rollback).
- **Verified locally end-to-end** against the real local CSV data
  (venv Spark 4.0.4 + Delta 4.0.1, using flattened 2-level table names since
  there's no real Unity Catalog locally): raw Bronze load = exactly
  **735,136 rows / 1,480 files** (matches the Silver notebook's own cited
  baseline exactly); Silver output = **633,641 rows**, with
  `branch_name` nulls at exactly **8,986 (1.42%)** — matches the README's
  documented "3 women's colleges excluded" limitation precisely. Re-running
  the overwrite twice produced no duplication (idempotent).
- **Found and fixed a real, pre-existing bug** while verifying: Silver
  Cell 8's DQ-report loop did `F.col(col) == ''` generically across
  `key_cols`, including numeric columns (`mhtcet_score`, `cap_round_num`).
  Comparing a DOUBLE/INT column to an empty string errors under **ANSI SQL
  mode** (`CAST_INVALID_INPUT`) — this is Spark 4.x's default and may or may
  not be Databricks' current default on DBR 17.3 (untested), but it's a
  latent portability bug regardless. Fixed by only applying the `== ''`
  check to string-typed columns (numeric columns just use `isNull()`,
  since they can't hold `''` anyway). Same output either way — this is a
  robustness fix, not a behavior change.
- **New finding, not in the README's Known Limitations table:** `seat_type`
  is null on **26,368 rows (4.16%)** of Silver — a real, currently-existing
  gap in the deployed table, surfaced by actually running the DQ loop
  end-to-end. Worth a first-class rule in Phase 3's `dq_metrics` table, and
  worth mentioning to the team since it's undocumented today.

---

## Phase 3 — Formal Data Quality Framework
**Modules:** 05 (Data-Quality submodule) · **Pseudocode:** skipped

**Concept primer:** Module 5.6's framework has three pillars — **Input
Validation** (nulls/domain values — this already exists informally in
Silver's Cell 8 print statements), **Transformation Checks** (row-count
reconciliation across layers — currently absent entirely), and **Output
Verification**. The goal isn't to replace Cell 8's checks, it's to make them
**persisted and measurable over time** instead of console output that
disappears after the run.

**What we build:**
- `dq_checks.ipynb`: a `dq_metrics` table
  (`rankrangers_project_data.dq.dq_metrics`) with one row per
  `(run_id, layer, rule_name, rows_checked, rows_failed, passed)`.
- Reuse the exact same null/range checks Silver's Cell 8 already runs
  (`mhtcet_score`, `seat_type`, `seat_gender`, `seat_pool`, `clean_category`,
  `institute_name`, `branch_name`, `cap_round_num`) — just also write them
  to the table instead of only printing.
- **New:** row-count reconciliation rules for Bronze→Silver and Silver→Gold
  (`rows_in`, `rows_out`, `rows_dropped`, with a documented expected-drop
  reason — e.g. Silver's known ~1.42% drop for the 3 women's colleges plus
  VACANT/invalid-ID/null-score rows).

**Test on Databricks:** run the full pipeline twice; `dq_metrics` accumulates
two runs' worth of rows with consistent pass/fail counts.

**Status: ✅ implemented, verified locally end-to-end, needs Databricks
confirmation.**

Built `dq_checks.ipynb` (13 cells): Input Validation (Silver's 8 null/domain
checks, persisted with per-column WARN/FAIL thresholds instead of just
printed), Transformation Checks (Bronze→Silver row-count reconciliation),
and Output Verification (Gold not-empty, unique at the documented grain
`institute_code, branch_name, clean_category, seat_gender, is_ews,
is_tfws`, cutoff columns not all-null). Writes to
`rankrangers_project_data.dq.dq_metrics` in **append** mode (unlike
Silver/Gold's `overwrite`), so quality is trackable run-over-run.

Ran the **full chain locally** (Bronze→Silver→Gold→DQ, chained in one
SparkSession against the real local CSV data) for the first time. All 12
rules logged, all `PASS`:

| layer | rule | checked | failed | pct | status |
|---|---|---|---|---|---|
| silver | not_null.mhtcet_score | 633,641 | 0 | 0.0% | PASS |
| silver | not_null.seat_type | 633,641 | 26,368 | 4.16% | PASS (WARN threshold 5%) |
| silver | not_null.seat_gender | 633,641 | 0 | 0.0% | PASS |
| silver | not_null.seat_pool | 633,641 | 0 | 0.0% | PASS |
| silver | not_null.clean_category | 633,641 | 0 | 0.0% | PASS |
| silver | not_null.institute_name | 633,641 | 0 | 0.0% | PASS |
| silver | not_null.branch_name | 633,641 | 8,986 | 1.42% | PASS (WARN threshold 2%) |
| silver | not_null.cap_round_num | 633,641 | 0 | 0.0% | PASS |
| bronze_to_silver | row_count_reconciliation | 735,136 | 101,495 | 13.81% | PASS (WARN 16%) |
| gold | not_empty | 1 | 0 | 0.0% | PASS |
| gold | unique_grain | 30,630 | 0 | 0.0% | PASS |
| gold | not_all_cutoffs_null | 30,630 | 0 | 0.0% | PASS |

`seat_type` (4.16%) is intentionally `PASS` not `WARN` here — its threshold
is calibrated to the *known, tracked baseline* discovered in Phase 2, so it
only trips `WARN`/`FAIL` if the gap gets meaningfully worse, not for simply
existing. The root cause (parser regex gap vs. genuinely absent in source
PDFs) is still unresolved — tracked as a first-class rule with its baseline
recorded in `notes`, flagged to the team, not silently accepted or hard-failed.

**New finding while testing this phase — real bug in
`silver_to_gold_v2.ipynb` Cell 12, not caused by this work:** the metadata
dropdown builder (`app_metadata.json` for the Streamlit app) does
`sorted([r[0] for r in df_g.select('branch_name').distinct().collect()])`.
Gold's `branch_name` inherits Silver's 8,986 null rows (the 3 excluded
women's colleges) as their own group after `groupBy`, and Python 3's
`sorted()` cannot compare `None` to `str` — so this cell throws
`TypeError: '<' not supported between instances of 'NoneType' and 'str'`
whenever it's run against real data (confirmed locally: 105 Gold rows have
a null `branch_name`). This is in a notebook we don't otherwise touch in
Phase 3, so it wasn't fixed here — routed around only in the local test
harness by filtering `None` before `sorted()`. **Needs a decision:** either
fix Cell 12 to filter nulls before `sorted()` (one-line change,
`if r[0] is not None`), or confirm whether it already failed silently on
Databricks and `app_metadata.json` was produced some other way. Flagging
for the team / a small separate fix, since editing a teammate's actively-used
notebook again in this PR risks conflating concerns.

---

## Phase 4 — Security & Privacy (UC column masking)
**Modules:** 08 (Security and Privacy), 11 (Governance) · **Pseudocode:** included

**Concept primer — column masking:** Unity Catalog lets you attach a mask
function to a column; every query against it runs the function first (e.g.
"if caller is in an authorized group, show the real value, else `***`").
This is a per-table/column protection — it doesn't retroactively protect the
same data sitting unmasked elsewhere.

**What we build (targets the real, confirmed PII columns):**
- `candidate_name`, `application_id`, `gender` all live on
  `silver.mhcet_allotments` (confirmed, README §6.1–6.2) — **not** on either
  Gold table, which is pre-aggregated and never selects these columns. Gold
  is PII-free by construction; masking targets Silver only.
- Note: the app's own filtering logic uses the **derived** `seat_gender`
  column (M/F/ANY, computed from `seat_type`), not the raw `gender` PII
  column — so masking `gender` doesn't touch anything the app depends on.

**Pseudocode:**
```sql
CREATE FUNCTION rankrangers_project_data.silver.mask_pii(val STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('admins') THEN val ELSE '***' END;

ALTER TABLE rankrangers_project_data.silver.mhcet_allotments
ALTER COLUMN candidate_name SET MASK rankrangers_project_data.silver.mask_pii;
ALTER TABLE rankrangers_project_data.silver.mhcet_allotments
ALTER COLUMN application_id SET MASK rankrangers_project_data.silver.mask_pii;
```

**Feasibility caveats (carried over, still valid):** `is_account_group_member()`
needs account-level groups (higher privilege than workspace admin — may not
be available on this workspace; fall back to workspace-local `is_member()`
if blocked). Column masking needs a modern DBR/serverless SQL — your DBR
17.3 LTS clears this comfortably.

**Test on Databricks:** query `silver.mhcet_allotments` as yourself vs. a
restricted group and confirm masked vs. unmasked behavior; confirm the app
still works (it queries Gold, which was never masked).

---

## Phase 5 — Streaming (simulated incremental ingestion)
**Modules:** 07 (Streaming) · **Pseudocode:** included

**Concept primer — Auto Loader / why not true event-time streaming:**
carried over from the original plan — Auto Loader incrementally processes
only new files via a checkpoint; our PDFs have no natural event-time column,
so this is scoped to incremental file ingestion, not watermarked streaming.

**What we build:** this now targets the **Phase 2 Bronze Delta table**
(`bronze.mhcet_allotments_raw`), replacing its batch CSV-glob read with
Auto Loader watching the CSV Volume path
(`/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/mahacet_cutoffs_2025_csv/`),
`trigger(availableNow=True)`, with a checkpoint.

**Test on Databricks:** drop one new CSV into the Volume, re-run, confirm
only the new file is picked up (checkpoint prevents reprocessing).

---

## Phase 6 — Orchestration
**Modules:** — (net-new, practically motivated; Module 09 is actually Kafka/
cloud messaging, not Jobs — see Section 4 of the old plan's reasoning, still
accurate) · **Pseudocode:** included

**Open question, confirm before starting:** whether `data_engineering_final_project/`
is free to repurpose is **unconfirmed** — unclear if it's already tied to
something outside this repo's visible scope, so we're not assuming it's
dead weight anymore. Check with the team first; if confirmed unused, reuse
it here instead of adding a second, redundant Databricks Asset Bundle. If
it's off-limits, create a new bundle from scratch instead.

**What we build:** a Databricks Job/Workflow wiring `pdf_to_csv` →
`bronze_delta_ingest` → `bronze_to_silver_v2` → `silver_to_gold_v2` →
`dq_checks`, each as a `depends_on` task.

**Test on Databricks:** trigger the Job manually; confirm the DAG runs
tasks in the correct order and the run's task graph in the UI matches.

---

## Phase 7 — Visualization & Accessibility (enhance the app, don't fork it)
**Modules:** 06 (Visualisation & Accessibility) · **Pseudocode:** skipped

**Why this belongs here, not a separate notebook:** the Streamlit app is the
demo centerpiece and already exists — building a separate charting notebook
would duplicate effort and give graders/viewers two disconnected places to
look. Instead we add 1–2 accessible charts **into `app.py`** alongside its
existing results table.

**What we build:** a small, additive diff to `app.py` — e.g. a bar chart of
CAP-I through CAP-IV cutoff scores for the colleges returned by a search,
using a colorblind-safe palette and labeled axes (not relying on color
alone to distinguish rounds).

**Test on Databricks:** redeploy the app, confirm the chart renders and is
readable, and confirm the existing table/search behavior is unchanged.

---

## Phase 8 — Governance Wrap-up
**Modules:** 11 (Governance) · **Pseudocode:** skipped

**What we build:** table/column `COMMENT`s across Bronze/Silver/Gold for
Catalog Explorer documentation; a read-only group `GRANT` (e.g. `SELECT` on
Gold only, not Silver/Bronze) beyond the existing app service-principal
grant; confirm the lineage graph (Bronze Delta → Silver → Gold) renders
correctly in Catalog Explorer now that Phase 2 gives it a real Bronze node
to show; final README update reflecting all of the above.

**Test on Databricks:** view the lineage graph; confirm grants restrict
access as expected.

---

## 9. Optional Stretch (not required, flag only if there's time)

- `gold_seat_movement`-style table using `lag()` partitioned by
  `application_id` alone (not institute/branch) to track a candidate's
  status across CAP rounds — the window-function/`lag()` topic isn't
  exercised anywhere in the current pipeline. Would need masking-aware
  design since it touches `application_id`.

---

## Next Step
Confirm this revised plan (or request edits), then decide: start with
**Phase 1** (fast, low-risk cleanup) or jump straight to a specific topic
gap (Phase 2 Bronze Delta is the most structurally important one).
