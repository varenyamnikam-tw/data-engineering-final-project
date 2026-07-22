# MH-CET 2026 Merit Predictor

A data engineering project that ingests Maharashtra CET 2025 branch-wise allotment PDFs, transforms them through a medallion architecture on Databricks, and serves a public-facing college predictor web app.

**Live App:** https://rank-rangers-2464733314746848.aws.databricksapps.com

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Data Source](#4-data-source)
5. [Pipeline Internals](#5-pipeline-internals)
   - [Bronze — PDF to CSV](#51-bronze--pdf-to-csv)
   - [Bronze — CSV to Delta Table](#52-bronze--csv-to-delta-table)
   - [Silver — Clean & Standardise](#53-silver--clean--standardise)
   - [Gold — Cutoff Aggregation](#54-gold--cutoff-aggregation)
   - [Data Quality Checks](#55-data-quality-checks)
   - [Security & Privacy — Column Masking](#56-security--privacy--column-masking)
6. [Data Dictionary](#6-data-dictionary)
   - [Bronze CSVs](#61-bronze-csvs)
   - [Silver Delta Table](#62-silver-delta-table--silverrankrangers_project_datamhcet_allotments)
   - [Gold Delta Tables](#63-gold-delta-tables)
7. [Seat Type & Category Reference](#7-seat-type--category-reference)
8. [Web App](#8-web-app)
9. [Databricks Setup](#9-databricks-setup)
10. [Running the Pipeline End-to-End](#10-running-the-pipeline-end-to-end)
11. [Orchestration (Databricks Jobs)](#11-orchestration-databricks-jobs)
12. [Known Limitations](#12-known-limitations)
13. [Team & Access](#13-team--access)

---

## 1. Project Overview

Every year thousands of MH-CET students struggle to predict which colleges they can realistically get into. This project:

- **Ingests** 1,480 branch-wise allotment PDFs from MahaCET (CAP Rounds I–IV, 2025)
- **Parses** each PDF into structured candidate-level rows
- **Transforms** through Bronze → Silver → Gold medallion layers on Databricks
- **Exposes** a Streamlit web app where students enter their score, category, and branch and see a ranked table of reachable colleges with cutoffs per CAP round and a predicted likely round

**Business questions answered:**
1. Given a student's category, CET score, and preferred branch — which colleges are within reach?
2. In which CAP round is the student likely to receive an allotment?

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCE                                                             │
│  MahaCET Portal — 1,480 PDFs (CAP-I to CAP-IV, 2025)                   │
│  https://fe2025.mahacet.org/StaticPages/frmInstituteWiseAllotmentList   │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ downloaded manually
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (A) — PDF to CSV, Unity Catalog Volume                     │
│  /Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/               │
│  ├── mahacet_cutoffs_2025/        ← raw PDFs (652 MB, 1480 files)       │
│  └── mahacet_cutoffs_2025_csv/    ← parsed CSVs (1 per PDF)             │
│      Notebook: pdf_to_csv.ipynb                                          │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ Auto Loader (bronze_streaming_ingest.ipynb)
                            │ or full-overwrite batch (bronze_delta_ingest.ipynb)
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (B) — CSV to Delta Table                                   │
│  rankrangers_project_data.bronze.mhcet_allotments_raw                   │
│  735,136 rows / 1,480 files, with provenance columns                    │
│  Notebook: bronze_streaming_ingest.ipynb (or bronze_delta_ingest.ipynb)  │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ spark.table(bronze.*_raw)
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER — Delta Table                                              │
│  rankrangers_project_data.silver.mhcet_allotments                       │
│  ~633K clean candidate rows                                              │
│  Notebook: bronze_to_silver_v2.ipynb                                     │
│  PII columns (candidate_name/application_id/gender) masked for          │
│  non-admins — security_masking.ipynb, one-time setup                    │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ Spark aggregation
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER — Delta Tables                                               │
│  rankrangers_project_data.gold.mhcet_cutoffs          ← main app table  │
│  rankrangers_project_data.gold.mhcet_cutoffs_by_pool  ← pool-level      │
│  Notebook: silver_to_gold_v2.ipynb                                       │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ dq_checks.ipynb (Bronze/Silver/Gold validation)
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  DQ LAYER — Delta Table                                                  │
│  rankrangers_project_data.dq.dq_metrics (append, one row per run/rule)  │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ Databricks SDK Statement Execution API
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  WEB APP — Databricks Apps (Streamlit)                                   │
│  https://rank-rangers-2464733314746848.aws.databricksapps.com           │
│  streamlit_app/app.py                                                    │
└─────────────────────────────────────────────────────────────────────────┘
```

See [§11 Orchestration](#11-orchestration-databricks-jobs) for how
Bronze→Silver→Gold→DQ is wired into a Databricks Job, and
`PIPELINE_ORDER.md` for the full run-order rationale.

**Infrastructure:**
- **Cloud:** AWS (Databricks on AWS)
- **Workspace:** `dbc-ba3cda01-8312.cloud.databricks.com`
- **Catalog:** `rankrangers_project_data` (Unity Catalog)
- **Cluster:** `Varenyam Nikam's Cluster` — 2 cores, 16 GB, Databricks Runtime
- **SQL Warehouse:** `Test SQL Warehouse` (Small, ID: `11ce2a291fc6dc25`) — auto-starts on query
- **App Hosting:** Databricks Apps (OAuth M2M, no PAT required)

---

## 3. Repository Structure

```
DE-CET/
├── pdf_to_csv.ipynb              # Bronze: PDF → CSV parser
├── bronze_delta_ingest.ipynb     # Bronze: CSV → Delta table (full overwrite)
├── bronze_streaming_ingest.ipynb # Bronze: CSV → Delta table (Auto Loader, incremental)
├── bronze_to_silver_v2.ipynb     # Silver: clean + standardise
├── silver_to_gold_v2.ipynb       # Gold: cutoff aggregation
├── dq_checks.ipynb               # DQ: input/transformation/output checks -> dq.dq_metrics
├── security_masking.ipynb        # One-time: UC column mask on Silver PII columns
├── jobs/                         # Databricks Job definitions (exported via CLI)
│   ├── phase6_mhcet_pipeline_sandbox.json
│   └── phase6_mhcet_pipeline_production.json
├── streamlit_app/
│   ├── app.py                    # Streamlit web app
│   ├── app.yaml                  # Databricks Apps deployment config
│   ├── requirements.txt          # Python dependencies
│   └── .streamlit/
│       └── config.toml           # Theme config (secrets.toml is gitignored)
├── PIPELINE_ORDER.md              # Run-order rationale: recurring chain vs. one-time steps
├── SETUP.md                      # Local/Colab/Kaggle setup guide (Databricks setup is now in this README, §9)
├── .gitignore
└── README.md                     ← this file
```

**Two Bronze ingest notebooks exist by design, not as leftover duplication:**
`bronze_delta_ingest.ipynb` does a full `overwrite` every run (simple, but
re-reads everything). `bronze_streaming_ingest.ipynb` replaces it with Auto
Loader — a checkpoint tracks which files were already ingested, so it
`append`s only newly-discovered files and is a near-zero-cost no-op when
nothing's changed. The Job definitions in `jobs/` use the streaming version.

> **Note:** `data/` (652 MB of PDFs) and `.venv/` are gitignored. PDFs live in the Databricks Unity Catalog Volume.

---

## 4. Data Source

| Field | Value |
|---|---|
| Source | Maharashtra State CET Cell |
| URL | https://fe2025.mahacet.org/StaticPages/frmInstituteWiseAllotmentList |
| Year | 2025–26 |
| Rounds | CAP-I, CAP-II, CAP-III, CAP-IV |
| Coverage | 500+ engineering colleges, all branches |
| Format | PDF (one file per college per round) |
| Total PDFs | 1,480 (368 + 370 + 370 + 372) |
| Total size | 652 MB |

Each PDF contains the provisional allotment list for one college across all its branches, broken down by seat type (General, Ladies, PWD, Defence, EWS, TFWS, AI, Minority).

---

## 5. Pipeline Internals

### 5.1 Bronze — PDF to CSV

**Notebook:** `pdf_to_csv.ipynb`  
**Input:** `/Volumes/.../mahacet_cutoffs_2025/{CAP-I,II,III,IV}/*.pdf`  
**Output:** `/Volumes/.../mahacet_cutoffs_2025_csv/{CAP-I,II,III,IV}/*.csv`

#### Why this was hard

The PDFs are scanned allotment lists. `pdfplumber` extracts them differently on different machines:
- **Local Mac:** one field per line (token-stream format)
- **Databricks cluster:** one complete row per line (full-row format)

The parser was written for the Databricks layout (confirmed via diagnostic extraction on the cluster).

#### How the parser works

```
PDF page text (pdfplumber)
        │
        ▼ split('\n')
Each line classified as:
   ├── Boilerplate     → skip (govt header, legends, page numbers)
   ├── Institute header → "01002 Govt College of Engg, Amravati"
   ├── Branch header   → "0100219110 - Civil Engineering"
   ├── Status line     → "Status: Government Autonomous ..."
   ├── Metadata line   → "Sanction Intake: 60 CAP Seats: 60 ..."
   └── Candidate row   → "1 5369 98.42 EN25154658 RITHE RIDDHIMA F OBC LOPENS"
                                │
                                ▼ parse_row_line()
                         Anchor on Application ID (EN25XXXXXXXX)
                         BEFORE anchor → sr_no, merit_no, score
                         AFTER anchor  → seat_type (from right)
                                       → gender (M/F, standalone token)
                                       → category (between gender & seat_type)
                                       → name (everything before gender)
```

#### Key design decisions
- **Application ID as anchor** — `EN25XXXXXXXX` is unique and reliable in every row
- **Parse right-to-left for seat_type and gender** — these always appear at the end
- **`str(pdf_path)` not `Path` object** — forces pdfplumber to open a fresh file handle per thread (thread safety)
- **4→2 threads** — pdfplumber is I/O bound; 2 threads on a 2-core machine is optimal
- **Resume mode** — skips PDFs that already have a CSV (idempotent reruns)

#### CAP-II/III/IV specific handling

Rounds II, III, IV add allotment-change markers before seat types:

| Marker | Meaning |
|---|---|
| `^` | Admitted to Institute — no change from previous round |
| `~` | No change in allotment |
| `*` | Betterment in Choice Code |
| `@` | Betterment in Seat Type |
| `&` | Newly allotted this round |

These are stored in the `seat_marker` column.

---

### 5.2 Bronze — CSV to Delta Table

**Notebooks:** `bronze_delta_ingest.ipynb` (full overwrite) or
`bronze_streaming_ingest.ipynb` (Auto Loader, incremental)  
**Input:** 1,480 CSVs (`pdf_to_csv.ipynb` output)  
**Output:** `rankrangers_project_data.bronze.mhcet_allotments_raw` (735,136 rows)

Formalizes Bronze as a real Delta table instead of Silver reading the raw
CSV glob fresh every run — gives a single frozen, schema-enforced,
versioned artifact between the parser and Silver, plus provenance columns
(`_source_file`, `_ingest_ts`, `_batch_id`).

`bronze_streaming_ingest.ipynb` is the preferred version for scheduled runs
(used in the Job — see [§11](#11-orchestration-databricks-jobs)): Auto
Loader's checkpoint means re-running when no new files exist costs almost
nothing (`new_rows == new_files == 0`), instead of re-reading and
re-writing all 735K rows like the full-overwrite version does.

---

### 5.3 Silver — Clean & Standardise

**Notebook:** `bronze_to_silver_v2.ipynb`  
**Input:** `bronze.mhcet_allotments_raw` Delta table  
**Output:** `rankrangers_project_data.silver.mhcet_allotments` (~633K rows)

#### What was wrong with the raw data

The PDF parser missed **Home University (H)** and **Other (O)** seat type pools entirely. These seat type codes got concatenated into the `candidate_category` field:

```
Raw CSV candidate_category: "OPEN ^ GOPENH"
Should be:  category="OPEN", marker="^", seat_type="GOPENH"

Raw CSV candidate_category: "NT 2 (NT-C)/PH1 ^ PWDRNT2S"
Should be:  category="NT2", seat_type="PWDRNT2S"
```

This was discovered by scanning all 735K raw rows before writing any transformation code.

#### Transformations applied

| Step | What |
|---|---|
| 1 | Remove VACANT rows and invalid application IDs |
| 2 | Extract seat_type + seat_marker from end of candidate_category using regex |
| 3 | Strip category suffixes: `$`, `#`, `@`, `/DEF1`, `/PH1`, `(NT-X)` |
| 4 | Standardise category to clean names (OPEN, OBC, SC, ST, NT1/2/3, VJ/DT, SBC, SEBC, EWS, PWD, DEFENCE) |
| 5 | Derive `seat_gender` from seat_type prefix (G→M, L→F) |
| 6 | Derive `seat_pool` from seat_type suffix (S→State, H→HomeUniv, O→Other, AI, MI, TFWS, EWS, PWD, Defence) |
| 7 | Cast numeric columns, derive `cap_round_num` (1–4) |
| 8 | Drop rows with null `branch_name` (3 women's colleges with non-standard PDF layout — 8,986 rows, 1.42%) |

#### Data quality after Silver

| Column | Nulls |
|---|---|
| mhtcet_score | 0 |
| seat_type | 0 |
| seat_gender | 0 |
| seat_pool | 0 |
| clean_category | 0 |
| institute_name | 0 |
| branch_name | 0 |
| cap_round_num | 0 |

---

### 5.4 Gold — Cutoff Aggregation

**Notebook:** `silver_to_gold_v2.ipynb`  
**Input:** `silver.mhcet_allotments`  
**Output:** `gold.mhcet_cutoffs`, `gold.mhcet_cutoffs_by_pool`

#### Cutoff definition

> **Cutoff = minimum MHT-CET score allotted** in that round for that college + branch + category + gender combination.

The minimum score represents the floor — the lowest score that actually received a seat. This is the most honest number to show a student.

#### Aggregation logic

```sql
GROUP BY institute_code, institute_name, branch_name, clean_category, seat_gender
AGG:
  MIN(score WHERE round=1) → cap1_cutoff   (most competitive)
  MIN(score WHERE round=2) → cap2_cutoff
  MIN(score WHERE round=3) → cap3_cutoff
  MIN(score WHERE round=4) → cap4_cutoff   (most accessible floor)
  COUNT(*) → total_seats_filled
```

Rows with no CAP-IV cutoff (seat never filled in final round) are excluded.

#### Likely round logic (computed at query time)

```python
if student_score >= cap1_cutoff: → "CAP-I"   (gets it in round 1)
elif student_score >= cap2_cutoff: → "CAP-II"
elif student_score >= cap3_cutoff: → "CAP-III"
elif student_score >= cap4_cutoff: → "CAP-IV"
else: → "Unlikely"
```

#### Excluded pools

`AllIndia`, `Minority`, `Orphan` are excluded from the main Gold table — these require different eligibility (JEE score, specific community membership) that the app doesn't handle.

---

### 5.5 Data Quality Checks

**Notebook:** `dq_checks.ipynb`  
**Input:** Bronze, Silver, Gold tables  
**Output:** `rankrangers_project_data.dq.dq_metrics` (append — one row per `run_id` + `rule_name`, trackable run-over-run)

Three pillars:
1. **Input Validation** — null/domain checks on Silver's key columns (same
   rules as Silver's own Cell 8, now persisted instead of just printed).
2. **Transformation Checks** — Bronze→Silver row-count reconciliation
   (flags if more data is silently dropped than the ~13.8% baseline).
3. **Output Verification** — Gold isn't empty, has no duplicate rows at its
   documented grain, and cutoff columns aren't universally null.

Each rule gets a `PASS`/`WARN`/`FAIL` status against a threshold — known,
already-documented gaps (e.g. the 3 excluded women's colleges, §12) are
tuned to `WARN`, not `FAIL`, so the checks flag genuine regressions rather
than re-flagging accepted limitations every run.

Run this after `bronze_delta_ingest.ipynb`/`bronze_streaming_ingest.ipynb`
→ `bronze_to_silver_v2.ipynb` → `silver_to_gold_v2.ipynb`.

---

### 5.6 Security & Privacy — Column Masking

**Notebook:** `security_masking.ipynb` (one-time setup, not part of the recurring chain)  
**Target:** `rankrangers_project_data.silver.mhcet_allotments`

Attaches a Unity Catalog mask function to `candidate_name`,
`application_id`, and `gender` — non-`admins` group members see `***`,
admins see the real value. Gold is pre-aggregated and never selects these
columns, so it's PII-free by construction and unaffected.

The mask is stored as Unity Catalog table metadata (not tied to the Delta
write), so it survives table rewrites and does **not** need to re-run every
pipeline execution — see `PIPELINE_ORDER.md` for when it does need
re-running (table dropped/recreated from scratch, or re-pointed at a new
catalog).

---

## 6. Data Dictionary

### 6.1 Bronze CSVs

One CSV per PDF. Located at `/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/mahacet_cutoffs_2025_csv/{CAP-I|II|III|IV}/CAPR-{round}_{institute_code}.csv`

| Column | Type | Description |
|---|---|---|
| `cap_round` | string | CAP-I / CAP-II / CAP-III / CAP-IV |
| `institute_code` | string | 5-digit institute code (e.g. `01002`) |
| `institute_name` | string | Full institute name |
| `branch_code` | string | 10-digit branch code |
| `branch_name` | string | e.g. `Computer Science and Engineering` |
| `is_ews` | bool | True if this is the EWS quota section |
| `is_tfws` | bool | True if this is the TFWS section |
| `institute_status` | string | Government / Un-Aided / Autonomous etc. |
| `home_university` | string | Affiliated university |
| `sanction_intake` | int | Total sanctioned seats |
| `cap_seats` | int | CAP quota seats |
| `ms_seats` | int | Maharashtra State seats |
| `minority_seats` | int | Minority quota seats |
| `ai_seats` | int | All-India quota seats |
| `sr_no` | int | Serial number within branch section |
| `merit_no` | int | State general merit number |
| `mhtcet_score` | float | MHT-CET percentile score |
| `application_id` | string | `EN25XXXXXXXX` |
| `candidate_name` | string | Full name |
| `gender` | string | M / F |
| `candidate_category` | string | Raw category string from PDF (may contain seat_type suffix in raw) |
| `seat_type` | string | Allotment code: GOPENS, LOBCS, GOPENH, PWDRNT2S etc. |
| `seat_marker` | string | `^` `~` `*` `@` `&` — CAP-II/III/IV change indicator |
| `is_vacant` | bool | True if seat was unfilled |
| `_validation_warnings` | string | Pipe-separated parser flags (empty = clean row) |

---

### 6.2 Silver Delta Table — `rankrangers_project_data.silver.mhcet_allotments`

All Bronze columns plus:

| Column | Type | Description |
|---|---|---|
| `clean_category` | string | Standardised: OPEN / OBC / SC / ST / NT1 / NT2 / NT3 / VJ/DT / SBC / SEBC / EWS / PWD / DEFENCE |
| `seat_gender` | string | M (G-prefix seats) / F (L-prefix seats) / ANY (AI, MI, TFWS, EWS) |
| `seat_pool` | string | State / HomeUniv / Other / AllIndia / Minority / TFWS / EWS / PWD / Defence / Orphan |
| `cap_round_num` | int | 1 / 2 / 3 / 4 |
| `_source_file` | string | Full path of source CSV |
| `_load_ts` | timestamp | When this row was loaded into Silver |

**Row count:** ~633,000  
**Excluded:** VACANT rows, invalid app IDs, null scores, null branch_name (3 women's colleges)

---

### 6.3 Gold Delta Tables

#### `rankrangers_project_data.gold.mhcet_cutoffs` — Main App Table

One row per `institute + branch + category + gender` combination.

| Column | Type | Description |
|---|---|---|
| `institute_code` | string | 5-digit code |
| `institute_name` | string | Full college name |
| `branch_name` | string | Branch name |
| `clean_category` | string | Standardised category |
| `seat_gender` | string | M / F / ANY |
| `is_ews` | bool | EWS quota flag |
| `is_tfws` | bool | TFWS quota flag |
| `cap1_cutoff` | float | Min score allotted in CAP-I |
| `cap2_cutoff` | float | Min score allotted in CAP-II |
| `cap3_cutoff` | float | Min score allotted in CAP-III |
| `cap4_cutoff` | float | Min score allotted in CAP-IV (primary floor) |
| `cap1_max` | float | Max score allotted in CAP-I |
| `cap2_max` | float | Max score allotted in CAP-II |
| `cap3_max` | float | Max score allotted in CAP-III |
| `cap4_max` | float | Max score allotted in CAP-IV |
| `total_seats_filled` | int | Total candidates allotted across all rounds |
| `cap1_seats` | int | Seats filled in CAP-I |
| `cap2_seats` | int | Seats filled in CAP-II |
| `cap3_seats` | int | Seats filled in CAP-III |
| `cap4_seats` | int | Seats filled in CAP-IV |
| `seat_pools_available` | array | Which pools contributed (State, HomeUniv, Other etc.) |

> Excludes AllIndia, Minority, Orphan pools.  
> Only rows with a valid `cap4_cutoff` are included.

#### `rankrangers_project_data.gold.mhcet_cutoffs_by_pool` — Pool-Level Table

Same as above but with an additional `seat_pool` column — one row per `institute + branch + category + gender + pool`.

---

## 7. Seat Type & Category Reference

### Seat Type Structure

Seat type codes follow the pattern: `[prefix][category][pool]`

| Prefix | Meaning |
|---|---|
| `G` | General (Male eligible) |
| `L` | Ladies only |
| `PWD` / `PWDR` | Persons with Disability (Reserved) |
| `DEF` / `DEFR` | Defence candidates |
| `AI` | All-India candidature (JEE score) |
| `MI` / `MI-MH` | Minority Maharashtra |

| Pool Suffix | Meaning |
|---|---|
| `S` | State Level seats |
| `H` | Home University seats |
| `O` | Other than Home University |

**Examples:**
- `GOPENS` = General + OPEN category + State level
- `LOBCH` = Ladies + OBC + Home University
- `GSCO` = General + SC + Other than Home Univ
- `PWDRNT2S` = PWD Reserved + NT2 + State
- `GOPENH` = General + OPEN + Home University

### Allotment Change Markers (CAP-II/III/IV only)

| Marker | Meaning |
|---|---|
| `^` | No change — admitted same college/branch as previous round |
| `~` | No change in allotment |
| `*` | Betterment in Choice Code |
| `@` | Betterment in Seat Type |
| `&` | Newly allotted this round |

### Category Codes

| Code | Full Name |
|---|---|
| OPEN | Open / General |
| OBC | Other Backward Class |
| SC | Scheduled Caste |
| ST | Scheduled Tribe |
| VJ/DT | Vimukta Jati / De-notified Tribes |
| NT1 | Nomadic Tribe A |
| NT2 | Nomadic Tribe B/C |
| NT3 | Nomadic Tribe D |
| SBC | Special Backward Class |
| SEBC | Socially and Educationally Backward Class |
| EWS | Economically Weaker Section |
| PWD | Persons with Disability |
| DEFENCE | Defence candidates |

---

## 8. Web App

**URL:** https://rank-rangers-2464733314746848.aws.databricksapps.com  
**Framework:** Streamlit  
**Hosting:** Databricks Apps  
**Auth:** OAuth M2M (service principal — no PAT required)

### How it works

```
Student fills form
  │ branch + category + gender + score
  ▼
app.py → WorkspaceClient() (auto OAuth via DATABRICKS_CLIENT_ID/SECRET)
  │
  ▼
Statement Execution API → SQL Warehouse (auto-starts if stopped)
  │
  ▼ SQL query on gold.mhcet_cutoffs
SELECT institute_name, cap1_cutoff, cap2_cutoff, cap3_cutoff, cap4_cutoff,
       CASE WHEN cap1_cutoff <= score THEN 'CAP-I' ... END AS likely_round
WHERE clean_category = ? AND seat_gender IN (?, 'ANY')
  AND branch_name = ? AND cap4_cutoff <= score
ORDER BY cap1_cutoff DESC
  │
  ▼
Results table with:
  College | CAP-I Cutoff | CAP-II | CAP-III | CAP-IV | Likely Round | Seats
```

### Deployment config (`app.yaml`)

```yaml
command:
  - "streamlit"
  - "run"
  - "app.py"
  - "--server.port=8000"          # Databricks Apps uses port 8000
  - "--server.address=0.0.0.0"
  - "--server.enableCORS=false"   # Required for Databricks Apps proxy
  - "--server.enableXsrfProtection=false"
  - "--server.headless=true"
```

### Environment variables (auto-injected by Databricks Apps)

| Variable | Value |
|---|---|
| `DATABRICKS_HOST` | `dbc-ba3cda01-8312.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | `56beaa57-5a57-4fc8-85f0-17d30c0a14c7` |
| `DATABRICKS_CLIENT_SECRET` | (auto-injected, never hardcoded) |
| `DATABRICKS_APP_PORT` | `8000` |

### Permissions required (Unity Catalog)

The app's service principal needs:
```sql
GRANT USE CATALOG ON CATALOG rankrangers_project_data TO `<client_id>`;
GRANT USE SCHEMA ON SCHEMA rankrangers_project_data.gold TO `<client_id>`;
GRANT SELECT ON TABLE rankrangers_project_data.gold.mhcet_cutoffs TO `<client_id>`;
```

---

## 9. Databricks Setup

### Workspace

| Setting | Value |
|---|---|
| Host | `dbc-ba3cda01-8312.cloud.databricks.com` |
| Workspace ID | `2464733314746848` |
| Cloud | AWS |
| Cluster ID | `0713-165757-6izemia7` (Single User mode — see §11 for why Silver/Gold/DQ tasks in the Job use serverless instead) |
| SQL Warehouse ID | `11ce2a291fc6dc25` |

### Unity Catalog paths

| Layer | Path |
|---|---|
| Raw PDFs | `/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/mahacet_cutoffs_2025/` |
| Bronze CSVs | `/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/mahacet_cutoffs_2025_csv/` |
| Logs | `/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/logs/` |
| Silver table | `rankrangers_project_data.silver.mhcet_allotments` |
| Gold table | `rankrangers_project_data.gold.mhcet_cutoffs` |
| Gold (by pool) | `rankrangers_project_data.gold.mhcet_cutoffs_by_pool` |

### Databricks notebooks (in workspace)

```
/Users/sajal.rajabhoj@thoughtworks.com/data-engineering-final-project/
├── pdf_to_csv
├── bronze_delta_ingest
├── bronze_streaming_ingest
├── bronze_to_silver_v2
├── silver_to_gold_v2
├── dq_checks
└── security_masking
```

> Also mirrored in `/Users/varenyam.nikam@thoughtworks.com/...` and various
> `phaseN_cli_test_scratch/` folders used during development — the path
> above is the canonical, up-to-date copy the Job definitions in `jobs/`
> point at.

---

## 10. Running the Pipeline End-to-End

> **Prerequisite:** PDFs must be uploaded to the Volume before starting.

### Step 1 — PDF to CSV (`pdf_to_csv.ipynb`)

1. Open `pdf_to_csv` notebook in Databricks
2. Run **Cell 1** — installs pdfplumber, detects environment
3. Skip **Cell 1b** — Databricks Apps setup cell (not needed for pipeline)
4. Run **Cell 2** — sets `PDF_ROOT` to Volume path (already configured)
5. Run **Cell 3** — loads parser engine
6. Run **Cell 4** — smoke test on 4 PDFs, verify output looks correct
7. Run **Cell 5** — full batch conversion (2 threads, ~35–40 mins on 2-core cluster)
8. Run **Cell 6** — conversion report (success/failure counts)
9. Run **Cell 7** — verify CSV folder structure
10. Run **Cell 8** — self-heal: retry any failed PDFs

**Expected output:** 1,480 CSVs across 4 CAP round folders.

### Step 2 — CSVs to Bronze Delta Table

Use **either** notebook (see [§5.2](#52-bronze--csv-to-delta-table) for
when to prefer which):

- `bronze_delta_ingest.ipynb` — full overwrite, simplest for a one-off run.
- `bronze_streaming_ingest.ipynb` — Auto Loader, incremental; the one used
  in the Job. First run ingests everything (expect 735,136 rows / 1,480
  files, checked by Cell 4); re-running with no new source files is a
  near-zero-cost no-op (`new_rows == new_files == 0`, checked by Cell 3).

**Expected output:** `bronze.mhcet_allotments_raw` with 735,136 rows.

### Step 3 — Bronze to Silver (`bronze_to_silver_v2.ipynb`)

1. Run all cells in order
2. Watch **Cell 4** — `Rows still missing seat_type` should be 0
3. Watch **Cell 8** — all key columns should show ✅ 0 nulls
4. **Cell 9** writes the Silver Delta table

**Expected output:** ~633K rows in `silver.mhcet_allotments`

### Step 4 — Silver to Gold (`silver_to_gold_v2.ipynb`)

1. Run all cells in order
2. **Cell 3** builds the main cutoff table
3. **Cell 5** builds the pool-level table
4. **Cell 6** saves dropdown metadata JSON
5. **Cell 7** runs a sample query — verify results look correct

**Expected output:** Gold tables with ~X rows (unique college+branch+category+gender combos)

### Step 5 — Data Quality Checks (`dq_checks.ipynb`)

1. Run all cells in order — appends one row per rule to `dq.dq_metrics`
2. Check the printed summary for any `FAIL` status before trusting Gold/the app

### Step 6 — Deploy App

1. Push code to GitHub (`main` and `master` branches)
2. In Databricks Apps → Redeploy (or Create new app pointing to repo)
3. Grant permissions to the app service principal (see Section 8)
4. Access the app URL

---

## 11. Orchestration (Databricks Jobs)

The recurring chain — everything after the manual PDF download —
is wired into a 4-task Databricks Job with `depends_on` chaining:

```
bronze_streaming_ingest -> bronze_to_silver_v2 -> silver_to_gold_v2 -> dq_checks
```

Job definitions (exported via `databricks jobs get`) live in `jobs/`:
- `jobs/phase6_mhcet_pipeline_sandbox.json` — points at the `sandbox` catalog copies, used to validate the DAG before touching production.
- `jobs/phase6_mhcet_pipeline_production.json` — points at the `rankrangers_project_data` catalog notebooks under `/Users/sajal.rajabhoj@thoughtworks.com/data-engineering-final-project/`.

**Compute split within the Job:** `bronze_streaming_ingest` runs on the
existing cluster (`existing_cluster_id`); `bronze_to_silver_v2`,
`silver_to_gold_v2`, and `dq_checks` run on **serverless** compute. Reason:
Silver has a Unity Catalog column mask (`security_masking.ipynb`), and row
filters/column masks are not enforced on assigned/Single User clusters —
Databricks blocks (rather than silently bypasses) the query with
`ROW_COLUMN_ACCESS_POLICIES_NOT_SUPPORTED_ON_ASSIGNED_CLUSTERS` if you try.
Serverless supports masked-table queries; the existing cluster does not.

**`pdf_to_csv.ipynb` is deliberately excluded from the Job** — it has no
checkpoint (re-parses everything every run for zero benefit on this static
dataset) and its input (manually downloaded PDFs) isn't something a Job
can refresh. It's a manual bootstrap step run once, outside the Job. See
`PIPELINE_ORDER.md` and `HANDOFF_PHASE5_6.md` for the full reasoning.

**Recreating/updating a Job from its JSON:**
```bash
databricks jobs reset --json '{"job_id": <id>, "new_settings": <contents of the jobs/*.json file, minus the top-level "settings" wrapper>}'
```

---

## 12. Known Limitations

| Limitation | Detail |
|---|---|
| 3 women's colleges excluded | Cummins College for Women, Siddheshwar Women's College, Bharati Vidyapeeth Women's — non-standard PDF layout, parser couldn't detect branch headers. 8,986 rows dropped (1.42%). |
| No city filter | PDFs don't contain city data directly — would need a separate institute master |
| No nearby branch suggestions | Strict branch match only — student must know exact branch name |
| 2025 data only | No historical years for trend analysis |
| AI/Minority seats excluded | Different eligibility criteria (JEE score, community membership) |
| SQL Warehouse cold start | First query after inactivity takes ~2–3 mins to start warehouse |
| PDF ingestion is manual | New CAP-round PDFs must be downloaded and dropped into the Volume by hand — everything from Bronze onward is scheduled/orchestrated (§11), but there's no automated fetch from the MahaCET portal itself |
| ~4.16% of Silver rows have null `seat_type` | Found by `dq_checks.ipynb`, not previously documented — likely a parser gap for some seat-type suffix pattern; root cause not yet identified |

---

## 13. Team & Access

| Person | Role | Databricks Email |
|---|---|---|
| Varenyam Nikam | Data Engineer (owner) | varenyam.nikam@thoughtworks.com |
| Sajal Rajabhoj | Team member | sajal.rajabhoj@thoughtworks.com |
| Devesh Mankar | Team member | Devesh.mankar@thoughtworks.com |
| Abhishek Sridhar | Team member | abhishek.sridhar@thoughtworks.com |

### Sharing CSV data with teammates

```python
# Paste in any Databricks notebook
csv_root = '/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/mahacet_cutoffs_2025_csv'
df = spark.read.option("header", True).csv(f'{csv_root}/**/*.csv')

# Or query Silver directly
df = spark.table('rankrangers_project_data.silver.mhcet_allotments')

# Or query Gold
df = spark.table('rankrangers_project_data.gold.mhcet_cutoffs')
```

To grant a teammate Volume read access (run as owner):
```sql
GRANT READ VOLUME ON VOLUME rankrangers_project_data.pdf.cet_raw_pdfs
TO `teammate@thoughtworks.com`;
```
