# Archive

Superseded notebooks, kept for historical reference only — **do not run these**.

| File | Superseded by | Why archived |
|---|---|---|
| `bronze_to_silver.ipynb` | `../bronze_to_silver_v2.ipynb` | v1 missed the Home University (`H`) and Other (`O`) seat-type suffixes, mis-parsing them into `candidate_category`. It also has a live typo bug — `CSV_ROOT` points at `/Volumes/rankrankers_project_data/...` (missing a `g`) instead of `rankrangers_project_data` — so it fails if run as-is. v2 fixes both. |
| `silver_to_gold.ipynb` | `../silver_to_gold_v2.ipynb` | Built on top of the flawed v1 Silver output above; superseded once v2 existed. |

The current pipeline (per `README.md` §10, "Running the Pipeline End-to-End")
only ever references the `_v2` notebooks — these were never part of the
maintained runbook.
