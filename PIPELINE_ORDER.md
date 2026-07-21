# Pipeline Run Order

## Recurring chain (run every time data refreshes)

```
1. pdf_to_csv.ipynb          PDFs (Volume)        -> CSVs (Volume)
2. bronze_delta_ingest.ipynb CSVs                 -> bronze.mhcet_allotments_raw
3. bronze_to_silver_v2.ipynb bronze.*_raw          -> silver.mhcet_allotments
4. silver_to_gold_v2.ipynb   silver.mhcet_allotments -> gold.mhcet_cutoffs, gold.mhcet_cutoffs_by_pool
5. dq_checks.ipynb           bronze/silver/gold    -> dq.dq_metrics (append)
```
Step 1 is manual/as-needed (new PDFs), 2-5 are re-run as a batch. This is
the chain Phase 6 (Orchestration) will wire into a Job.

## One-time setup step (not part of the recurring chain)

```
security_masking.ipynb   attaches a column mask to silver.mhcet_allotments
                          (candidate_name, application_id, gender)
```

**Run once, any time after step 3 has created `silver.mhcet_allotments` for
the first time.** Verified on the `sandbox` catalog: the mask survives a
full table rewrite (`CREATE OR REPLACE TABLE ... AS SELECT *`, which is a
stronger operation than `bronze_to_silver_v2.ipynb`'s own
`overwriteSchema=True` write) — masking is stored as Unity Catalog table
metadata, not tied to the Delta write. So it does **not** need to re-run on
every pipeline execution, and it doesn't sit "between" Silver and Gold in
the data-flow sense — Gold never selects the masked columns at all, so
Gold is unaffected regardless of when masking is applied.

Re-run it only if: the table is dropped and recreated from scratch (rather
than overwritten), or you're re-pointing it at a new catalog (e.g. sandbox
-> `rankrangers_project_data`).
