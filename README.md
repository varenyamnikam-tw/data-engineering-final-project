# 🛠️ Setup Guide — MH-CET PDF → CSV Pipeline

> **Data size reality check:** Your PDF folder is **652 MB** (1,480 PDFs), not 100 GB.  
> This changes storage options significantly — see the [Storage section](#-where-to-store-the-data) below.

---

## 📁 Project Structure

```
DE-CET/
├── pdf_to_csv.ipynb                  ← The notebook (run this)
├── SETUP.md                          ← This file
└── data/
    ├── mahacet_cutoffs_2025/         ← Source PDFs (652 MB)
    │   ├── CAP-I/    (368 PDFs, ~153 MB)
    │   ├── CAP-II/   (370 PDFs, ~168 MB)
    │   ├── CAP-III/  (370 PDFs, ~165 MB)
    │   └── CAP-IV/   (372 PDFs, ~167 MB)
    ├── mahacet_cutoffs_2025_csv/     ← Output CSVs (auto-created, mirrors above)
    │   ├── CAP-I/
    │   ├── CAP-II/
    │   ├── CAP-III/
    │   └── CAP-IV/
    └── logs/                         ← Conversion logs (auto-created)
        ├── conversion_<ts>.jsonl
        └── failures_<ts>.txt
```

---

## 💾 Where to Store the Data

Your total PDF dataset is **652 MB**. Here's where to store it depending on your platform:

| Platform | Recommended Storage | Why |
|---|---|---|
| **Local** | Local disk (current) | 652 MB is nothing — just keep it local |
| **Databricks (Azure)** | Azure Data Lake Storage Gen2 (ADLS) | Native integration, cheapest at this size |
| **Databricks (AWS)** | AWS S3 | Native integration, ~$0.01/month at 652 MB |
| **Databricks (GCP)** | Google Cloud Storage (GCS) | Native integration |
| **Colab** | Google Drive (free 15 GB tier) | Easy mount, free |
| **Kaggle** | Kaggle Dataset | Free, 100 GB limit, public or private |
| **GitHub** | Git LFS | Free up to 1 GB — fits easily |

### 💡 Recommendation
- **For Databricks:** Upload to **ADLS / S3 / GCS** (whichever your Databricks workspace runs on) and mount it. The notebook has pre-written mount code for all three.
- **For Colab/Kaggle:** Zip the folder and upload — 652 MB uploads in ~2 minutes.
- **For version control:** Use **Git LFS** — `git lfs track "*.pdf"` and push. GitHub free tier handles it.

### ⚠️ What NOT to do
- Don't commit raw PDFs without LFS — git will reject files over 100 MB
- Don't use DBFS root (`dbfs:/`) for persistent storage on Databricks — it gets wiped on cluster termination; use a mounted external store instead

---

## 🖥️ Option 1 — Local (macOS / Linux)

### Step 1 — Create and activate a virtual environment

```bash
# Navigate to the project root
cd /Users/varenyamnikam/Documents/data-engineering/DE-CET

# Create venv
python3 -m venv .venv

# Activate it
source .venv/bin/activate

# Confirm you're in the venv
which python   # → .../DE-CET/.venv/bin/python
```

### Step 2 — Install dependencies

```bash
pip install --upgrade pip
pip install pdfplumber pandas tqdm jupyter notebook
```

> `pdftotext` (poppler) is already installed via Homebrew on this machine and acts as an automatic fallback. To install on a fresh machine:
> ```bash
> brew install poppler            # macOS
> sudo apt-get install poppler-utils  # Ubuntu/Debian
> ```

### Step 3 — Launch Jupyter

```bash
jupyter notebook pdf_to_csv.ipynb
# OR
jupyter lab pdf_to_csv.ipynb
```

### Step 4 — Run cells in order

| Cell | What it does |
|------|-------------|
| **Cell 1** | Auto-detects environment, installs packages |
| **Cell 1b** | Databricks setup — **skip this on local** |
| **Cell 2** | Set paths (pre-filled for local) |
| **Cell 3** | Loads parser engine |
| **Cell 4** | Smoke test on 1 PDF — check output looks right |
| **Cell 5** | 🚀 Full batch: all 1,480 PDFs |
| **Cell 6** | Report: success/fail counts, warnings |
| **Cell 7** | Verify mirrored folder structure |
| **Cell 8** | Self-heal: retry any failed PDFs |

### Deactivate when done

```bash
deactivate
```

---

## 🧱 Option 2 — Databricks

**Yes, the notebook works on Databricks** with a few setup steps. No rewrite needed.

### Does it work on Databricks?

| Feature | Support |
|---|---|
| `pdfplumber` | ✅ Install via `%pip install` in notebook |
| `pdftotext` (poppler) | ✅ Install via init script or `apt-get` in cell |
| `pathlib.Path` on DBFS | ✅ Works with `/dbfs/mnt/...` paths |
| `pandas` + CSV output | ✅ Native |
| `tqdm` progress bar | ✅ Works in Databricks notebooks |
| Spark parallelism | ✅ Optional — Cell 5 can be upgraded to use `sc.parallelize` for faster conversion |
| Delta Lake output | 🔜 Can be added — write CSVs then `spark.read.csv(...).write.delta(...)` |

### Step 1 — Upload PDFs to cloud storage

**Azure (ADLS Gen2):**
```bash
# From your local machine with Azure CLI installed
az storage fs directory upload \
  --account-name <your-storage-account> \
  --file-system <your-container> \
  --source ./data/mahacet_cutoffs_2025 \
  --destination mahacet_cutoffs_2025 \
  --recursive
```

**AWS S3:**
```bash
aws s3 cp ./data/mahacet_cutoffs_2025 \
  s3://<your-bucket>/mahacet_cutoffs_2025/ \
  --recursive
```

**GCS:**
```bash
gsutil -m cp -r ./data/mahacet_cutoffs_2025 \
  gs://<your-bucket>/mahacet_cutoffs_2025/
```

### Step 2 — Create a Cluster Init Script (recommended)

This installs poppler on every worker node at cluster start so pdftotext is available as a fallback.

Go to **Compute → your cluster → Edit → Advanced Options → Init Scripts** and add a new script with this content:

```bash
#!/bin/bash
apt-get install -y -q poppler-utils
```

Save it to DBFS: `dbfs:/databricks/init-scripts/install_poppler.sh`

### Step 3 — Import the notebook

- Databricks Workspace → Import → select `pdf_to_csv.ipynb`
- It imports as a native Databricks notebook

### Step 4 — Run Cell 1, then Cell 1b

Cell 1b has pre-written mount code for Azure, AWS, and GCP. **Uncomment the block for your cloud**, fill in your storage account/bucket name and credentials (use Databricks Secrets — never hardcode):

```python
# Example for Azure — edit these values:
configs = {
    'fs.azure.account.oauth2.client.id': '<your-app-id>',
    'fs.azure.account.oauth2.client.secret': dbutils.secrets.get(scope='kv', key='sp-secret'),
    ...
}
dbutils.fs.mount(
    source='abfss://<container>@<storage-account>.dfs.core.windows.net/',
    mount_point='/mnt/mahacet',
    extra_configs=configs
)
```

### Step 5 — Update Cell 2 paths

Cell 2 auto-detects Databricks and sets:
```python
PDF_ROOT = Path('/dbfs/mnt/mahacet/mahacet_cutoffs_2025')
```
If your mount point differs, just edit this line.

### Step 6 — Run remaining cells normally

Cell 5 runs sequentially by default (works fine for 1,480 PDFs). If you want to use Spark to parallelize across workers, add this at the end of Cell 5:

```python
# Optional: Spark-parallelized version (replaces the for-loop in Cell 5)
from pyspark.sql import SparkSession
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Build list of (pdf_path_str, cap_round, csv_out_str)
pending_str = [(str(p), r, str(c)) for p, r, c in pending]
rdd = spark.sparkContext.parallelize(pending_str, numSlices=16)

def convert_one(args):
    pdf_str, cap_round, csv_str = args
    from pathlib import Path
    import pdfplumber, pandas as pd
    # parser must be re-imported inside the lambda (workers are separate processes)
    # → move parse_pdf logic to a separate .py module and import it here
    rows, warnings, method = parse_pdf(Path(pdf_str), cap_round)
    if rows:
        Path(csv_str).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(csv_str, index=False)
        return (pdf_str, 'ok', len(rows))
    return (pdf_str, 'failed', 0)

results = rdd.map(convert_one).collect()
ok  = [r for r in results if r[1]=='ok']
bad = [r for r in results if r[1]=='failed']
print(f"✅ {len(ok)} succeeded | ❌ {len(bad)} failed")
```

> **Note:** For Spark parallelism to work, move the parser functions (Cell 3) into a standalone `parser.py` file and import it inside `convert_one`. Worker nodes can't see notebook-scope functions.

---

## ☁️ Option 3 — Google Colab

### Step 1 — Upload data (choose one)

**Option A — Google Drive mount (recommended):**
```python
from google.colab import drive
drive.mount('/content/drive')
# Then set PDF_ROOT in Cell 2:
# PDF_ROOT = Path('/content/drive/MyDrive/mahacet_cutoffs_2025')
```

**Option B — Upload ZIP directly:**
```python
from google.colab import files
uploaded = files.upload()  # upload mahacet_cutoffs_2025.zip
import zipfile
with zipfile.ZipFile('mahacet_cutoffs_2025.zip') as z:
    z.extractall('/content/')
```

### Step 2 — Upload and open notebook

- [colab.research.google.com](https://colab.research.google.com) → File → Upload Notebook
- Select `pdf_to_csv.ipynb`

### Step 3 — Skip Cell 1b, run the rest

Cell 1 auto-installs everything. Cell 2 auto-detects Colab.

### Step 4 — Download output

```python
import shutil
from google.colab import files
shutil.make_archive('/content/mahacet_cutoffs_2025_csv', 'zip', '/content/mahacet_cutoffs_2025_csv')
files.download('/content/mahacet_cutoffs_2025_csv.zip')
```

---

## 🏁 Option 4 — Kaggle

### Step 1 — Upload as a Kaggle Dataset

- [kaggle.com/datasets](https://www.kaggle.com/datasets) → New Dataset
- Zip `mahacet_cutoffs_2025/` and upload
- Name it `mahacet-cutoffs-2025`

### Step 2 — New notebook, add dataset

- New Notebook → Upload `pdf_to_csv.ipynb`
- Add Data → search `mahacet-cutoffs-2025`

### Step 3 — Enable Internet + run

Settings → Internet → On (needed for `pip install pdfplumber`)

Output goes to `/kaggle/working/` — downloadable from the Output panel.

---

## 📊 Output CSV Columns

| Column | Type | Description |
|--------|------|-------------|
| `cap_round` | str | CAP-I / CAP-II / CAP-III / CAP-IV |
| `institute_code` | str | 5-digit institute code |
| `institute_name` | str | Full institute name |
| `branch_code` | str | 10-digit branch code |
| `branch_name` | str | e.g. Computer Science and Engineering |
| `is_ews` | bool | EWS quota section |
| `is_tfws` | bool | Tuition Fee Waiver Scheme section |
| `institute_status` | str | Government / Un-Aided / Autonomous etc. |
| `home_university` | str | Affiliated university |
| `sanction_intake` | int | Total sanctioned seats |
| `cap_seats` | int | CAP quota seats |
| `ms_seats` | int | Maharashtra State seats |
| `minority_seats` | int | Minority quota seats |
| `ai_seats` | int | All-India quota seats |
| `sr_no` | int | Serial number within branch-section |
| `merit_no` | int | State general merit number |
| `mhtcet_score` | float | MHT-CET percentile score |
| `application_id` | str | EN25XXXXXXXX |
| `candidate_name` | str | Full name |
| `gender` | str | M / F |
| `candidate_category` | str | OPEN / OBC / SC / ST / NT / SBC / EWS etc. |
| `seat_type` | str | Allotment code: GOPENS, LOBCS, GSCS, AI, MI etc. |
| `seat_marker` | str | `^` `~` `*` `@` `&` — CAP-II/III/IV change indicators |
| `is_vacant` | bool | True if seat was unfilled |
| `_validation_warnings` | str | Pipe-separated data issues (empty = clean) |

### Seat Marker Legend (CAP-II / III / IV only)

| Marker | Color | Meaning |
|--------|-------|---------|
| `^` | Gray | Admitted to Institute — no change from previous round |
| `~` | Red | No change in allotment |
| `*` | Green | Betterment in Choice Code |
| `@` | Blue | Betterment in Seat Type |
| `&` | Black | Newly allotted this round |

### Seat Type Legend

| Prefix | Meaning | Suffix | Category |
|--------|---------|--------|---------|
| `G` | General | `OPENS` | Open/General |
| `L` | Ladies only | `OBCS` | OBC |
| `AI` | All-India | `SCS` | SC |
| `MI` | Minority | `STS` | ST |
| `PWDR` | PWD reserved | `NT1S/NT2S/NT3S` | NT-A/B/C |
| `DEFR` | Defence reserved | `VJS` | VJ/DT |

---

## ❓ Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: pdfplumber` | Run Cell 1; or `pip install pdfplumber` in terminal |
| `AssertionError: PDF root not found` | Check `PDF_ROOT` in Cell 2 |
| PDF returns 0 rows | Run Cell 8 (self-heal); check `logs/failures_*.txt` |
| `seat_type` column empty for CAP-II rows | Marker stripping — update `KNOWN_SEAT_TYPES` in Cell 3 |
| Databricks: `Path not found` | Verify mount with `dbutils.fs.ls('/mnt/mahacet/')` |
| Databricks: `pdfplumber not found` | Run `%pip install pdfplumber` and restart cluster |
| Colab session reset mid-run | Re-run Cell 1→3, then Cell 8 (resume — existing CSVs skipped) |
| `Jupyter command not found` | Activate venv first: `source .venv/bin/activate` |
