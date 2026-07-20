# Databricks notebook source
# MAGIC %md
# MAGIC # 🏛️ MH-CET Merit List PDF → CSV Pipeline
# MAGIC
# MAGIC Converts all CAP Round PDFs (CAP-I through CAP-IV) into structured CSVs, **mirroring the source folder structure**.
# MAGIC
# MAGIC **Output columns per row:**
# MAGIC `cap_round | institute_code | institute_name | branch_code | branch_name | seat_pool | is_ews | is_tfws | institute_status | home_university | sanction_intake | cap_seats | ms_seats | minority_seats | ai_seats | sr_no | merit_no | mhtcet_score | application_id | candidate_name | gender | candidate_category | seat_type | is_vacant`
# MAGIC
# MAGIC ---
# MAGIC **Works on:** Local (macOS/Linux) | Google Colab | Kaggle Notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Cell 1 — Environment Detection & Package Install

# COMMAND ----------

import sys, os, subprocess, importlib

# ── Detect environment ──────────────────────────────────────────────────────
IN_COLAB  = 'google.colab' in sys.modules or os.path.exists('/content')
IN_KAGGLE = os.path.exists('/kaggle')
IS_LOCAL  = not IN_COLAB and not IN_KAGGLE

env_name = 'Google Colab' if IN_COLAB else ('Kaggle' if IN_KAGGLE else 'Local')
print(f'🌍 Environment detected: {env_name}')

# ── Install Python packages ─────────────────────────────────────────────────
REQUIRED = ['pdfplumber', 'pandas', 'tqdm']

for pkg in REQUIRED:
    if importlib.util.find_spec(pkg) is None:
        print(f'📦 Installing {pkg}...')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg, '-q'])
    else:
        print(f'✅ {pkg} already available')

# ── Check pdftotext (system tool — used as fallback) ────────────────────────
def check_pdftotext():
    try:
        result = subprocess.run(['which', 'pdftotext'], capture_output=True, text=True)
        if result.returncode == 0:
            print('✅ pdftotext (poppler) found — will use as fallback')
            return True
    except Exception:
        pass
    # Try to install poppler on Colab/Linux
    if IN_COLAB or IN_KAGGLE:
        print('⬇️  Installing poppler-utils...')
        subprocess.run(['apt-get', 'install', '-y', '-q', 'poppler-utils'], capture_output=True)
        return True
    print('⚠️  pdftotext not found. Only pdfplumber will be used.')
    return False

HAS_PDFTOTEXT = check_pdftotext()
print('\n✅ Environment ready!')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 Cell 1b — Databricks-Specific Setup (Skip if not on Databricks)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════
# DATABRICKS SETUP CELL
# Run this cell ONLY when executing on a Databricks cluster.
# Skip it entirely on Local / Colab / Kaggle.
# ═══════════════════════════════════════════════════════════════════════

import sys
IN_DATABRICKS = 'dbruntime' in sys.modules or 'databricks' in str(type(spark)) if 'spark' in dir() else False

if not IN_DATABRICKS:
    print('⏭️  Not on Databricks — skip this cell')
else:
    # 1. Install Python libraries (cluster-scoped for this session)
    %pip install pdfplumber tqdm -q

    # 2. Install poppler (pdftotext fallback) via shell on each worker
    #    Note: For persistent installs across restarts, add this to your
    #    cluster Init Script instead (see SETUP.md → Databricks section)
    import subprocess
    subprocess.run(['sudo', 'apt-get', 'install', '-y', '-q', 'poppler-utils'], check=False)
    print('✅ poppler-utils installed on driver')

    # 3. Mount cloud storage (edit ONE of the blocks below for your cloud)
    # ── Option A: Azure Data Lake Storage Gen2 (ADLS) ──────────────────
    # configs = {
    #     'fs.azure.account.auth.type': 'OAuth',
    #     'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
    #     'fs.azure.account.oauth2.client.id': '<your-client-id>',
    #     'fs.azure.account.oauth2.client.secret': dbutils.secrets.get(scope='kv', key='sp-secret'),
    #     'fs.azure.account.oauth2.client.endpoint': 'https://login.microsoftonline.com/<tenant-id>/oauth2/token',
    # }
    # dbutils.fs.mount(
    #     source='abfss://<container>@<storage-account>.dfs.core.windows.net/',
    #     mount_point='/mnt/mahacet',
    #     extra_configs=configs
    # )

    # ── Option B: AWS S3 ────────────────────────────────────────────────
    # dbutils.fs.mount(
    #     source='s3a://<your-bucket>/mahacet/',
    #     mount_point='/mnt/mahacet',
    #     extra_configs={
    #         'fs.s3a.access.key': dbutils.secrets.get(scope='aws', key='access-key'),
    #         'fs.s3a.secret.key': dbutils.secrets.get(scope='aws', key='secret-key'),
    #     }
    # )

    # ── Option C: GCS ───────────────────────────────────────────────────
    # dbutils.fs.mount(
    #     source='gs://<your-bucket>/mahacet/',
    #     mount_point='/mnt/mahacet',
    # )

    # 4. Verify mount
    # display(dbutils.fs.ls('/mnt/mahacet/mahacet_cutoffs_2025/'))

    # 5. Override environment flag so Cell 2 picks Databricks paths
    IN_COLAB  = False
    IN_KAGGLE = False
    IS_LOCAL  = False
    IN_DATABRICKS = True
    print('✅ Databricks environment ready')
    print('👉 Next: verify your mount, then update PDF_ROOT in Cell 2')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Cell 2 — Configuration (Edit These Paths)

# COMMAND ----------

# DBTITLE 1,Cell 7
from pathlib import Path

# ── SOURCE: folder containing CAP-I / CAP-II / CAP-III / CAP-IV ─────────────
if 'IN_DATABRICKS' in dir() and IN_DATABRICKS:
    # DBFS path — update <container> to match your mount point
    PDF_ROOT = Path('/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/mahacet_cutoffs_2025')
elif IN_COLAB:
    # Mount Drive first if needed:
    # from google.colab import drive; drive.mount('/content/drive')
    PDF_ROOT = Path('/content/mahacet_cutoffs_2025')
elif IN_KAGGLE:
    PDF_ROOT = Path('/kaggle/input/mahacet-cutoffs-2025')
else:
    # LOCAL — adjust if your path differs
    PDF_ROOT = Path('/Users/varenyamnikam/Documents/data-engineering/DE-CET/data/mahacet_cutoffs_2025')

# ── OUTPUT: mirrored CSV folder (auto-created) ───────────────────────────────
CSV_ROOT = PDF_ROOT.parent / 'mahacet_cutoffs_2025_csv'  # → /Volumes/.../my_files/data/mahacet_cutoffs_2025_csv

# ── LOGS ─────────────────────────────────────────────────────────────────────
LOG_DIR  = PDF_ROOT.parent / 'logs'

# ── SETTINGS ─────────────────────────────────────────────────────────────────
SKIP_EXISTING   = True   # Resume mode: skip PDFs that already have a CSV
VALIDATE_ROWS   = True   # Flag rows with suspicious data
INCLUDE_VACANT  = True   # Include VACANT seat rows

# ── Verify source exists ──────────────────────────────────────────────────────
try:
    _root_exists = PDF_ROOT.exists()
except PermissionError:
    _root_exists = False
assert _root_exists, (
    f'❌ PDF root not found or permission denied: {PDF_ROOT}\n'
    f'   Update PDF_ROOT above to a valid UC Volume path where your PDFs are stored.'
)

rounds = sorted([d.name for d in PDF_ROOT.iterdir() if d.is_dir() and d.name.startswith('CAP')])
total_pdfs = sum(len(list((PDF_ROOT/r).glob('*.pdf'))) for r in rounds)

print(f'📂 PDF Root  : {PDF_ROOT}')
print(f'📂 CSV Root  : {CSV_ROOT}')
print(f'📝 Log Dir   : {LOG_DIR}')
print(f'📦 Rounds    : {rounds}')
print(f'📄 Total PDFs: {total_pdfs}')


# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Cell 3 — PDF Parser (Core Engine)

# COMMAND ----------

import re
import pdfplumber
import subprocess
from dataclasses import dataclass, field
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# REGEX PATTERNS
# ─────────────────────────────────────────────────────────────────────────────
RE_INSTITUTE   = re.compile(r'^(\d{5})\s+(.+)$')
RE_BRANCH      = re.compile(r'^(\d{10}(?:\[EWS\])?(?:T)?)\s*-\s*(.+)$')
RE_BRANCH_CODE = re.compile(r'^(\d{10})(\[EWS\])?(T)?$')
RE_STATUS      = re.compile(r'^Status:\s*(.+?)(?:\s+Home University\s*:\s*(.+))?$')
RE_INTAKE      = re.compile(r'Sanction Intake:\s*(\d+)')
RE_CAP_SEATS   = re.compile(r'CAP Seats:\s*(\d+)')
RE_MS_SEATS    = re.compile(r'MS Seats:\s*(\d+)')
RE_MIN_SEATS   = re.compile(r'Minority Seats\s*:\s*(\d+)')
RE_AI_SEATS    = re.compile(r'AI Seats:\s*(\d+)')
RE_APP_ID      = re.compile(r'^[A-Z]{2}\d{8}$')
RE_SCORE       = re.compile(r'^\d+\.\d+$')
RE_MERIT_NO    = re.compile(r'^\d{1,6}$')
RE_SR_NO       = re.compile(r'^\d{1,4}$')
RE_PAGE        = re.compile(r'^Page \d+ of \d+$')
RE_SEAT_TYPE   = re.compile(r'^(G|L)(OPEN|OBC|SC|ST|VJ|NT1|NT2|NT3|SBC|EWS|SEB|DT)S$|'
                             r'^(PWDR|DEFR)(OPEN|OBC|SC|ST|NT[123]|SBC|SEB)?S?$|'
                             r'^AI$|^MI$')
RE_GENDER      = re.compile(r'^[MF]$')

SKIP_LINES = {
    'Government of Maharashtra',
    'State Common Entrance Test Cell',
    'Provisional Allotment List',
    'Degree Courses In Engineering',
    'Integrated 5 Years',
    'Name of the Candidate',
    'Gender',
    'Candidate',
    'Category',
    'Seat Type',
    'Merit',
    'Meri',
    'MHT-CET',
    'Score',
    'Application ID',
    'Sr.',
    'No.',
    'State Level Seats',
    'All India Seats',
    'All India Seats Allotted to All India Candidature Candidates with JEE(Main) Score',
    'Institute Seats',
    'Minority Seats Allotted to Minority Candidates',
    'P',
    'r',
    '00',
    '0',
}
SKIP_PREFIXES = (
    'Legends for SeatType',
    'Legends for ChoiceCode',
    'Seat Type :',          # CAP-II/III/IV color-marker legend line
    '~ Red Color',
    '& Black Color',
    'MI-Minority',
    'PWDR :',
    'DEFR :',
    'Merit No :',
    'Provisional Allotment List',
    'Degree Courses',
    'Master of Engineering',
    '(Integrated',
    'for the Year',
    'for the Admission',
)

# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class BranchMeta:
    branch_code_raw: str = ''
    branch_code: str = ''
    branch_name: str = ''
    is_ews: bool = False
    is_tfws: bool = False
    institute_status: str = ''
    home_university: str = ''
    sanction_intake: str = ''
    cap_seats: str = ''
    ms_seats: str = ''
    minority_seats: str = ''
    ai_seats: str = ''

@dataclass
class ParseState:
    institute_code: str = ''
    institute_name: str = ''
    branch: BranchMeta = field(default_factory=BranchMeta)
    # token buffer for building a candidate row
    tokens: list = field(default_factory=list)
    rows: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

# ─────────────────────────────────────────────────────────────────────────────
# TEXT EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
def extract_text_pdfplumber(pdf_path: Path) -> str:
    """Primary extraction using pdfplumber."""
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=3, y_tolerance=3)
            if text:
                pages.append(text)
    return '\n'.join(pages)

def extract_text_pdftotext(pdf_path: Path) -> str:
    """Fallback extraction using system pdftotext."""
    result = subprocess.run(
        ['pdftotext', '-layout', str(pdf_path), '-'],
        capture_output=True, text=True, timeout=60
    )
    return result.stdout

def extract_text(pdf_path: Path) -> tuple[str, str]:
    """Returns (text, method_used). Tries pdfplumber first, falls back to pdftotext."""
    try:
        text = extract_text_pdfplumber(pdf_path)
        if len(text.strip()) > 100:
            return text, 'pdfplumber'
    except Exception:
        pass
    if HAS_PDFTOTEXT:
        try:
            text = extract_text_pdftotext(pdf_path)
            if len(text.strip()) > 100:
                return text, 'pdftotext'
        except Exception:
            pass
    return '', 'failed'

# ─────────────────────────────────────────────────────────────────────────────
# LINE CLEANING
# ─────────────────────────────────────────────────────────────────────────────
def clean_lines(raw_text: str) -> list[str]:
    lines = []
    for line in raw_text.split('\n'):
        line = line.strip()
        if not line:
            continue
        if line in SKIP_LINES:
            continue
        if any(line.startswith(p) for p in SKIP_PREFIXES):
            continue
        if RE_PAGE.match(line):
            continue
        # strip bracket artifacts like [ MS Seats: 60 ]
        line = re.sub(r'^\[\s*', '', line)
        line = re.sub(r'\s*\]$', '', line)
        line = line.strip()
        if line:
            lines.append(line)
    return lines

# ─────────────────────────────────────────────────────────────────────────────
# SEAT TYPE NORMALIZER
# ─────────────────────────────────────────────────────────────────────────────
KNOWN_SEAT_TYPES = {
    'GOPENS','LOPENS','GOBCS','LOBCS','GSCS','LSCS','GSTS','LSTS',
    'GNT1S','LNT1S','GNT2S','LNT2S','GNT3S','LNT3S','GVJS','LVJS',
    'GSEBCS','LSEBCS','GEWSS','LEWSS',
    'PWDROPENS','PWDROBCS','PWDRSCS','PWDRSTS','PWDRSEBCS',
    'DEFROPENS','DEFROBCS','DEFRSCS','DEFRSTS','DEFRSEBCS',
    'DEFRNT3S','DEFRNT2S','DEFRNT1S',
    'AI', 'MI',
}

def is_seat_type(token: str) -> bool:
    return token in KNOWN_SEAT_TYPES

# ─────────────────────────────────────────────────────────────────────────────
# CANDIDATE CATEGORY NORMALIZER  (handles OBC$#, SC/DEF1, NT 2 (NT-C) etc.)
# ─────────────────────────────────────────────────────────────────────────────
def is_candidate_category(token: str) -> bool:
    base = re.sub(r'[$#@/0-9()\- ]+', '', token)
    KNOWN_CATS = {
        'OPEN','OBC','SC','ST','VJ','DT','NT','NT1','NT2','NT3',
        'SBC','SEBC','EWS','TFWS','DEF','PWD','PWDOPEN','OPEN'
    }
    return base in KNOWN_CATS or any(k in token for k in ['OPEN','OBC','SC','ST','NT','SBC','EWS','DEF','PWD'])

# ─────────────────────────────────────────────────────────────────────────────
# TOKEN STREAM → ROW ASSEMBLER
# ─────────────────────────────────────────────────────────────────────────────
def flush_tokens(state: ParseState, cap_round: str):
    """Try to assemble a candidate row from the token buffer."""
    toks = state.tokens
    if not toks:
        return

    # Handle VACANT rows: pattern is [sr_no, 'VACANT', seat_type]
    if 'VACANT' in toks:
        if INCLUDE_VACANT:
            sr_no = next((t for t in toks if RE_SR_NO.match(t)), '')
            seat_type = next((t for t in toks if is_seat_type(t)), '')
            row = _build_row(state, cap_round, sr_no=sr_no, merit_no='', score='',
                             app_id='', name='VACANT', gender='', category='',
                             seat_type=seat_type, is_vacant=True)
            state.rows.append(row)
        state.tokens = []
        return

    # Find anchor: Application ID (EN25XXXXXXXX)
    app_idx = next((i for i, t in enumerate(toks) if RE_APP_ID.match(t)), None)
    if app_idx is None:
        state.tokens = []
        return

    before = toks[:app_idx]
    after  = toks[app_idx+1:]
    app_id = toks[app_idx]

    # Extract sr_no, merit_no, score from tokens before app_id
    numbers = [t for t in before if re.match(r'^\d+(\.\d+)?$', t)]
    score    = next((t for t in numbers if '.' in t), '')
    int_nums = [t for t in numbers if '.' not in t]
    sr_no    = int_nums[0] if len(int_nums) >= 2 else (int_nums[0] if int_nums else '')
    merit_no = int_nums[1] if len(int_nums) >= 2 else ''

    # After app_id: name tokens, then gender (M/F), then category, then seat_type
    seat_type = ''
    seat_marker = ''
    gender    = ''
    category  = ''
    name_parts = []

    i = 0
    while i < len(after):
        t = after[i]
        if is_seat_type(t) and not seat_type:
            clean, marker = strip_marker(t)
            seat_type = clean
            seat_marker = marker
            i += 1
            continue
        # handle marker token appearing alone before seat_type token
        if t in SEAT_TYPE_MARKERS and not seat_type:
            seat_marker = t
            i += 1
            continue
        if RE_GENDER.match(t) and not gender:
            gender = t
            i += 1
            continue
        if gender and not category and is_candidate_category(t):
            # category can be multi-word e.g. "NT 2 (NT-C)"
            cat_parts = [t]
            j = i + 1
            while j < len(after) and not RE_GENDER.match(after[j]) and not is_seat_type(after[j]):
                cat_parts.append(after[j])
                j += 1
            category = ' '.join(cat_parts)
            i = j
            continue
        if not gender:  # still in name territory
            name_parts.append(t)
        i += 1

    name = ' '.join(name_parts).strip()

    if not app_id:
        state.tokens = []
        return

    row = _build_row(state, cap_round, sr_no=sr_no, merit_no=merit_no, score=score,
                     app_id=app_id, name=name, gender=gender, category=category,
                     seat_type=seat_type, seat_marker=seat_marker, is_vacant=False)

    # Validation flags
    if VALIDATE_ROWS:
        issues = []
        if not RE_APP_ID.match(app_id):      issues.append(f'bad_app_id:{app_id}')
        if not score:                         issues.append('missing_score')
        if not gender:                        issues.append('missing_gender')
        if not seat_type:                     issues.append('missing_seat_type')
        if not name:                          issues.append('missing_name')
        if issues:
            row['_validation_warnings'] = '|'.join(issues)
            state.warnings.append(f"Row {app_id}: {', '.join(issues)}")
        else:
            row['_validation_warnings'] = ''
    else:
        row['_validation_warnings'] = ''

    state.rows.append(row)
    state.tokens = []


def _build_row(state: ParseState, cap_round: str, **kwargs) -> dict:
    b = state.branch
    return {
        'cap_round':          cap_round,
        'institute_code':     state.institute_code,
        'institute_name':     state.institute_name,
        'branch_code':        b.branch_code,
        'branch_name':        b.branch_name,
        'is_ews':             b.is_ews,
        'is_tfws':            b.is_tfws,
        'institute_status':   b.institute_status,
        'home_university':    b.home_university,
        'sanction_intake':    b.sanction_intake,
        'cap_seats':          b.cap_seats,
        'ms_seats':           b.ms_seats,
        'minority_seats':     b.minority_seats,
        'ai_seats':           b.ai_seats,
        'sr_no':              kwargs.get('sr_no', ''),
        'merit_no':           kwargs.get('merit_no', ''),
        'mhtcet_score':       kwargs.get('score', ''),
        'application_id':     kwargs.get('app_id', ''),
        'candidate_name':     kwargs.get('name', ''),
        'gender':             kwargs.get('gender', ''),
        'candidate_category': kwargs.get('category', ''),
        'seat_type':          kwargs.get('seat_type', ''),
        'seat_marker':        kwargs.get('seat_marker', ''),  # ^~*@& allotment change indicator (CAP-II/III/IV)
        'is_vacant':          kwargs.get('is_vacant', False),
    }

# ─────────────────────────────────────────────────────────────────────────────
# MAIN PARSER
# ─────────────────────────────────────────────────────────────────────────────
def parse_pdf(pdf_path: Path, cap_round: str) -> tuple[list[dict], list[str], str]:
    """
    Parse a single PDF.
    Returns: (rows, warnings, extraction_method)
    """
    raw_text, method = extract_text(pdf_path)
    if not raw_text.strip():
        return [], [f'EMPTY: could not extract text from {pdf_path.name}'], method

    lines  = clean_lines(raw_text)
    state  = ParseState()

    # Parse institute header from filename as bootstrap
    fname_match = re.match(r'CAPR-(?:I{1,3}V?|IV)_(\d{5})\.pdf', pdf_path.name, re.IGNORECASE)
    if fname_match:
        state.institute_code = fname_match.group(1)

    i = 0
    while i < len(lines):
        line = lines[i]

        # ── Institute header: "01002 Government College of Engineering, Amravati"
        m = RE_INSTITUTE.match(line)
        if m and len(m.group(1)) == 5:
            flush_tokens(state, cap_round)
            state.institute_code = m.group(1)
            state.institute_name = m.group(2).strip()
            i += 1
            continue

        # ── Branch header: "0100219110 - Civil Engineering" or "0100219110 [EWS] - ..."
        # Also handles: "0100219111T - Civil Engineering"
        m = re.match(r'^(\d{10}(?:\[EWS\])?T?)\s*-\s*(.+)$', line)
        if m:
            flush_tokens(state, cap_round)
            raw_code  = m.group(1)
            bname     = m.group(2).strip()
            is_ews    = '[EWS]' in raw_code
            is_tfws   = raw_code.endswith('T') and not is_ews
            base_code = re.sub(r'\[EWS\]|T$', '', raw_code)
            state.branch = BranchMeta(
                branch_code_raw=raw_code,
                branch_code=base_code,
                branch_name=bname,
                is_ews=is_ews,
                is_tfws=is_tfws,
                # carry forward last-seen metadata until new Status: line
                institute_status=state.branch.institute_status,
                home_university=state.branch.home_university,
            )
            i += 1
            continue

        # ── Status line
        m = RE_STATUS.match(line)
        if m:
            state.branch.institute_status = (m.group(1) or '').strip()
            state.branch.home_university  = (m.group(2) or '').strip()
            i += 1
            continue

        # ── Seat metadata (may span adjacent lines; greedy single-line parse)
        if RE_INTAKE.search(line):   state.branch.sanction_intake = RE_INTAKE.search(line).group(1)
        if RE_CAP_SEATS.search(line): state.branch.cap_seats      = RE_CAP_SEATS.search(line).group(1)
        if RE_MS_SEATS.search(line):  state.branch.ms_seats       = RE_MS_SEATS.search(line).group(1)
        if RE_MIN_SEATS.search(line): state.branch.minority_seats = RE_MIN_SEATS.search(line).group(1)
        if RE_AI_SEATS.search(line):  state.branch.ai_seats       = RE_AI_SEATS.search(line).group(1)

        if any(pat.search(line) for pat in [RE_INTAKE, RE_CAP_SEATS, RE_MS_SEATS, RE_MIN_SEATS, RE_AI_SEATS]):
            i += 1
            continue

        # ── Candidate data token — add to buffer
        # A new sr_no (small integer 1-4 digits) signals start of a new candidate row
        if RE_SR_NO.match(line) and not state.tokens:
            state.tokens = [line]
        elif state.tokens or RE_APP_ID.match(line) or RE_SCORE.match(line):
            state.tokens.append(line)
            # Flush when we have a seat_type token (end of row)
            if is_seat_type(line):
                flush_tokens(state, cap_round)
        elif line == 'VACANT':
            state.tokens.append(line)

        i += 1

    # Final flush
    flush_tokens(state, cap_round)

    return state.rows, state.warnings, method

print('✅ Parser engine loaded')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Cell 4 — Smoke Test on a Single PDF

# COMMAND ----------

import pandas as pd

# Pick first available PDF from CAP-I for a quick sanity check
test_pdfs = sorted((PDF_ROOT / 'CAP-I').glob('*.pdf'))
test_pdf  = test_pdfs[0]
print(f'🔬 Smoke-testing: {test_pdf.name}')

rows, warnings, method = parse_pdf(test_pdf, cap_round='CAP-I')

df_test = pd.DataFrame(rows)
print(f'\n📊 Extraction method : {method}')
print(f'📄 Total rows parsed  : {len(df_test)}')
print(f'⚠️  Validation warnings: {len(warnings)}')
print(f'🌿 Branches found     : {df_test["branch_name"].nunique() if len(df_test) else 0}')

if len(df_test):
    print('\n📋 Sample rows:')
    display(df_test.head(10))
    print('\n📋 Column dtypes:')
    print(df_test.dtypes)
    print('\n📋 Branches:')
    print(df_test.groupby(['branch_code','branch_name','is_ews','is_tfws']).size().reset_index(name='candidates'))

if warnings:
    print(f'\n⚠️ First 10 warnings:')
    for w in warnings[:10]:
        print(f'  • {w}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Cell 5 — Full Batch Conversion Pipeline

# COMMAND ----------

#%%
import json
import time
from datetime import datetime
try:
    from tqdm.notebook import tqdm
except Exception:
    from tqdm import tqdm

# ── Setup output dirs ────────────────────────────────────────────────────────
CSV_ROOT.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

run_ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = LOG_DIR / f'conversion_{run_ts}.jsonl'
fail_log = LOG_DIR / f'failures_{run_ts}.txt'

# ── Collect all PDFs ─────────────────────────────────────────────────────────
all_pdfs = []
for round_dir in sorted(PDF_ROOT.iterdir()):
    if not round_dir.is_dir() or not round_dir.name.startswith('CAP'):
        continue
    cap_round = round_dir.name  # e.g. CAP-I
    for pdf in sorted(round_dir.glob('*.pdf')):
        csv_out = CSV_ROOT / cap_round / (pdf.stem + '.csv')
        all_pdfs.append((pdf, cap_round, csv_out))

# ── Filter already done (resume mode) ────────────────────────────────────────
if SKIP_EXISTING:
    pending  = [(p, r, c) for p, r, c in all_pdfs if not c.exists()]
    skipped  = len(all_pdfs) - len(pending)
    print(f'⏭️  Skipping {skipped} already-converted PDFs (SKIP_EXISTING=True)')
else:
    pending  = all_pdfs
    skipped  = 0

print(f'🔄 PDFs to convert: {len(pending)} / {len(all_pdfs)}')

# ── Batch loop ───────────────────────────────────────────────────────────────
stats = {
    'total': len(all_pdfs),
    'skipped': skipped,
    'success': 0,
    'failed': 0,
    'total_rows': 0,
    'failures': [],
}

with open(log_file, 'w') as lf, open(fail_log, 'w') as ff:
    ff.write(f'Failure log — run {run_ts}\n{"-"*60}\n')

    for pdf_path, cap_round, csv_out in tqdm(pending, desc='Converting PDFs'):
        t0 = time.time()

        try:
            # 1. Parse
            rows, warnings, method = parse_pdf(pdf_path, cap_round)

            if not rows:
                raise ValueError('Parser returned 0 rows — likely extraction failure')

            # 2. Build DataFrame
            df = pd.DataFrame(rows)

            # 3. Type casting
            for col in ['sr_no', 'merit_no', 'sanction_intake', 'cap_seats',
                        'ms_seats', 'minority_seats', 'ai_seats']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['mhtcet_score'] = pd.to_numeric(df['mhtcet_score'], errors='coerce')
            df['is_ews']    = df['is_ews'].astype(bool)
            df['is_tfws']   = df['is_tfws'].astype(bool)
            df['is_vacant'] = df['is_vacant'].astype(bool)

            # 4. Write CSV
            csv_out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_out, index=False, encoding='utf-8-sig')  # utf-8-sig for Excel compat

            elapsed = round(time.time() - t0, 2)
            warn_count = len(warnings)

            # 5. Log entry
            log_entry = {
                'ts': datetime.now().isoformat(),
                'file': pdf_path.name,
                'round': cap_round,
                'rows': len(df),
                'branches': int(df['branch_name'].nunique()),
                'warnings': warn_count,
                'method': method,
                'elapsed_s': elapsed,
                'status': 'ok',
            }
            lf.write(json.dumps(log_entry) + '\n')

            stats['success']    += 1
            stats['total_rows'] += len(df)

        except Exception as e:
            elapsed = round(time.time() - t0, 2)
            reason  = f'{type(e).__name__}: {e}'

            log_entry = {
                'ts': datetime.now().isoformat(),
                'file': pdf_path.name,
                'round': cap_round,
                'rows': 0,
                'warnings': 0,
                'method': 'unknown',
                'elapsed_s': elapsed,
                'status': 'failed',
                'error': reason,
            }
            lf.write(json.dumps(log_entry) + '\n')
            ff.write(f'{pdf_path.name} | {cap_round} | {reason}\n')

            stats['failed'] += 1
            stats['failures'].append({'file': pdf_path.name, 'round': cap_round, 'reason': reason})

print('\n' + '═'*60)
print('✅ BATCH CONVERSION COMPLETE')
print('═'*60)
print(f"  Total PDFs         : {stats['total']}")
print(f"  Skipped (existing) : {stats['skipped']}")
print(f"  ✅ Succeeded       : {stats['success']}")
print(f"  ❌ Failed          : {stats['failed']}")
print(f"  📊 Total rows      : {stats['total_rows']:,}")
print(f"  📄 Log             : {log_file}")
print(f"  📄 Fail log        : {fail_log}")

if stats['failures']:
    print(f"\n❌ FAILURES ({stats['failed']})")
    print('-'*60)
    for f in stats['failures']:
        print(f"  [{f['round']}] {f['file']}")
        print(f"    → {f['reason']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Cell 6 — Post-Conversion Report & Validation Summary

# COMMAND ----------

# ── Read all log entries ──────────────────────────────────────────────────────
import glob
import pandas as pd
import json

log_entries = []
for lf in sorted(glob.glob(str(LOG_DIR / 'conversion_*.jsonl'))):
    with open(lf) as f:
        for line in f:
            line = line.strip()
            if line:
                log_entries.append(json.loads(line))

if not log_entries:
    print('No log entries found — run Cell 5 first.')
else:
    df_log = pd.DataFrame(log_entries)

    print('📊 CONVERSION REPORT')
    print('='*60)

    # ── Per round summary ─────────────────────────────────────────────────────
    summary = df_log.groupby(['round', 'status']).agg(
        files=('file', 'count'),
        total_rows=('rows', 'sum'),
        avg_rows=('rows', 'mean'),
        avg_time_s=('elapsed_s', 'mean'),
    ).round(2)
    display(summary)

    # ── Extraction method breakdown ───────────────────────────────────────────
    print('\n🔧 Extraction method used:')
    ok = df_log[df_log['status'] == 'ok']
    if 'method' in ok.columns:
        print(ok['method'].value_counts().to_string())

    # ── Files with validation warnings ───────────────────────────────────────
    print('\n⚠️  Files with validation warnings:')
    if 'warnings' in df_log.columns:
        warned = df_log[(df_log['status'] == 'ok') & (df_log['warnings'] > 0)]
        if len(warned):
            display(warned[['file', 'round', 'rows', 'warnings']]
                    .sort_values('warnings', ascending=False)
                    .head(20)
                    .reset_index(drop=True))
        else:
            print('  ✅ No warnings')
    else:
        print('  (no warnings column in log)')

    # ── Failed files ──────────────────────────────────────────────────────────
    failed = df_log[df_log['status'] == 'failed']
    if len(failed):
        print(f'\n❌ Failed files ({len(failed)}):')
        display(failed[['file', 'round', 'error']].reset_index(drop=True))
    else:
        print('\n🎉 No failures!')

    # ── Overall totals ────────────────────────────────────────────────────────
    print('\n' + '═'*60)
    print(f"  Total files processed : {len(df_log)}")
    print(f"  ✅ Succeeded          : {len(df_log[df_log['status']=='ok'])}")
    print(f"  ❌ Failed             : {len(failed)}")
    print(f"  📊 Total rows         : {int(df_log['rows'].sum()):,}")
    print(f"  ⏱️  Avg time per PDF   : {df_log['elapsed_s'].mean():.2f}s")
    print(f"  ⏱️  Total time         : {df_log['elapsed_s'].sum()/60:.1f} mins")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Cell 7 — Verify Output Folder Structure

# COMMAND ----------

print(f'📂 Output structure: {CSV_ROOT}\n')

total_csvs = 0
for round_dir in sorted(CSV_ROOT.iterdir()):
    if not round_dir.is_dir():
        continue
    csvs = list(round_dir.glob('*.csv'))
    total_csvs += len(csvs)
    pdfs_in_round = len(list((PDF_ROOT / round_dir.name).glob('*.pdf')))
    pct = round(len(csvs)/pdfs_in_round*100, 1) if pdfs_in_round else 0
    print(f'  {round_dir.name}/  →  {len(csvs)}/{pdfs_in_round} CSVs ({pct}%)')

print(f'\n  Total CSVs created: {total_csvs}')

# Quick peek at one CSV
sample_csvs = list(CSV_ROOT.glob('**/*.csv'))
if sample_csvs:
    print(f'\n📋 Sample CSV preview: {sample_csvs[0].name}')
    display(pd.read_csv(sample_csvs[0]).head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Cell 8 — Self-Heal: Retry Failed PDFs

# COMMAND ----------

# This cell re-runs ONLY previously failed PDFs.
# It reads the latest failure log and retries each one.

fail_logs = sorted(glob.glob(str(LOG_DIR / 'failures_*.txt')))
if not fail_logs:
    print('✅ No failure logs found — nothing to retry!')
else:
    latest_fail_log = fail_logs[-1]
    print(f'📄 Reading failures from: {latest_fail_log}')

    retry_queue = []
    with open(latest_fail_log) as f:
        for line in f:
            line = line.strip()
            if '|' not in line or line.startswith('Failure'):
                continue
            parts = line.split(' | ')
            if len(parts) >= 2:
                fname, cap_round = parts[0].strip(), parts[1].strip()
                pdf_path = PDF_ROOT / cap_round / fname
                csv_out  = CSV_ROOT / cap_round / (Path(fname).stem + '.csv')
                if pdf_path.exists():
                    retry_queue.append((pdf_path, cap_round, csv_out))

    print(f'🔁 Retrying {len(retry_queue)} failed PDF(s)...')
    retry_stats = {'success': 0, 'still_failed': 0, 'failures': []}

    for pdf_path, cap_round, csv_out in tqdm(retry_queue, desc='Retrying'):
        try:
            rows, warnings, method = parse_pdf(pdf_path, cap_round)
            if not rows:
                raise ValueError('0 rows after retry')
            df = pd.DataFrame(rows)
            csv_out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_out, index=False, encoding='utf-8-sig')
            retry_stats['success'] += 1
            print(f'  ✅ {pdf_path.name} → {len(df)} rows')
        except Exception as e:
            retry_stats['still_failed'] += 1
            reason = f'{type(e).__name__}: {e}'
            retry_stats['failures'].append({'file': pdf_path.name, 'reason': reason})
            print(f'  ❌ {pdf_path.name}: {reason}')

    print(f'\n🔁 Retry results: {retry_stats["success"]} recovered, {retry_stats["still_failed"]} still failing')
    if retry_stats['failures']:
        print('\nStill failing files (manual inspection needed):')
        for f in retry_stats['failures']:
            print(f"  {f['file']}: {f['reason']}")