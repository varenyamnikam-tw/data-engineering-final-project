import os
import pandas as pd

csv_root = '/Volumes/rankrangers_project_data/pdf/cet_raw_pdfs/data/mahacet_cutoffs_2025_csv'

# 1. Check access + how many CSVs are ready
for r in ['CAP-I', 'CAP-II', 'CAP-III', 'CAP-IV']:
    p = f'{csv_root}/{r}'
    count = len(os.listdir(p)) if os.path.exists(p) else 0
    print(f'{r}: {count} CSVs')

    # 2. Preview a sample file
sample = f'{csv_root}/CAP-I/CAPR-I_01002.csv'
df = pd.read_csv(sample)
display(df.head(10))
print(f'Columns: {list(df.columns)}')
print(f'Rows: {len(df)}')

