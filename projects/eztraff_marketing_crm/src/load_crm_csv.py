import pandas as pd
from pathlib import Path

def load_crm_data():
    raw_dir = Path('data/raw/crm_data')
    files = []

    for file in list(raw_dir.iterdir()):
        temp = pd.read_csv(file)
        files.append(temp)

    df = pd.concat(files, ignore_index=True)

    raw_dir_crm = Path('data/raw')
    raw_dir_crm.mkdir(parents=True, exist_ok=True)

    output_path = raw_dir_crm / f'crm_data.csv'
    df.to_csv(output_path, index=False)

    return df