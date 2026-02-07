from __future__ import annotations
import pandas as pd
from pathlib import Path
import json

from src.dq_checks import (
    run_partd_checks,
    run_provider_checks,
    run_merged_checks
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / 'data' / 'raw'
CLEAN_DIR = PROJECT_ROOT/ 'data' / 'clean'
CLEAN_DIR.mkdir(parents = True, exist_ok = True)

def load_raw_folder(folder: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for path in sorted(folder.glob('page_*.json')):
        with path.open('r', encoding = 'utf-8') as f:
            data = json.load(f)
        frames.append(pd.DataFrame(data))

    if not frames:
        raise RuntimeError(f'No page_*.json files found in {folder}')
    
    df = pd.concat(frames, ignore_index = True)
    print(f'Loaded {len(df)} rows from {folder}')
    return df

def run_pipeline(run_date: str):
    # load
    provider_dir = RAW_DIR / 'cms_provider' / run_date
    partd_dir = RAW_DIR / 'cms_partd' / run_date

    provider_df = load_raw_folder(provider_dir)
    partd_df = load_raw_folder(partd_dir)

    # checks
    run_provider_checks(provider_df)
    run_partd_checks(partd_df)

    # merge
    merged_df = partd_df.merge(
        provider_df,
        left_on = 'Prscrbr_NPI',
        right_on = 'Rndrng_NPI',
        how = 'left'
    )

    merged_df = merged_df.rename(columns = {
        'Tot_Benes_x': 'PartD_Tot_Benes',
        'Tot_Benes_y': 'Prov_Tot_Benes',
        'Tot_Srvcs': 'Prov_Tot_Srvcs',
        'Tot_Mdcr_Pymt_Amt': 'Prov_Tot_Mdcr_Pymt_Amt',
        'Tot_Mdcr_Alowd_Amt': 'Prov_Tot_Mdcr_Alowd_Amt'
    })

    # merged checks
    run_merged_checks(partd_df, merged_df)

    # save clean
    output_path = CLEAN_DIR / f'medicare_partd_provider_clean_{run_date}.parquet'
    merged_df.to_parquet(output_path, index = False)

    print(f'Saved clean merged data to {output_path}')
    return output_path

if __name__ == '__main__':
    run_pipeline(run_date = '2026-02-01')