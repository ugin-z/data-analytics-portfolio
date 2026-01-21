import pandas as pd

from src.ingest import load_partd, load_provider
from src.dq_checks import (
    run_partd_checks,
    run_provider_checks,
    run_merged_checks
)

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR = PROJECT_ROOT/ 'data' / 'clean'
CLEAN_DIR.mkdir(parents = True, exist_ok = True)

def run_pipeline():
    # load
    partd_df = load_partd()
    provider_df = load_provider()

    # checks
    run_partd_checks(partd_df)
    run_provider_checks(provider_df)

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
    output_path = CLEAN_DIR / 'medicare_partd_provider_clean.csv'
    merged_df.to_csv(output_path, index = False)

    print(f'Saved clean merged data to {output_path}')

if __name__ == '__main__':
    run_pipeline()
