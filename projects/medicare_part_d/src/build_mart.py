from pathlib import Path
import pandas as pd


def get_project_paths(run_date):

    project_root = Path(__file__).resolve().parents[1]

    data_dir = project_root / 'data'
    clean_dir = data_dir / 'clean'
    mart_dir = data_dir / 'mart'

    return {
        'clean_path': clean_dir / f'medicare_partd_provider_clean_{run_date}.parquet',
        'base_mart_path': mart_dir / f'mart_prescriber_drug_year_{run_date}.parquet',
        'prescriber_year_mart_path': mart_dir / f'mart_prescriber_year_{run_date}.parquet',
        'drug_year_mart_path': mart_dir / f'mart_drug_year_{run_date}.parquet',
        'mart_dir': mart_dir,
    }

def build_mart_prescriber_drug_year(df):

    mart = (
        df
        .groupby(['Prscrbr_NPI', 'Gnrc_Name', 'year'], as_index=False)
        .agg(
            total_claim_count=('Tot_Clms', 'sum'),
            total_drug_cost=('Tot_Drug_Cst', 'sum'),
        )
        .rename(columns={
            'Prscrbr_NPI': 'npi',
            'Gnrc_Name': 'generic_name',
        })
    )

    assert not mart.duplicated(
        subset=['npi', 'generic_name', 'year']
    ).any(), 'Duplicate rows found in mart_prescriber_drug_year'

    assert (mart['total_claim_count'] >= 0).all()
    assert (mart['total_drug_cost'] >= 0).all()

    return mart

def build_mart_prescriber_year(df_base):

    mart = (
        df_base
        .groupby(['npi', 'year'], as_index=False)
        .agg(
            total_claim_count=('total_claim_count', 'sum'),
            total_drug_cost=('total_drug_cost', 'sum'),
            distinct_drug_count=('generic_name', 'nunique'),
        )
        .sort_values('total_drug_cost', ascending=False)
        .reset_index(drop=True)
    )

    return mart

def build_mart_drug_year(df_base):

    mart = (
        df_base
        .groupby(['generic_name', 'year'], as_index=False)
        .agg(
            total_claim_count=('total_claim_count', 'sum'),
            total_drug_cost=('total_drug_cost', 'sum'),
            distinct_prescriber_count=('npi', 'nunique'),
        )
        .sort_values('total_drug_cost', ascending=False)
        .reset_index(drop=True)
    )

    assert not mart.duplicated(
        subset=['generic_name', 'year']
    ).any(), 'Duplicate rows found in mart_drug_year'

    return mart

def main():
    run_date = '2026-02-01'

    paths = get_project_paths(run_date)

    df_clean = pd.read_parquet(paths['clean_path'])
    df_clean['year'] = 2023

    base_mart = build_mart_prescriber_drug_year(df_clean)

    paths['mart_dir'].mkdir(parents=True, exist_ok=True)
    base_mart.to_parquet(paths['base_mart_path'], index=False)

    prescriber_year_mart = build_mart_prescriber_year(base_mart)
    prescriber_year_mart.to_parquet(
        paths['prescriber_year_mart_path'], index=False
    )

    drug_year_mart = build_mart_drug_year(base_mart)
    drug_year_mart.to_parquet(
        paths['drug_year_mart_path'], index=False
    )

if __name__ == '__main__':
    main()