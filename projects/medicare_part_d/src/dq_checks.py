from __future__ import annotations
import pandas as pd

# ========== PART D CHECKS ==========

PARTD_REQUIRED_COLS = [
    'Prscrbr_NPI',
    'Prscrbr_Last_Org_Name',
    'Prscrbr_First_Name',
    'Prscrbr_City',
    'Prscrbr_State_Abrvtn',
    'Prscrbr_State_FIPS',
    'Prscrbr_Type',
    'Prscrbr_Type_Src',
    'Brnd_Name',
    'Gnrc_Name',
    'Tot_Clms',
    'Tot_30day_Fills',
    'Tot_Day_Suply',
    'Tot_Drug_Cst',
    'Tot_Benes',
    'GE65_Sprsn_Flag',
    'GE65_Tot_Clms',
    'GE65_Tot_30day_Fills',
    'GE65_Tot_Drug_Cst',
    'GE65_Tot_Day_Suply',
    'GE65_Bene_Sprsn_Flag',
    'GE65_Tot_Benes',
]

PARTD_NUMERIC_COLS = [
    'Tot_Clms',
    'Tot_30day_Fills',
    'Tot_Day_Suply',
    'Tot_Drug_Cst',
    'Tot_Benes',
    'GE65_Tot_Clms',
    'GE65_Tot_30day_Fills',
    'GE65_Tot_Drug_Cst',
    'GE65_Tot_Day_Suply',
    'GE65_Tot_Benes',
]

def run_partd_checks(df):
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

    missing = [c for c in PARTD_REQUIRED_COLS if c not in df.columns]
    assert not missing

    for c in PARTD_NUMERIC_COLS:
        col = df[c].astype('string').str.strip()
        original_missing = col.isna() | (col == '')
        coerced = pd.to_numeric(df[c], errors = 'coerce')
        bad_mask = coerced.isna() & ~original_missing
        bad_count = bad_mask.sum()

        if bad_count > 0:
            example = col[bad_mask].unique()[:10]
            raise AssertionError(
                f'Column {c} has non-numeric values (excluding empty-as-missing).'
                f'Bad count: {bad_count}. Examples: {list(example)}'
            )
        
        df[c] = coerced
    
    print('run_partd_checks: numeric columns OK (empty strings treated as missing)')

    npi = df['Prscrbr_NPI']
    npi_str = npi.astype('string').str.replace(r'\.0$', '', regex = True).str.strip()
    npi_digits = npi_str.str.fullmatch(r'\d{10}')
    assert npi_str.notna().all(), 'Prscrbr_NPI has missing values'

    bad_npi = (~npi_digits).sum()
    assert bad_npi == 0, f'Prscrbr_NPI column has invalid format (expected 10 digits). Bad rows: {bad_npi}'

# ========== PROVIDER CHECKS ==========

PROVIDER_REQUIRED_COLS = [
    'Tot_Srvcs',
    'Tot_Benes',
    'Tot_Mdcr_Pymt_Amt',
    'Tot_Mdcr_Alowd_Amt',
    'Rndrng_NPI'
]

PROVIDER_NUMERIC_COLS = [
    'Tot_Srvcs',
    'Tot_Benes',
    'Tot_Mdcr_Pymt_Amt',
    'Tot_Mdcr_Alowd_Amt'
]

def run_provider_checks(df):
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

    missing = [c for c in PROVIDER_REQUIRED_COLS if c not in df.columns]
    assert not missing

    for c in PROVIDER_NUMERIC_COLS:
        coerced = pd.to_numeric(df[c], errors = 'coerce')
        bad_count = coerced.isna().sum() - df[c].isna().sum()
        assert bad_count == 0, f'Column {c} has non-numeric values'

    npi = df['Rndrng_NPI']
    npi_str = npi.astype('string').str.replace(r'\.0$', '', regex = True).str.strip()
    npi_digits = npi_str.str.fullmatch(r'\d{10}')
    assert npi_str.notna().all(), 'Rndrng_NPI has missing values'

    npi_bad = (~npi_digits).sum()
    assert npi_bad == 0, f'Rndrng_NPI column has invalid format (expected 10 digits). Bad rows: {npi_bad}'

# ========== MERGED CHECKS ==========

PROVIDER_COLS = [
    'Prov_Tot_Srvcs',
    'Prov_Tot_Benes',
    'Prov_Tot_Mdcr_Pymt_Amt',
    'Prov_Tot_Mdcr_Alowd_Amt'
]

def run_merged_checks(partd_df: pd.DataFrame, merged_df: pd.DataFrame) -> None:
    assert len(merged_df) == len(partd_df), (
    f'Row count changed after merge:'
    f'partd = {len(partd_df)}, merged = {len(merged_df)}'
    )

    available_provider_cols = [c for c in PROVIDER_COLS if c in merged_df.columns]
    assert available_provider_cols, 'No provider columns found in merged df'

    provider_na_rate = (
        merged_df[available_provider_cols].isna().all(axis = 1).mean()
    )
    print(f'Provider match missing rate: {provider_na_rate:.2%}')

    if provider_na_rate > 0.2:
        print('WARNING: More than 20% of Part D rows have no matched provider data')

    key_cols = ['Prscrbr_NPI', 'Gnrc_Name']
    dupl_before = partd_df.duplicated(subset = key_cols).sum()
    dupl_after = merged_df.duplicated(subset = key_cols).sum()
    dupl_diff = dupl_after - dupl_before

    print(
        f'Duplicate rows by key {key_cols}: '
        f'before = {dupl_before}, after = {dupl_after}, diff = {dupl_diff}'
    )

    print('Merged checks passed')


