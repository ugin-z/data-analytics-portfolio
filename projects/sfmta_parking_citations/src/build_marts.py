from pathlib import Path
import pandas as pd

# Load clean data

def load_clean_df(project_root):
    clean_path = project_root / 'data' / 'clean' / 'parking_citations_clean.parquet'
    if not clean_path.exists():
        raise FileNotFoundError(f'Clean parquet not found: {clean_path}')
    
    df = pd.read_parquet(clean_path)

    return df

# Build mart: mart_citations_year

def build_mart_citations_year(df_clean):
    required_cols = ['violation_description', 'year', 'fine_amount']

    for col in required_cols:
        if col not in df_clean.columns:
            raise KeyError(f"Expected column '{col}' is missing.")
        
    df = df_clean[required_cols].copy()

    df = df.dropna(subset=['violation_description', 'year'])

    grouped = (
        df
        .groupby(['violation_description', 'year'], as_index=False)
        .agg(
            citations_count=('violation_description', 'size'),
            total_fines_amount=('fine_amount', 'sum'),
            avg_fine_amount=('fine_amount', 'mean')
        )
    )

    total_citations = grouped['citations_count'].sum()
    total_fines = grouped['total_fines_amount'].sum()

    grouped['share_of_total_citations'] = grouped['citations_count'] / total_citations
    grouped['share_of_total_fines'] = grouped['total_fines_amount'] / total_fines

    return grouped

def validate_mart_citations_year(df_mart):
    grain_cols = ['violation_description', 'year']
    grain_dups = df_mart.duplicated(subset=grain_cols).sum()
    assert grain_dups == 0, f'Found {grain_dups} duplicates for grain {grain_cols}'

    for col in grain_cols:
        nulls = df_mart[col].isna().sum()
        assert nulls == 0, f'Column {col} contains {nulls} empty values in mart'

    if 'citations_count' in df_mart.columns:
        negative_fines = (df_mart['citations_count'] < 0).sum()
        assert negative_fines == 0, f"Negative values 'citations_count' are present."

    if 'total_fines_amount' in df_mart.columns:
        negative_amount = (df_mart['total_fines_amount'] < 0).sum()
        assert negative_amount == 0, f"Negative values 'total_fines_amount' are present."

    esp = 1e-6
    if 'share_of_total_citations' in df_mart.columns:
        bad_share_cit = (
            (df_mart['share_of_total_citations'] < -esp) |
            (df_mart['share_of_total_citations'] > 1 + esp)
        ).sum()
        assert bad_share_cit == 0, "Incorrect 'share_of_total_citations'"

    if 'share_of_total_fines' in df_mart.columns:
        bad_share = (
            (df_mart['share_of_total_fines'] < -esp) |
            (df_mart['share_of_total_fines'] > 1 + esp)
        ).sum()
        assert bad_share == 0, "Incorrect 'share_of_total_fines'"

def save_mart_citations_year(df_mart, project_root):
    mart_dir = project_root / 'data' / 'mart'
    mart_dir.mkdir(parents=True, exist_ok=True)

    mart_path = mart_dir / 'mart_citations_year.parquet'
    df_mart.to_parquet(mart_path, index=False)

    print(f'Saved mart_citations_year to: {mart_path}')

# Build mart: mart_state_year

def build_mart_state_year(df_clean):
    required_cols = ['vehicle_state', 'year', 'fine_amount']

    for col in required_cols:
        if col not in df_clean.columns:
            raise KeyError(f"Expected column '{col}' is missing.")
        
    df = df_clean[required_cols].copy()

    df['vehicle_state'] = df['vehicle_state'].str.upper()

    df['vehicle_state'] = df['vehicle_state'].fillna('UNKNOWN')

    df = df.dropna(subset=['year'])

    grouped = (
        df
        .groupby(['vehicle_state', 'year'], as_index=False)
        .agg(
            citations_count=('vehicle_state', 'size'),
            total_fines_amount=('fine_amount', 'sum'),
            avg_fine_amount=('fine_amount', 'mean')
        )
    )

    grouped['year_total_citations'] = grouped.groupby('year')['citations_count'].transform('sum')
    grouped['total_year_fines'] = grouped.groupby('year')['total_fines_amount'].transform('sum')

    grouped['share_of_total_citations'] = grouped['citations_count'] / grouped['year_total_citations']
    grouped['share_of_total_fines'] = grouped['total_fines_amount'] / grouped['total_year_fines']

    return grouped

def validate_mart_state_year(df_mart):
    grain_cols = ['vehicle_state', 'year']

    for col in grain_cols:
        nulls = df_mart[col].isna().sum()
        assert nulls == 0, f'Column {col} contains {nulls} empty values in mart'

    assert (df_mart['citations_count'] >= 0).all(), "Negative values 'citations_count' are present."
    assert (df_mart['total_fines_amount'] >= 0).all(), "Negative values 'total_fines_amount' are present."

    esp = 1e-6
    for col in ['share_of_total_citations', 'share_of_total_fines']:
        if col in df_mart.columns:
            bad_share = (
                (df_mart[col] < -esp) |
                (df_mart[col] > 1 + esp)
            ).sum()
            assert bad_share == 0, f"Incorrect '{col}'"

def save_mart_state_year(df_mart, project_root):
    mart_dir = project_root / 'data' / 'mart'
    mart_dir.mkdir(parents=True, exist_ok=True)

    mart_path = mart_dir / 'mart_state_year.parquet'
    df_mart.to_parquet(mart_path, index=False)

    print(f'Saved mart_state_year to: {mart_path}')

# Build mart: mart_citations_month

def build_mart_citations_month(df_clean):
    required_cols = ['year', 'month', 'fine_amount']

    for col in required_cols:
        if col not in df_clean.columns:
            raise KeyError(f"Expected column '{col}' is missing.")
        
    df = df_clean[required_cols].copy()

    df = df.dropna(subset=['month', 'year'])

    grouped = (
        df
        .groupby(['month', 'year'], as_index=False)
        .agg(
            citations_count=('month', 'size'),
            total_fines_amount=('fine_amount', 'sum'),
            avg_fine_amount=('fine_amount', 'mean')
        )
    )

    grouped['year_total_citations'] = grouped.groupby('year')['citations_count'].transform('sum')
    grouped['total_year_fines'] = grouped.groupby('year')['total_fines_amount'].transform('sum')

    grouped['share_of_total_citations'] = grouped['citations_count'] / grouped['year_total_citations']
    grouped['share_of_total_fines'] = grouped['total_fines_amount'] / grouped['total_year_fines']

    return grouped

def validate_mart_citations_month(df_mart):
    grain_cols = ['month', 'year']
    grain_dups = df_mart.duplicated(subset=grain_cols).sum()
    assert grain_dups == 0, f'Found {grain_dups} duplicates for grain {grain_cols}'

    for col in grain_cols:
        nulls = df_mart[col].isna().sum()
        assert nulls == 0, f'Column {col} contains {nulls} empty values in mart'

    assert (df_mart['citations_count'] >= 0).all(), "Negative values 'citations_count' are present."
    assert (df_mart['total_fines_amount'] >= 0).all(), "Negative values 'total_fines_amount' are present."

    esp = 1e-6
    for col in ['share_of_total_citations', 'share_of_total_fines']:
        if col in df_mart.columns:
            bad_share = (
                (df_mart[col] < -esp) |
                (df_mart[col] > 1 + esp)
            ).sum()
            assert bad_share == 0, f"Incorrect '{col}'"

def save_mart_citations_month(df_mart, project_root):
    mart_dir = project_root / 'data' / 'mart'
    mart_dir.mkdir(parents=True, exist_ok=True)

    mart_path = mart_dir / 'mart_citations_month.parquet'
    df_mart.to_parquet(mart_path, index=False)

    print(f'Saved mart_citations_month to: {mart_path}')

def main():
    project_root = Path(__file__).resolve().parents[1]

    df_clean = load_clean_df(project_root)
    print('Clean shape:', df_clean.shape)

    df_mart_citations = build_mart_citations_year(df_clean)
    print('mart_citations_year shape:', df_mart_citations.shape)
    print(df_mart_citations.head())
    validate_mart_citations_year(df_mart_citations)
    save_mart_citations_year(df_mart_citations, project_root)

    df_mart_state = build_mart_state_year(df_clean)
    print('mart_state_year shape:', df_mart_state.shape)
    print(df_mart_state.head())
    validate_mart_state_year(df_mart_state)
    save_mart_state_year(df_mart_state, project_root)

    df_mart_citations_month = build_mart_citations_month(df_clean)
    print('mart_citations_month shape:', df_mart_citations_month.shape)
    print(df_mart_citations_month.head())
    validate_mart_citations_month(df_mart_citations_month)
    save_mart_citations_month(df_mart_citations_month, project_root)

if __name__ == "__main__":
    main()