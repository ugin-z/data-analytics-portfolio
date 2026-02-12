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

    df = df.dropna(subset=['violation_description', 'year', 'fine_amount'])

    grouped = (
        df
        .groupby(['violation_description', 'year'], as_index=False)
        .agg(
            citations_count=('fine_amount', 'size'),
            total_fines_amount=('fine_amount', 'sum')
        )
    )

    grouped['avg_fine_amount'] = grouped['total_fines_amount'] / grouped['citations_count'].where(grouped['citations_count'] != 0)

    grouped['share_of_year_citations'] = (grouped['citations_count'] / grouped.groupby('year')['citations_count'].transform('sum'))
    grouped['share_of_year_fines'] = (grouped['total_fines_amount'] / grouped.groupby('year')['total_fines_amount'].transform('sum'))

    return grouped

def validate_mart_citations_year(df_mart):
    grain_cols = ['violation_description', 'year']

    grain_dups = df_mart.duplicated(subset=grain_cols).sum()
    assert grain_dups == 0, f'Found {grain_dups} duplicates for grain {grain_cols}'

    for col in grain_cols:
        nulls = df_mart[col].isna().sum()
        assert nulls == 0, f'Column {col} contains {nulls} null values in mart'

    bad_year = ((df_mart['year'] < 2021) | (df_mart['year'] > 2025)).sum()
    assert bad_year == 0, f'Found {bad_year} rows with year outside 2021..2025'

    bad_count = (df_mart['citations_count'] <= 0).sum()
    assert bad_count == 0, f"Found {bad_count} rows with citations_count <= 0"

    bad_amount = (df_mart['total_fines_amount'] < 0).sum()
    assert bad_amount == 0, "Negative values in total_fines_amount are present"

    if 'avg_fine_amount' in df_mart.columns:
        bad_avg = (df_mart['avg_fine_amount'].fillna(0) < 0).sum()
        assert bad_avg == 0, "Negative values in avg_fine_amount are present"

    eps = 1e-6
    for col in ['share_of_year_citations', 'share_of_year_fines']:
        assert col in df_mart.columns, f"Missing expected column: {col}"

        bad_share = (
            (df_mart[col] < -eps) |
            (df_mart[col] > 1 + eps)
        ).sum()
        assert bad_share == 0, f"Incorrect '{col}' values (outside [0,1])"

        s = df_mart.groupby('year')[col].sum()
        assert ((s - 1.0).abs() < 1e-3).all(), f"{col} does not sum to ~1 by year"

def save_mart_citations_year(df_mart, project_root):
    mart_dir = project_root / 'data' / 'mart'
    mart_dir.mkdir(parents=True, exist_ok=True)

    mart_path = mart_dir / 'mart_citations_year.parquet'
    df_mart.to_parquet(mart_path, index=False)

    print(f'Saved mart_citations_year to: {mart_path}')

# Build mart: mart_citations_year_month

def build_mart_citations_year_month(df_clean):
    required_cols = ['violation_description', 'month', 'year', 'fine_amount']

    for col in required_cols:
        if col not in df_clean.columns:
            raise KeyError(f"Expected column '{col}' is missing.")
        
    df = df_clean[required_cols].copy()

    df = df.dropna(subset=['violation_description', 'year', 'month'])

    grouped = (
        df
        .groupby(['violation_description', 'year', 'month'], as_index=False)
        .agg(
            citations_count=('fine_amount', 'size'),
            total_fines_amount=('fine_amount', 'sum')
        )
    )

    grouped['avg_fine_amount'] = grouped['total_fines_amount'] / grouped['citations_count'].where(grouped['citations_count'] != 0)

    grouped['share_of_year_citations'] = grouped['citations_count'] / grouped.groupby('year')['citations_count'].transform('sum')
    grouped['share_of_year_fines'] = grouped['total_fines_amount'] / grouped.groupby('year')['total_fines_amount'].transform('sum')

    return grouped

def validate_mart_citations_year_month(df_mart):
    grain_cols = ['violation_description', 'year', 'month']
    grain_dups = df_mart.duplicated(subset=grain_cols).sum()
    assert grain_dups == 0, f'Found {grain_dups} duplicates for grain {grain_cols}'

    for col in grain_cols:
        nulls = df_mart[col].isna().sum()
        assert nulls == 0, f'Column {col} contains {nulls} empty values in mart'

    bad_month = ((df_mart['month'] < 1) | (df_mart['month'] > 12)).sum()
    assert bad_month == 0, f'Found {bad_month} rows with month outside [1-12]'

    assert (df_mart['citations_count'] >= 0).all()
    assert (df_mart['total_fines_amount'] >= 0).all()

    eps = 1e-6
    for col in ['share_of_year_citations', 'share_of_year_fines']:
        bad = ((df_mart[col] < -eps) | (df_mart[col] > 1 + eps)).sum()
        assert bad == 0, f"Incorrect '{col}' values"

    s1 = df_mart.groupby('year')['share_of_year_fines'].sum()
    assert ((s1 - 1.0).abs() < 1e-3).all(), "share_of_year_fines does not sum to ~1 by year"

def save_mart_citations_year_month(df_mart, project_root):
    mart_dir = project_root / 'data' / 'mart'
    mart_dir.mkdir(parents=True, exist_ok=True)

    mart_path = mart_dir / 'mart_citations_year_month.parquet'
    df_mart.to_parquet(mart_path, index=False)

    print(f'Saved mart_citations_year_month to: {mart_path}')

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

    df_mart_citations_year_month = build_mart_citations_year_month(df_clean)
    print('mart_citations_year_month shape:', df_mart_citations_year_month.shape)
    print(df_mart_citations_year_month.head())
    validate_mart_citations_year_month(df_mart_citations_year_month)
    save_mart_citations_year_month(df_mart_citations_year_month, project_root)

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