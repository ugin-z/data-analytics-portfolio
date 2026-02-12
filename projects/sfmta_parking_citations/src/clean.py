import pandas as pd
from pathlib import Path
import datetime as dt

def load_raw_df(project_root):

    raw_path = project_root / 'data' / 'raw'
    parquet_path = raw_path / 'parking_citations_raw.parquet'

    df = pd.read_parquet(parquet_path)

    return df
    
def select_and_rename_columns(df):

    column_mapping = {
        'Citation Number': 'citation_id',
        'Citation Issued DateTime': 'issued_date_raw',
        'Violation Description': 'violation_description',
        'Violation': 'violation_code',
        'Fine Amount': 'fine_amount',
        'Citation Location': 'location',
        'Vehicle Plate State': 'vehicle_state',
        'Latitude': 'latitude',
        'Longitude': 'longitude'
    }

    existing_mapping = {src : dst for src, dst in column_mapping.items() if src in df.columns}

    df = df[list(existing_mapping.keys())].rename(columns = existing_mapping)

    return df

def coerce_dtypes(df):

    if 'issued_date_raw' in df.columns:
        
        df['issued_date_raw'] = pd.to_datetime(df['issued_date_raw'], errors = 'coerce')
        df['issued_date'] = df['issued_date_raw'].dt.date
        df['month'] = df['issued_date_raw'].dt.month
        df['year'] = df['issued_date_raw'].dt.year

    if 'fine_amount' in df.columns:

        df['fine_amount'] = pd.to_numeric(df['fine_amount'], errors = 'coerce')

    if 'citation_id' in df.columns:

        df['citation_id'] = df['citation_id'].astype('string')

    for col in ['violation_description', 'violation_code', 'location', 'vehicle_state', 'latitude', 'longitude']:
        if col in df.columns:
            df[col] = df[col].astype('string')
    
    return df 

def save_clean_df(df, project_root):

    clean_dir = project_root / 'data' / 'clean'
    clean_dir.mkdir(parents = True, exist_ok = True)

    clean_path = clean_dir / 'parking_citations_clean.parquet'
    df.to_parquet(clean_path, index = False)

    print(f'Saved clean data to: {clean_path}')

def basic_dg_checks(df):
    if 'citation_id' in df.columns:
        missing_ids = df['citation_id'].isna().sum()
        assert missing_ids == 0, f'Missing citation_id: {missing_ids}'

    if 'fine_amount' in df.columns:
        negative_fines = (df['fine_amount'] < 0).sum()
        assert negative_fines == 0, 'Negative fines!!!'

    if 'issued_date' in df.columns:
        missing_date_ratio = df['issued_date'].notna().mean()
        assert missing_date_ratio > 0.95, f"Too many empty 'issued_date'. Filled in: {missing_date_ratio:.2%}"

def main():
    project_root = Path(__file__).resolve().parents[1]
    
    df_raw = load_raw_df(project_root)
    df_clean = select_and_rename_columns(df_raw)
    df_clean = coerce_dtypes(df_clean)

    basic_dg_checks(df_clean)

    print('Clean shape:', df_clean.shape)
    print(df_clean[[
        'citation_id', 
        'fine_amount', 
        'issued_date', 
        'month', 
        'year', 
        'violation_description', 
        'violation_code', 
        'location', 
        'vehicle_state', 
        'latitude', 
        'longitude'
    ]].head())
    print(df_clean.dtypes)

    save_clean_df(df_clean, project_root)

if __name__ == '__main__':
    main()

    