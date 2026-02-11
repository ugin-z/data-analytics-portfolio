import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = PROJECT_ROOT / 'data' / 'raw'
CSV_PATH = RAW_PATH / 'sfmta_parking_citations.csv'
PARQUET_PATH = RAW_PATH / 'parking_citations_raw.parquet'

def main():

    if not CSV_PATH.exists():
        raise FileNotFoundError(f'CSV hot founr: {CSV_PATH}')
    
    df = pd.read_csv(CSV_PATH)

    print('Rows read:', len(df))

    RAW_PATH.mkdir(parents = True, exist_ok = True)
    df.to_parquet(PARQUET_PATH, index = False)

    print('Saved to:', PARQUET_PATH)



if __name__ == '__main__':
    main()