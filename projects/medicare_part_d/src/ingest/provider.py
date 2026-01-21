# %%
import pandas as pd
import requests
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / 'data' / 'raw'
RAW_DATA_DIR.mkdir(parents = True, exist_ok = True)

BASE_URL = 'https://data.cms.gov/data-api/v1/dataset'
UUID = '8889d81e-2ee7-448f-8713-f071038289b5'

def fetch_page(offset: int = 0, limit: int = 5000) -> list[dict]:
    
    headers = {
    'accept': 'application/json'
    }

    params = {
        'offset': offset,
        'size': limit
    }
    
    url = f'{BASE_URL}/{UUID}/data'
    
    response = requests.get(
        url,
        headers = headers,
        params = params,
        timeout = 50
    )

    if response.status_code == 429:
        time.sleep(5)
        return None

    response.raise_for_status()
    return response.json()

def fetch_all(max_rows: int | None = None):

    pre_data = []
    offset = 0
    limit = 5000

    while True:
        page = fetch_page(offset = offset, limit = limit)

        if page is None:
            continue

        if not page:
            break

        if max_rows is not None and len(pre_data) >= max_rows:
            pre_data = pre_data[:max_rows]
            break

        offset += limit
        pre_data.extend(page)

    return pd.DataFrame(pre_data)


def save_raw(df: pd.DataFrame):
    
    run_date = datetime.today().strftime('%Y-%m-%d')
    output_raw = RAW_DATA_DIR / f'raw_date_provider_{run_date}.csv'
    df.to_csv(output_raw, index = False)
    print(f'Saved raw data to {output_raw}')
    return output_raw

def load_provider(max_rows: int | None = None) -> pd.DataFrame:

    df = fetch_all(max_rows = 20000)
    save_raw(df)
    return df

if __name__ == '__main__':
    df = load_provider(max_rows = 20000)
    print(f'Rows fetched: {len(df)}')
    print(df.head())
