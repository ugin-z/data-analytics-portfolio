from __future__ import annotations
import requests
import time
import json
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / 'data' / 'raw' / 'cms_partd'
RAW_DATA_DIR.mkdir(parents = True, exist_ok = True)

BASE_URL = 'https://data.cms.gov/data-api/v1/dataset'
UUID = '9552739e-3d05-4c1b-8eff-ecabf391e2e5'

MAX_RETRIES = 5
BASE_SLEEP = 2
PAGE_SIZE = 5000

def today_str() -> str:
    return datetime.today().strftime('%Y-%m-%d')

def sleep_with_backoff(attempt: int, retry_after: int | None, reason: str) -> None:

    if retry_after is not None:
        sleep_time = retry_after
    else:
        sleep_time = BASE_SLEEP * (2 ** (attempt - 1))

    print(f'{reason}. Sleeping {sleep_time}s (attempt {attempt} / {MAX_RETRIES})...')
    time.sleep(sleep_time)

def fetch_page(offset: int = 0, limit: int = 5000) -> list[dict]:
    
    headers = {
    'accept': 'application/json'
    }

    params = {
        'offset': offset,
        'size': limit
    }
    
    url = f'{BASE_URL}/{UUID}/data'

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                headers = headers,
                params = params,
                timeout = 50
            )

            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                retry_after = int(retry_after) if retry_after else None

                if attempt == MAX_RETRIES:
                    print('Got 429 too many times, giving up.')
                    raise requests.exceptions.HTTPError(response = response)
                
                sleep_with_backoff(attempt, retry_after, 'Got 429 (rate limited)')
                continue

            response.raise_for_status()
            data = response.json()
        
            if isinstance(data, list):
                return data
            else:
                return [data]
        
        except requests.exceptions.Timeout:
            if attempt == MAX_RETRIES:
                print('Request timed out, giving up.')
                raise

            sleep_with_backoff(attempt, None, 'Timeout')

        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                print('Request failed, giving up.')
                raise

            sleep_with_backoff(attempt, None, f'Request failed {e}')
    
    return []

def dump_raw_pages(max_rows: int | None = None, run_date: str | None = None) -> Path:
    if run_date is None:
        run_date = today_str()

    run_dir = RAW_DATA_DIR / run_date
    run_dir.mkdir(parents = True, exist_ok = True)

    offset = 0
    page_num = 1
    rows_fetched = 0

    while True:
        page = fetch_page(offset = offset, limit = PAGE_SIZE)

        if not page:
            break

        if max_rows is not None:
            remaining = max_rows - rows_fetched

            if remaining <= 0:
                break

            if len(page) > remaining:
                page = page[:remaining]

        output_path = run_dir / f'page_{page_num:05d}.json'

        with output_path.open('w', encoding = 'utf-8') as f:
            json.dump(page, f)

        print(f'Saved page {page_num} with {len(page)} rows to {output_path}')

        rows_fetched += len(page)
        page_num += 1
        offset += len(page)

        if max_rows is not None and rows_fetched >= max_rows:
            break

    print(f'Total rows fetched (raw pages): {rows_fetched}')
    return run_dir

if __name__ == '__main__':
    run_dir = dump_raw_pages(max_rows = None)
    print(f'Raw pages saved under: {run_dir}')
