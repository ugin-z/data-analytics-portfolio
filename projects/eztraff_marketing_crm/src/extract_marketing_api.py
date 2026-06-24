import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path

def date_to_unix(date_str):
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def extract_eztraff_data(from_date, to_date):

    load_dotenv()

    API_KEY = os.getenv('API_KEY_COMP')
    BASE_URL = os.getenv('BASE_URL')

    endpoint = 'comp/list.json?id='
    url = BASE_URL + endpoint + API_KEY

    payload = {
        'from': date_to_unix(from_date),
        'to': date_to_unix(to_date)
    }

    response = requests.post(
        url,
        data=payload
    )

    response.raise_for_status()

    data = response.json()

    df = pd.json_normalize(data)
    
    raw_dir = Path('data/raw')
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_path = raw_dir / f'marketing_data_{from_date}_{to_date}.csv'
    df.to_csv(output_path, index=False)

    return df