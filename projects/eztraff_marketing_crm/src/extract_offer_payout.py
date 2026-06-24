import os
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

def extract_offer_payout(date):

    load_dotenv()

    API_KEY = os.getenv('API_KEY_WM')
    BASE_URL = os.getenv('BASE_URL')

    endpoint = 'wm/offers.json?id='
    url = BASE_URL + endpoint + API_KEY

    response = requests.post(url)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data)
    
    raw_dir = Path('data/raw')
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_path = raw_dir / f'offer_payout_{date}.csv'
    df.to_csv(output_path, index=False)

    return data