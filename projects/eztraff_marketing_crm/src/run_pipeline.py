import logging
from extract_marketing_api import extract_eztraff_data
from load_crm_csv import load_crm_data
from transform_data import transform_data
from load_to_bigquery import load_to_bigquery
from extract_offer_payout import extract_offer_payout
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)

logger = logging.getLogger(__name__)

FROM_DATE = '2025-07-01'
TO_DATE = '2025-12-31'
CURRENT_DATE = date.today()

PROJECT_ID = 'eztraff-analytics'

CSV_PATH_PROCESSED = f'data/processed/processed_data_{FROM_DATE}_{TO_DATE}.csv'
CSV_PATH_OFFER = f'data/processed/processed_offer_payout_{CURRENT_DATE}.csv'

TABLE_ID_PROCESSED = f'{PROJECT_ID}.raw.marketing_crm_q1_q2'
TABLE_ID_OFFER = f'{PROJECT_ID}.raw.offer_payout'

def main():
    df_mrkt = extract_eztraff_data(FROM_DATE, TO_DATE)
    df_crm = load_crm_data()
    data_offers = extract_offer_payout(CURRENT_DATE)

    df_transform = transform_data(
        df_mrkt,
        df_crm,
        data_offers,
        FROM_DATE,
        TO_DATE
    )

    df_transform['df_processed'].to_csv(CSV_PATH_PROCESSED, index=False)
    df_transform['df_offer_processed'].to_csv(CSV_PATH_OFFER, index=False)

    load_to_bigquery(CSV_PATH_PROCESSED, TABLE_ID_PROCESSED, PROJECT_ID)
    load_to_bigquery(CSV_PATH_OFFER, TABLE_ID_OFFER, PROJECT_ID)

    logger.info('Pipeline completed successfully.')

if __name__ == '__main__':
    main() 