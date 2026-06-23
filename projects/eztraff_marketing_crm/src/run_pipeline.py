from extract_marketing_api import extract_eztraff_data
from load_crm_csv import load_crm_data
from transform_data import transform_data
from bigquery_loader import bigquery_loader

FROM_DATE = '2025-07-01'
TO_DATE = '2025-12-31'

PROJECT_ID = 'eztraff-analytics'
CSV_PATH = f'data/processed/processed_data_{FROM_DATE}_{TO_DATE}.csv'
TABLE_ID = f'{PROJECT_ID}.raw.marketing_crm_q1_q2'

def main():
    df_mrkt = extract_eztraff_data(FROM_DATE, TO_DATE)
    df_crm = load_crm_data()

    df_transform = transform_data(df_mrkt, df_crm, FROM_DATE, TO_DATE)
    df_transform.to_csv(CSV_PATH, index=False)

    bigquery_loader(CSV_PATH, TABLE_ID, PROJECT_ID)

    print('Pipeline completed successfully.')

if __name__ == '__main__':
    main()