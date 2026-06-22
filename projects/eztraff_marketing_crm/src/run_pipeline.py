from extract_marketing_api import extract_eztraff_data
from load_crm_csv import load_crm_data
from transform_data import transform_data

from_date = '2025-07-01'
to_date = '2025-12-31'

def main():
    df_mrkt = extract_eztraff_data(from_date, to_date)
    df_crm = load_crm_data()
    df_transform = transform_data(df_mrkt, df_crm, from_date, to_date)
    df_transform.to_csv(f'data/clean/clean_data_{from_date}_{to_date}.csv', index=False)

    print('Pipeline completed successfully.')

if __name__ == '__main__':
    main()