import logging
import pandas as pd

logger = logging.getLogger(__name__)

MARKETING_COLUMN_MAP = {
    'id': 'lead_id',
    'offer': 'offer_id',
    'offername': 'offer_name',
    'wm': 'wm_id',
    'status': 'status_name',
    'reason': 'reason',
    'phase': 'phase',
    'site': 'site_id',
    'siteurl': 'site_url',
    'ip': 'ip',
    'time': 'time_start',
    'done': 'time_finish',
    'paid': 'paid',
    'name': 'name',
    'gender': 'gender',
    'phone': 'phone',
    'country': 'country',
    'currency': 'currency',
    'tracking.source': 'ad_channel_name',
    'tracking.campaign': 'ad_feed_type',
    'tracking.content': 'ad_id',
    'tracking.term': 'ad_placement_id',
    'tracking.medium': 'ad_medium'
}

MARKETING_COLUMNS_KEEP = [
    'lead_id',
    'offer_id',
    'offer_name',
    'wm_id',
    'status_name',
    'reason',
    'phase',
    'site_id',
    'site_url',
    'ip',
    'time_start',
    'time_finish',
    'paid',
    'name',
    'gender',
    'phone',
    'country',
    'currency',
    'ad_channel_name',
    'ad_feed_type',
    'ad_id',
    'ad_placement_id',
    'ad_medium'
]

CRM_COLUMN_MAP = {
    0: 'first_name',
    1: 'last_name',
    2: 'phone',
    3: 'delivery_address',
    4: 'delivery_region',
    5: 'delivery_city',
    6: 'order_status',
    7: 'cancel_reason',
    10: 'delivery_status',
    12: 'date_lead',
    13: 'date_processed',
    14: 'date_dispatched',
    16: 'date_paid',
    17: 'gender',
    18: 'age',
    21: 'ip',
    23: 'lead_id',
    24: 'webmaster_id',
    26: 'manager_name',
    28: 'sales_dep_id',
    29: 'geo',
    34: 'offer_name',
    38: 'total_cost'
}

CRM_COLUMNS_KEEP = [
    'lead_id',
    'first_name',
    'last_name',
    'phone',
    'delivery_address',
    'delivery_region',
    'delivery_city',
    'order_status',
    'cancel_reason',
    'delivery_status',
    'date_lead',
    'date_processed',
    'date_dispatched',
    'date_paid',
    'gender',
    'age',
    'ip',
    'webmaster_id',
    'manager_name',
    'sales_dep_id',
    'geo',
    'offer_name',
    'total_cost'
]

CRM_DATE_COLUMNS = [
    'date_lead',
    'date_processed',
    'date_dispatched',
    'date_paid'
]

OFFER_COLUMNS = [
    'offer_id',
    'geo_payout',
    'payout'
]

def require_columns(df, required_cols, dataset_name):
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f'{dataset_name} is missing required columns: {missing_cols}')

def require_not_empty(df, dataset_name):
    if df.empty:
        raise ValueError(f'{dataset_name} is empty.')

def normalize_phone(series):
    return (
        series
        .astype(str)
        .str.replace(r'\D', '', regex=True)
    )

def processed_data_mrkt(df):

    df = df.copy()

    df = df.rename(columns=MARKETING_COLUMN_MAP)

    df = df[MARKETING_COLUMNS_KEEP]

    if 'lead_id' in df.columns:
        df = df.drop_duplicates(subset=['lead_id'])

    if 'time_start' in df.columns:
        df['time_start'] = pd.to_numeric(df['time_start'], errors='coerce')
        df['time_start'] = pd.to_datetime(df['time_start'], unit='s', errors='coerce')

    if 'time_finish' in df.columns:
        df['time_finish'] = pd.to_numeric(df['time_finish'], errors='coerce')
        df['time_finish'] = pd.to_datetime(df['time_finish'], unit='s', errors='coerce')

    if 'phone' in df.columns:
        df['phone'] = (
            df['phone']
            .astype(str)
            .str.replace(r'\D', '', regex=True)
        )

    return df

def processed_data_crm(df):

    df = df.copy()

    df.columns = range(len(df.columns))

    df = df.rename(columns=CRM_COLUMN_MAP)

    df = df[CRM_COLUMNS_KEEP]

    for col in CRM_DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col],
                format='%d.%m.%Y %H:%M:%S',
                errors='coerce'
            )

    if 'lead_id' in df.columns:
        df = df.drop_duplicates(subset=['lead_id'])

    if 'phone' in df.columns:
        df['phone'] = (
            df['phone']
            .astype(str)
            .str.replace(r'\D', '', regex=True)
        )

    return df

def processed_data_crm_filter(df, from_date, to_date):

    df = df[
        (df['date_lead'] >= from_date) &
        (df['date_lead'] <= to_date)
    ]

    return df

def validate_data_mrkt(df):

    required_cols = [
        'lead_id',
        'wm_id',
        'offer_id',
        'time_start'
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f'Missing columns in Marketing Data: {missing_cols}')

    if df.empty:
        raise ValueError('Marketing Data is empty.')

    if df['lead_id'].duplicated().any():
        raise ValueError('Marketing Data has duplicate Lead Ids.')

    if df['lead_id'].isna().any():
        raise ValueError('Marketing Data has missing Lead Ids.')

    if df['time_start'].isna().any():
        raise ValueError('Marketing Data has missing Time Start values.')

    return df

def validate_data_crm(df):

    required_cols = [
        'lead_id',
        'date_lead'
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f'Missing columns in CRM Data: {missing_cols}')

    missing_leads = df['lead_id'].isna().sum()

    if df['lead_id'].isna().any():
        logger.warning(f'CRM Data has missing {missing_leads} Lead Ids.')

    if df.empty:
        raise ValueError('CRM Data is empty.')

    if df['lead_id'].duplicated().any():
        raise ValueError('CRM Data has duplicate Lead Ids.')

    if df['date_lead'].isna().any():
        raise ValueError('CRM Data has missing Date Lead values.')

    return df

def merge_data(df_a, df_b):
    df = pd.merge(
        df_a,
        df_b,
        on='lead_id',
        how='left'
    )

    return df

def merged_data_validate(df_final, df_a):

    if df_final.empty:
        raise ValueError('Merged Data is empty.')

    if len(df_final) != len(df_a):
        raise ValueError(f'Row count changed after merge: before={len(df_a)}, after={len(df_final)}')

    unmatched = df_final['order_status'].isna().sum()

    logger.info(f'Unmached Marketing Rows: {unmatched}')
    logger.info(f'Match Rate: {(1 - unmatched / len(df_a)):.2%}')

    if unmatched / len(df_a) > 0.3:
        raise ValueError('More than 30% of Marketing Data rows are unmatched after merge.')

def processed_data_offer(data):

    rows = []

    if isinstance(data, dict):
        offers = data.values()
    else:
        offers = data

    for offer in offers:

        if not isinstance(offer, dict):
            continue

        if 'geo' not in offer:
            continue

        for geo in offer['geo'].values():
            offer_id = offer['id']
            geo_payout = geo['code']
            payout = geo['price']

            rows.append({
                'offer_id': offer_id,
                'geo_payout': geo_payout,
                'payout': payout
            })

    df = pd.DataFrame(rows, columns=OFFER_COLUMNS)
    df = df.drop_duplicates(subset=['offer_id', 'geo_payout'])

    return df

def validate_data_offer(df):

    required_cols = [
        'offer_id',
        'geo_payout',
        'payout'
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise ValueError(f'Missing columns in Offer List: {missing_cols}')

    missing_leads = df['offer_id'].isna().sum()

    if df['offer_id'].isna().any():
        raise ValueError(f'Warning: Offer List has missing {missing_leads} Offer IDs.')

    if df.empty:
        raise ValueError('Offer List is empty.')

    if df[['offer_id', 'geo_payout']].duplicated().any():
        raise ValueError('Offer List has duplicate Offer ID + GEO rows.')

    return df

def transform_data(df_mrkt, df_crm, data_offers, from_date, to_date):

    df_mrkt_processed = processed_data_mrkt(df_mrkt)
    df_crm_processed = processed_data_crm(df_crm)
    df_offer_processed = processed_data_offer(data_offers)

    df_crm_processed_filtered = processed_data_crm_filter(df_crm_processed, from_date, to_date)

    validate_data_mrkt(df_mrkt_processed)
    validate_data_crm(df_crm_processed_filtered)
    validate_data_offer(df_offer_processed)

    df_processed = merge_data(df_mrkt_processed, df_crm_processed_filtered)
    merged_data_validate(df_processed, df_mrkt_processed)

    obtained = {
        'df_processed': df_processed,
        'df_offer_processed': df_offer_processed
    }

    return obtained