from pathlib import Path
import pandas as pd
from google.cloud import bigquery

def bigquery_loader(
        csv_path,
        table_id,
        project_id,
        write_disposition='WRITE_TRUNCATE'
):

    client = bigquery.Client(project=project_id)

    df = pd.read_csv(csv_path)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
    )

    job.result()

    print(f'Loaded {job.output_rows} rows to {table_id}')
