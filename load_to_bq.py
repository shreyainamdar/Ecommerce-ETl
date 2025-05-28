from google.cloud import bigquery
import pandas as pd
import os

def load_data_to_bigquery():

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_key.json"

    client = bigquery.Client()

    # Load cleaned data
    df = pd.read_csv("output/cleaned_data.csv")

    # Define table ID
    table_id = "ecommerce-etl-project-459315.ecommerce_dataset.fact_sales"

    # Define schema manually or auto-infer
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
    )

    # Upload to BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print("✅ Data uploaded to BigQuery.")
