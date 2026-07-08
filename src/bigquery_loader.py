from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)

def get_bigquery_client(project):
    client = bigquery.Client(project=project)
    return client

def create_bigquery_table(gcs_uri, table_id, bigquery_client):
    table_object = bigquery.Table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:
        bigquery_client.get_table(table_id)
        logging.info(f'Table {table_id} already exists. Updating data.')
    except Exception:
        logging.info(f'Table {table_id} not found. Creating it now using schema from: {gcs_uri}')
        
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )    
    
    load_job.result()
    
    destination_table = bigquery_client.get_table(table_id)
    logging.info(f'Table sync complete. Total rows in table: {destination_table.num_rows}')
    
    
if __name__ == '__main__':
    import config
    import collector
    import cloud_storage
    from storage import save_to_parquet
    from pathlib import Path
    from datetime import date

    config_data = config.load_pipeline_config()
    api_key = config_data['api_key']
    tickers = config_data['tickers']
    
    project = 'market-intelligence-pipeline'
    bucket_name = 'market-intelligence-pipeline-bucket'
    dataset = 'market_intelligence'
    table = 'market-intelligence-raw'
    
    storage_client = cloud_storage.get_gcs_client(project)
    bigquery_client = get_bigquery_client(project)
    
    table_id = project+"."+dataset+"."+table

    cloud_storage.create_gcp_bucket(bucket_name, storage_client)
    
    uploaded = False
    
    for ticker in tickers:
        raw = collector.fetch_daily_prices(ticker, api_key)
        prices = collector.transform_api_response(raw)
        logging.info(f'{ticker}: {len(prices)} records fetched')
        output_dict = save_to_parquet(prices, ticker)
        output = output_dict["dir"] / output_dict["filename"]
        logging.info(f'API response succesfully saved to {output}')

        if output.exists():
            gcs_uri = cloud_storage.upload_to_gcs (output, bucket_name, ticker, storage_client)
            uploaded = True
        else:
            logging.info(f'{output} doesn\'t exist.')
    
    today = date.today().isoformat()
    today = 'date=' + today
    destination_blob_name = f"daily_prices/{today}/*.parquet"    
    uri = f"gs://{bucket_name}/{destination_blob_name}"
    
    try:
        bigquery_loader.create_bigquery_table(uri, table_id, bigquery_client)
    except Exception as e:
        logging.error(f'Failed to create/upload to a table: {e}')