from google.cloud import storage
from datetime import date
import logging


logging.basicConfig(level=logging.INFO)

def get_gcs_client(project):
    storage_client = storage.Client(project=project)
    return storage_client

def create_gcp_bucket(bucket_name, storage_client):
    if storage_client.bucket(bucket_name).exists():
        logging.info(f'Bucket {bucket_name} already exists.')
    else:
        bucket = storage_client.create_bucket(bucket_name)
        logging.info(f'Succesfully created {bucket_name}')


def upload_to_gcs(local_path: str, bucket_name: str, symbol: str, storage_client) -> str:
    today = date.today().isoformat()
    today = 'date=' + today

    destination_blob_name = f"daily_prices/{today}/{symbol}.parquet"
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob_name = f"gs://{bucket_name}/{destination_blob_name}"

    if blob.exists():
        logging.info(f'{destination_blob_name} already exist.')
    else:
        blob.upload_from_filename(local_path)
        logging.info(f'Response uploaded to {blob_name}')
    
    return blob_name


if __name__ == '__main__':
    import config
    import collector
    from pathlib import Path
    from storage import save_to_parquet

    config_data = config.load_pipeline_config()
    api_key = config_data['api_key']
    tickers = config_data['tickers']
    project = 'market-intelligence-pipeline'

    storage_client = get_gcs_client(project)

    bucket_name = 'market-intelligence-pipeline-bucket'
    create_gcp_bucket(bucket_name, storage_client)

    for ticker in tickers:
        raw = collector.fetch_daily_prices(ticker, api_key)
        prices = collector.transform_api_response(raw)
        logging.info(f'{ticker}: {len(prices)} records fetched')
        output_dict = save_to_parquet(prices, ticker)
        output = output_dict["dir"] / output_dict["filename"]
        logging.info(f'API response succesfully saved to {output}')

        if output.exists():
            upload_to_gcs (output, bucket_name, ticker, storage_client)
        else:
            logging.info(f'{output} doesn\'t exist.')