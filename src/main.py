if __name__ == '__main__':
    import config
    import collector
    import cloud_storage
    import logging
    import bigquery_loader
    import schemas
    import time
    from storage import save_to_parquet
    from pathlib import Path
    from datetime import date


    logging.basicConfig(level=logging.INFO)

    config_data = config.load_pipeline_config()
    api_key = config_data['api_key']
    tickers = config_data['tickers']
    
    project = 'market-intelligence-pipeline'
    bucket_name = 'market-intelligence-pipeline-bucket'
    dataset = 'market_intelligence'
    table = 'market-intelligence-raw'

    table_id = project+"."+dataset+"."+table
    
    storage_client = cloud_storage.get_gcs_client(project)
    bigquery_client = bigquery_loader.get_bigquery_client(project)

    cloud_storage.create_gcp_bucket(bucket_name, storage_client)
    
    uploaded = False
    
    for ticker in tickers:
        try:
            raw = collector.fetch_daily_prices(ticker, api_key)
            prices = collector.transform_api_response(raw)
            logging.info(f'{ticker}: {len(prices)} records fetched')
            output_dict = save_to_parquet(prices, ticker)
            output = output_dict["dir"] / output_dict["filename"]
            logging.info(f'API response succesfully saved to {output}')
            time.sleep(15)

            if output.exists():
                gcs_uri = cloud_storage.upload_to_gcs (output, bucket_name, ticker, storage_client)
                uploaded = True
            else:
                logging.info(f'{output} doesn\'t exist.')
        
            logging.info(f'Succesfully processed {ticker}.')
            print('\n','='*80)
        
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            print('\n','='*80)
            
    today = date.today().isoformat()
    today = 'date=' + today
    destination_blob_name = f"daily_prices/{today}/*.parquet"    
    uri = f"gs://{bucket_name}/{destination_blob_name}"
    
    try:
        bigquery_loader.create_bigquery_table(uri, table_id, bigquery_client)
    except Exception as e:
        logging.error(f'Failed to create/upload to a table: {e}')