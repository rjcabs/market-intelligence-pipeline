from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import config
import collector
import cloud_storage
import bigquery_loader
import logging
import time
import os
import subprocess
import google.auth
import google.auth.transport.requests

from storage import save_to_parquet
from pathlib import Path

def run_dbt_command(command_type="run"):
    credentials, project = google.auth.default()
    auth_request = google.auth.transport.requests.Request()
    credentials.refresh(auth_request)
    
    # Inject the temporary token into the container's environment memory
    env = os.environ.copy()
    env["DBT_BIGQUERY_TOKEN"] = credentials.token
    
    dbt_path = "/opt/airflow/project/market_intelligence_dbt"
    
    # Run the dbt command inside your folder
    subprocess.run(
        ["dbt", command_type, "--profiles-dir", "."], 
        cwd=dbt_path, 
        env=env, 
        check=True
    )

def fetch_and_save_data(bucket_name,project):  
    config_data = config.load_pipeline_config()
    api_key = config_data['api_key']
    tickers = config_data['tickers'] 
    
    storage_client = cloud_storage.get_gcs_client(project)
    cloud_storage.create_gcp_bucket(bucket_name, storage_client)	

    for ticker in tickers:
        try:
            raw = collector.fetch_daily_prices(ticker, api_key)
            prices = collector.transform_api_response(raw)
            logging.info(f'{ticker}: {len(prices)} records fetched')
            output_dict = save_to_parquet(prices, ticker)
            output_path = output_dict['dir'] / output_dict['filename']
            logging.info(f'API response successfully saved to {output_path}')
            time.sleep(15)
        
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            print('\n','='*80)
            
    
def upload_to_GCS(path,bucket_name,project):
    folder_path = create_folder_path(path)
    
    config_data = config.load_pipeline_config()
    tickers = config_data['tickers']

    storage_client = cloud_storage.get_gcs_client(project)
    folder_path = Path(folder_path)
    
    for ticker in tickers:
        parquet_file = folder_path / (ticker + '.parquet')
        
        if parquet_file.exists():
            gcs_uri = cloud_storage.upload_to_gcs (parquet_file, bucket_name, ticker, storage_client)
            logging.info(f'Succesfully processed {ticker}.')
        else:
            logging.info(f'{parquet_file} doesn\'t exist.')

        print('\n','='*80)

def load_to_bigquery_task(bucket_name, project, dataset, table):
    from datetime import date
    bigquery_client = bigquery_loader.get_bigquery_client(project)
    today = 'date=' + date.today().isoformat()
    gcs_uri = f"gs://{bucket_name}/daily_prices/{today}/*.parquet"
    table_id = f"{project}.{dataset}.{table}"
    bigquery_loader.create_bigquery_table(gcs_uri, table_id, bigquery_client)

def create_folder_path(path):
    root_dir = Path(path)
    today = date.today().isoformat()
    today = 'date=' + today
    output = root_dir / 'data' / 'raw' / 'daily_prices' / today
    return output
    
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/project/market_intelligence_dbt && dbt run --profiles-dir .'
)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logging.basicConfig(level=logging.INFO)

project = 'market-intelligence-pipeline'
bucket_name = 'market-intelligence-pipeline-bucket'
dataset = 'market_intelligence'
table = 'market-intelligence-raw'
#path = 'D:/Projects/market-intelligence-pipeline'
#dbt_path = 'D:/Projects/market-intelligence-pipeline/market_intelligence_dbt'

path = '/opt/airflow/project'
dbt_path = '/opt/airflow/project/market_intelligence_dbt'

table_id = project+"."+dataset+"."+table


with DAG(
    'market-intelligence-pipeline',
    default_args = DEFAULT_ARGS,
    description = 'ETL pipeline fetching data, uploading to GCS, loading to BQ, and running dbt',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    task_fetch_and_save  = PythonOperator(
        task_id = 'fetch_and_save',
        python_callable = fetch_and_save_data,
        op_kwargs={
            'bucket_name': bucket_name,
            'project': project
        }
    )
    
    task_upload_to_gcs = PythonOperator(
        task_id = 'upload_to_gcs',
        python_callable = upload_to_GCS,
        op_kwargs={
            'path': str(path),
            'bucket_name': bucket_name,
            'project': project
        }
    )
    
    task_load_to_bigquery = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery_task,
        op_kwargs={
            'bucket_name': bucket_name,
            'project': project,
            'dataset': dataset,
            'table': table
        }
    )
    
    task_dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_command,
        op_kwargs={"command_type": "run"}
    )
    
    task_dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_command,
        op_kwargs={"command_type": "test"}
    )
    
    task_fetch_and_save >> task_upload_to_gcs >> task_load_to_bigquery >> task_dbt_run >> task_dbt_test
    