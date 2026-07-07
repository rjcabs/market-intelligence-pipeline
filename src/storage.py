import pandas as pd
from pathlib import Path
import logging
from datetime import date


logging.basicConfig(level=logging.INFO)


def save_to_parquet(prices: list, symbol: str) -> dict:
    root_dir = Path.cwd()

    parsed_list = [m.model_dump() for m in prices]

    today = date.today().isoformat()

    today = 'date=' + today

    filename = symbol + '.parquet'

    output = root_dir / 'data' / 'raw' / 'daily_prices' / today

    if not output.exists():
        output.mkdir(parents=True, exist_ok=True)
        logging.info(f'"{output}" directory succesfully created!')
    else:
        logging.info(f'"{output}" already exist.')

    parsed_df = pd.DataFrame(parsed_list)
    parsed_df.to_parquet(f'{output}/{filename}', index=False)

    output_dict = {
        'dir': output,
        'filename': filename
    }
    return output_dict
    


if __name__ == '__main__':
    import config
    import collector

    config_data = config.load_pipeline_config()
    api_key = config_data['api_key']
    tickers = config_data['tickers']

    for ticker in tickers:
        raw = collector.fetch_daily_prices(ticker, api_key)
        prices = collector.transform_api_response(raw)
        logging.info(f'{ticker}: {len(prices)} records fetched')
        output_dict = save_to_parquet(prices, ticker)
        output = output_dict["dir"] / output_dict["filename"]
        logging.info(f'API response succesfully saved to {output}')