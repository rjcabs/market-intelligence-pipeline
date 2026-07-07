import os
from dotenv import load_dotenv


def load_pipeline_config():
    # Load .env variables
    load_dotenv()

    # Access secrets
    api_key = os.getenv('ALPHAVANTAGE_API_KEY')
    if not api_key:
        raise ValueError('CRITICAL ERROR: ALPHAVANTAGE_API_KEY is missing from .env file')

    tickers = os.getenv('TICKERS')
    if not tickers:
        raise ValueError('CRITICAL ERROR: TICKERS is missing from .env file')

    ticker_list = [ticker.strip() for ticker in tickers.split(',') if ticker.strip()]
    if not ticker_list:
        raise ValueError('TICKERS was found but contains no symbols')

    return {
        "api_key": api_key,
        "tickers" : ticker_list
    }


if __name__ == '__main__':
    try:
        config = load_pipeline_config()
        print('Configuration loaded successfully!')
        print(f'API KEY: {config['api_key'][:4]}...[REDACTED]')
        print(f'TARGET SYMBOLS: {config['tickers']}')
    except ValueError as e:
        print(f'Initialization Failed: {e}')