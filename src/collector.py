import requests 
import logging
import schemas


logging.basicConfig(level=logging.INFO)

def fetch_daily_prices(symbol: str, api_key: str) -> dict:
    url = 'https://www.alphavantage.co/query'

    params = {
        'function' : 'TIME_SERIES_DAILY',
        'symbol' : symbol,
        'apikey' : api_key,
        'outputsize' : 'compact'
    }
    
    logging.info(f'Fetching data for ticker: {symbol}')    
    response = requests.get(url, params=params, timeout=10)

    response.raise_for_status()

    data = response.json()

    # Alpha Vantage error responses
    if "Error Message" in data:
        raise RuntimeError(data["Error Message"])

    if "Information" in data:
        raise RuntimeError(data["Information"])

    if "Note" in data:
        raise RuntimeError(data["Note"])

    if "Time Series (Daily)" not in data:
        raise RuntimeError(f"Unexpected API response: {data}")

    return data

def transform_api_response(json_raw: dict) -> list[schemas.DailyPrice]:
    parsed_response = schemas.ApiResponse(**json_raw)

    ticker_symbol = parsed_response.metadata.symbol

    flattened_prices = []

    for date_str, ohlcv in parsed_response.time_series_daily.items():
        clean_record = schemas.DailyPrice(
                            symbol=ticker_symbol,
                            date=date_str,
                            open=ohlcv.open,
                            high=ohlcv.high,
                            low=ohlcv.low,
                            close=ohlcv.close,
                            volume=ohlcv.volume
                        )

        flattened_prices.append(clean_record)

    return flattened_prices


if __name__ == '__main__':
    import config
    import schemas
    
    config_data = config.load_pipeline_config()
    api_key = config_data['api_key']
    tickers = config_data['tickers']

    for ticker in tickers:
        raw = fetch_daily_prices(ticker, api_key)
        prices = transform_api_response(raw)
        logging.info(f'{ticker}: {len(prices)} records fetched')
        if prices:
            logging.info(prices[0])