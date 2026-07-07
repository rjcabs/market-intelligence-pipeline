from pydantic import BaseModel, Field, ValidationError, field_validator
from datetime import date

class TimeSeriesDaily(BaseModel):
    open: float = Field(alias='1. open')
    high: float = Field(alias='2. high')
    low: float = Field(alias='3. low')
    close: float = Field(alias='4. close')
    volume: int = Field(alias='5. volume')

class Metadata(BaseModel):
    information: str = Field(alias='1. Information')
    symbol: str = Field(alias='2. Symbol')
    last_refreshed: date = Field(alias='3. Last Refreshed')
    output_size: str = Field(alias='4. Output Size')
    time_zone: str = Field(alias='5. Time Zone')

class ApiResponse(BaseModel):
    metadata: Metadata = Field(alias='Meta Data')
    time_series_daily: dict[str,TimeSeriesDaily] = Field(alias='Time Series (Daily)')

class DailyPrice(BaseModel):
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @field_validator('close')
    @classmethod
    def is_close_valid(cls, close):
        assert close > 0, 'close must be positive value'
        return close

if __name__ == '__main__':
    # Test successful validation
    try:
        record = DailyPrice(
            symbol = 'IBM',
            date = '2026-07-02',
            open = '283.1400',
            high ='290.9300',
            low = '282.2800',
            close = '289.5200',
            volume = '5950158'
        )
        print('Successfully created a record!')
        print(f'Symbol: {record.symbol}')
        print(f'Date: {record.date}')
        print(f'Close: {record.close}')
    except ValidationError as e:
        print(f'Failed to create a record: {e}')

    print('\n' + '='*40 + '\n')

    # Trigerring an error
    try:
        record = DailyPrice(
            symbol = 'IBM',
            date = 'not a date',
            open = '283.1400',
            high ='290.9300',
            low = '282.2800',
            close = '-1.0',
            volume = '5950158'
        )
        print('Successfully created a record!')
        print(f'Symbol: {record.symbol}')
        print(f'Date: {record.date}')
        print(f'Close: {record.close}')
    except ValidationError as e:
        print(f'Failed to create a record: {e}')
