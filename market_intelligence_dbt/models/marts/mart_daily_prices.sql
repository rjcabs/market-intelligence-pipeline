{{
  config(
    materialized='table'
  )
}}

with staging_data as (

    select * from {{ ref('stg_daily_prices') }}

),

calculated_metrics as (

    select
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,

        -- 1. Daily Return: ((current_close - previous_close) / previous_close) * 100
        safe_divide(
            (close - lag(close, 1) over (partition by symbol order by date)),
            lag(close, 1) over (partition by symbol order by date)
        ) * 100.0 as daily_return,

        -- 2. Daily Range: High minus Low (Measures intraday price volatility)
        (high - low) as daily_range,

        -- 3. 7-Day Moving Average of Close Price
        avg(close) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        ) as avg_close_7d,

        -- 4. 7-Day Moving Average of Volume
        avg(volume) over (
            partition by symbol 
            order by date 
            rows between 6 preceding and current row
        ) as avg_volume_7d

    from staging_data

)

select * from calculated_metrics
