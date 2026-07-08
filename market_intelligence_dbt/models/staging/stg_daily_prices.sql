with source_data as (

    select * from {{ source('market_intelligence', 'market-intelligence-raw') }}

),
renamed_and_casted as (

    select
        cast(date as date) as date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        current_timestamp() as dbt_processed_at
    from source_data
)
select * from renamed_and_casted

