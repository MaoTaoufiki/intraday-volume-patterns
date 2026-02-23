{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'daily_bars') }}
),

cleaned as (
    select
        ticker,
        trade_date,
        open,
        high,
        low,
        close,
        toUInt64(volume)    as volume,

        -- daily return for volatility calculations downstream
        close / lagInFrame(close) over (
            partition by ticker
            order by trade_date
            rows between 1 preceding and current row
        ) - 1               as daily_return,

        ingested_at

    from source
    where
        volume > 0
        and close > 0
)

select * from cleaned