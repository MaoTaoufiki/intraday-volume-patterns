{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'ticker_metadata') }}
),

cleaned as (
    select
        ticker,
        company_name,
        sector,
        industry,
        toFloat64(market_cap)   as market_cap,
        market_cap_band,
        country,
        currency,
        fetched_at
    from source
    where ticker != ''
)

select * from cleaned