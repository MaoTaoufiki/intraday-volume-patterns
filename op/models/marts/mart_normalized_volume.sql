{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(ticker, bar_time)'
    )
}}

-- Step 1: compute average daily total volume per ticker
-- used to normalize so we can compare volume patterns across tickers
-- regardless of their absolute trading volumes
with daily_avg_volume as (
    select
        ticker,
        trade_date,
        sum(volume)             as daily_total_volume
    from {{ ref('stg_intraday_bars') }}
    group by ticker, trade_date
),

avg_volume as (
    select
        ticker,
        avg(daily_total_volume) as avg_daily_volume
    from daily_avg_volume
    group by ticker
),

-- Step 2: join normalized volume back to bar level
normalized as (
    select
        b.ticker,
        b.sector,
        b.market_cap_band,
        b.bar_time,
        b.trade_date,
        b.bar_hour,
        b.bar_minute,
        b.minutes_since_open,
        b.day_of_week,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,
        a.avg_daily_volume,

        -- normalized volume: 1.0 = this bar has avg volume for its ticker
        b.volume / nullIf(a.avg_daily_volume, 0) as normalized_volume

    from {{ ref('stg_intraday_bars') }} b
    left join avg_volume a using (ticker)
)

select * from normalized