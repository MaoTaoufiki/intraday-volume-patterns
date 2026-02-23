{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(vol_regime, minutes_since_open)'
    )
}}

-- The most interesting mart: does the U-curve shape change on high vs low vol days?
-- Join intraday bars to the daily volatility regime, then aggregate the curve
with bars_with_regime as (
    select
        n.ticker,
        n.sector,
        n.market_cap_band,
        n.minutes_since_open,
        n.bar_hour,
        n.bar_minute,
        n.trade_date,
        n.normalized_volume,
        v.vol_regime,
        v.realized_vol_20d

    from {{ ref('mart_normalized_volume') }} n
    left join {{ ref('mart_daily_volatility') }} v
        on n.ticker = v.ticker
        and n.trade_date = v.trade_date
)

select
    vol_regime,
    minutes_since_open,
    bar_hour,
    bar_minute,

    count()                         as sample_size,
    avg(normalized_volume)          as avg_normalized_volume,
    median(normalized_volume)       as median_normalized_volume,
    stddevPop(normalized_volume)    as stddev_normalized_volume,
    avg(realized_vol_20d)           as avg_realized_vol

from bars_with_regime
where vol_regime is not null
group by vol_regime, minutes_since_open, bar_hour, bar_minute
order by vol_regime, minutes_since_open