{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(ticker, trade_date)'
    )
}}

-- Rolling 20-day realized volatility (annualized) per ticker
-- Used to classify each trading day into a volatility regime:
-- high vol vs low vol — then joined to volume curves for regime analysis
with vol_calc as (
    select
        ticker,
        trade_date,
        daily_return,

        -- 20-day rolling realized vol (annualized: * sqrt(252))
        stddevPop(daily_return) over (
            partition by ticker
            order by trade_date
            rows between 19 preceding and current row
        ) * sqrt(252)               as realized_vol_20d

    from {{ ref('stg_daily_bars') }}
),

with_regime as (
    select
        ticker,
        trade_date,
        daily_return,
        realized_vol_20d,

        -- simple regime classification using rolling median as threshold
        case
            when realized_vol_20d > quantileExact(0.5)(realized_vol_20d) over (partition by ticker)
            then 'high'
            else 'low'
        end                         as vol_regime

    from vol_calc
    where realized_vol_20d is not null
)

select * from with_regime