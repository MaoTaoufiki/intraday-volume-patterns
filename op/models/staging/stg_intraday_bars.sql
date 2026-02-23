{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'intraday_bars') }}
),

cleaned as (
    select
        ticker,
        sector,
        market_cap_band,

        -- time dimensions — derived here once, used everywhere downstream
        bar_time,
        toDate(bar_time)                             as trade_date,
        toHour(bar_time)                             as bar_hour,
        toMinute(bar_time)                           as bar_minute,
        -- minutes since market open (NYSE: 14:30 UTC)
        dateDiff('minute',
            toDateTime(concat(toString(toDate(bar_time)), ' 14:30:00'), 'UTC'),
            bar_time
        )                                            as minutes_since_open,
        toDayOfWeek(bar_time)                        as day_of_week,  -- 1=Mon, 7=Sun

        open,
        high,
        low,
        close,
        toUInt64(volume)                             as volume,
        interval,
        ingested_at

    from source
    where
        -- keep only regular trading hours (14:30–21:00 UTC = 09:30–16:00 EST)
        toHour(bar_time) >= 14
        and bar_time < toDateTime(concat(toString(toDate(bar_time)), ' 21:00:00'), 'UTC')
        and volume > 0
        and close > 0
)

select * from cleaned