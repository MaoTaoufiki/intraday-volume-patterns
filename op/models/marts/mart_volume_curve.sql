{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(minutes_since_open)'
    )
}}

-- The core output: average normalized volume at each 5-minute interval
-- across all tickers and all trading days — this is the U-curve
select
    minutes_since_open,
    bar_hour,
    bar_minute,

    -- aggregate across all tickers and days
    count()                             as sample_size,
    avg(normalized_volume)              as avg_normalized_volume,
    median(normalized_volume)           as median_normalized_volume,
    stddevPop(normalized_volume)        as stddev_normalized_volume,

    -- upper/lower bands for visualization
    avg(normalized_volume)
        + stddevPop(normalized_volume)  as upper_band,
    avg(normalized_volume)
        - stddevPop(normalized_volume)  as lower_band

from {{ ref('mart_normalized_volume') }}
group by minutes_since_open, bar_hour, bar_minute
order by minutes_since_open