{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(market_cap_band, minutes_since_open)'
    )
}}

-- U-curve by market cap band — small caps often have more pronounced
-- open/close spikes relative to their average volume than large caps
select
    market_cap_band,
    minutes_since_open,
    bar_hour,
    bar_minute,

    count()                         as sample_size,
    avg(normalized_volume)          as avg_normalized_volume,
    median(normalized_volume)       as median_normalized_volume,
    stddevPop(normalized_volume)    as stddev_normalized_volume

from {{ ref('mart_normalized_volume') }}
group by market_cap_band, minutes_since_open, bar_hour, bar_minute
order by market_cap_band, minutes_since_open