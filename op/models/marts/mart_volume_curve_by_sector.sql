{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(sector, minutes_since_open)'
    )
}}

-- U-curve broken down by sector — lets you compare whether
-- e.g. Energy opens with more volume spike than Technology
select
    sector,
    minutes_since_open,
    bar_hour,
    bar_minute,

    count()                     as sample_size,
    avg(normalized_volume)      as avg_normalized_volume,
    median(normalized_volume)   as median_normalized_volume,
    stddevPop(normalized_volume) as stddev_normalized_volume

from {{ ref('mart_normalized_volume') }}
group by sector, minutes_since_open, bar_hour, bar_minute
order by sector, minutes_since_open