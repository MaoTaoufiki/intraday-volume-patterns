CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.intraday_bars
(
    ticker          LowCardinality(String),
    sector          LowCardinality(String),
    market_cap_band LowCardinality(String), 
    bar_time        DateTime('UTC'),
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          UInt64,
    interval        LowCardinality(String),
    ingested_at     DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(bar_time)
ORDER BY (ticker, bar_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS raw.ticker_metadata
(
    ticker          LowCardinality(String),
    company_name    String,
    sector          LowCardinality(String),
    industry        String,
    market_cap      Float64,
    market_cap_band LowCardinality(String),
    country         String,
    currency        LowCardinality(String),
    fetched_at      DateTime('UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY ticker;

CREATE TABLE IF NOT EXISTS raw.daily_bars
(
    ticker      LowCardinality(String),
    trade_date  Date,
    open        Float64,
    high        Float64,
    low         Float64,
    close       Float64,
    volume      UInt64,
    ingested_at DateTime('UTC') DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYear(trade_date)
ORDER BY (ticker, trade_date)
SETTINGS index_granularity = 8192;