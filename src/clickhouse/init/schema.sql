CREATE DATABASE IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.features
(
    symbol LowCardinality(String),
    exchange_ts DateTime64(9, 'UTC'),
    sequence_num UInt64,
    price Float64,
    bid Float64,
    ask Float64,
    bid_qty Float64,
    ask_qty Float64,
    mid Float64,
    spread Float64,
    spread_bps Float64,
    imbalance Float64
)
ENGINE = MergeTree()
PARTITION BY toDate(exchange_ts)
ORDER BY (symbol, exchange_ts);

CREATE TABLE IF NOT EXISTS market.kafka_features
(
    symbol String,
    exchange_ts String,
    sequence_num UInt64,
    price Float64,
    bid Float64,
    ask Float64,
    bid_qty Float64,
    ask_qty Float64,
    mid Float64,
    spread Float64,
    spread_bps Float64,
    imbalance Float64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'coinbase.features',
    kafka_group_name = 'clickhouse_features_auto',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.features_mv
TO market.features
AS
SELECT
    symbol,
    toDateTime64(
        replaceRegexpOne(
            replaceRegexpOne(exchange_ts, 'T', ' '),
            'Z',
            ''
        ),
        9,
        'UTC'
    ) AS exchange_ts,
    sequence_num,
    price,
    bid,
    ask,
    bid_qty,
    ask_qty,
    mid,
    spread,
    spread_bps,
    imbalance
FROM market.kafka_features;

CREATE TABLE IF NOT EXISTS market.candles_1s
(
    symbol LowCardinality(String),
    ts DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64
)
ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (symbol, ts);

CREATE TABLE IF NOT EXISTS market.candles_1m
(
    symbol LowCardinality(String),
    ts DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64
)
ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (symbol, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.candles_1s_mv
TO market.candles_1s
AS
SELECT
    symbol,
    toStartOfSecond(exchange_ts) AS ts,
    argMin(price, exchange_ts) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, exchange_ts) AS close,
    sum(bid_qty + ask_qty) AS volume
FROM market.features
GROUP BY symbol, ts;

CREATE MATERIALIZED VIEW IF NOT EXISTS market.candles_1m_mv
TO market.candles_1m
AS
SELECT
    symbol,
    toStartOfInterval(exchange_ts, INTERVAL 1 MINUTE) AS ts,
    argMin(price, exchange_ts) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, exchange_ts) AS close,
    sum(bid_qty + ask_qty) AS volume
FROM market.features
GROUP BY symbol, ts;

CREATE TABLE IF NOT EXISTS market.candles_5m
(
    symbol LowCardinality(String),
    ts DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64
)
ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (symbol, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.candles_5m_mv
TO market.candles_5m
AS
SELECT
    symbol,
    toStartOfInterval(exchange_ts, INTERVAL 5 MINUTE) AS ts,
    argMin(price, exchange_ts) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, exchange_ts) AS close,
    sum(bid_qty + ask_qty) AS volume
FROM market.features
GROUP BY symbol, ts;

CREATE TABLE IF NOT EXISTS market.candles_15m
(
    symbol LowCardinality(String),
    ts DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64
)
ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY (symbol, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.candles_15m_mv
TO market.candles_15m
AS
SELECT
    symbol,
    toStartOfInterval(exchange_ts, INTERVAL 15 MINUTE) AS ts,
    argMin(price, exchange_ts),
    max(price),
    min(price),
    argMax(price, exchange_ts),
    sum(bid_qty + ask_qty)
FROM market.features
GROUP BY symbol, ts;
