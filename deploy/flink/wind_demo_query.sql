-- Model query: wind-demo  (version 1)  — SPEC-streaming.md Section 7 & 8
-- inputs : elexon-wind-forecast, elexon-demand-forecast, elexon-system-price
-- output : features.wind-demo.v1
--
-- The control plane RESOLVES the input feed IDs to topics + schemas, GENERATES
-- the CREATE TABLE declarations below from the registered feed schemas, and
-- appends the author's INSERT. This file is the fully-resolved artifact for the
-- first vertical slice.
--
-- Joins are stream-stream INTERVAL joins (valid on unbounded streams); the price
-- feature is the latest system price at delivery (temporal join). A rolling
-- price_mean_8h is a v2 feature. VERIFY field names against the live feeds.

CREATE TABLE elexon_wind_forecast (
  event_time     TIMESTAMP(3),
  published_at   TIMESTAMP(3),
  region         STRING,
  delivery_start TIMESTAMP(3),
  forecast_mw    DOUBLE,
  WATERMARK FOR published_at AS published_at - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'feeds.elexon.wind_forecast',
  'properties.bootstrap.servers' = 'redpanda-0.redpanda.redpanda.svc.cluster.local:9093',
  'properties.group.id' = 'mq-wind-demo-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE elexon_demand_forecast (
  event_time     TIMESTAMP(3),
  published_at   TIMESTAMP(3),
  region         STRING,
  delivery_start TIMESTAMP(3),
  demand_mw      DOUBLE,
  WATERMARK FOR published_at AS published_at - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'feeds.elexon.demand_forecast',
  'properties.bootstrap.servers' = 'redpanda-0.redpanda.redpanda.svc.cluster.local:9093',
  'properties.group.id' = 'mq-wind-demo-v1',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Versioned table for the temporal (latest-price-at-delivery) join.
CREATE TABLE elexon_system_price (
  event_time    TIMESTAMP(3),
  published_at  TIMESTAMP(3),
  price_gbp_mwh DOUBLE,
  PRIMARY KEY (event_time) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'feeds.elexon.system_price',
  'properties.bootstrap.servers' = 'redpanda-0.redpanda.redpanda.svc.cluster.local:9093',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE features_wind_demo (
  model_id            STRING,
  feature_set_version INT,
  prediction_key      STRING,
  delivery_start      TIMESTAMP(3),
  `as_of`             TIMESTAMP(3),
  features            ROW<wind_forecast_mw DOUBLE, demand_forecast_mw DOUBLE, price_gbp_mwh DOUBLE>
) WITH (
  'connector' = 'kafka',
  'topic' = 'features.wind-demo.v1',
  'properties.bootstrap.servers' = 'redpanda-0.redpanda.redpanda.svc.cluster.local:9093',
  'format' = 'json'
);

INSERT INTO features_wind_demo
SELECT
  'wind-demo'                                                               AS model_id,
  1                                                                         AS feature_set_version,
  CONCAT('GB|', DATE_FORMAT(w.delivery_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''')) AS prediction_key,
  w.delivery_start                                                          AS delivery_start,
  GREATEST(w.published_at, COALESCE(d.published_at, w.published_at))        AS `as_of`,
  ROW(w.forecast_mw, d.demand_mw, p.price_gbp_mwh)                          AS features
FROM elexon_wind_forecast AS w
LEFT JOIN elexon_demand_forecast AS d
  ON  d.delivery_start = w.delivery_start
  AND d.published_at BETWEEN w.published_at - INTERVAL '1' HOUR
                         AND w.published_at + INTERVAL '1' HOUR
LEFT JOIN elexon_system_price FOR SYSTEM_TIME AS OF w.delivery_start AS p
  ON p.event_time = w.delivery_start;
