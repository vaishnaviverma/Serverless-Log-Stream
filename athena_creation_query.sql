CREATE EXTERNAL TABLE IF NOT EXISTS wk09_logs.logs_parsed (
  host STRING,
  ident STRING,
  user STRING,
  ts_str STRING,
  method STRING,
  path STRING,
  protocol STRING,
  status INT,
  referrer STRING,
  user_agent STRING,
  ts TIMESTAMP,
  bytes BIGINT
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
STORED AS PARQUET
LOCATION 's3://au2025-csed516-vaishver/lab09/glue_output/logs_parsed/';
