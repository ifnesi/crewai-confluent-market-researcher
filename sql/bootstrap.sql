-- ---------------------------------------------------------------------------
-- Flink bootstrap (standard Apache Flink SQL).
--
-- Derive the less-verbose, dashboard-ready `crewai-logs-stats` stream from the
-- raw `crewai-logs` topic: drop the heavy `data` field and add `latency_ms`.
-- Both topics key on the raw username string (so the UI keeps fanning logs out
-- per user); on the sink, `username` is also copied into the Avro value so
-- Elasticsearch/Kibana can break down by user if needed.
--
-- `latency_ms` = ms since the previous event for the same (username, report_id).
-- An LLM call is an `input` immediately followed by `output`, and a tool call a
-- `tool_call` followed by `tool_result`, so on the response rows this gap is the
-- call's latency. Ordered by PROCTIME because events per (username, report_id)
-- are produced sequentially, so arrival order matches event order.
-- ---------------------------------------------------------------------------

SET 'execution.runtime-mode' = 'streaming';
SET 'pipeline.name' = 'crewai-logs-stats';

-- Source. Self-managed Flink has no Confluent-Cloud topic→table auto-mapping, so
-- the source table is declared explicitly. We list only the columns we keep;
-- avro-confluent resolves them by name and ignores the rest (the big `data`).
CREATE TABLE `crewai-logs` (
  `username`      STRING,
  `agent_name`    STRING,
  `report_id`     STRING,
  `timestamp`     TIMESTAMP(3),
  `type`          STRING,
  `tokens`        INT,
  `prompt_tokens` INT,
  `cost`          DOUBLE,
  `tool_name`     STRING,
  `model`         STRING,
  `proc` AS PROCTIME(),
  -- Epoch milliseconds, rebuilt from the TIMESTAMP(3). Flink's TIMESTAMPDIFF
  -- truncates to whole seconds, so compute ms directly: whole-second epoch from
  -- UNIX_TIMESTAMP plus the sub-second part from EXTRACT(MILLISECOND). The
  -- session-tz offset is constant and cancels in the latency subtraction below.
  `ts_ms` AS UNIX_TIMESTAMP(DATE_FORMAT(`timestamp`, 'yyyy-MM-dd HH:mm:ss')) * 1000
             + EXTRACT(MILLISECOND FROM `timestamp`)
) WITH (
  'connector' = 'kafka',
  'topic' = 'crewai-logs',
  'properties.bootstrap.servers' = 'broker:29092',
  'properties.group.id' = 'flink-crewai-logs-stats',
  'scan.startup.mode' = 'earliest-offset',
  'key.format' = 'raw',
  'key.fields' = 'username',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- Sink. `username` is written both as the raw Kafka key (UI fan-out) and inside
-- the Avro value (Elasticsearch), hence key.fields=username + fields-include=ALL.
CREATE TABLE `crewai-logs-stats` (
  `username`      STRING,
  `agent_name`    STRING,
  `report_id`     STRING,
  `timestamp`     TIMESTAMP(3),
  `type`          STRING,
  `tokens`        INT,
  `prompt_tokens` INT,
  `cost`          DOUBLE,
  `tool_name`     STRING,
  `model`         STRING,
  `latency_ms`    BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'crewai-logs-stats',
  'properties.bootstrap.servers' = 'broker:29092',
  'key.format' = 'raw',
  'key.fields' = 'username',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://schema-registry:8081',
  'value.fields-include' = 'ALL'
);

INSERT INTO `crewai-logs-stats`
SELECT
  `username`,
  `agent_name`,
  `report_id`,
  `timestamp`,
  `type`,
  `tokens`,
  `prompt_tokens`,
  `cost`,
  `tool_name`,
  `model`,
  `ts_ms` - LAG(`ts_ms`) OVER (
    PARTITION BY `username`, `report_id`, `agent_name`
    ORDER BY `proc`
  ) AS `latency_ms`
FROM `crewai-logs`;
