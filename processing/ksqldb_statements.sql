-- ============================================================================
-- HealthStream v2 — ksqlDB Stream Processing (Step 5)
-- ============================================================================
-- Run: cat processing/ksqldb_statements.sql | docker exec -i ksqldb ksql http://localhost:8088
-- ============================================================================

SET 'auto.offset.reset' = 'earliest';

-- ════════════════════════════════════════════════════════════════
-- STEP 5A: Create STREAMS over existing Kafka topics
-- ════════════════════════════════════════════════════════════════

-- Stream over patient-vitals (Source A: CSV Producer)
CREATE STREAM IF NOT EXISTS patient_vitals_stream (
    patient_id VARCHAR KEY,
    encounter_id VARCHAR,
    `timestamp` VARCHAR,
    vital_type VARCHAR,
    vital_description VARCHAR,
    value DOUBLE,
    units VARCHAR,
    patient_age INT,
    patient_gender VARCHAR,
    source_system VARCHAR
) WITH (
    KAFKA_TOPIC = 'patient-vitals',
    VALUE_FORMAT = 'AVRO'
);

-- Stream over patient-conditions (Source B: JDBC Connector)
CREATE STREAM IF NOT EXISTS patient_conditions_stream (
    patient_id VARCHAR KEY,
    id INT,
    condition_code VARCHAR,
    condition_name VARCHAR,
    diagnosed_date VARCHAR,
    status VARCHAR,
    severity VARCHAR,
    notes VARCHAR,
    created_at VARCHAR,
    updated_at VARCHAR
) WITH (
    KAFKA_TOPIC = 'patient-conditions',
    VALUE_FORMAT = 'AVRO'
);

-- Stream over patient-medications (Source C: Debezium CDC)
CREATE STREAM IF NOT EXISTS patient_medications_stream (
    patient_id VARCHAR KEY,
    id INT,
    medication_name VARCHAR,
    dosage VARCHAR,
    frequency VARCHAR,
    route VARCHAR,
    prescriber VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    status VARCHAR,
    notes VARCHAR,
    created_at VARCHAR,
    updated_at VARCHAR
) WITH (
    KAFKA_TOPIC = 'patient-medications',
    VALUE_FORMAT = 'AVRO'
);

-- ════════════════════════════════════════════════════════════════
-- STEP 5B: Materialized TABLE for latest conditions per patient
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS patient_conditions_latest AS
    SELECT
        patient_id,
        COLLECT_LIST(condition_name) AS conditions,
        COLLECT_LIST(severity) AS severities,
        COUNT(*) AS condition_count,
        MAX(updated_at) AS last_updated
    FROM patient_conditions_stream
    GROUP BY patient_id
    EMIT CHANGES;

-- ════════════════════════════════════════════════════════════════
-- STEP 5C: Materialized TABLE for latest medications per patient
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS patient_medications_latest AS
    SELECT
        patient_id,
        COLLECT_LIST(medication_name) AS medications,
        COLLECT_LIST(dosage) AS dosages,
        COUNT(*) AS medication_count,
        MAX(updated_at) AS last_updated
    FROM patient_medications_stream
    GROUP BY patient_id
    EMIT CHANGES;

-- ════════════════════════════════════════════════════════════════
-- STEP 5D: Windowed vital sign aggregations (trends over 1 hour)
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS vitals_hourly_stats AS
    SELECT
        patient_id,
        vital_type,
        WINDOWSTART AS window_start,
        WINDOWEND AS window_end,
        COUNT(*) AS reading_count,
        AVG(value) AS avg_value,
        MIN(value) AS min_value,
        MAX(value) AS max_value,
        LATEST_BY_OFFSET(value) AS latest_value,
        EARLIEST_BY_OFFSET(value) AS earliest_value
    FROM patient_vitals_stream
    WINDOW TUMBLING (SIZE 1 HOUR)
    GROUP BY patient_id, vital_type
    EMIT CHANGES;

-- ════════════════════════════════════════════════════════════════
-- STEP 5E: Data quality monitoring stream
-- ════════════════════════════════════════════════════════════════

CREATE STREAM IF NOT EXISTS integration_health_stream AS
    SELECT
        source_system,
        vital_type,
        CASE
            WHEN value IS NULL THEN 'NULL_VALUE'
            WHEN value < 0 THEN 'NEGATIVE_VALUE'
            ELSE 'VALID'
        END AS quality_status,
        value,
        `timestamp` AS event_time
    FROM patient_vitals_stream
    EMIT CHANGES;

-- ════════════════════════════════════════════════════════════════
-- STEP 5F: Source health — message rates per source per minute
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS source_throughput AS
    SELECT
        source_system,
        WINDOWSTART AS window_start,
        COUNT(*) AS message_count
    FROM patient_vitals_stream
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY source_system
    EMIT CHANGES;

-- ════════════════════════════════════════════════════════════════
-- STEP 5G: Anomaly detection — vitals outside normal clinical ranges
-- ════════════════════════════════════════════════════════════════

CREATE STREAM IF NOT EXISTS vital_anomalies AS
    SELECT
        patient_id,
        patient_age,
        patient_gender,
        vital_type,
        vital_description,
        value,
        units,
        `timestamp` AS event_time,
        CASE
            WHEN vital_type = '8867-4' AND value > 120 THEN 'TACHYCARDIA'
            WHEN vital_type = '8867-4' AND value < 50 THEN 'BRADYCARDIA'
            WHEN vital_type = '8480-6' AND value > 180 THEN 'HYPERTENSIVE_CRISIS'
            WHEN vital_type = '8480-6' AND value < 90 THEN 'HYPOTENSION'
            WHEN vital_type = '2708-6' AND value < 92 THEN 'HYPOXEMIA'
            WHEN vital_type = '9279-1' AND value > 25 THEN 'TACHYPNEA'
            WHEN vital_type = '8310-5' AND value > 100.4 THEN 'FEVER'
            ELSE 'UNKNOWN'
        END AS anomaly_type,
        'csv_lab_export' AS source_system
    FROM patient_vitals_stream
    WHERE
        (vital_type = '8867-4' AND (value > 120 OR value < 50))
        OR (vital_type = '8480-6' AND (value > 180 OR value < 90))
        OR (vital_type = '2708-6' AND value < 92)
        OR (vital_type = '9279-1' AND value > 25)
        OR (vital_type = '8310-5' AND value > 100.4)
    EMIT CHANGES;
