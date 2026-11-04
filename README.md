# HealthStream v2 - Real-Time Healthcare Decision Intelligence

An end-to-end portfolio project that takes a hospital from monitoring to decision intelligence, fusing real-time patient vitals with machine-learning risk predictions into a single ranked action list.

Kafka streaming -> Snowflake Cortex ML -> closed-loop scoring -> role-based decision dashboard -> 4 GPT-4o agents

## Architecture

```mermaid
flowchart LR
    CSV[Synthea CSV<br/>observations.csv + patients.csv<br/>data/synthea - not in repo] --> PROD[producer/csv_producer.py<br/>VitalsProducer<br/>validate_vital + AvroSerializer]
    PG[(PostgreSQL EHR)] --> JDBC[connectors/jdbc-source-postgres.json<br/>JdbcSourceConnector] --> COND[patient-conditions]
    MY[(MySQL Pharmacy)] --> DEB[connectors/debezium-source-mysql.json<br/>Debezium CDC] --> MEDS[patient-medications]
    PROD -->|Avro, Schema Registry| VITALS[patient-vitals]
    PROD -->|send_to_dlq, with error_reason| DLQ[dead-letter-queue]
    VITALS --> KSQL[processing/ksqldb_statements.sql<br/>streams + latest tables]
    COND --> KSQL
    MEDS --> KSQL
    VITALS --> MAT[context-engine/context_materializer.py]
    COND --> MAT
    MEDS --> MAT
    DLQ --> MAT
    MAT -->|patient:{pid}, 24h TTL| REDIS[(Redis)]
    SF[(Snowflake<br/>HEALTHSTREAM.ML.PATIENT_RISK_LEVELS<br/>existing table, training not in repo)] --> PUB[risk_score_publisher.py] --> RISK[patient-risk-scores] --> CONS[risk_score_consumer.py] -->|risk:{pid}, 24h TTL| REDIS
    REDIS --> DASH[dashboard/api.py<br/>Flask, localhost:5050<br/>Action Queue, /api/executive]
    REDIS --> AGENTS[4 GPT-4o agents<br/>HealthStreamAgent, AnalyticsAgent,<br/>IntegrationAgent, DLQMonitorAgent]
```

Note: All data is synthetic (Synthea). Accuracy figures illustrate the method, not clinical results.

## Quick demo

Documented commands, not a verified end-to-end run. The full demo has not
been verified end-to-end in this repo state.

```bash
docker compose up -d
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python producer/csv_producer.py
python context-engine/context_materializer.py
python dashboard/api.py
```

What you should see: producer stats (produced / DLQ counts), materializer
Redis writes, and the dashboard API at `localhost:5050`.

Caveats, plainly stated:

- The full demo needs a `.env` file with Snowflake and OpenAI variables
  (see `.env.example` for the variable names) and Synthea CSVs in
  `data/synthea/` (not in the repo; that directory is gitignored).
- Local database passwords for the Docker containers come from `.env`
  only. Connector JSON files contain `${VAR}` placeholders, and
  `scripts/deploy_connectors.sh` substitutes them from the environment
  at deploy time. `.env` itself is gitignored and must never be committed.
- The MySQL Debezium user is created at container init by
  `databases/mysql-init-debezium.sh` from the container environment,
  not from a password in a tracked SQL file.

See [Quick Start](#quick-start) below for the existing detailed steps.

---

## What It Does

Most hospital systems are good at monitoring. HealthStream v2 goes one step further, fusing two signals that normally live in separate systems:

- Acute signal: live vitals streaming through Kafka (what is wrong right now)
- Chronic signal: readmission risk from a Snowflake Cortex ML model (what is likely next)

These combine into a ranked Action Priority Queue that tells clinicians who to see first and why.

## Architecture - Three Layers, Four Audiences

Layer 1 - Real-Time Operations: CSV -> Python producer (Avro-validated) -> Kafka (3 brokers) -> ksqlDB -> materializer -> Redis -> dashboard

Layer 2 - Lakehouse + ML: Snowflake (RAW / ANALYTICS / ML schemas) + the existing model-output table HEALTHSTREAM.ML.PATIENT_RISK_LEVELS. No Cortex training SQL is in this repo.

Layer 3 - Decision Intelligence: Action Queue, risk explainability, financial impact, and 4 AI agents

Serves four roles: Doctor (Action Queue), Analyst (Analytics Assistant), Engineer (reliability + DLQ), Leadership (Executive Command Center)

## Key Features

- Closed-loop scoring: risk scores flow Snowflake -> Kafka -> Redis -> dashboard, back to the bedside
- Action Priority Queue: fuses acute + chronic signals into one ranked list with recommended actions
- Risk explainability: every score traces to factors (prior admissions, conditions, age) vs cohort average
- Executive Command Center: live leadership view - readmission rate, at-risk population, penalty exposure, avoidable cost
- 4 GPT-4o agents: role-aware clinical/operations assistant, plain-English-to-Snowflake analytics assistant, data-onboarding agent, data-quality monitor
- Data quality: Avro + Schema Registry validation, dead-letter queue with error diagnosis
- 3 ingestion patterns: custom producer, JDBC Source Connector, Debezium CDC

## Tech Stack

Apache Kafka, ksqlDB, Avro / Schema Registry, Snowflake + Cortex ML, Redis, Flask, GPT-4o, Docker, Python, SQL

Production path (Docker -> AWS): MSK, Managed Flink, ElastiCache, ECS/Lambda, Glue Schema Registry

## Project Structure

producer/            CSV producer (Avro-validated Kafka publisher)
context-engine/      Materializer: Kafka to Redis patient context
dashboard/           Flask API + role-based dashboard (provider/admin/executive)
agent/               4 GPT-4o agents
connectors/          JDBC + Debezium source connectors
schemas/             Avro schemas
databases/           Postgres + MySQL init
monitoring/          Health monitor
security/            Kafka ACLs
risk_score_publisher.py / risk_score_consumer.py   Closed-loop scoring
docker-compose.yml   10-container infrastructure

## Design Decisions

- Avro and Schema Registry at the producer. Malformed records are rejected
  before any consumer reads them. Failed records go to the
  dead-letter-queue with the payload and the error reason, so nothing is
  silently dropped.
- Custom validation before Avro: `validate_vital` in
  `producer/csv_producer.py` checks numeric value, medical range, known
  patient, and a present date before serialization. A serializer failure
  in `produce_vital` is routed to the dead-letter-queue as a
  `PRODUCTION_ERROR`.
- Redis for current patient context: the dashboard reads the latest state
  from Redis.
- Scores flow Snowflake -> Kafka topic `patient-risk-scores` -> Redis:
  `risk_score_publisher.py` reads
  `HEALTHSTREAM.ML.PATIENT_RISK_LEVELS` and publishes to
  `patient-risk-scores`; `risk_score_consumer.py` writes `risk:{pid}` to
  Redis with a 24-hour TTL.
- Three ingestion patterns exist: a custom CSV producer, a JDBC source
  connector, and Debezium CDC.
- ksqlDB streams and tables are defined in
  `processing/ksqldb_statements.sql`.
- Agents in this repo, as named in code, each with `MODEL = "gpt-4o"`:
  `HealthStreamAgent`, `AnalyticsAgent`, `DLQMonitorAgent`,
  `IntegrationAgent`.

## Limitations

- All data is synthetic (Synthea). Accuracy figures show the method, not
  clinical performance.
- Runs on one machine with Docker Compose.
- The AWS production path listed in this README (MSK, Managed Flink,
  ElastiCache, ECS/Lambda, Glue Schema Registry) has not been deployed.
- No Cortex training SQL is in this repo. Only the existing
  model-output table `HEALTHSTREAM.ML.PATIENT_RISK_LEVELS` is referenced.
- Dashboard screenshots are TODO from a real demo run. None are included
  here, and no placeholder screenshots are used.
- The full demo has not been verified end-to-end. See
  [Quick demo](#quick-demo).
- The mocked pytest suite in `tests/` covers producer validation, DLQ
  routing, and the risk-score consumer with mocked external services
  only. It does not cover the full Docker/Kafka/Snowflake/OpenAI demo
  end-to-end. Install pytest for tests only with
  `pip install pytest`; it is not added to `requirements.txt`.

## Quick Start

1. Start infrastructure: docker compose up -d
2. Set up environment: python -m venv venv, source venv/bin/activate, pip install -r requirements.txt
3. Run the pipeline:
   python producer/csv_producer.py
   python context-engine/context_materializer.py
   python dashboard/api.py   (dashboard at localhost:5050)

Requires a .env file with SNOWFLAKE_* and OPENAI_API_KEY variables. Secrets are never committed.

## Author

Built by [L P Revanth Kumar Potu](https://github.com/Revanthpotu) · [LinkedIn](https://linkedin.com/in/revanthpotu)
