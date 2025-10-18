# Carpark Real‑Time (Option B: Kafka + Databricks DLT)

This repo streams a real-time REST API into **Kafka** and processes it on **Databricks** with **Delta Live Tables** (continuous).

## Components
- `src/producer/app.py` — Producer that calls the API and publishes flattened records to Kafka.
- `src/dlt/pipeline.py` — DLT pipeline that reads from Kafka → Bronze/Silver/Gold Delta tables.
- `workflows/bundle.dab.yaml` — Databricks Bundle definition to deploy the DLT pipeline.
- `infra/secrets.md` — Steps to create the required secret scope and keys.

## Unity Catalog targets
- Catalog: **dev**
- Schema: **realtime**
- Kafka topic: **carpark.raw**

### Tables created
- `dev.realtime.carpark_bronze`
- `dev.realtime.carpark_silver`
- `dev.realtime.carpark_gold_latest`

## Quick start

1. **Create Databricks secrets**
   See `infra/secrets.md`.

2. **Deploy the bundle**
```bash
databricks bundle deploy
```
   Then start the pipeline in the UI (Continuous) or:
```bash
databricks pipelines start --pipeline-id "<pipeline-id>"
```

3. **Run the producer** (locally or a small VM/Function):
```bash
python -m venv .venv && . .venv/bin/activate
pip install -r src/producer/requirements.txt
export API_URL="https://api.data.gov.sg/v1/transport/carpark-availability"
export KAFKA_BOOTSTRAP="<cluster:9092>"
export KAFKA_USERNAME="<api-key>"
export KAFKA_PASSWORD="<api-secret>"
export KAFKA_TOPIC="carpark.raw"
python src/producer/app.py
```

## Notes
- The pipeline uses **exactly-once** semantics with event IDs + Delta transactions.
- Adjust the watermark window in `pipeline.py` to match API lateness characteristics.
- For higher throughput, increase Kafka partitions and cluster autoscaling.
