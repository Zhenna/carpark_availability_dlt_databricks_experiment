# Secrets and configuration

Create a secret scope named `carpark` and add the keys used by the Bundle:

```bash
databricks secrets create-scope --scope carpark
databricks secrets put --scope carpark --key kafka_bootstrap      # e.g. pkc-xxxxx.gcp.confluent.cloud:9092
databricks secrets put --scope carpark --key kafka_username       # Confluent Cloud API key
databricks secrets put --scope carpark --key kafka_password       # Confluent Cloud API secret
```
