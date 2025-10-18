import os, json, time, requests
from confluent_kafka import Producer

API_URL = os.environ.get("API_URL", "https://api.data.gov.sg/v1/transport/carpark-availability")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "carpark.raw")

conf = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_USERNAME"],
    "sasl.password": os.environ["KAFKA_PASSWORD"],
    "linger.ms": 50,
}
producer = Producer(conf)

def delivery(err, msg):
    if err:
        print("Delivery failed:", err)

def flatten(payload: dict):
    items = payload.get("items", [])
    if not items:
        return []
    ts = items[0].get("timestamp")
    records = []
    for cp in items[0].get("carpark_data", []):
        cp_no = cp.get("carpark_number")
        for info in cp.get("carpark_info", []):
            records.append({
                "timestamp": ts,
                "carpark_number": cp_no,
                "lot_type": info.get("lot_type"),
                "total_lots": info.get("total_lots"),
                "lots_available": info.get("lots_available"),
            })
    return records

def main():
    poll_secs = int(os.environ.get("POLL_SECONDS", "15"))
    while True:
        r = requests.get(API_URL, timeout=10)
        r.raise_for_status()
        payload = r.json()
        for rec in flatten(payload):
            key = f"{rec.get('carpark_number','')}-{rec.get('lot_type','')}"
            producer.produce(KAFKA_TOPIC, json.dumps(rec), key=key, callback=delivery)
        producer.flush()
        time.sleep(poll_secs)

if __name__ == "__main__":
    main()
