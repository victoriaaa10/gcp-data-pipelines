import json
from kafka import KafkaConsumer

# ── Constants ─────────────────────────────────────────────────
BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "green-trips"
TOTAL_RECORDS = 49416  # total records sent by producer 

# ── Consumer ──────────────────────────────────────────────────
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[BOOTSTRAP_SERVER],
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# ── Count trips with distance > 5.0 ───────────────────────────
count = 0
total = 0

for message in consumer:
    trip = message.value
    if trip.get("trip_distance", 0) > 5.0:
        count += 1
    total += 1
    if total >= TOTAL_RECORDS:
        break

print(f"✓ Trips with distance > 5.0 km: {count:,}")