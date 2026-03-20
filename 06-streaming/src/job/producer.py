import json
import pandas as pd
from kafka import KafkaProducer
from pathlib import Path
from time import time

# ── Constants ─────────────────────────────────────────────────
BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "green-trips"
DATA_PATH = Path("data/green_tripdata_2025-10.parquet")

COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]

# ── Serializer ────────────────────────────────────────────────
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# ── Producer ──────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVER],
    value_serializer=json_serializer,
)

# ── Load & Filter Data ────────────────────────────────────────
df = pd.read_parquet(DATA_PATH, columns=COLUMNS)

# Convert datetime columns to strings for JSON serialization
df["lpep_pickup_datetime"] = df["lpep_pickup_datetime"].astype(str)
df["lpep_dropoff_datetime"] = df["lpep_dropoff_datetime"].astype(str)

# ── Send to Kafka ─────────────────────────────────────────────
t0 = time()

for _, row in df.iterrows():
    producer.send(TOPIC_NAME, value=row.to_dict())

producer.flush()

t1 = time()
print(f"✓ Sent {len(df):,} records in {(t1 - t0):.2f} seconds")