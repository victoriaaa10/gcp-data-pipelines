from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# ── Environment Setup ─────────────────────────────────────────
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)  # green-trips has 1 partition

t_env = StreamTableEnvironment.create(env)

# ── Source: Read from Kafka (Redpanda) ────────────────────────
t_env.execute_sql("""
    CREATE TABLE green_trips (
        lpep_pickup_datetime VARCHAR,
        lpep_dropoff_datetime VARCHAR,
        PULocationID INT,
        DOLocationID INT,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:9092',
        'properties.group.id' = 'flink-green-trips-q6',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# ── Sink: Write to PostgreSQL ─────────────────────────────────
t_env.execute_sql("""
    CREATE TABLE hourly_tip_amounts (
        window_start TIMESTAMP(3),
        total_tip_amount DOUBLE
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'hourly_tip_amounts',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
""")

# ── Q6: 1-hour Tumbling Window — Total Tip Amount ─────────────
t_env.execute_sql("""
    INSERT INTO hourly_tip_amounts
    SELECT
        TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) AS window_start,
        SUM(tip_amount) AS total_tip_amount
    FROM
        green_trips
    GROUP BY
        TUMBLE(event_timestamp, INTERVAL '1' HOUR)
""")