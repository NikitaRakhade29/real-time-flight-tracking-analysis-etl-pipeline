from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    round,
    to_timestamp,
    from_unixtime,
    current_timestamp,
    count,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import os

# LOADING ENVIRONMENT VARIABLES


dotenv_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    ".env"
)

load_dotenv(dotenv_path)

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

S3_CLEAN_PATH = os.getenv("S3_CLEAN_PATH")
S3_CLEAN_CHECKPOINT = os.getenv("S3_CLEAN_CHECKPOINT")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT=os.getenv("MYSQL_PORT")


# SPARK SESSION

spark = (
    SparkSession.builder
    .appName("flight-tracking")
    .config("spark.sql.streaming.metricsEnabled", "false")
    .config("spark.sql.streaming.ui.enabled", "false")
    .config("spark.metrics.conf.*.sink*console.class","")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("===================================")
print("Spark Started Successfully")
print("Kafka Server :", KAFKA_SERVER)
print("Kafka Topic  :", KAFKA_TOPIC)
print("===================================")


# KAFKA SOURCE


kafka_stream = (
    spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("subscribe", KAFKA_TOPIC).option("startingOffsets","latest").option("failOnDataLoss","false").load()
)

print("Kafka Source Loaded")


# SCHEMA


flight_schema = StructType([
    StructField("event_time", LongType(), True),
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True)
])


# PARSE JSON


parsed_df = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            flight_schema
        ).alias("data")
    )
    .select("data.*")
)


# CLEAN + TRANSFORM

clean_df = (
    parsed_df
    .filter(col("longitude").isNotNull())
    .filter(col("latitude").isNotNull())
    .filter(col("icao24").isNotNull())
    .withColumn(
        "speed_kmh",
        round(col("velocity") * 3.6, 2)
    )
    .withColumn(
        "event_timestamp",
        to_timestamp(
            from_unixtime(col("event_time"))
        )
    )
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
)


# FOREACH BATCH


def process_batch(batch_df, batch_id):
  try:
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty")
        return

    print(f"Processing Batch {batch_id}")


    # WRITE CLEAN DATA TO S3


    (
        batch_df.write
        .mode("append")
        .parquet(S3_CLEAN_PATH)
    )

    print(f"Batch {batch_id} written to S3")

    # DEDUPLICATE AIRCRAFT WITHIN BATCH

    unique_flights_df = (
        batch_df
        .dropDuplicates(["icao24"])
    )

    # COUNTRY AGGREGATION

    country_stats = (
        unique_flights_df
        .groupBy("origin_country")
        .agg(
            count("*").alias("flight_count"),
            round(avg("altitude"), 2).alias("avg_altitude"),
            round(avg("speed_kmh"), 2).alias("avg_speed")
        )
    )

    rows = country_stats.collect()

    if len(rows) == 0:
        return

    # MYSQL CONNECTION

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
	port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

    cursor = conn.cursor()

    upsert_sql = """
    INSERT INTO country_flight_stats
    (
        window_start,
        window_end,
        origin_country,
        flight_count,
        avg_altitude,
        avg_speed
    )
    VALUES (%s,%s,%s,%s,%s,%s)

    ON DUPLICATE KEY UPDATE
        flight_count = VALUES(flight_count),
        avg_altitude = VALUES(avg_altitude),
        avg_speed = VALUES(avg_speed),
        window_start = VALUES(window_start),
        window_end = VALUES(window_end)
    """

    now = datetime.utcnow()

    for row in rows:

        cursor.execute(
            upsert_sql,
            (
                now,
                now,
                row["origin_country"],
                int(row["flight_count"]),
                float(row["avg_altitude"])
                if row["avg_altitude"] is not None
                else None,
                float(row["avg_speed"])
                if row["avg_speed"] is not None
                else None
            )
        )

    conn.commit()

    cursor.close()
    conn.close()

    print(f"Batch {batch_id} written to MySQL")
  except Exception as e:
     print(f"Batch {batch_id} failed: {e}")

# START STREAM

query = (
    clean_df.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        S3_CLEAN_CHECKPOINT
    )
    .trigger(processingTime="60 seconds")
    .start()
)

print("Streaming Query Started")

query.awaitTermination()