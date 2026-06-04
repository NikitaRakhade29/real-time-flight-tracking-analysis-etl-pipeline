import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, from_json, round, to_timestamp, from_unixtime, current_timestamp, to_date, hour, count, avg


dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

KAFKA_SERVER = os.getenv('KAFKA_SERVER')
S3_CLEAN_PATH = os.getenv('S3_CLEAN_PATH')
S3_CLEAN_CHECKPOINT = os.getenv('S3_CLEAN_CHECKPOINT')
MYSQL_URL = os.getenv('MYSQL_URL')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

spark = SparkSession.builder.appName("RealTimeFlightTracking").getOrCreate()

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

kafka_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_SERVER).option("subscribe", "raw-flights") \
    .option("startingOffsets", "latest").option("failOnDataLoss", "false").option("maxOffsetsPerTrigger", "500").load()

parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_value").select(from_json(col("json_value"), flight_schema).alias("data")) \
    .select("data.*").filter(col("longitude").isNotNull() & col("latitude").isNotNull() & col("icao24").isNotNull()) \
    .withColumn("speed_kmh", round(col("velocity") * 3.6, 2)) \
    .withColumn("event_timestamp", to_timestamp(from_unixtime(col("event_time")))) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("date", to_date("event_timestamp")) \
    .withColumn("hour", hour("event_timestamp"))

def process_micro_batch(batch_df, batch_id):
    # Keeping only unique records within this micro-batch
    clean_df = batch_df.dropDuplicates(["icao24", "event_time"])
    clean_df.cache()
    
    try:
        print(f"--- Processing Batch {batch_id} ---")
        
        agg_df = clean_df.groupBy("origin_country").agg(
            count("*").alias("flight_count"),
            round(avg("altitude"), 2).alias("avg_altitude"),
            round(avg("speed_kmh"), 2).alias("avg_speed")
        ).withColumn("window_start", current_timestamp()) \
         .withColumn("window_end", current_timestamp())

        mysql_payload = agg_df.select(
            "window_start", "window_end", "origin_country", 
            "flight_count", "avg_altitude", "avg_speed"
        )

        # print(f"Writing staging metrics to MySQL for batch {batch_id}...")
        # mysql_payload.write \
        #     .format("jdbc") \
        #     .option("url", f"{MYSQL_URL}?rewriteBatchedStatements=true") \
        #     .option("dbtable", "temp_country_flight_stats") \
        #     .option("user", MYSQL_USER) \
        #     .option("password", MYSQL_PASSWORD) \
        #     .option("driver", "com.mysql.cj.jdbc.Driver") \
        #     .mode("overwrite") \
        #     .save()

        # print(f"Executing Upsert statement in MySQL RDS for batch {batch_id}...")
        # db_properties = spark._jvm.java.util.Properties()
        # db_properties.setProperty("user", MYSQL_USER)
        # db_properties.setProperty("password", MYSQL_PASSWORD)
        
        # conn = spark._jvm.java.sql.DriverManager.getConnection(MYSQL_URL, db_properties)
        # stmt = conn.createStatement()
        # try:
        #     upsert_query = """
        #     INSERT INTO country_flight_stats (window_start, window_end, origin_country, flight_count, avg_altitude, avg_speed)
        #     SELECT window_start, window_end, origin_country, flight_count, avg_altitude, avg_speed 
        #     FROM temp_country_flight_stats
        #     ON DUPLICATE KEY UPDATE
        #         window_start = VALUES(window_start),
        #         window_end = VALUES(window_end),
        #         flight_count = VALUES(flight_count),
        #         avg_altitude = VALUES(avg_altitude),
        #         avg_speed = VALUES(avg_speed);
        #     """
        #     stmt.executeUpdate(upsert_query)
        # finally:
        #     stmt.close()
        #     conn.close()
            
        print(f"Batch {batch_id} complete.")

    except Exception as e:
        print(f"Error executing batch {batch_id}: {e}")
    finally:
        clean_df.unpersist()


print("Initializing Streaming Query Pipeline...")
query = parsed_stream.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", S3_CLEAN_CHECKPOINT) \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()