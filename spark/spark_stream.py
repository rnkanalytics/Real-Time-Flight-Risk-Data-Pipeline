import functools
print = functools.partial(print, flush=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, round as spark_round, current_timestamp, row_number, desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.window import Window

schema = StructType([
    StructField("icao24",        StringType(),  True),
    StructField("callsign",      StringType(),  True),
    StructField("registration",  StringType(),  True),
    StructField("aircraft_type", StringType(),  True),
    StructField("description",   StringType(),  True),
    StructField("latitude",      DoubleType(),  True),
    StructField("longitude",     DoubleType(),  True),
    StructField("altitude",      DoubleType(),  True),
    StructField("altitude_geom", DoubleType(),  True),
    StructField("velocity",      DoubleType(),  True),
    StructField("heading",       DoubleType(),  True),
    StructField("vertical_rate", DoubleType(),  True),
    StructField("squawk",        StringType(),  True),
    StructField("emergency",     StringType(),  True),
    StructField("category",      StringType(),  True),
    StructField("source",        StringType(),  True),
    StructField("seen_pos",      DoubleType(),  True),
    StructField("distance_km",   DoubleType(),  True),
    StructField("timestamp",     StringType(),  True),
])

spark = SparkSession.builder \
    .appName("FlightStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flights-raw") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

cleaned = parsed \
    .filter(col("latitude").isNotNull()) \
    .filter(col("longitude").isNotNull()) \
    .filter(col("icao24").isNotNull()) \
    .withColumn("callsign",     trim(col("callsign"))) \
    .withColumn("altitude",     spark_round(col("altitude"), 1)) \
    .withColumn("velocity",     spark_round(col("velocity"), 1)) \
    .withColumn("heading",      spark_round(col("heading"),  1)) \
    .withColumn("velocity_kmh", spark_round(col("velocity") * 1.852, 1)) \
    .withColumn("created_at",   current_timestamp())


def write_to_bigquery(batch_df, batch_id):
    # Deduplicate — keep only latest position per aircraft in this batch
    window = Window.partitionBy("icao24").orderBy(desc("created_at"))
    deduped = batch_df.withColumn("rn", row_number().over(window)) \
                      .filter(col("rn") == 1) \
                      .drop("rn")

    count = deduped.count()
    print(f"Writing batch {batch_id} — {count} flights to BigQuery")
    if count == 0:
        return
    deduped.write \
        .format("bigquery") \
        .option("table", "flights-490708.flight_data.flights") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} written successfully")


cleaned.writeStream \
    .foreachBatch(write_to_bigquery) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start() \
    .awaitTermination()