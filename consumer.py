from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, sum as pyspark_sum
from pyspark.sql.types import *
import time
from typing import List

from drivers.mongodb import aggregate_mongo_batch
from drivers.sql import add_sql_batch
from main import CHECKPOINT_LOCATION

KAFKA_TOPIC_NAME = "posStreaming"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

results = []

def process_batch(df, epoch_id):
    aggregated_df = df.groupBy("store_id", "item_id").agg(pyspark_sum("quantity").alias("delta_quantity")).orderBy(
        "delta_quantity", ascending=True)
    insert_time = add_sql_batch(df)
    end_time = time.time()
    avg_sent_time = df.agg(avg("sent_time")).first()[0]
    if avg_sent_time is not None:
        latency = end_time - avg_sent_time
        results.append({"rows": df.count(), "latency": latency, "insert_time": insert_time})
        df.show()
        print(epoch_id)

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("CIS533 POS Streaming Consumer")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.postgresql:postgresql:42.2.23")
        .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/cis533.single_latency")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = kafka_df.selectExpr("CAST(value as STRING)", "timestamp")

    inventory_changed_schema = (
        StructType()
        .add("trans_id", StringType())
        .add("item_id", IntegerType())
        .add("store_id", IntegerType())
        .add("date_time", StringType())
        .add("quantity", IntegerType())
        .add("change_type_id", IntegerType())
        .add("sent_time", DoubleType())
    )

    info_dataframe = base_df.select(
        from_json(col("value"), inventory_changed_schema).alias("data"), "timestamp"
    ).select("data.*", "timestamp")

    query = info_dataframe.writeStream.foreachBatch(process_batch).start()
    query.awaitTermination()
