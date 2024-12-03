from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

from main import CHECKPOINT_LOCATION

KAFKA_TOPIC_NAME = "posStreaming"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def process_batch(df, epoch_id):
    start_time = time.time()
    aggregated_df = df.groupBy("store_id", "item_id").agg(sum("quantity").alias("delta_quantity")).orderBy(
        "delta_quantity", ascending=True)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Batch {epoch_id} GroupBy operation took {duration:.5f} seconds")
    aggregated_df.show()

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("CIS533 POS Streaming Consumer")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.streaming.concurrentJobs", "4")
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
    )

    info_dataframe = base_df.select(
        from_json(col("value"), inventory_changed_schema).alias("data"), "timestamp"
    ).select("data.*", "timestamp")

    query = info_dataframe.writeStream.foreachBatch(process_batch).start()
    query.awaitTermination()
