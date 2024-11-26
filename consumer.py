from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from main import CHECKPOINT_LOCATION

KAFKA_TOPIC_NAME = "posStreaming"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("CIS533 POS Streaming Consumer")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
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

    # Merge df with schema
    info_dataframe = base_df.select(
        from_json(col("value"), inventory_changed_schema).alias("data"), "timestamp"
    ).select("data.*", "timestamp")

    info_dataframe.printSchema()

    aggregated_df = info_dataframe.groupBy("store_id", "item_id").agg(sum("quantity").alias("delta_quantity")).orderBy(
        "delta_quantity", ascending=True)

    query = aggregated_df.writeStream.format("console").trigger(
        processingTime='5 seconds').outputMode("complete").start()
    query.awaitTermination()
