from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME = "posStreaming"
KAFKA_SINK_TOPIC = "posSink"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# CHECKPOINT_LOCATION = "LOCAL DIRECTORY LOCATION (FOR DEBUGGING PURPOSES)"
CHECKPOINT_LOCATION = "/Users/gmcreynolds/PycharmProjects/cis531/pySparkProject/checkpoint/a"

if __name__ == "__main__":
    # STEP 1 : creating spark session object

    spark = (
        SparkSession.builder.appName("CIS533 POS Streaming")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # STEP 2 : reading a data stream from a kafka topic

    sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
    base_df.printSchema()

    # STEP 3 : Applying suitable schema

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
        from_json(col("date_time"), inventory_changed_schema).alias("date_obj"), "timestamp"
    )

    info_dataframe.printSchema()

    # STEP 4 : Creating query using structured streaming

    query = info_dataframe.groupBy("col_a").agg(
        approx_count_distinct("col_b").alias("col_b_alias"),
        count(col("col_c")).alias("col_c_alias"),
    )

    # query = query.withColumn("query", lit("QUERY3"))
    result_1 = query.selectExpr(
        "CAST(col_a AS STRING)",
        "CAST(col_b_alias AS STRING)",
        "CAST(col_c_alias AS STRING)",
    ).withColumn("value", to_json(struct("*")).cast("string"), )

    result = (
        result_1.select("value")
        .writeStream.trigger(processingTime="10 seconds")
        .outputMode("complete")
        .format("kafka")
        .option("topic", KAFKA_SINK_TOPIC)
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
        .awaitTermination()
    )

    # result.awaitTermination()
