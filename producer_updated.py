from kafka import KafkaProducer
import json
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Initialize Spark
spark = SparkSession.builder.appName("POS Preprocessing").getOrCreate()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPICS = {
    "InventorySnapshotOnline": "InventorySnapshotOnline",
    "InventorySnapshotStore001": "InventorySnapshotStore001",
    "InventoryChangeOnline": "InventoryChangeOnline",
    "InventoryChangeStore001": "InventoryChangeStore001",
    "InventoryChangeType": "InventoryChangeType",
    "Store": "Store",
    "Item": "Item"
}

# Define schemas for all datasets
inventory_snapshot_schema = StructType([
    StructField('item_id', IntegerType()),
    StructField('employee_id', IntegerType()),
    StructField('store_id', IntegerType()),
    StructField('date_time', StringType()),  
    StructField('quantity', IntegerType())
])

inventory_change_schema = StructType([
    StructField('trans_id', StringType()),
    StructField('item_id', IntegerType()),
    StructField('store_id', IntegerType()),
    StructField('date_time', StringType()),  
    StructField('quantity', IntegerType()),
    StructField('change_type_id', IntegerType())
])

store_schema = StructType([
    StructField('store_id', IntegerType()),
    StructField('name', StringType())
])

item_schema = StructType([
    StructField('item_id', IntegerType()),
    StructField('name', StringType()),
    StructField('supplier_id', IntegerType()),
    StructField('safety_stock_quantity', IntegerType())
])

inventory_change_type_schema = StructType([
    StructField('change_type_id', IntegerType()),
    StructField('change_type', StringType())
])

# Map Kafka topics to their corresponding file paths and schemas
base_path = "data/"
topic_file_map = {
    "InventorySnapshotOnline": (f"{base_path}inventory_snapshot_online.csv", inventory_snapshot_schema),
    "InventorySnapshotStore001": (f"{base_path}inventory_snapshot_store001.csv", inventory_snapshot_schema),
    "InventoryChangeOnline": (f"{base_path}inventory_change_online.csv", inventory_change_schema),
    "InventoryChangeStore001": (f"{base_path}inventory_change_store001.csv", inventory_change_schema),
    "Store": (f"{base_path}store.csv", store_schema),
    "Item": (f"{base_path}item.csv", item_schema),
    "InventoryChangeType": (f"{base_path}inventory_change_type.csv", inventory_change_type_schema)
}

# Preprocessing and sending to Kafka
def preprocess_and_send_to_kafka(file_path, schema, topic, producer):
    """Read, preprocess, and send data to Kafka."""
    df = spark.read.csv(file_path, header=True, schema=schema)
    
    # Preprocessing specific to the topic
    if topic in ["InventoryChangeOnline", "InventoryChangeStore001", "InventorySnapshotOnline", "InventorySnapshotStore001"]:
        df = df.withColumn('date_time', f.to_timestamp('date_time', 'M/d/yyyy H:mm'))
    
    if topic in ["InventoryChangeOnline", "InventoryChangeStore001"]:
        df = df.withColumn('trans_id', f.expr('substring(trans_id, 2, length(trans_id)-2)'))
        
    data = df.toJSON().collect()  
    
    # Stream each JSON record to Kafka
    for record in data:
        producer.send(topic, json.loads(record))
    producer.flush()

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Preprocess and send datasets to Kafka
    for topic, (file_path, schema) in topic_file_map.items():
        preprocess_and_send_to_kafka(file_path, schema, topic, producer)

if __name__ == "__main__":
    main()
