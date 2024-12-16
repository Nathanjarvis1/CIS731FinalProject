from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import csv

# Initialize Spark session
spark = SparkSession.builder.appName("Static Data Setup").getOrCreate()

DATA_DIR = "data/"
ITEM_FILE = f"{DATA_DIR}item.csv"
STORE_FILE = f"{DATA_DIR}store.csv"
CHANGE_TYPE_FILE = f"{DATA_DIR}inventory_change_type.csv"

# Define schemas based off of databricks notebooks
item_schema = StructType([
    StructField('item_id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('supplier_id', IntegerType(), True),
    StructField('safety_stock_quantity', IntegerType(), True)
])

store_schema = StructType([
    StructField('store_id', IntegerType(), True),
    StructField('name', StringType(), True)
])

inventory_change_type_schema = StructType([
    StructField('change_type_id', IntegerType(), True),
    StructField('change_type', StringType(), True)
])

# Load static files into spark dfs
item_df = spark.read.csv(ITEM_FILE, schema=item_schema, header=True)
store_df = spark.read.csv(STORE_FILE, schema=store_schema, header=True)
change_type_df = spark.read.csv(CHANGE_TYPE_FILE, schema=inventory_change_type_schema, header=True)

# Convert spark dfs to dictionaries
ITEM_LOOKUP = {
    row["item_id"]: {
        "name": row["name"],
        "supplier_id": row["supplier_id"],
        "safety_stock_quantity": row["safety_stock_quantity"]
    } 
    for row in item_df.collect()
}

STORE_LOOKUP = {row["store_id"]: row["name"] for row in store_df.collect()}

CHANGE_TYPE_LOOKUP = {row["change_type_id"]: row["change_type"] for row in change_type_df.collect()}

spark.stop()
