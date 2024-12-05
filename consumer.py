from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import plotly.express as px
import dash_bootstrap_components as dbc
import threading
import pandas as pd

from main import CHECKPOINT_LOCATION

KAFKA_TOPIC_NAME = "posStreaming"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Initialize Spark session
spark = (
    SparkSession.builder.appName("CIS533 POS Streaming Consumer")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m")
    .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Read Kafka stream
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

aggregated_df = info_dataframe.groupBy("store_id", "item_id").agg(sum("quantity").alias("delta_quantity"))

# Global variable to store the latest batch of data
latest_data = []

def process_batch(df, epoch_id):
    global latest_data
    latest_data = df.toPandas().to_dict(orient='records')

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    dcc.Interval(id='interval-component', interval=5*1000, n_intervals=0),
    dcc.Graph(id='live-update-graph')
])

@app.callback(Output('live-update-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    global latest_data
    if latest_data:
        pdf = pd.DataFrame(latest_data)
        fig = px.bar(pdf, x='item_id', y='delta_quantity', color='store_id')
        return fig
    return {}

def run_dash():
    app.run_server(debug=True, use_reloader=False)

if __name__ == "__main__":
    # Start the Dash app in a separate thread
    threading.Thread(target=run_dash).start()

    # Start the Spark streaming query
    query = aggregated_df.writeStream.foreachBatch(process_batch).outputMode("update").start()
    query.awaitTermination()
