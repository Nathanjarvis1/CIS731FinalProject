import time


def add_sql_batch(df):
    """
   Add a batch to the sql database
   :return: The time to add the batch
   """
    try:
        start_time = time.time()
        df.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/cis533") \
            .option("dbtable", "single_latency") \
            .option("user", "postgres") \
            .option("password", "3.14528*e=mc!") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append").save()
        end_time = time.time()
        return end_time - start_time
    except Exception as e:
        print(f"Error adding to SQL: {e}")

