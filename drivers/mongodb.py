import time


def add_mongo_batch(aggregated_df):
    """
    Add a batch to the mongo database
    :return: The time to add the batch
    """
    try:
        start_time = time.time()
        aggregated_df.write.format("mongodb").mode("append").save()
        end_time = time.time()
        return end_time - start_time
    except Exception as e:
        print(f"Error adding to mongo: {e}")
