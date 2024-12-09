import time

from pymongo import MongoClient, UpdateMany


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


def aggregate_mongo_batch(aggregated_df):
    """
    Read from the mongo collection aggregated_deltas and sum up delta_quantity by item_id and store_id
    :param aggregated_df:
    :return: The time to add/insert the document for the item_id and store_id
    """
    try:
        start_time = time.time()
        client = MongoClient("mongodb://localhost:27017/cis533.aggregate_latency")
        db = client["cis533"]
        input_collection = db["aggregate_latency"]
        bulk_operations = []
        rows = aggregated_df.collect()
        for row in rows:
            bulk_operations.append(
                UpdateMany(
                    {
                        "item_id": row["item_id"],
                        "store_id": row["store_id"]
                    },
                    {
                        "$inc": {
                            "delta_quantity": row["delta_quantity"]
                        }
                    },
                    upsert=True
                )
            )
        if bulk_operations:
            result = input_collection.bulk_write(bulk_operations)
            print(
                f"Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}")
        end_time = time.time()
        return end_time - start_time
    except Exception as e:
        print(f"Error adding to mongo: {e}")
