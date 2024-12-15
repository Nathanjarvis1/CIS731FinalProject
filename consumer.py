from data_dictionaries import ITEM_LOOKUP, STORE_LOOKUP, CHANGE_TYPE_LOOKUP
from kafka import KafkaConsumer
import pandas as pd
import json

def consume_and_export_all():
    consumer = KafkaConsumer(
        "InventoryChangeOnline",
        "InventoryChangeStore001",
        "InventorySnapshotOnline",
        "InventorySnapshotStore001",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )

    print("Subscribed to topics:", consumer.subscription())

    # Collect snapshots and changes
    snapshots = []
    changes = []
    for message in consumer:
        record = message.value
        if "Snapshot" in message.topic:
            record["source_type"] = "snapshot_only"
            record["item_name"] = ITEM_LOOKUP.get(record.get("item_id"), {}).get("name", "N/A")
            record["store_name"] = STORE_LOOKUP.get(record.get("store_id"), "Unknown")
            record["action"] = "inventory report"  # Add action for snapshots
            snapshots.append(record)
        elif "Change" in message.topic:
            record["source_type"] = "change_only"
            record["item_name"] = ITEM_LOOKUP.get(record.get("item_id"), {}).get("name", "N/A")
            record["store_name"] = STORE_LOOKUP.get(record.get("store_id"), "Unknown")
            record["change_type"] = CHANGE_TYPE_LOOKUP.get(record.get("change_type_id"), "N/A")
            record["action"] = record["change_type"]  # Add action for changes
            changes.append(record)

    print(f"Total snapshots collected: {len(snapshots)}")
    print(f"Total changes collected: {len(changes)}")

    # Convert to DataFrames
    snapshots_df = pd.DataFrame(snapshots)
    changes_df = pd.DataFrame(changes)

    # Combine snapshots and changes
    combined_df = pd.concat([snapshots_df, changes_df], ignore_index=True)
    combined_df["date_time"] = pd.to_datetime(combined_df["date_time"])
    combined_df = combined_df.sort_values(by=["store_id", "item_id", "date_time"])

    # Process final quantities
    combined_df["final_quantity"] = None
    for (store_id, item_id), group in combined_df.groupby(["store_id", "item_id"]):
        final_quantity = None
        for index, row in group.iterrows():
            if row["source_type"] == "snapshot_only":
                final_quantity = row["quantity"]
            elif row["source_type"] == "change_only":
                if final_quantity is not None:
                    final_quantity += row["quantity"]
                    combined_df.at[index, "source_type"] = "snapshot_with_change"
            combined_df.at[index, "final_quantity"] = final_quantity

    # Ensure employee_id is consistent
    combined_df.loc[combined_df["store_name"] == "online", "employee_id"] = None

    # Drop unwanted columns
    combined_df = combined_df.drop(columns=["item_name"])

    # Export to CSV
    export_to_csv(combined_df, "enriched_data.csv")
    print("Export completed.")

def export_to_csv(dataframe, output_file):
    """Save DataFrame to a CSV file."""
    try:
        dataframe.to_csv(output_file, index=False, encoding="utf-8")
        print(f"CSV export successful: {output_file}")
    except Exception as e:
        print(f"Error writing CSV: {e}")

if __name__ == "__main__":
    consume_and_export_all()
