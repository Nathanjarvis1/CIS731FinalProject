from kafka import KafkaProducer
import csv
import json

KAFKA_TOPIC_NAME = "posStreaming"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def read_data(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['item_id'] = int(row['item_id'])
            row['store_id'] = int(row['store_id'])
            row['quantity'] = int(row['quantity'])
            row['change_type_id'] = int(row['change_type_id'])
            yield row

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for record in read_data('data/inventoryChanged.csv'):
        producer.send(KAFKA_TOPIC_NAME, record)
        producer.flush()

if __name__ == "__main__":
    main()
