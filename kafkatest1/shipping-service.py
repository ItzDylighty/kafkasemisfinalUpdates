import json
from kafka import KafkaConsumer


TOPIC_VALIDATED = "validated_orders"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC_VALIDATED,
    bootstrap_servers=KAFKA_BROKER
)

for message in consumer:
    order = json.loads(message.value.decode("utf-8"))
    if order.get("status") == "Available":
        print(f"Shipping order for {order['customer']}, Book Title: {order['book']}")
    elif order.get("status") == "Out of Stock":
        print(f"Cannot ship order for {order['customer']}, Book Title: {order['book']} due to Inavailability")
    else:
        print(f"Cannot ship order for {order['customer']}, Book Title: {order['book']} (Status: {order['status']})")