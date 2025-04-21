import json
from kafka import KafkaConsumer


TOPIC_NOTIFICATIONS = "notifications"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC_NOTIFICATIONS,
    bootstrap_servers=KAFKA_BROKER
)

for message in consumer:
    try:
        notification = json.loads(message.value.decode("utf-8"))
        if "customer" in notification and "message" in notification:
            print(f"Notification: {notification['message']} for Customer {notification['customer']}")
        else:
            print("Invalid notification message")
    except Exception as e:
        print(f"Error in notification consumer: {e}")