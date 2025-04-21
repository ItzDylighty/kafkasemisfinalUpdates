import json
from kafka import KafkaConsumer, KafkaProducer


TOPIC_VALIDATED = "validated_orders"
TOPIC_NOTIFICATIONS = "notifications"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC_VALIDATED,
    bootstrap_servers=KAFKA_BROKER
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

for message in consumer:
    try:
        order = json.loads(message.value.decode("utf-8"))
        if not all(key in order for key in ["customer", "book", "status", "payment_option"]):
            print("Invalid")
            continue

        if order["status"] == "Available":
            print(f"Processing payment for {order['customer']}, Book Title {order['book']} using Payment option: {order['payment_option']}")
            payment_status = "Payment Confirmed"
        else:
            print(f"Payment failed for {order['customer']}, Book Title {order['book']} (Status: {order['status']})")
            payment_status = "Payment Failed"

        notification = {
            "customer": order["customer"],
            "message": payment_status
        }

        producer.send(TOPIC_NOTIFICATIONS, json.dumps(notification).encode("utf-8"))

    except Exception as e:
        print(f"Error in payment consumer: {e}")