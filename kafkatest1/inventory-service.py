import json
from kafka import KafkaConsumer, KafkaProducer


TOPIC_ORDERS = "book_orders"
TOPIC_VALIDATED = "validated_orders"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC_ORDERS,
    bootstrap_servers=KAFKA_BROKER
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER
)

inventory = {
    1: {"name": "Harry Potter", "quantity": 50},
    2: {"name": "Pride", "quantity": 50},
    3: {"name": "Monster", "quantity": 30},
    4: {"name": "Alchemist", "quantity": 20},
    5: {"name": "The Hobbit", "quantity": 10}
}

def print_inventory():
    print("\nCurrent Inventory Status:")
    for book_id, book in inventory.items():
        print(f"Book: {book['name']}, Available Quantity: {book['quantity']}")

print_inventory()

for message in consumer:
    try:
        order = json.loads(message.value.decode("utf-8"))
        required_fields = ["book_id", "quantity", "customer", "payment_option"]
        if not all(field in order for field in required_fields):
            print(" Invalid order data. Missing required fields.")
            continue

        book_id = order["book_id"]
        quantity = order["quantity"]
        customer = order["customer"]
        payment_option = order["payment_option"]

        book = inventory.get(book_id)
        if not book:
            status = "Invalid Book"
            book_name = "Unknown"
        elif quantity <= 0:
            status = "Invalid Quantity"
            book_name = book["name"]
        elif book["quantity"] < quantity:
            status = "Out of Stock"
            book_name = book["name"]
        else:
            book["quantity"] -= quantity
            status = "Available"
            book_name = book["name"]

        response = {
            "book_id": book_id,
            "book": book_name,
            "status": status,
            "customer": customer,
            "payment_option": payment_option
        }

        producer.send(TOPIC_VALIDATED, json.dumps(response).encode("utf-8"))
        print(f"Inventory processed order: {response}")

    except Exception as e:
        print(f"Error processing order: {e}")