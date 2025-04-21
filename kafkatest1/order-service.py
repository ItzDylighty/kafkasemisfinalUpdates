from fastapi import FastAPI
from kafka import KafkaProducer
import json

app = FastAPI()

TOPIC_ORDERS = "book_orders"
KAFKA_BROKER = "localhost:9092"

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

@app.post("/placeOrder/") #endpoint
def placeOrder(book_id: int, quantity: int, customer: str, payment_option: str):
    if book_id not in inventory:
        return {"error": f"Invalid book_id: {book_id}"}
    
    if quantity <= 0:
        return {"error": "Quantity must be greater than 0"}
    
    if not customer.strip():
        return {"error": "Customer name cannot be empty"}
    
    if payment_option.lower() not in ["cash", "card"]:
        return {"error": "Invalid payment option. Choose 'cash' or 'card'"}

    order = {
        "book_id": book_id,
        "quantity": quantity,
        "customer": customer.strip(),
        "payment_option": payment_option.lower()
    }

    producer.send(TOPIC_ORDERS, json.dumps(order).encode("utf-8"))
    return {"message": "Order placed successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5000)