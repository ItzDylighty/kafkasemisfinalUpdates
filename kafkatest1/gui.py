import json
import requests
from kafka import KafkaProducer, KafkaConsumer
import tkinter as tk
from tkinter import messagebox, ttk
import threading

KAFKA_BROKER = "localhost:9092"
TOPIC_ORDERS = "book_orders"
TOPIC_VALIDATED = "validated_orders"
TOPIC_NOTIFICATIONS = "notifications"

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
consumer = KafkaConsumer(
    TOPIC_VALIDATED,
    bootstrap_servers=KAFKA_BROKER
)

inventory = {
    1: {"name": "Harry Potter", "quantity": 50},
    2: {"name": "Pride", "quantity": 50},
    3: {"name": "Monster", "quantity": 30},
    4: {"name": "Alchemist", "quantity": 20},
    5: {"name": "The Hobbit", "quantity": 10}
}

order_history = []
shipped_orders = []
processed_orders = set()

root = tk.Tk()
root.title("Bookstore Order System")
root.geometry("800x600")

notebook = ttk.Notebook(root)
notebook.pack(pady=10, fill="both", expand=True)

main_page_frame = tk.Frame(notebook)
inventory_page_frame = tk.Frame(notebook)
history_page_frame = tk.Frame(notebook)
shipping_page_frame = tk.Frame(notebook)
notifications_page_frame = tk.Frame(notebook)

notebook.add(main_page_frame, text="Main Page")
notebook.add(inventory_page_frame, text="Inventory")
notebook.add(shipping_page_frame, text="Shipping")
notebook.add(history_page_frame, text="History")
notebook.add(notifications_page_frame, text="Notifications")

inventory_label = tk.Label(inventory_page_frame, justify="left", anchor="nw")
inventory_label.pack(padx=10, pady=10, fill="both", expand=True)

history_label = tk.Label(history_page_frame, justify="left", anchor="nw")
history_label.pack(padx=10, pady=10, fill="both", expand=True)

shipping_label = tk.Label(shipping_page_frame, justify="left", anchor="nw")
shipping_label.pack(padx=10, pady=10, fill="both", expand=True)

notifications_label = tk.Label(notifications_page_frame, justify="left", anchor="nw")
notifications_label.pack(padx=10, pady=10, fill="both", expand=True)

def print_inventory():
    return "\n".join(
        [f"Book ID: {bid} | Title: {info['name']} | Quantity: {info['quantity']}" for bid, info in inventory.items()]
    )

def update_inventory_display():
    inventory_label.config(text=print_inventory())

def display_shipping():
    if not shipped_orders:
        shipping_label.config(text="No orders have been shipped yet.")
    else:
        shipping_label.config(text="\n".join(
            [f"Shipped {i+1}: {o['customer']} | Book: {o['book']} | Quantity: {o['quantity']} | Payment: {o['payment_option']}"
             for i, o in enumerate(shipped_orders)]
        ))

def display_history():
    if not order_history:
        history_label.config(text="No orders placed yet.")
    else:
        history_content = "\n".join(
            [f"Order {i+1} for {o['customer']}:\nBook Title: {o['book']} | Quantity: {o['quantity']} | Status: {o['status']} | Payment: {o['payment_option']}"
             for i, o in enumerate(order_history)]
        )
        history_label.config(text=history_content)

def display_notifications():
    if not order_history:
        notifications_label.config(text="No notifications yet.")
    else:
        notifications_content = "\n".join(
            [f"Notification: {o['message']} for Customer {o['customer']}" for o in order_history if 'message' in o]
        )
        notifications_label.config(text=notifications_content)

def place_order(book_id_entry, quantity_entry, customer_entry, payment_option_var):
    try:
        book_id = int(book_id_entry.get())
        quantity = int(quantity_entry.get())
        customer = customer_entry.get().strip()
        payment_option = payment_option_var.get().lower()

        if not customer:
            messagebox.showerror("Error", "Customer name cannot be empty")
            return

        if book_id not in inventory:
            messagebox.showerror("Error", "Invalid Book ID")
            return

        if quantity > inventory[book_id]["quantity"]:
            messagebox.showerror("Error", "Insufficient stock available")
            return

        payload = {
            "book_id": book_id,
            "quantity": quantity,
            "customer": customer,
            "payment_option": payment_option
        }

        response = requests.post("http://127.0.0.1:5000/placeOrder/", params=payload)

        if response.status_code == 200:
            res_json = response.json()
            if "error" in res_json:
                messagebox.showerror("Order Failed", res_json["error"])
            else:
                messagebox.showinfo("Order Placed", res_json["message"])

                inventory[book_id]["quantity"] -= quantity

                order_status = {
                    "book_id": book_id,
                    "book": inventory[book_id]["name"],
                    "quantity": quantity,
                    "status": "Available" if "message" in res_json and "placed" in res_json["message"].lower() else "Out of Stock",
                    "customer": customer,
                    "payment_option": payment_option,
                    "message": res_json["message"]
                }
                
                order_history.append(order_status)
                shipped_orders.append(order_status)
                update_inventory_display()
                display_history()
        else:
            messagebox.showerror("Error", f"Failed to place order. Status code: {response.status_code}")
    except ValueError:
        messagebox.showerror("Error", "Invalid number input")

def show_place_order_form():
    for widget in main_page_frame.winfo_children():
        widget.destroy()

    tk.Label(main_page_frame, text="Books Available", font=("Arial", 12, "bold")).grid(row=0, column=0, columnspan=2, pady=(10, 0))
    books_text = "\n".join([f"{bid}. {info['name']}" for bid, info in inventory.items()])
    tk.Label(main_page_frame, text=books_text, justify="left").grid(row=1, column=0, columnspan=2, pady=(0, 10))

    # Form fields
    tk.Label(main_page_frame, text="Customer Name:").grid(row=2, column=0, sticky="e", padx=10, pady=5)
    customer_entry = tk.Entry(main_page_frame)
    customer_entry.grid(row=2, column=1, padx=10, pady=5)

    tk.Label(main_page_frame, text="Book ID:").grid(row=3, column=0, sticky="e", padx=10, pady=5)
    book_id_entry = tk.Entry(main_page_frame)
    book_id_entry.grid(row=3, column=1, padx=10, pady=5)

    tk.Label(main_page_frame, text="Quantity:").grid(row=4, column=0, sticky="e", padx=10, pady=5)
    quantity_entry = tk.Entry(main_page_frame)
    quantity_entry.grid(row=4, column=1, padx=10, pady=5)

    tk.Label(main_page_frame, text="Payment Option:").grid(row=5, column=0, sticky="e", padx=10, pady=5)
    payment_option_var = tk.StringVar(value="Cash")
    tk.OptionMenu(main_page_frame, payment_option_var, "Cash", "Card").grid(row=5, column=1, padx=10, pady=5)

    tk.Button(main_page_frame, text="Place Order", command=lambda: place_order(
        book_id_entry, quantity_entry, customer_entry, payment_option_var)).grid(row=6, column=0, columnspan=2, pady=10)

def on_tab_change(event):
    selected = notebook.tab(notebook.select(), "text")
    if selected == "Main Page":
        show_place_order_form()
    elif selected == "Inventory":
        update_inventory_display()
    elif selected == "Shipping":
        display_shipping()
    elif selected == "History":
        display_history()
    elif selected == "Notifications":
        display_notifications()

notebook.bind("<<NotebookTabChanged>>", on_tab_change)
show_place_order_form()

def consume_notifications():
    for message in consumer:
        try:
            order = json.loads(message.value.decode("utf-8"))
            if not all(key in order for key in ["customer", "book", "status", "payment_option"]):
                print("Skipping invalid payment message")
                continue

            if order["customer"] in processed_orders:
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

            def delayed_popup():
                messagebox.showinfo("Payment Status", payment_status)

            root.after(5000, delayed_popup)
            processed_orders.add(order["customer"])

        except Exception as e:
            print(f"Error in payment consumer: {e}")

thread = threading.Thread(target=consume_notifications)
thread.daemon = True
thread.start()

root.mainloop()
