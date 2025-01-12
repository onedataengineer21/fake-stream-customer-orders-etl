import csv
from faker import Faker
import random
import time

fake = Faker()

product_list = [
    'Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Smartwatch', 
    'Keyboard', 'Mouse', 'Monitor', 'Printer', 'Camera'
]

def generate_order(order_id):
    return {
        'customer_id': fake.uuid4(),
        'customer_name': fake.name(),
        'order_id': order_id,
        'product_name': random.choice(product_list),
        'product_id': fake.uuid4(),
        'product_price': round(random.uniform(10.0, 1500.0), 2),
        'qty': random.randint(1, 5)
    }

csv_filename = "streamed_orders_with_products.csv"
csv_columns = ['customer_id', 'customer_name', 'order_id', 'product_name', 'product_id', 'product_price', 'qty']

with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=csv_columns)
    writer.writeheader()

order_id = 1
try:
    while True:
        order = generate_order(order_id)
        with open(csv_filename, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=csv_columns)
            writer.writerow(order)
        order_id += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("\nOrder streaming has been stopped.")
