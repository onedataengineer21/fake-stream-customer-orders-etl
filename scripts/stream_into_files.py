import os
import psycopg2
import random
import time
from faker import Faker
import pandas as pd
from datetime import datetime
import csv

# Set up Faker
fake = Faker()

# PostgreSQL connection details (replace with your actual credentials)
DB_HOST = "localhost"  # Change to your host if it's different (e.g., '127.0.0.1')
DB_NAME = "mybusiness"  # Replace with your database name
DB_USER = "postgres"  # Replace with your username
DB_PASSWORD = "admin123"  # Replace with your password
DB_PORT = 5433  # Replace with the correct port number if it's different from the default

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def fetch_customers_and_products():
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT customer_id, customer_name FROM customers;")
    customers = cursor.fetchall()
    cursor.execute("SELECT product_id, product_name, product_price FROM products;")
    products = cursor.fetchall()
    cursor.close()
    connection.close()
    return customers, products

def generate_order_data(customers, products):
    order_data = []
    customer = random.choice(customers)
    product = random.choice(products)
    qty = random.randint(1, 5)
    order_data.append({
        "Customer ID": customer[0],
        "Customer Name": customer[1],
        "Order ID": fake.uuid4(),
        "Product Name": product[1],
        "Product ID": product[0],
        "Product Price": product[2],
        "Quantity": qty
    })
    return order_data

def create_folders():
    now = datetime.now()
    year, month, day = now.year, now.month, now.day
    base_folder = os.path.join(str(year), f"{month:02d}", f"{day:02d}")
    csv_folder = os.path.join(base_folder, "csv")
    parquet_folder = os.path.join(base_folder, "parquet")
    os.makedirs(csv_folder, exist_ok=True)
    os.makedirs(parquet_folder, exist_ok=True)
    return csv_folder, parquet_folder

def write_to_csv(order_data, csv_folder):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(csv_folder, f"orders_{timestamp}.csv")
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=order_data[0].keys())
        writer.writeheader()
        writer.writerows(order_data)

def write_to_parquet(order_data, parquet_folder):
    try:
        df = pd.DataFrame(order_data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = os.path.join(parquet_folder, f"orders_{timestamp}.parquet")
        df.to_parquet(file_path, engine='pyarrow', compression='snappy')
    except Exception as e:
        print(f"Error writing Parquet file: {e}")

def generate_orders():
    customers, products = fetch_customers_and_products()
    accumulated_orders = []
    last_written_time = time.time()
    while True:
        order_data = generate_order_data(customers, products)
        accumulated_orders.extend(order_data)
        current_time = time.time()
        if current_time - last_written_time >= 60:
            csv_folder, parquet_folder = create_folders()
            if accumulated_orders:
                write_to_csv(accumulated_orders, csv_folder)
                write_to_parquet(accumulated_orders, parquet_folder)
            accumulated_orders = []
            last_written_time = current_time
        time.sleep(1)

if __name__ == "__main__":
    generate_orders()