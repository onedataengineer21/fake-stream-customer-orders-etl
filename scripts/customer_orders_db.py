import psycopg2
import random
from faker import Faker
import time
import csv

# Set up Faker
fake = Faker()

# PostgreSQL connection details (replace with your actual credentials)
DB_HOST = "localhost"  # Change to your host if it's different (e.g., '127.0.0.1')
DB_NAME = "mybusiness"  # Replace with your database name
DB_USER = "postgres"  # Replace with your username
DB_PASSWORD = "admin123"  # Replace with your password
DB_PORT = 5433  # Replace with the correct port number if it's different from the default

# Connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT  # Specify the port here
    )

# Fetch customers and products from the database
def fetch_customers_and_products():
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch customers
    cursor.execute("SELECT customer_id, customer_name FROM customers;")
    customers = cursor.fetchall()

    # Fetch products
    cursor.execute("SELECT product_id, product_name, product_price FROM products;")
    products = cursor.fetchall()
    print("Products ::: " + str(products))

    cursor.close()
    connection.close()

    return customers, products

# Generate and print orders
def generate_orders():
    customers, products = fetch_customers_and_products()

    # Prepare to write to CSV
    with open('generated_orders.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Customer ID", "Customer Name", "Order ID", "Product Name", "Product ID", "Product Price", "Quantity"])

        order_id = 1  # Starting order ID

        while True:
            # Randomly select customer and product
            customer = random.choice(customers)
            product = random.choice(products)

            # Generate random quantity
            qty = random.randint(1, 5)

            # Write the order record to CSV
            writer.writerow([
                customer[0],  # Customer ID
                customer[1],  # Customer Name
                order_id,      # Order ID
                product[1],    # Product Name
                product[0],    # Product ID
                product[2],    # Product Price
                qty            # Quantity
            ])

            order_id += 1  # Increment order ID
            time.sleep(1)  # Wait for 1 second before generating the next order

# Run the script
if __name__ == "__main__":
    generate_orders()
