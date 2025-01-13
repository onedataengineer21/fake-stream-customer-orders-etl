-- Create Customers Table
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL
);

-- Create Products Table
CREATE TABLE products (
    product_id UUID PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_price DECIMAL(10, 2) NOT NULL
);

-- Create Orders Table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id UUID REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Order_Items Table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id UUID REFERENCES products(product_id),
    qty INT NOT NULL,
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders (order_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products (product_id)
);
