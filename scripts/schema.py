# schema.py
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_schema():
    """
    Creates the necessary tables for the e-commerce PostgreSQL database.
    
    Tables:
    - customers: Stores customer information
    - product_categories: Stores product category information
    - products: Stores product information with category references
    - orders: Stores order header information
    - order_items: Stores order line items
    - payment_methods: Stores payment method information
    """
    # Connection parameters from docker-compose.yml
    conn_params = {
        'dbname': 'ecommerce',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**conn_params)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # Drop tables if they exist (for clean setup)
    cursor.execute("""
    DROP TABLE IF EXISTS order_items CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS products CASCADE;
    DROP TABLE IF EXISTS product_categories CASCADE;
    DROP TABLE IF EXISTS payment_methods CASCADE;
    DROP TABLE IF EXISTS customers CASCADE;
    """)
    
    # Create tables
    # Customers table
    cursor.execute("""
    CREATE TABLE customers (
        customer_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        location VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Product Categories table
    cursor.execute("""
    CREATE TABLE product_categories (
        category_id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        description TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Products table
    cursor.execute("""
    CREATE TABLE products (
        product_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        category_id INTEGER REFERENCES product_categories(category_id),
        price DECIMAL(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Payment Methods table
    cursor.execute("""
    CREATE TABLE payment_methods (
        payment_method_id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Orders table
    cursor.execute("""
    CREATE TABLE orders (
        order_id VARCHAR(20) PRIMARY KEY,
        customer_id INTEGER REFERENCES customers(customer_id),
        order_date DATE NOT NULL,
        payment_method_id INTEGER REFERENCES payment_methods(payment_method_id),
        status VARCHAR(20) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # Order Items table
    cursor.execute("""
    CREATE TABLE order_items (
        order_item_id SERIAL PRIMARY KEY,
        order_id VARCHAR(20) REFERENCES orders(order_id),
        product_id INTEGER REFERENCES products(product_id),
        quantity INTEGER NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        total DECIMAL(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    print("Database schema created successfully!")
    
    # Close connections
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_schema()