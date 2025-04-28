# load_data.py
import pandas as pd
import psycopg2
import os
from psycopg2.extras import execute_batch

def load_csv_data(csv_file):
    """
    Load data from the e-commerce CSV file into PostgreSQL tables.
    
    Args:
        csv_file (str): Path to the CSV file containing e-commerce data
    """
    # Connection parameters from docker-compose.yml
    conn_params = {
        'dbname': 'ecommerce',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Read CSV file
    print(f"Reading data from {csv_file}...")
    df = pd.read_csv(csv_file)
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    try:
        # Begin transaction
        conn.autocommit = False
        
        # 1. Load unique payment methods
        print("Loading payment methods...")
        payment_methods = df['Payment Method'].unique()
        payment_method_map = {}
        
        for method in payment_methods:
            cursor.execute(
                "INSERT INTO payment_methods (name) VALUES (%s) RETURNING payment_method_id",
                (method,)
            )
            payment_method_id = cursor.fetchone()[0]
            payment_method_map[method] = payment_method_id
        
        # 2. Load unique customers
        print("Loading customers...")
        unique_customers = df[['Customer Name', 'Customer Location']].drop_duplicates()
        customer_map = {}
        
        for _, row in unique_customers.iterrows():
            cursor.execute(
                "INSERT INTO customers (name, location) VALUES (%s, %s) RETURNING customer_id",
                (row['Customer Name'], row['Customer Location'])
            )
            customer_id = cursor.fetchone()[0]
            customer_map[(row['Customer Name'], row['Customer Location'])] = customer_id
        
        # 3. Load unique product categories
        print("Loading product categories...")
        categories = df['Category'].unique()
        category_map = {}
        
        for category in categories:
            cursor.execute(
                "INSERT INTO product_categories (name) VALUES (%s) RETURNING category_id",
                (category,)
            )
            category_id = cursor.fetchone()[0]
            category_map[category] = category_id
        
        # 4. Load unique products
        print("Loading products...")
        unique_products = df[['Product', 'Category', 'Price']].drop_duplicates()
        product_map = {}
        
        for _, row in unique_products.iterrows():
            cursor.execute(
                """
                INSERT INTO products (name, category_id, price) 
                VALUES (%s, %s, %s) RETURNING product_id
                """,
                (row['Product'], category_map[row['Category']], row['Price'])
            )
            product_id = cursor.fetchone()[0]
            product_map[row['Product']] = product_id
        
        # 5. Load orders
        print("Loading orders...")
        unique_orders = df[['Order ID', 'Date', 'Customer Name', 'Customer Location', 'Payment Method', 'Status']].drop_duplicates()
        
        order_data = []
        for _, row in unique_orders.iterrows():
            customer_key = (row['Customer Name'], row['Customer Location'])
            
            # Convert date to PostgreSQL format (YYYY-MM-DD)
            date_str = row['Date']
            try:
                # Parse the date assuming DD-MM-YY format
                date_parts = date_str.split('-')
                if len(date_parts) == 3 and len(date_parts[2]) == 2:
                    # Expand YY to YYYY (assume 20YY)
                    formatted_date = f"20{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"
                else:
                    formatted_date = date_str  # Keep as is if not in expected format
            except Exception:
                formatted_date = date_str  # Keep as is if parsing fails
            
            order_data.append((
                row['Order ID'],
                customer_map[customer_key],
                formatted_date,  # Use the formatted date
                payment_method_map[row['Payment Method']],
                row['Status']
            ))
        
        execute_batch(cursor, 
            """
            INSERT INTO orders (order_id, customer_id, order_date, payment_method_id, status) 
            VALUES (%s, %s, %s, %s, %s)
            """, 
            order_data
        )
        
        # 6. Load order items
        print("Loading order items...")
        order_items = []
        
        for _, row in df.iterrows():
            order_items.append((
                row['Order ID'],
                product_map[row['Product']],
                row['Quantity'],
                row['Price'],
                row['Total Sales']
            ))
        
        execute_batch(cursor, 
            """
            INSERT INTO order_items (order_id, product_id, quantity, price, total)
            VALUES (%s, %s, %s, %s, %s)
            """, 
            order_items
        )
        
        # Commit the transaction
        conn.commit()
        print("Data loaded successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # you will need to change this to match the exact path for your computer
    csv_file = "C:/Users/Abi/Documents/GitHub/infoManagmentFinal/data/amazon_sales_data 2025.csv"
    load_csv_data(csv_file)