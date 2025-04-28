# db_operations.py
import psycopg2
from psycopg2 import extras
import pandas as pd
from pymongo import MongoClient
from influxdb_client import InfluxDBClient
import json
from datetime import datetime

# Connection parameters
PG_CONN_PARAMS = {
    'dbname': 'ecommerce',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

MONGO_CONN_STRING = "mongodb://localhost:27017/"
MONGO_DB = "ecommerce"
MONGO_COLLECTION = "product_reviews"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-token"
INFLUX_ORG = "my-org"
INFLUX_BUCKET = "ecommerce_metrics"

def get_pg_connection():
    """Create and return a PostgreSQL connection"""
    return psycopg2.connect(**PG_CONN_PARAMS)

def get_mongo_connection():
    """Create and return a MongoDB connection"""
    client = MongoClient(MONGO_CONN_STRING)
    return client[MONGO_DB]

def get_influx_connection():
    """Create and return an InfluxDB connection"""
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

# Function 1: Get order details (using PostgreSQL)
def get_order_details(order_id):
    """
    Function 1: Retrieve comprehensive order details from PostgreSQL,
    including customer, products, and payment information.
    
    Args:
        order_id (str): The order ID to retrieve details for
        
    Returns:
        dict: A dictionary containing order details
    """
    conn = get_pg_connection()
    cursor = conn.cursor()
    
    try:
        # Query to get all order details
        query = """
        SELECT 
            o.order_id, 
            o.order_date, 
            o.status,
            c.customer_id, 
            c.name AS customer_name, 
            c.location AS customer_location,
            pm.name AS payment_method,
            SUM(oi.total) AS order_total
        FROM 
            orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN payment_methods pm ON o.payment_method_id = pm.payment_method_id
            JOIN order_items oi ON o.order_id = oi.order_id
        WHERE 
            o.order_id = %s
        GROUP BY 
            o.order_id, o.order_date, o.status, c.customer_id, 
            c.name, c.location, pm.name
        """
        
        cursor.execute(query, (order_id,))
        order_header = cursor.fetchone()
        
        if not order_header:
            return {"error": f"Order {order_id} not found"}
        
        # Get order items
        items_query = """
        SELECT 
            p.name AS product_name,
            pc.name AS category_name,
            oi.quantity,
            oi.price,
            oi.total
        FROM 
            order_items oi
            JOIN products p ON oi.product_id = p.product_id
            JOIN product_categories pc ON p.category_id = pc.category_id
        WHERE 
            oi.order_id = %s
        """
        
        cursor.execute(items_query, (order_id,))
        items = cursor.fetchall()
        
        # Format the response
        result = {
            "order_id": order_header[0],
            "order_date": order_header[1].strftime("%Y-%m-%d"),
            "status": order_header[2],
            "customer": {
                "id": order_header[3],
                "name": order_header[4],
                "location": order_header[5]
            },
            "payment_method": order_header[6],
            "order_total": float(order_header[7]),
            "items": []
        }
        
        for item in items:
            result["items"].append({
                "product_name": item[0],
                "category": item[1],
                "quantity": item[2],
                "price": float(item[3]),
                "total": float(item[4])
            })
        
        # Return the detailed order information
        return result
    
    finally:
        cursor.close()
        conn.close()

# Function 2: Get top selling products by category (PostgreSQL)
def get_top_products_by_category(limit=5):
    """
    Function 2: Get the top selling products for each category.
    
    Args:
        limit (int): Number of top products to retrieve per category
        
    Returns:
        dict: A dictionary with categories as keys and lists of top products as values
    """
    conn = get_pg_connection()
    cursor = conn.cursor()
    
    try:
        # Query to get categories
        cursor.execute("SELECT category_id, name FROM product_categories")
        categories = cursor.fetchall()
        
        result = {}
        
        for cat_id, cat_name in categories:
            # Query to get top products for this category
            query = """
            SELECT 
                p.name AS product_name,
                SUM(oi.quantity) AS total_sold,
                SUM(oi.total) AS total_revenue
            FROM 
                products p
                JOIN order_items oi ON p.product_id = oi.product_id
            WHERE 
                p.category_id = %s
            GROUP BY 
                p.product_id, p.name
            ORDER BY 
                total_sold DESC
            LIMIT %s
            """
            
            cursor.execute(query, (cat_id, limit))
            products = cursor.fetchall()
            
            category_products = []
            for prod in products:
                category_products.append({
                    "product_name": prod[0],
                    "total_sold": prod[1],
                    "total_revenue": float(prod[2])
                })
            
            result[cat_name] = category_products
        
        return result
    
    finally:
        cursor.close()
        conn.close()

# Function 3: Add product review (PostgreSQL to MongoDB)
def add_product_review(product_name, customer_name, rating, review_text):
    """
    Function 3: Add a product review to MongoDB and update product rating in PostgreSQL.
    This function demonstrates integration between PostgreSQL and MongoDB.
    
    Args:
        product_name (str): The name of the product being reviewed
        customer_name (str): The name of the customer leaving the review
        rating (int): Rating from 1-5
        review_text (str): The text content of the review
        
    Returns:
        dict: Status of the operation
    """
    # First get the product_id from PostgreSQL
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    try:
        # Get product ID from PostgreSQL
        pg_cursor.execute("SELECT product_id FROM products WHERE name = %s", (product_name,))
        result = pg_cursor.fetchone()
        
        if not result:
            return {"error": f"Product {product_name} not found"}
        
        product_id = result[0]
        
        # Get customer ID from PostgreSQL
        pg_cursor.execute("SELECT customer_id FROM customers WHERE name = %s", (customer_name,))
        result = pg_cursor.fetchone()
        
        if not result:
            return {"error": f"Customer {customer_name} not found"}
        
        customer_id = result[0]
        
        # Connect to MongoDB and add the review
        mongo_db = get_mongo_connection()
        reviews_collection = mongo_db[MONGO_COLLECTION]
        
        review_doc = {
            "product_id": product_id,
            "product_name": product_name,
            "customer_id": customer_id,
            "customer_name": customer_name,
            "rating": rating,
            "review_text": review_text,
            "created_at": datetime.now()
        }
        
        # Insert into MongoDB
        review_id = reviews_collection.insert_one(review_doc).inserted_id
        
        # Also record a metric in InfluxDB
        influx_client = get_influx_connection()
        write_api = influx_client.write_api()
        
        # Prepare point for InfluxDB
        point = [
            {
                "measurement": "product_reviews",
                "tags": {
                    "product_id": str(product_id),
                    "product_name": product_name
                },
                "fields": {
                    "rating": rating,
                    "customer_id": customer_id
                },
                "time": datetime.utcnow()
            }
        ]
        
        # Write to InfluxDB
        write_api.write(bucket=INFLUX_BUCKET, record=point)
        
        return {
            "status": "success",
            "message": "Review added successfully",
            "review_id": str(review_id)
        }
    
    except Exception as e:
        return {"error": str(e)}
    
    finally:
        pg_cursor.close()
        pg_conn.close()

# Function 4: Get product reviews (MongoDB)
def get_product_reviews(product_name):
    """
    Function 4: Get all reviews for a product from MongoDB.
    
    Args:
        product_name (str): The name of the product
        
    Returns:
        dict: Product review information
    """
    # First get the product_id from PostgreSQL
    pg_conn = get_pg_connection()
    pg_cursor = pg_conn.cursor()
    
    try:
        # Get product details from PostgreSQL
        pg_cursor.execute("""
            SELECT p.product_id, p.name, pc.name as category, p.price 
            FROM products p
            JOIN product_categories pc ON p.category_id = pc.category_id
            WHERE p.name = %s
        """, (product_name,))
        
        result = pg_cursor.fetchone()
        
        if not result:
            return {"error": f"Product {product_name} not found"}
        
        product_id, name, category, price = result
        
        # Get reviews from MongoDB
        mongo_db = get_mongo_connection()
        reviews_collection = mongo_db[MONGO_COLLECTION]
        
        reviews = list(reviews_collection.find({"product_id": product_id}))
        
        # Format for return
        formatted_reviews = []
        total_rating = 0
        
        for review in reviews:
            formatted_reviews.append({
                "customer_name": review["customer_name"],
                "rating": review["rating"],
                "review_text": review["review_text"],
                "created_at": review["created_at"].strftime("%Y-%m-%d %H:%M:%S")
            })
            total_rating += review["rating"]
        
        avg_rating = total_rating / len(reviews) if reviews else 0
        
        return {
            "product": {
                "id": product_id,
                "name": name,
                "category": category,
                "price": float(price)
            },
            "review_count": len(reviews),
            "average_rating": round(avg_rating, 1),
            "reviews": formatted_reviews
        }
    
    except Exception as e:
        return {"error": str(e)}
    
    finally:
        pg_cursor.close()
        pg_conn.close()

# Function 5: Get sales metrics over time (PostgreSQL + InfluxDB)
def get_sales_metrics(days):
    """
    Get sales metrics over time from PostgreSQL.
    
    Args:
        days (int): Number of days to look back
    
    Returns:
        dict: Sales metrics data
    """
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()  # Use regular cursor instead of DictCursor
        
        # Use proper interval syntax with quotes
        cursor.execute("""
            SELECT 
                o.order_date,
                SUM(oi.total) as daily_revenue,
                COUNT(DISTINCT o.order_id) as order_count,
                SUM(oi.quantity) as items_sold
            FROM 
                orders o
            JOIN 
                order_items oi ON o.order_id = oi.order_id
            WHERE 
                o.order_date >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY 
                o.order_date
            ORDER BY 
                o.order_date
        """, (days,))
        
        results = []
        for row in cursor:
            # Format the row data as a dictionary
            results.append({
                'order_date': row[0].strftime('%Y-%m-%d'),
                'daily_revenue': float(row[1]),
                'order_count': row[2],
                'items_sold': row[3]
            })
            
        cursor.close()
        conn.close()
        
        return results
        
    except Exception as e:
        return {"error": str(e)}

# Function 6: Get customer purchase history (PostgreSQL)
def get_customer_purchase_history(customer_name):
    """
    Function 6: Get a customer's complete purchase history from PostgreSQL
    
    Args:
        customer_name (str): The name of the customer
        
    Returns:
        dict: Customer's order history
    """
    conn = get_pg_connection()
    cursor = conn.cursor()
    
    try:
        # Get customer ID
        cursor.execute("""
            SELECT customer_id, name, location 
            FROM customers 
            WHERE name = %s
        """, (customer_name,))
        
        customer = cursor.fetchone()
        
        if not customer:
            return {"error": f"Customer {customer_name} not found"}
        
        customer_id, name, location = customer
        
        # Get all orders for this customer
        cursor.execute("""
            SELECT 
                o.order_id, 
                o.order_date, 
                o.status,
                pm.name AS payment_method,
                SUM(oi.total) AS order_total
            FROM 
                orders o
                JOIN payment_methods pm ON o.payment_method_id = pm.payment_method_id
                JOIN order_items oi ON o.order_id = oi.order_id
            WHERE 
                o.customer_id = %s
            GROUP BY 
                o.order_id, o.order_date, o.status, pm.name
            ORDER BY 
                o.order_date DESC
        """, (customer_id,))
        
        orders = cursor.fetchall()
        
        # Format for return
        formatted_orders = []
        total_spent = 0
        
        for order in orders:
            order_id, order_date, status, payment_method, order_total = order
            
            # Get order items
            cursor.execute("""
                SELECT 
                    p.name AS product_name,
                    pc.name AS category,
                    oi.quantity,
                    oi.price,
                    oi.total
                FROM 
                    order_items oi
                    JOIN products p ON oi.product_id = p.product_id
                    JOIN product_categories pc ON p.category_id = pc.category_id
                WHERE 
                    oi.order_id = %s
            """, (order_id,))
            
            items = cursor.fetchall()
            formatted_items = []
            
            for item in items:
                formatted_items.append({
                    "product": item[0],
                    "category": item[1],
                    "quantity": item[2],
                    "price": float(item[3]),
                    "total": float(item[4])
                })
            
            formatted_orders.append({
                "order_id": order_id,
                "date": order_date.strftime("%Y-%m-%d"),
                "status": status,
                "payment_method": payment_method,
                "total": float(order_total),
                "items": formatted_items
            })
            
            total_spent += float(order_total)
        
        # Get customer's favorite categories
        cursor.execute("""
            SELECT 
                pc.name AS category,
                COUNT(*) AS purchase_count,
                SUM(oi.total) AS total_spent
            FROM 
                orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products p ON oi.product_id = p.product_id
                JOIN product_categories pc ON p.category_id = pc.category_id
            WHERE 
                o.customer_id = %s
            GROUP BY 
                pc.name
            ORDER BY 
                purchase_count DESC
        """, (customer_id,))
        
        categories = cursor.fetchall()
        favorite_categories = []
        
        for category in categories:
            favorite_categories.append({
                "category": category[0],
                "purchase_count": category[1],
                "total_spent": float(category[2])
            })
        
        return {
            "customer": {
                "id": customer_id,
                "name": name,
                "location": location
            },
            "order_count": len(orders),
            "total_spent": total_spent,
            "favorite_categories": favorite_categories,
            "orders": formatted_orders
        }
    
    finally:
        cursor.close()
        conn.close()

# Main function to test all operations
def main():
    """
    Test all the database operations
    """
    # Test the functions
    print("\n1. Get Order Details:")
    order_id = "ORD12345"  # Replace with a valid order ID from your data
    print(json.dumps(get_order_details(order_id), indent=2))
    
    print("\n2. Get Top Products by Category:")
    print(json.dumps(get_top_products_by_category(), indent=2))
    
    print("\n3. Add Product Review:")
    product_name = "Product XYZ"  # Replace with a valid product
    customer_name = "Customer ABC"  # Replace with a valid customer
    print(json.dumps(add_product_review(product_name, customer_name, 5, "Great product!"), indent=2))
    
    print("\n4. Get Product Reviews:")
    print(json.dumps(get_product_reviews(product_name), indent=2))
    
    print("\n5. Get Sales Metrics:")
    print(json.dumps(get_sales_metrics(30), indent=2))
    
    print("\n6. Get Customer Purchase History:")
    print(json.dumps(get_customer_purchase_history(customer_name), indent=2))

if __name__ == "__main__":
    main()