# get_test_data.py
import psycopg2
from pymongo import MongoClient

def get_test_data():
    """
    Retrieve sample data for testing from PostgreSQL and MongoDB
    """
    # PostgreSQL connection
    pg_conn = psycopg2.connect(
        dbname='ecommerce',
        user='postgres',
        password='password',
        host='localhost',
        port='5432'
    )
    pg_cursor = pg_conn.cursor()
    
    # MongoDB connection
    mongo_client = MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['ecommerce']
    
    try:
        # Get sample Order ID
        pg_cursor.execute("SELECT order_id FROM orders LIMIT 3")
        order_ids = [row[0] for row in pg_cursor.fetchall()]
        print("Sample Order IDs for testing:")
        for oid in order_ids:
            print(f"  - {oid}")
        
        # Get sample Customer Names
        pg_cursor.execute("SELECT name FROM customers LIMIT 3")
        customer_names = [row[0] for row in pg_cursor.fetchall()]
        print("\nSample Customer Names for testing:")
        for name in customer_names:
            print(f"  - {name}")
        
        # Get sample Product Names
        pg_cursor.execute("SELECT name FROM products LIMIT 3")
        product_names = [row[0] for row in pg_cursor.fetchall()]
        print("\nSample Product Names for testing:")
        for name in product_names:
            print(f"  - {name}")
        
        # Check MongoDB reviews
        review_count = mongo_db.product_reviews.count_documents({})
        print(f"\nMongoDB Review Count: {review_count}")
        
        if review_count > 0:
            sample_review = mongo_db.product_reviews.find_one()
            print(f"Sample Review Product: {sample_review.get('product_name', 'N/A')}")
            print(f"Sample Review Customer: {sample_review.get('customer_name', 'N/A')}")
        
    finally:
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    get_test_data()