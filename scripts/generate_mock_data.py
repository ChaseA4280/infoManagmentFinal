# generate_mock_data.py
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2
from config import PG_CONFIG, MONGO_CONFIG

def generate_mock_reviews():
    """
    Generate mock product reviews in MongoDB based on products from PostgreSQL.
    """
    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**PG_CONFIG)
    pg_cursor = pg_conn.cursor()
    
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_CONFIG['connection_string'])
    mongo_db = mongo_client[MONGO_CONFIG['database']]
    reviews_collection = mongo_db[MONGO_CONFIG['collections']['product_reviews']]
    
    # Clear existing reviews
    reviews_collection.delete_many({})
    
    try:
        # Get all products from PostgreSQL
        pg_cursor.execute("""
            SELECT product_id, name FROM products
        """)
        products = pg_cursor.fetchall()
        
        # Get all customers from PostgreSQL
        pg_cursor.execute("""
            SELECT customer_id, name FROM customers
        """)
        customers = pg_cursor.fetchall()
        
        # Mock review texts
        review_texts = [
            "Great product, exactly what I was looking for!",
            "Good quality for the price.",
            "I'm a bit disappointed with this product.",
            "Works as expected, no complaints.",
            "Excellent product, highly recommend!",
            "Not worth the money, to be honest.",
            "Very satisfied with my purchase.",
            "Average product, nothing special.",
            "Decent quality but overpriced.",
            "This product exceeded my expectations!"
        ]
        
        # Generate reviews
        reviews = []
        for _ in range(100):  # Generate 100 random reviews
            product = random.choice(products)
            customer = random.choice(customers)
            rating = random.randint(1, 5)
            review_text = random.choice(review_texts)
            
            # Random date within the last 60 days
            days_ago = random.randint(0, 60)
            created_at = datetime.now() - timedelta(days=days_ago)
            
            review = {
                "product_id": product[0],
                "product_name": product[1],
                "customer_id": customer[0],
                "customer_name": customer[1],
                "rating": rating,
                "review_text": review_text,
                "created_at": created_at
            }
            
            reviews.append(review)
        
        # Insert reviews into MongoDB
        if reviews:
            reviews_collection.insert_many(reviews)
            
        print(f"Successfully generated {len(reviews)} mock reviews.")
        
    finally:
        pg_cursor.close()
        pg_conn.close()

if __name__ == "__main__":
    generate_mock_reviews()
