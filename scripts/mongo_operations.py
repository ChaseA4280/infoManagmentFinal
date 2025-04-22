from pymongo import MongoClient
from datetime import datetime
from config import MONGO_CONFIG

def get_mongo_connection():
    """Create and return a MongoDB connection"""
    client = MongoClient(MONGO_CONFIG['connection_string'])
    return client[MONGO_CONFIG['database']]

def store_product_details(product_id, name, description, features, images):
    """Store rich product details in MongoDB"""
    db = get_mongo_connection()
    product_details = db.product_details
    
    document = {
        "product_id": product_id,
        "name": name,
        "description": description,
        "features": features,
        "images": images,
        "last_updated": datetime.now()
    }
    
    return product_details.update_one(
        {"product_id": product_id},
        {"$set": document},
        upsert=True
    )

def store_user_profile(customer_id, preferences, viewing_history):
    """Store user profile information in MongoDB"""
    db = get_mongo_connection()
    user_profiles = db.user_profiles
    
    document = {
        "customer_id": customer_id,
        "preferences": preferences,
        "viewing_history": viewing_history,
        "last_updated": datetime.now()
    }
    
    return user_profiles.update_one(
        {"customer_id": customer_id},
        {"$set": document},
        upsert=True
    )

def get_product_details(product_id):
    """Retrieve rich product details from MongoDB"""
    db = get_mongo_connection()
    return db.product_details.find_one({"product_id": product_id})

def get_user_profile(customer_id):
    """Retrieve user profile from MongoDB"""
    db = get_mongo_connection()
    return db.user_profiles.find_one({"customer_id": customer_id})

def add_to_viewing_history(customer_id, product_id):
    """Add a product to user's viewing history"""
    db = get_mongo_connection()
    user_profiles = db.user_profiles
    
    return user_profiles.update_one(
        {"customer_id": customer_id},
        {
            "$push": {
                "viewing_history": {
                    "product_id": product_id,
                    "viewed_at": datetime.now()
                }
            }
        },
        upsert=True
    )

def get_product_recommendations(customer_id):
    """Get product recommendations based on viewing history"""
    db = get_mongo_connection()
    user_profile = db.user_profiles.find_one({"customer_id": customer_id})
    
    if not user_profile or "viewing_history" not in user_profile:
        return []
    
    # Get products similar to those in viewing history
    viewed_products = [vh["product_id"] for vh in user_profile["viewing_history"]]
    
    # Find products with similar features
    recommended_products = db.product_details.find({
        "product_id": {"$nin": viewed_products}
    }).limit(5)
    
    return list(recommended_products)
