from mongo_operations import *
from generate_mock_data import generate_mock_reviews
import json
import psycopg2
from datetime import datetime

def test_mongodb_connection():
    """Test MongoDB connection"""
    try:
        db = get_mongo_connection()
        print("MongoDB connection successful!")
        return True
    except Exception as e:
        print(f"MongoDB connection failed: {str(e)}")
        return False

def test_product_details():
    """Test product details operations"""
    print("\nTesting Product Details...")
    
    test_data = {
        "product_id": 1,
        "name": "Test Product",
        "description": "A detailed test product description",
        "features": ["Feature 1", "Feature 2"],
        "images": ["image1.jpg", "image2.jpg"]
    }
    
    try:
        # Store product details
        result = store_product_details(
            test_data["product_id"],
            test_data["name"],
            test_data["description"],
            test_data["features"],
            test_data["images"]
        )
        print("Product details stored successfully")
        
        # Retrieve product details
        stored_product = get_product_details(test_data["product_id"])
        print("\nRetrieved product details:")
        print(json.dumps(stored_product, default=str, indent=2))
        return True
    except Exception as e:
        print(f"Error in product details test: {str(e)}")
        return False

def test_user_profiles():
    """Test user profile operations"""
    print("\nTesting User Profiles...")
    
    test_data = {
        "customer_id": 1,
        "preferences": {
            "favorite_categories": ["Electronics"],
            "price_range": "100-500"
        },
        "viewing_history": []
    }
    
    try:
        # Store user profile
        result = store_user_profile(
            test_data["customer_id"],
            test_data["preferences"],
            test_data["viewing_history"]
        )
        print("User profile stored successfully")
        
        # Add viewing history
        add_to_viewing_history(test_data["customer_id"], 1)
        add_to_viewing_history(test_data["customer_id"], 2)
        
        # Retrieve profile
        stored_profile = get_user_profile(test_data["customer_id"])
        print("\nRetrieved user profile:")
        print(json.dumps(stored_profile, default=str, indent=2))
        return True
    except Exception as e:
        print(f"Error in user profiles test: {str(e)}")
        return False

def test_recommendations():
    """Test product recommendations"""
    print("\nTesting Product Recommendations...")
    try:
        # Verify PostgreSQL schema exists
        conn = psycopg2.connect(
            dbname='ecommerce',
            user='postgres',
            password='password',
            host='localhost'
        )
        cursor = conn.cursor()

        # Check if products table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'products'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            # Generate mock reviews first
            generate_mock_reviews()
            print("Generated mock reviews")
            
            # Get recommendations
            recommendations = get_product_recommendations(1)
        else:
            print("Products table not found - skipping recommendations test")
            return True
        
        print("\nProduct recommendations:")
        print(json.dumps(recommendations, default=str, indent=2))
        return True
    except Exception as e:
        print(f"Error in recommendations test: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def run_all_tests():
    """Run all MongoDB integration tests"""
    print("=== Starting MongoDB Integration Tests ===\n")
    
    tests = [
        ("MongoDB Connection", test_mongodb_connection),
        ("Product Details", test_product_details),
        ("User Profiles", test_user_profiles),
        ("Recommendations", test_recommendations)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\nRunning {test_name} test...")
        success = test_func()
        results.append((test_name, success))
        print(f"{test_name} test {'passed' if success else 'failed'}")
    
    print("\n=== Test Results Summary ===")
    for test_name, success in results:
        print(f"{test_name}: {'✓' if success else '✗'}")

if __name__ == "__main__":
    run_all_tests()
