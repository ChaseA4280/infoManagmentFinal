from mongo_operations import *
from generate_mock_data import generate_mock_reviews
import json

def test_product_details():
    """Test storing and retrieving product details"""
    print("\nTesting Product Details Storage...")
    
    # Test data
    test_product = {
        "product_id": 1,
        "name": "Test Product",
        "description": "A detailed product description",
        "features": ["Feature 1", "Feature 2", "Feature 3"],
        "images": ["image1.jpg", "image2.jpg"]
    }
    
    try:
        # Store product details
        result = store_product_details(
            test_product["product_id"],
            test_product["name"],
            test_product["description"],
            test_product["features"],
            test_product["images"]
        )
        print("Product details stored successfully")
        
        # Retrieve product details
        stored_product = get_product_details(test_product["product_id"])
        print("\nRetrieved product details:")
        print(json.dumps(stored_product, default=str, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")

def test_user_profiles():
    """Test user profile operations"""
    print("\nTesting User Profiles...")
    
    # Test data
    test_user = {
        "customer_id": 1,
        "preferences": {
            "favorite_categories": ["Electronics", "Books"],
            "preferred_price_range": "100-500"
        },
        "viewing_history": []
    }
    
    try:
        # Store user profile
        result = store_user_profile(
            test_user["customer_id"],
            test_user["preferences"],
            test_user["viewing_history"]
        )
        print("User profile stored successfully")
        
        # Add some viewing history
        add_to_viewing_history(test_user["customer_id"], 1)
        add_to_viewing_history(test_user["customer_id"], 2)
        
        # Retrieve user profile
        stored_profile = get_user_profile(test_user["customer_id"])
        print("\nRetrieved user profile:")
        print(json.dumps(stored_profile, default=str, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")

def test_product_recommendations():
    """Test product recommendations"""
    print("\nTesting Product Recommendations...")
    
    try:
        # Generate some mock reviews first
        generate_mock_reviews()
        print("Generated mock reviews")
        
        # Get recommendations for a user
        recommendations = get_product_recommendations(1)
        print("\nProduct recommendations:")
        print(json.dumps(recommendations, default=str, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")

def main():
    """Main test function"""
    print("=== MongoDB Integration Tests ===")
    
    test_product_details()
    test_user_profiles()
    test_product_recommendations()

if __name__ == "__main__":
    main()
