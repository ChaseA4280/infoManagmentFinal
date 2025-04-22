# demo.py
import json
from db_operations import (
    get_order_details,
    get_top_products_by_category,
    add_product_review,
    get_product_reviews,
    get_sales_metrics,
    get_customer_purchase_history
)

def run_demo():
    """
    Run a demonstration of all database operations for the presentation.
    """
    print("=" * 80)
    print("E-COMMERCE MULTI-DATABASE SYSTEM DEMONSTRATION")
    print("=" * 80)
    
    # Demo 1: Get Order Details (PostgreSQL)
    print("\n1. GETTING ORDER DETAILS FROM POSTGRESQL")
    print("-" * 50)
    
    # Replace with an actual order ID from your database
    order_id = "ORD12345"
    print(f"Getting details for Order ID: {order_id}")
    result = get_order_details(order_id)
    print(json.dumps(result, indent=2))
    
    # Demo 2: Top Products by Category (PostgreSQL)
    print("\n2. TOP SELLING PRODUCTS BY CATEGORY FROM POSTGRESQL")
    print("-" * 50)
    
    result = get_top_products_by_category(3)  # Top 3 per category
    print(json.dumps(result, indent=2))
    
    # Demo 3: Add Product Review (PostgreSQL + MongoDB + InfluxDB)
    print("\n3. ADDING A PRODUCT REVIEW (POSTGRESQL + MONGODB + INFLUXDB)")
    print("-" * 50)
    
    # Replace with actual product and customer from your database
    product_name = "Wireless Headphones"
    customer_name = "Jane Smith"
    
    print(f"Adding review for {product_name} by {customer_name}")
    result = add_product_review(
        product_name, 
        customer_name, 
        5, 
        "These headphones are amazing! Great sound quality and battery life."
    )
    print(json.dumps(result, indent=2))
    
    # Demo 4: Get Product Reviews (MongoDB)
    print("\n4. GETTING PRODUCT REVIEWS FROM MONGODB")
    print("-" * 50)
    
    print(f"Getting reviews for {product_name}")
    result = get_product_reviews(product_name)
    print(json.dumps(result, indent=2))
    
    # Demo 5: Sales Metrics (PostgreSQL + InfluxDB)
    print("\n5. SALES METRICS OVER TIME (POSTGRESQL + INFLUXDB)")
    print("-" * 50)
    
    print("Getting sales metrics for the last 30 days")
    result = get_sales_metrics(30)
    print(json.dumps(result, indent=2))
    
    # Demo 6: Customer Purchase History (PostgreSQL)
    print("\n6. CUSTOMER PURCHASE HISTORY (POSTGRESQL)")
    print("-" * 50)
    
    print(f"Getting purchase history for {customer_name}")
    result = get_customer_purchase_history(customer_name)
    print(json.dumps(result, indent=2))
    
    print("\n" + "=" * 80)
    print("DEMONSTRATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    run_demo()