# main.py
import os
import sys
from schema import create_schema
from load_data import load_csv_data
from db_operations import (
    get_order_details,
    get_top_products_by_category,
    add_product_review,
    get_product_reviews,
    get_sales_metrics,
    get_customer_purchase_history
)

def display_menu():
    """Display the main menu options"""
    print("\n===== E-Commerce Database System =====")
    print("1. Initialize Database Schema")
    print("2. Load CSV Data")
    print("3. Get Order Details")
    print("4. Get Top Products by Category")
    print("5. Add Product Review")
    print("6. Get Product Reviews")
    print("7. Get Sales Metrics")
    print("8. Get Customer Purchase History")
    print("9. Exit")
    return input("Select an option (1-9): ")

def main():
    """Main application function"""
    while True:
        choice = display_menu()
        
        if choice == "1":
            # Initialize Database Schema
            print("\nInitializing database schema...")
            try:
                create_schema()
                print("Database schema initialized successfully!")
            except Exception as e:
                print(f"Error initializing schema: {str(e)}")
        
        elif choice == "2":
            # Load CSV Data
            csv_file = input("Enter the CSV file path (default: amazon_sales_data 2025.csv): ") or "amazon_sales_data 2025.csv"
            
            if not os.path.exists(csv_file):
                print(f"Error: File {csv_file} not found!")
                continue
                
            try:
                load_csv_data(csv_file)
            except Exception as e:
                print(f"Error loading data: {str(e)}")
        
        elif choice == "3":
            # Get Order Details
            order_id = input("Enter Order ID: ")
            try:
                result = get_order_details(order_id)
                if "error" in result:
                    print(f"Error: {result['error']}")
                else:
                    print("\nOrder Details:")
                    print(f"Order ID: {result['order_id']}")
                    print(f"Date: {result['order_date']}")
                    print(f"Status: {result['status']}")
                    print(f"Customer: {result['customer']['name']} ({result['customer']['location']})")
                    print(f"Payment Method: {result['payment_method']}")
                    print(f"Total: ${result['order_total']:.2f}")
                    
                    print("\nItems:")
                    for item in result['items']:
                        print(f"- {item['product_name']} (${item['price']:.2f} x {item['quantity']} = ${item['total']:.2f})")
            except Exception as e:
                print(f"Error getting order details: {str(e)}")
        
        elif choice == "4":
            # Get Top Products by Category
            limit = input("Enter the number of products per category (default: 5): ") or 5
            try:
                result = get_top_products_by_category(int(limit))
                
                print("\nTop Products by Category:")
                for category, products in result.items():
                    print(f"\n{category}:")
                    for i, product in enumerate(products, 1):
                        print(f"{i}. {product['product_name']} - {product['total_sold']} sold (${product['total_revenue']:.2f})")
            except Exception as e:
                print(f"Error getting top products: {str(e)}")
        
        elif choice == "5":
            # Add Product Review
            product_name = input("Enter Product Name: ")
            customer_name = input("Enter Customer Name: ")
            
            try:
                rating = int(input("Enter Rating (1-5): "))
                if rating < 1 or rating > 5:
                    print("Rating must be between 1 and 5!")
                    continue
            except ValueError:
                print("Invalid rating! Please enter a number between 1 and 5.")
                continue
                
            review_text = input("Enter Review Text: ")
            
            try:
                result = add_product_review(product_name, customer_name, rating, review_text)
                if "error" in result:
                    print(f"Error: {result['error']}")
                else:
                    print(f"Success: {result['message']} (ID: {result['review_id']})")
            except Exception as e:
                print(f"Error adding review: {str(e)}")
        
        elif choice == "6":
            # Get Product Reviews
            product_name = input("Enter Product Name: ")
            
            try:
                result = get_product_reviews(product_name)
                if "error" in result:
                    print(f"Error: {result['error']}")
                else:
                    print(f"\nProduct: {result['product']['name']} (${result['product']['price']:.2f})")
                    print(f"Category: {result['product']['category']}")
                    print(f"Average Rating: {result['average_rating']} ({result['review_count']} reviews)")
                    
                    if result['reviews']:
                        print("\nReviews:")
                        for i, review in enumerate(result['reviews'], 1):
                            print(f"\n{i}. {review['customer_name']} - {review['rating']}/5 stars")
                            print(f"   {review['review_text']}")
                            print(f"   Posted on: {review['created_at']}")
                    else:
                        print("\nNo reviews yet for this product.")
            except Exception as e:
                print(f"Error getting product reviews: {str(e)}")
        
        elif choice == "7":
            # Get Sales Metrics for a specific date
            date = input("Enter the date for analysis (format: YYYY-MM-DD): ")
            
            try:
                result = get_sales_metrics(date)  # Pass the specific date to the function
                if "error" in result:
                    print(f"Error: {result['error']}")
                else:
                    print(f"\nSales Metrics for {date}:")
                    print(f"Total Sales: ${result['total_sales']:.2f}")
                    print(f"Total Orders: {result['order_count']}")
                    print(f"Total Items Sold: {result['items_sold']}")
            except Exception as e:
                print(f"Error getting sales metrics: {str(e)}")
                
        elif choice == "8":
            # Get Customer Purchase History
            customer_name = input("Enter Customer Name: ")
            
            try:
                result = get_customer_purchase_history(customer_name)
                if "error" in result:
                    print(f"Error: {result['error']}")
                else:
                    customer = result['customer']
                    print(f"\nCustomer: {customer['name']} ({customer['location']})")
                    print(f"Total Orders: {result['order_count']}")
                    print(f"Total Spent: ${result['total_spent']:.2f}")
                    
                    print("\nFavorite Categories:")
                    for i, category in enumerate(result['favorite_categories'][:3], 1):
                        print(f"{i}. {category['category']} - {category['purchase_count']} purchases (${category['total_spent']:.2f})")
                    
                    print("\nOrder History:")
                    for order in result['orders']:
                        print(f"\nOrder {order['order_id']} ({order['date']}) - {order['status']}")
                        print(f"Payment Method: {order['payment_method']}")
                        print(f"Total: ${order['total']:.2f}")
                        
                        print("Items:")
                        for item in order['items']:
                            print(f"- {item['product']} (${item['price']:.2f} x {item['quantity']} = ${item['total']:.2f})")
            except Exception as e:
                print(f"Error getting customer history: {str(e)}")
        
        elif choice == "9":
            # Exit
            print("Thank you for using the E-Commerce Database System!")
            sys.exit(0)
        
        else:
            print("Invalid choice! Please select a number between 1 and 9.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()