import csv
import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-token"
INFLUX_ORG = "my-org" 
INFLUX_BUCKET = "ecommerce_metrics"         

def parse_date(date_str):
    try:
        day, month, year = date_str.split('-')
        return f"20{year}-{month}-{day}"
    except Exception as e:
        print(f"Error parsing date {date_str}: {e}")
        return None

def clear_influxdb_data():
    """
    Clear data from InfluxDB bucket. If measurement is provided,
    only data from that measurement will be deleted.
    """
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    delete_api = client.delete_api()
    
    try:
        print(f"Deleting ALL data from bucket '{INFLUX_BUCKET}'...")
        
        # Delete from beginning of time to now
        delete_api.delete(
            start="1970-01-01T00:00:00Z",
            stop=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            predicate='',
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG
        )
        print("Data deletion completed successfully")
        
    except Exception as e:
        print(f"Error deleting data: {e}")
    
    finally:
        client.close()

def csv_to_influxdb(csv_path):
    # Clear existing data
    clear_influxdb_data()
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    records = []
    
    try:
        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            
            for row in csv_reader:
                date_str = parse_date(row['Date'])
                if not date_str:
                    continue
                
                point = Point("orders") \
                    .tag("order_id", row['Order ID']) \
                    .tag("category", row['Category']) \
                    .field("price", float(row['Price'])) \
                    .field("quantity", int(row['Quantity'])) \
                    .field("total_sales", float(row['Total Sales'])) \
                    .field("customer_name", row['Customer Name']) \
                    .time(date_str)
                
                records.append(point)
                
            write_api.write(bucket=INFLUX_BUCKET, record=records)
            print(f"Successfully imported {len(records)} records to InfluxDB")
    
    except Exception as e:
        print(f"Error processing CSV file: {e}")
    
    finally:
        client.close()
