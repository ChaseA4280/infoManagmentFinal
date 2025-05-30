# config.py
# PostgreSQL connection parameters
PG_CONFIG = {
    'dbname': 'ecommerce',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

# MongoDB connection parameters
MONGO_CONFIG = {
    'connection_string': 'mongodb://localhost:27017/',
    'database': 'ecommerce',
    'collections': {
        'product_reviews': 'product_reviews',
        'product_details': 'product_details',
        'user_profiles': 'user_profiles'
    }
}

# InfluxDB connection parameters
INFLUX_CONFIG = {
    'url': 'http://localhost:8086',
    'token': 'my-token',
    'org': 'my-org',
    'bucket': 'ecommerce_metrics'
}
