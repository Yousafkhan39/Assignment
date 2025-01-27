import pymongo
import psycopg2
from psycopg2.extras import execute_values

# MongoDB connection details
MONGO_URI = "mongodb://etl_user:aP8fwfgftempRhkgGa9@3.251.75.195:27017/?authSource=sales"
MONGO_DB = "sales"
MONGO_COLLECTION = "sales"

# PostgreSQL connection details
POSTGRES_HOST = "rds-module.cnc6gugkeq4f.eu-west-1.rds.amazonaws.com"
POSTGRES_DB = "sales_db3"
POSTGRES_USER = "yousef"
POSTGRES_PASSWORD = "yousef"
POSTGRES_TABLE = "sales"

def fetch_data_from_mongodb():
    """Fetch data from MongoDB collection."""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Fetch all documents from the MongoDB collection
    data = list(collection.find())
    client.close()
    return data

def transform_data(data):
    """Transform MongoDB data for PostgreSQL."""
    transformed_data = []
    for doc in data:
        # Remove MongoDB-specific fields like '_id' or convert as needed
        transformed_doc = {
           "event_time": doc.get("event_time"),
            "order_id": doc.get("order_id"),
            "product_id": doc.get("product_id"),
            "category_id": doc.get("category_id"),
            "category_code": doc.get("category_code"),
            "brand": doc.get("brand"),
            "price": doc.get("price"),
            "user_id": doc.get("user_id"),
        }
        transformed_data.append(tuple(transformed_doc.values()))
    return transformed_data

def insert_data_into_postgresql(data):
    """Insert data into PostgreSQL table."""
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = connection.cursor()

    # Define the insert query
    insert_query = f"""
    INSERT INTO {POSTGRES_TABLE} (event_time, order_id, product_id,category_id,category_code,brand,price,user_id)
    VALUES %s
    
    """
#ON CONFLICT (primary_key_column) DO NOTHING; -- Adjust as needed
    # Use execute_values for batch insertion
    execute_values(cursor, insert_query, data)

    connection.commit()
    cursor.close()
    connection.close()

def main():
    """Main function to orchestrate the data pipeline."""
    print("Fetching data from MongoDB...")
    mongodb_data = fetch_data_from_mongodb()

    print("Transforming data...")
    transformed_data = transform_data(mongodb_data)

    print("Inserting data into PostgreSQL...")
    insert_data_into_postgresql(transformed_data)

    print("Data pipeline completed successfully!")

if __name__ == "__main__":
    main()