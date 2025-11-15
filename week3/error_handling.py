import pandas as pd
from pymongo import MongoClient

def extract_sales_data(csv_file):
    try:
        sales_df = pd.read_csv(csv_file)
        print(f"Successfully read {len(sales_df)} sales records.")
        return sales_df
    except FileNotFoundError:
        print(f"Error: {csv_file} not found.")

def transform_sales_data(df):
    df['total_revenue'] = df['quantity'] * df['price']
    print(f"Successfully transformed {len(df)} sales records.")
    return df

def load_data(db, df):
    try:
        sales_collection = db.sales
        
        sales_records = df.to_dict('records')
        
        sales_collection.delete_many({})
        
        result = sales_collection.insert_many(sales_records)
        print(f"Inserted {len(result.inserted_ids)} sales records into MongoDB")
    except Exception as e:
        print(f"Error loading patient data: {e}")
        return False

def connect_to_mongodb(connection_string, db_name):
    try:
        client = MongoClient(connection_string)
        db = client[db_name]
        print(f"Successfully connected to MongoDB database: {db_name}")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def run_pipeline(csv_file='./sales.csv',
        connection_string='mongodb://localhost:27017/',
        db_name='sales_db'):
    
    db = connect_to_mongodb(connection_string, db_name)

    df = extract_sales_data(csv_file)
    
    df = transform_sales_data(df)
    
    load_data(db, df)

if __name__ == "__main__":
    run_pipeline(
        csv_file='./sales.csv',
        connection_string='mongodb://localhost:27017/',
        db_name='sales_db'
    )