from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from pymongo import MongoClient

def extract_sales_data(**context):
    try:
        csv_url = 'https://raw.githubusercontent.com/mohdkassar/AAI_634O/main/week3/sales.csv'
        sales_df = pd.read_csv(csv_url)
        print(f"Successfully read {len(sales_df)} sales records.")
        # Push data to XCom for next task
        context['ti'].xcom_push(key='sales_data', value=sales_df.to_json())
        return True
    except FileNotFoundError:
        print(f"Error: {csv_url} not found.")
        return None

def transform_sales_data(**context):
    # Pull data from previous task
    sales_json = context['ti'].xcom_pull(key='sales_data', task_ids='extract')
    df = pd.read_json(sales_json)
    
    df['total_revenue'] = df['quantity'] * df['price']
    print(f"Successfully transformed {len(df)} sales records.")
    
    # Push transformed data to next task
    context['ti'].xcom_push(key='transformed_data', value=df.to_json())
    return True

def load_data(**context):
    try:
        # Pull data from previous task
        transformed_json = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
        df = pd.read_json(transformed_json)
        
        client = MongoClient('mongodb://172.17.0.2:27017/')
        db = client['sales_db']
        sales_collection = db.sales
        sales_records = df.to_dict('records')
        sales_collection.delete_many({})
        result = sales_collection.insert_many(sales_records)
        print(f"Inserted {len(result.inserted_ids)} sales records into MongoDB")
        return True
    except Exception as e:
        print(f"Error loading sales data: {e}")
        return False

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'etl_pipeline_mongodb',
    default_args=default_args,
    schedule='0 6 * * *',  # Run every day at 6:00 AM
)

# Create tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_sales_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_sales_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

# Set the order of tasks
extract_task >> transform_task >> load_task