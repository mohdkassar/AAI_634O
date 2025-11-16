from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
import requests
import time

def extract_sales_data(**context):
    try:
        csv_url = 'https://raw.githubusercontent.com/mohdkassar/AAI_634O/refs/heads/main/project/sales_v2.csv'
        sales_df = pd.read_csv(csv_url)
        print(f"Successfully read {len(sales_df)} sales records.")
        # Push data to XCom for next task
        context['ti'].xcom_push(key='sales_data', value=sales_df.to_json())
        return True
    except FileNotFoundError:
        print(f"Error: {csv_url} not found.")
        return None

def get_city_coordinates(**context):
    try:
        # Pull data from previous task
        transformed_json = context['ti'].xcom_pull(key='sales_data', task_ids='extract')
        df = pd.read_json(transformed_json)
        
        unique_cities = df['store_location'].unique()
        print(f"Found {len(unique_cities)} unique cities: {list(unique_cities)}")

        city_coords = {}
        base_url = "https://nominatim.openstreetmap.org/search"

        # Fetch coordinates for each unique city
        for city in unique_cities:
            try:
                params = {
                    'q': city,
                    'format': 'json',
                    'limit': 1
                }
                
                headers = {
                    'User-Agent': 'SalesDataETL/1.0'
                }
                
                # Make API call
                response = requests.get(base_url, params=params, headers=headers)
                response.raise_for_status()
                
                data = response.json()
                
                if data and len(data) > 0:
                    lat = float(data[0]['lat'])
                    lon = float(data[0]['lon'])
                    city_coords[city] = {'latitude': lat, 'longitude': lon}
                    print(f"{city}: ({lat}, {lon})")
                else:
                    city_coords[city] = {'latitude': None, 'longitude': None}
                    print(f"{city}: No coordinates found")
                
                # Rate limiting - Nominatim requires max 1 request per second
                time.sleep(1)
                
            except Exception as e:
                print(f"Error fetching coordinates for {city}: {e}")
                city_coords[city] = {'latitude': None, 'longitude': None}
        
        # Add latitude and longitude columns to the DataFrame
        df['latitude'] = df['store_location'].map(lambda x: city_coords.get(x, {}).get('latitude'))
        df['longitude'] = df['store_location'].map(lambda x: city_coords.get(x, {}).get('longitude'))
        # Push transformed data to next task
        context['ti'].xcom_push(key='sales_city_data', value=df.to_json())
        return True
    except Exception as e:
        print(f"Error pulling weather data: {e}")
        return False

def pull_weather_data(**context):
    try:
        # Pull data from previous task
        transformed_json = context['ti'].xcom_pull(key='sales_city_data', task_ids='city_coordinates')
        df = pd.read_json(transformed_json)
        
        temperatures = []
        humidities = []

        for index, row in df.iterrows():
            city = row['store_location']
            date = row['date']
            lat = row['latitude']
            long = row['longitude']
    
            formatted_date = date.strftime("%Y-%m-%d")
            print(formatted_date)

            print(f"City {city} Timestamp: {formatted_date} Lat {lat} Long {long}")

            try:
                weather_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={long}&start_date={formatted_date}&end_date={formatted_date}&hourly=temperature_2m,relative_humidity_2m&timezone=America/New_York"
                print(weather_url)
                weather_response = requests.get(weather_url)
                weather_data = weather_response.json()
                temperatures.append(weather_data['hourly']['temperature_2m'][0])
                humidities.append(weather_data['hourly']['relative_humidity_2m'][0])
            except Exception as e:
                print(f"Error pulling weather data: {e}")
                print(e)
                temperatures.append(0)
                humidities.append(0)

        df['temperatures'] = temperatures
        df['humidities'] = humidities

        # Push transformed data to next task
        context['ti'].xcom_push(key='full_data', value=df.to_json())
        return True
    except Exception as e:
        print(f"Error pulling weather data: {e}")
        return False

def load_data(**context):
    try:
        # Pull data from previous task
        transformed_json = context['ti'].xcom_pull(key='full_data', task_ids='pull_weather_data')
        df = pd.read_json(transformed_json)
        
        client = MongoClient('mongodb://localhost:27017/')
        db = client['project']
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
    'data_engineering_project',
    default_args=default_args,
    schedule='0 6 * * *',  # Run every day at 6:00 AM
)

# Create tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_sales_data,
    dag=dag
)

get_city_coordinates_task = PythonOperator(
    task_id='city_coordinates',
    python_callable=get_city_coordinates,
    dag=dag
)

pull_weather_data_task = PythonOperator(
    task_id='pull_weather_data',
    python_callable=pull_weather_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

# Set the order of tasks
extract_task >> get_city_coordinates_task >> pull_weather_data_task >> load_task