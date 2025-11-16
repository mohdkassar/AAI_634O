
import requests
import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient

load_dotenv()

api_key = os.getenv('OPEN_WEATHER_API_KEY')
mongo_connection_string = os.getenv('MONGO_CONNECTION_STRING')

client = MongoClient(mongo_connection_string)
db = client['sales_db']

def fetch_weather_data(city, date):
    base_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(base_url)
    data = response.json()
    # Extract temperature, humidity, and weather description
    temperature = data['main']['temp'] - 273.15 # Convert from Kelvin to Celsius
    humidity = data['main']['humidity']
    weather_description = data['weather'][0]['description']
    return temperature, humidity, weather_description

csv_link = "https://raw.githubusercontent.com/DrManalJalloul/Introduction-to-Data-Engineering/refs/heads/main/sales_data.csv"

sales_data = pd.read_csv(csv_link)

print(sales_data.head())

temperatures = []
humidities = []
weather_descriptions = []

for index, row in sales_data.iterrows():
    city = row['store_location']
    date = row['date']
    
    temperature, humidity, weather_description = fetch_weather_data(city, date)
    
    temperatures.append(temperature)
    humidities.append(humidity)
    weather_descriptions.append(weather_description)

sales_data['temperature'] = temperatures
sales_data['humidity'] = humidities
sales_data['weather_description'] = weather_descriptions

print(sales_data.head())

sales_records = sales_data.to_dict('records')
result = db.sales.insert_many(sales_records)
print(f"Inserted {len(result.inserted_ids)} sales records into MongoDB")