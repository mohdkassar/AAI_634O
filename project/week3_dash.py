import dash
from dash import dcc, html
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb+srv://kassar:L75frmR1eHhy6MOG@cluster1.987e4me.mongodb.net/")
db = client.project
sales_collection = db.sales

# Aggregation pipeline to retrieve sales by store location
sales_by_location = list(sales_collection.aggregate([
    {
        '$group': {
            '_id': '$store_location',
            'sales_amount': {'$sum': '$sales_amount'}
        }
    },
    {
        '$project': {
            'store_location': '$_id',
            'sales_amount': 1,
            '_id': 0
        }
    }
]))

# Aggregation pipeline to retrieve sales count by product ID
sales_by_product = list(sales_collection.aggregate([
    {
        '$group': {
            '_id': '$product_id',
            'sales_count': {'$sum': 1}
        }
    },
    {
        '$project': {
            'product_id': '$_id',
            'sales_count': 1,
            '_id': 0
        }
    }
]))

# Aggregation pipeline for weather impact on sales
weather_sales = list(sales_collection.aggregate([
    {
        '$project': {
            'store_location': 1,
            'sales_amount': 1,
            'temperatures': 1,
            'humidities': 1,
            'product_id': 1,
            '_id': 0
        }
    }
]))

# Aggregation pipeline for average weather by location
avg_weather_by_location = list(sales_collection.aggregate([
    {
        '$group': {
            '_id': '$store_location',
            'avg_temperature': {'$avg': '$temperatures'},
            'avg_humidity': {'$avg': '$humidities'},
            'total_sales': {'$sum': '$sales_amount'}
        }
    },
    {
        '$project': {
            'store_location': '$_id',
            'avg_temperature': 1,
            'avg_humidity': 1,
            'total_sales': 1,
            '_id': 0
        }
    }
]))

app = dash.Dash(__name__)

# Create DataFrames
sales_location_df = pd.DataFrame(sales_by_location)
sales_product_df = pd.DataFrame(sales_by_product)
weather_sales_df = pd.DataFrame(weather_sales)
avg_weather_df = pd.DataFrame(avg_weather_by_location)

# Original figures
fig1 = px.bar(sales_location_df, x='store_location', y='sales_amount', 
              title='Sales by Store Location')

fig2 = px.bar(sales_product_df, x='product_id', y='sales_count', 
              title='Sales Count by Product ID')

# Scatter plot: Sales vs Temperature
fig3 = px.histogram(weather_sales_df, x='temperatures', y='sales_amount', 
                    histfunc='avg', nbins=10,
                    title='Average Sales by Temperature',
                    labels={'temperatures': 'Temperature (°C)', 
                            'sales_amount': 'Avg Sales ($)'})


# Average weather conditions by location
fig4 = px.bar(avg_weather_df,
    title='Avg Temperature',
    x=avg_weather_df['store_location'],
    y=avg_weather_df['avg_temperature'],
)

fig5 = px.box(weather_sales_df, x='product_id', y='temperatures', 
              color='product_id',
              title='Temperature Distribution by Product ID',
              labels={'temperatures': 'Temperature (°C)', 
                      'product_id': 'Product ID'})

app.layout = html.Div(children=[
    html.H1(children='Sales Dashboard', style={'textAlign': 'center'}),
    
    html.Div([
        html.Div([dcc.Graph(figure=fig1)], style={'width': '50%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig2)], style={'width': '50%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig3)], style={'width': '100%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig4)], style={'width': '100%', 'display': 'inline-block'}),
        html.Div([dcc.Graph(figure=fig5)], style={'width': '100%', 'display': 'inline-block'}),
    ]),
])

if __name__ == '__main__':
    app.run(debug=True, port=8050)