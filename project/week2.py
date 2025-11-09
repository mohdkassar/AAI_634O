import dash
from dash import dcc, html
import plotly.express as px
from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")
db = client.sales_db
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

app = dash.Dash(__name__)

sales_location_df = pd.DataFrame(sales_by_location)
sales_product_df = pd.DataFrame(sales_by_product)

fig = px.bar(sales_location_df, x='store_location', y='sales_amount', title='Sales by Store Location')

fig2 = px.bar(sales_product_df, x='product_id', y='sales_count', title='Sales Count by Product ID')

app.layout = html.Div(children=[
    html.H1(children='Sales Dashboard'), 
    dcc.Graph(figure=fig),
    dcc.Graph(figure=fig2)
    ])

if __name__ == '__main__':
    app.run(debug=True)