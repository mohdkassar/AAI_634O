import pandas as pd
import dash
from dash import dcc, html
import plotly.express as px
from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")
db = client.sales_db
sales_collection = db.sales

sales_data = list(sales_collection.find({}))
sales_data = pd.DataFrame(sales_data)

app = dash.Dash(__name__)

sales_by_location = sales_data.groupby('store_location')['sales_amount'].sum().reset_index()

fig = px.bar(sales_by_location, x='store_location', y='sales_amount', title='Sales by Store Location')

sales_by_product = sales_data.groupby('product_id').size().reset_index(name='sales_count')

fig2 = px.bar(sales_by_product, x='product_id', y='sales_count', title='Sales Count by Product ID')

app.layout = html.Div(children=[
    html.H1(children='Sales Dashboard'), 
    dcc.Graph(figure=fig),
    dcc.Graph(figure=fig2)
    ])

if __name__ == '__main__':
    app.run(debug=True)