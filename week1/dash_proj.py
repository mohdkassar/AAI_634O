import dash
from dash import dcc, html, dependencies
import plotly.express as px

# Load the dataset
df = px.data.gapminder()

# Display the first few rows of the dataset
df.head()
     
# Initialize the Dash app
app = dash.Dash(__name__)
     
# Define the layout of the app
app.layout = html.Div([
    html.H1("Interactive Data Visualization Dashboard", style={'text-align': 'center'}),

    # Dropdown for selecting the country
    dcc.Dropdown(
        id="selected-country",
        options=[{'label': country, 'value': country} for country in df['country'].unique()],
        value='India',  # Default value
        multi=False,
        style={'width': '50%'}
    ),

    # Graph for visualizing life expectancy over time
    dcc.Graph(id="line-chart"),

    # Dropdown for selecting the continent
    dcc.Dropdown(
        id="selected-continent",
        options=[{'label': continent, 'value': continent} for continent in df['continent'].unique()],
        value='Asia',  # Default value
        multi=False,
        style={'width': '50%'}
    ),

    # Graph for visualizing GDP vs Life Expectancy
    dcc.Graph(id="scatter-plot")
])

# Callback to update the line chart based on selected country
@app.callback(
    dependencies.Output('line-chart', 'figure'),
    [dependencies.Input('selected-country', 'value')]
)
def update_line_chart(selected_country):
    # Filter the data for the selected country
    filtered_df = df[df['country'] == selected_country]

    # Create the line chart
    fig = px.line(filtered_df, x="year", y="lifeExp", title=f'Life Expectancy in {selected_country}')
    return fig
     
# Callback to update the scatter plot based on selected continent
@app.callback(
    dependencies.Output('scatter-plot', 'figure'),
    [dependencies.Input('selected-continent', 'value')]
)
def update_scatter_plot(selected_continent):
    # Filter the data for the selected continent
    filtered_df = df[df['continent'] == selected_continent]

    # Create the scatter plot
    fig = px.scatter(filtered_df, x='gdpPercap', y='lifeExp', color='country',
                     size='pop', hover_name='country', log_x=True,
                     title=f'Life Expectancy and GDP in {selected_continent}')
    return fig

     
# Run the Dash app
if __name__ == '__main__':
    app.run(debug=True)

     
