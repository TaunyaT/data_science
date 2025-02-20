# %%
import requests
import pandas as pd

# Define API key and URl
api_key = 'cb6188953f8b54215f043c248b253a33'
api_url = 'http://api.openweathermap.org/data/2.5/forecast'

# %%
# User agent
headers = {'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"}

# %%
# List of cities and coordinates
cities = [
    {"name": "Toronto", "lat": 43.65107, "lon": -79.347015},
    {"name": "Vancouver", "lat": 49.2827291, "lon": -123.1207375},
    {"name": "Montreal", "lat": 45.5016889, "lon": -73.567256},
    {"name": "Calgary", "lat": 51.0447331, "lon": -114.0718831},
    {"name": "Ottawa", "lat": 45.4215296, "lon": -75.6971931},
    {"name": "Edmonton", "lat": 53.5461245, "lon": -113.4938229},
    {"name": "Winnipeg", "lat": 49.895136, "lon": -97.1383744},
    {"name": "Quebec City", "lat": 46.8138783, "lon": -71.2079809},
    {"name": "Hamilton", "lat": 43.2557206, "lon": -79.8711024},
    {"name": "Kitchener", "lat": 43.4516395, "lon": -80.4925337},
    {"name": "Halifax", "lat": 44.6488625, "lon": -63.5753196}
]

# %%
# Function to get weather forecast
def fetch_forecast_data(city):
    params = {
        'lat': city['lat'],
        'lon': city['lon'],
        'appid': api_key,
        'units': 'metric'
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} for {city['name']}")
        return None


# %%
# Fetch data for all cities
for city in cities:
    print(f"Fetching data for {city['name']}...")
    fetch_forecast_data(city)

# %%
# Collect weather forecast data for the cities
forecast_data = []
for city in cities:
    data = fetch_forecast_data(city)
    if data:
        for forecast in data['list']:
            forecast_data.append({
                'City': city['name'],
                "Lat": city['lat'],
                "Lng": city['lon'],
                'Datetime': pd.to_datetime(forecast['dt'], unit='s'),
                'Temperature Forecast': forecast['main']['temp'],
                'Weather': forecast['weather'][0]['description']
            })

# %%
# Create a DataFrame from the collected data
forecast_df = pd.DataFrame(forecast_data)
print(forecast_df.head())

# %%
forecast_df.describe()

# %%
forecast_df.info()

# %%
forecast_df

# %%
# Change to daily data
# Add a 'day' column to represent each unique day
forecast_df['day'] = forecast_df['Datetime'].dt.date

# %%
# Aggregate the data by the 'day' column, calculating the mean for temperature for each day
daily_forecast_df = forecast_df.groupby(['City', 'day']).agg({
    'Temperature Forecast': ['mean', 'min', 'max'],
    'Lat': 'first',  
    'Lng': 'first'   
}).reset_index()

# %%
daily_forecast_df

# %%
# Flatten the column hierarchy and rename columns for clarity
daily_forecast_df.columns = ['City', 'Date', 'Av. Temp. Forecast', 'Min Temperature', 'Max Temperature', 'Lat', 'Lng']

# %%
daily_forecast_df

# %%
# Round the temperatures to one decimal place
daily_forecast_df['Av. Temp. Forecast'] = daily_forecast_df['Av. Temp. Forecast'].astype(float).round(1)
daily_forecast_df['Min Temperature'] = daily_forecast_df['Min Temperature'].astype(float).round(1)
daily_forecast_df['Max Temperature'] = daily_forecast_df['Max Temperature'].astype(float).round(1)

# %%
daily_forecast_df

# %%
!pip install sqlalchemy pyodbc

# %%
import sqlalchemy as sa
from sqlalchemy import create_engine

# %%
connection_url = sa.engine.URL.create(
    drivername = "mssql+pyodbc",
    username   = "ttansey",
    password   = "2024!Schulich",
    host       = "mban2024-ms-sql-server.c1oick8a8ywa.ca-central-1.rds.amazonaws.com",
    port       = "1433",
    database   = "ttansey_db",
    query = {
        "driver" : "ODBC Driver 18 for SQL Server",
        "TrustServerCertificate" : "yes"
    }
)

# %%
daily_forecast_df['Date'] = pd.to_datetime(daily_forecast_df['Date'], errors='coerce')

# %%
my_engine = sa.create_engine(connection_url)

# %%
daily_forecast_df.info()

# %%
# Write the DataFrame to SQL table
daily_forecast_df.to_sql(
    name='forecat_weather_data',
    con=my_engine,
    schema='uploads',
    if_exists='replace',
    index=False,
    dtype= {
        'City': sa.types.String,
        'Date': sa.types.DATE,
        'Av. Temp. Temperature': sa.types.Float,
        'Min Temperature': sa.types.Float,
        'Max Temperature': sa.types.Float,
        'Lat': sa.types.Float,
        'Lng': sa.types.Float
    },
    method='multi'
)


