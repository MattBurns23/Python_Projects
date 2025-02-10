import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pymysql
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure API_KEY exists
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY is missing. Check your .env file.")

# Change the working directory
project_dir = r"C:\Users\mattb\Documents\PythonProjects\ETL_Weather_API_Project"
if os.path.exists(project_dir):
    os.chdir(project_dir)
else:
    print(f"⚠️ Warning: Directory '{project_dir}' does not exist. Check your path.")

# Fetch database credentials from .env
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "weather_db"),
}

# Create a reusable SQLAlchemy engine
ENGINE = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

# Fetch weather data for cities
def fetch_weather_data(cities):
    weather_data = {}
    for city in cities:
        api_url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=imperial"
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
            weather_data[city] = response.json()
        except requests.RequestException as e:
            print(f"⚠️ Error fetching data for {city}: {e}")
            weather_data[city] = None
    
    return weather_data

# Transform raw weather data into a DataFrame
def transform_data(data):
    if not data:
        return None

    weather_info_list = []
    for city, city_data in data.items():
        if city_data:  # Ensure city data exists
            weather_info = {
                'City': city_data['name'],
                'Temperature': city_data['main']['temp'],
                'Feels Like (°F)': city_data["main"]["feels_like"],
                'Humidity (%)': city_data["main"]["humidity"],
                'Weather Condition': city_data["weather"][0]["description"],
                'Wind Speed (mph)': city_data["wind"]["speed"],
                'Timestamp': datetime.now()
            }
            weather_info_list.append(weather_info)

    return pd.DataFrame(weather_info_list)

# Clean column names for MySQL compatibility
def clean_columns(df):
    df.columns = (
        df.columns.str.strip()  # Remove leading/trailing spaces
        .str.replace(r'[^\w\s]', '', regex=True)  # Remove special characters
        .str.replace(' ', '_')  # Replace spaces with underscores
    )
    return df

# Create weather table in MySQL if it doesn't exist
def create_weather_table():
    with ENGINE.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS weather (
                id INT AUTO_INCREMENT PRIMARY KEY,
                City VARCHAR(255),
                Temperature FLOAT,
                Feels_Like_F FLOAT,
                Humidity  INT,
                Weather_Condition VARCHAR(255),
                Wind_Speed_mph FLOAT,
                Timestamp DATETIME
            );
        """))
        conn.commit()
    
    print("✅ Table 'weather' checked/created successfully!")
# Call function
create_weather_table()

# Load data to MySQL 
def load_to_mysql(df):
    if df is None or df.empty:
        print("No data to load into MySQL!")
        return

    try:
        # Convert timestamp to datetime for proper MySQL storage
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        # Load DataFrame into MySQL (append mode)
        df.to_sql('weather', con=ENGINE, if_exists='append', index=False)
        print("✅ Data successfully loaded into MySQL!")
    except Exception as e:
        print(f"⚠️ Error loading data into MySQL: {e}")
    

# Fetch data from MySQL
def fetch_from_mysql():
    # Read data into pandas DataFrame
    query = "SELECT * FROM weather"
    return pd.read_sql(query, con=ENGINE)

# Save Data to CSV
def save_to_csv(df, csv_file_path='weather_data.csv'):
    df.to_csv(csv_file_path, mode='a', header=not os.path.exists(csv_file_path), index=False)
    print("✅ Data successfully saved to CSV!")
    

# Job to fetch, transform, and load data
def job():
    print(f"Running job at {datetime.now()}")
    cities = ["Seattle", "Los Angeles", "Las Vegas", "Fort Worth", "Miami", "Nashville", "New York"]
    data = fetch_weather_data(cities)
    df = transform_data(data)

    if df is not None and not df.empty:
        df = clean_columns(df)
        load_to_mysql(df)
        save_to_csv(df)
        print("✅ Data pulled and loaded successfully!")
    else:
        print("⚠️ No valid data to process.")

# Run the job
job()
