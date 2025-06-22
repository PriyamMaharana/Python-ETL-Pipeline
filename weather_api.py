import pandas as pd
import json
import requests
from sqlalchemy import create_engine, text

## ETL pipeline model
## Extract
def extract_data():
    input_loc = input("\n📍 Enter location (or type 'exit or stop' to quit): ").strip()
    
    if input_loc.lower() == "exit" or input_loc == "stop":
        return None
    
    loc = input_loc.capitalize()
    # ensure to create your own API key
    api_url = f"http://api.weatherapi.com/v1/current.json?key=61b7b99628ca41fdae3180958252206&q={loc}"
    data = requests.get(api_url).json()
    return data

## Transform
def transform_data(data:dict):
    location_data = {
        "name": data["location"]["name"],
        "region": data["location"]["region"],
        "country": data["location"]["country"],
        "lat": data["location"]["lat"],
        "lon": data["location"]["lon"],
        "tz_id": data["location"]["tz_id"],
        "local_time": data["location"]["localtime"]
    }

    current_data = {
        "temp_c": data["current"]["temp_c"],
        "humidity": data["current"]["humidity"],
        "condition_text": data["current"]["condition"]["text"],
        "feelslike_c": data["current"]["feelslike_c"],
        "last_updated": data["current"]["last_updated"]
    }

    return location_data, current_data


## Loading
def load_data(location_data: dict, current_data: dict):
    engine = create_engine('postgresql+psycopg2://postgres:1234@localhost:5432/weather_db')

    with engine.begin() as conn:  # THIS ENSURES COMMIT
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS location (
                location_id SERIAL PRIMARY KEY,
                name VARCHAR,
                region VARCHAR,
                country VARCHAR,
                lat FLOAT,
                lon FLOAT,
                tz_id VARCHAR,
                local_time TIMESTAMP  -- renamed to avoid keyword conflict
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS current_weather (
                weather_id SERIAL PRIMARY KEY,
                location_id INT REFERENCES location(location_id),
                temp_c FLOAT,
                humidity INT,
                condition_text VARCHAR,
                feelslike_c FLOAT,
                last_updated TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

        location_id = conn.execute(
            text("""
                INSERT INTO location (name, region, country, lat, lon, tz_id, local_time)
                VALUES (:name, :region, :country, :lat, :lon, :tz_id, :local_time)
                RETURNING location_id
            """),
            location_data
        ).scalar()

        current_data["location_id"] = location_id
        conn.execute(
            text("""
                INSERT INTO current_weather (location_id, temp_c, humidity, condition_text, feelslike_c, last_updated)
                VALUES (:location_id, :temp_c, :humidity, :condition_text, :feelslike_c, :last_updated)
            """),
            current_data
        )
        
        result = conn.execute(text("SELECT COUNT(*) FROM current_weather"))
        print("🔍 Weather records in DB:", result.scalar())


# --- Main ETL Flow ---
if __name__ == "__main__":
    print("🌦️ Weather ETL Pipeline Started. Type 'exit or stop' to quit.")
    while True:
        try:
            raw_data = extract_data()
            
            if raw_data is None:
                print("👋 Exiting. Goodbye!")
                break
            
            city = raw_data["location"]["name"]
            country = raw_data["location"]["country"]
            location_data, current_data = transform_data(raw_data)
            load_data(location_data, current_data)
            print(f"✅ Weather data for {city}, {country} saved.")
            
        except Exception as e:
            print(f"❌ Error: {e}")