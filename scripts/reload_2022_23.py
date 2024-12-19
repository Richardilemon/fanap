import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, Float, MetaData, text
import requests
from io import StringIO
import time

print("Script started", flush=True)

# Define constants
season = "2022-23"
api_url = f"https://api.github.com/repos/vaastav/Fantasy-Premier-League/contents/data/{season}/players"
raw_base_url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/players"
database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"
schema_name = "players_2022_23"

# Set up the SQLAlchemy engine for database connection
engine = create_engine(database_url)
print("Database URL:", database_url)

# Ensure the schema exists
with engine.connect() as connection:
    try:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        connection.commit()
        print(f"Schema '{schema_name}' created or already exists.")
    except Exception as e:
        print(f"Failed to create schema '{schema_name}': {e}")

# Function to fetch data with retries
def fetch_with_retries(url, max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 503:
                print(f"503 Error encountered. Retry {attempt + 1}/{max_retries} in {delay} seconds...")
                time.sleep(delay)
            else:
                raise e
    raise Exception("Max retries reached.")

# Fetch the player directories
response = requests.get(api_url)
if response.status_code == 200:
    players = response.json()
    print(f"Found {len(players)} player directories for {season}.")

    for player in players:
        if player["type"] == "dir":  # Filter for directories only
            player_name = player["name"]
            player_gw_url = f"{raw_base_url}/{player_name}/gw.csv"
            print(f"Attempting to load data for {player_name} in season {season}")

            try:
                # Fetch and load player data
                player_response = fetch_with_retries(player_gw_url)
                csv_data = StringIO(player_response.text)
                df = pd.read_csv(csv_data)

                # Ensure the DataFrame is not empty
                if df.empty:
                    print(f"No data available for {player_name}. Skipping...")
                    continue
                
                print("DataFrame structure:")
                print(df.head())
                print(f"Columns: {df.columns.tolist()}")

                # Clean up the table name
                table_name = f"{player_name}_gw".replace(":", "_").replace(",", "_").replace("-", "_").replace(" ", "_")
                print(f"Using table name: {table_name}")

                # Drop the table if it already exists
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE"))

                # Dynamically define columns based on DataFrame
                metadata = MetaData()
                columns = []
                for col in df.columns:
                    if pd.api.types.is_integer_dtype(df[col]):
                        col_type = Integer
                    elif pd.api.types.is_float_dtype(df[col]):
                        col_type = Float
                    else:
                        col_type = String
                    columns.append(Column(col, col_type))
                
                # Create the table in the database
                table = Table(table_name, metadata, *columns, schema=schema_name)
                table.create(bind=engine)
                print(f"Table {schema_name}.{table_name} created successfully.")

                # Insert the data into the table
                df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists="replace", index=False)
                print(f"Data for {player_name} loaded successfully into {schema_name}.{table_name}.")

            except requests.exceptions.HTTPError as e:
                print(f"Failed to retrieve data for {player_name} in season {season}: {e}")
            except Exception as e:
                print(f"An error occurred with {player_name} in season {season}: {e}")

else:
    print(f"Failed to retrieve player directories for {season}: {response.status_code}")
