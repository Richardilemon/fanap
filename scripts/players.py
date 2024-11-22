import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, Float, MetaData, text
import requests
from io import StringIO

print("Script started", flush=True)

# Define the API URL and raw base URL for player data
api_url = "https://api.github.com/repos/vaastav/Fantasy-Premier-League/contents/data/2023-24/players"
raw_base_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/2023-24/players"
database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"

# Set up the SQLAlchemy engine for database connection
engine = create_engine(database_url)
print("Database URL:", database_url)

# Create or ensure the "players" schema exists
schema_name = "players"
with engine.connect() as connection:
    try:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        connection.commit()
        print(f"Schema '{schema_name}' created or already exists.")
    except Exception as e:
        print(f"Failed to create schema '{schema_name}': {e}")

# Fetch the player directories using the GitHub API
response = requests.get(api_url)
if response.status_code == 200:
    players = response.json()
    print(f"Found {len(players)} player directories.")

    for player in players:
        if player["type"] == "dir":  # Filter for directories only
            player_name = player["name"]
            player_gw_url = f"{raw_base_url}/{player_name}/gw.csv"
            print(f"Attempting to load data for {player_name}")

            try:
                # Request the gw.csv file for each player
                player_response = requests.get(player_gw_url)
                player_response.raise_for_status()
                
                # Load the CSV data into a DataFrame
                csv_data = StringIO(player_response.text)
                df = pd.read_csv(csv_data)

                # Ensure that the DataFrame is not empty and has the expected columns
                if df.empty:
                    print(f"No data available for {player_name}. Skipping...")
                    continue
                
                print("DataFrame structure:")
                print(df.head())
                print(f"Columns: {df.columns.tolist()}")
                
                # Clean up the table name (to ensure it's valid in PostgreSQL)
                table_name = f"{player_name}_gw".replace(":", "_").replace(",", "_").replace("-", "_").replace(" ", "_")
                print(f"Using table name: {table_name}")

                # Check if the table exists, create it if it doesn't
                inspector = inspect(engine)
                if not inspector.has_table(table_name, schema=schema_name):
                    print(f"Table {schema_name}.{table_name} does not exist, creating it...")

                    # Define columns dynamically based on DataFrame columns
                    metadata = MetaData()
                    columns = []
                    for col in df.columns:
                        # Define column types based on data
                        if pd.api.types.is_integer_dtype(df[col]):
                            col_type = Integer
                        elif pd.api.types.is_float_dtype(df[col]):
                            col_type = Float
                        else:
                            col_type = String

                        # Add the column to the list
                        columns.append(Column(col, col_type))
                    
                    # Create the table using the dynamically defined columns within the schema
                    table = Table(table_name, metadata, *columns, schema=schema_name)
                    table.create(bind=engine)
                else:
                    print(f"Table {schema_name}.{table_name} already exists.")
                
                # Insert the data into the database
                df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists="append", index=False)
                print(f"Data for {player_name} loaded successfully into table {schema_name}.{table_name}.")
            
            except requests.exceptions.HTTPError as e:
                print(f"Failed to retrieve data for {player_name}: {e}")
            except Exception as e:
                print(f"An error occurred with {player_name}: {e}")
else:
    print(f"Failed to retrieve player directories: {response.status_code}")
