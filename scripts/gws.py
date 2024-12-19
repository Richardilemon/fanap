import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, Float, MetaData, text
import requests
from io import StringIO

print("Script started", flush=True)

# Define database connection
database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"

# Set up the SQLAlchemy engine for database connection
engine = create_engine(database_url)
print("Database URL:", database_url)

# Define the years to collect data for
years = [
    f"{year}-{str(year + 1)[-2:]}" for year in range(2016, 2024)
]  # 2016/17 to 2023/24

# Create or ensure the "gws" schema exists
schema_name = "gws"
with engine.connect() as connection:
    try:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        connection.commit()
        print(f"Schema '{schema_name}' created or already exists.")
    except Exception as e:
        print(f"Failed to create schema '{schema_name}': {e}")

# Loop through each year and fetch gameweek data
for year in years:
    print(f"Processing data for season {year}...")

    # Define the API URL and raw base URL for gameweek data
    api_url = f"https://api.github.com/repos/vaastav/Fantasy-Premier-League/contents/data/{year}/gws"
    raw_base_url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{year}/gws"

    # Fetch the gameweek files using the GitHub API
    response = requests.get(api_url)
    if response.status_code == 200:
        gws_files = response.json()
        print(f"Found {len(gws_files)} gameweek files for {year}.")

        for gw_file in gws_files:
            if gw_file["type"] == "file" and gw_file["name"].endswith(".csv"):  # Filter for CSV files
                gw_file_name = gw_file["name"]
                gw_file_url = f"{raw_base_url}/{gw_file_name}"
                print(f"Attempting to load data for {gw_file_name} in season {year}")

                try:
                    # Request the gameweek CSV file
                    gw_response = requests.get(gw_file_url)
                    gw_response.raise_for_status()

                    # Load the CSV data into a DataFrame
                    csv_data = StringIO(gw_response.text)
                    df = pd.read_csv(csv_data)

                    # Ensure that the DataFrame is not empty and has the expected columns
                    if df.empty:
                        print(f"No data available in {gw_file_name} for season {year}. Skipping...")
                        continue

                    print("DataFrame structure:")
                    print(df.head())
                    print(f"Columns: {df.columns.tolist()}")

                    # Clean up the table name (to ensure it's valid in PostgreSQL)
                    table_name = f"{year}_{gw_file_name.replace('.csv', '').replace(' ', '_')}"
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
                    print(f"Data for {gw_file_name} in season {year} loaded successfully into table {schema_name}.{table_name}.")

                except requests.exceptions.HTTPError as e:
                    print(f"Failed to retrieve data for {gw_file_name} in season {year}: {e}")
                except Exception as e:
                    print(f"An error occurred with {gw_file_name} in season {year}: {e}")
    else:
        print(f"Failed to retrieve gameweek files for season {year}: {response.status_code}")
