import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, Float, MetaData, text
import requests
from io import StringIO

print("Script started", flush=True)

# Define the API URL and raw base URL for understat data
api_url = "https://api.github.com/repos/vaastav/Fantasy-Premier-League/contents/data/2023-24/understat"
raw_base_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/2023-24/understat"
database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"

# Set up the SQLAlchemy engine for database connection
engine = create_engine(database_url)
print("Database URL:", database_url)

# Create or ensure the "understat" schema exists
schema_name = "understat"
with engine.connect() as connection:
    try:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        connection.commit()
        print(f"Schema '{schema_name}' created or already exists.")
    except Exception as e:
        print(f"Failed to create schema '{schema_name}': {e}")

# Fetch the understat directories using the GitHub API
response = requests.get(api_url)
if response.status_code == 200:
    files = response.json()
    print(f"Found {len(files)} files in the 'understat' folder.")

    for file in files:
        if file["type"] == "file":  # Filter for files only
            file_name = file["name"]
            file_url = f"{raw_base_url}/{file_name}"
            table_name = file_name.replace(".csv", "").replace(":", "_").replace(",", "_").replace("-", "_").replace(" ", "_")
            print(f"Attempting to load data from {file_name}")

            try:
                # Request the CSV file
                file_response = requests.get(file_url)
                file_response.raise_for_status()

                # Load the CSV data into a DataFrame
                csv_data = StringIO(file_response.text)
                df = pd.read_csv(csv_data)

                # Ensure that the DataFrame is not empty and has the expected columns
                if df.empty:
                    print(f"No data available in {file_name}. Skipping...")
                    continue

                print("DataFrame structure:")
                print(df.head())
                print(f"Columns: {df.columns.tolist()}")

                # Fully qualified table name with schema
                qualified_table_name = f"{schema_name}.{table_name}"
                print(f"Using table name: {qualified_table_name}")

                # Check if the table exists, create it if it doesn't
                inspector = inspect(engine)
                if not inspector.has_table(table_name, schema=schema_name):
                    print(f"Table {qualified_table_name} does not exist, creating it...")

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

                    # Create the table in the specified schema
                    table = Table(table_name, metadata, *columns, schema=schema_name)
                    table.create(bind=engine)
                else:
                    print(f"Table {qualified_table_name} already exists.")

                # Insert the data into the database
                df.to_sql(name=table_name, con=engine, schema=schema_name, if_exists="append", index=False)
                print(f"Data from {file_name} loaded successfully into table {qualified_table_name}.")

            except requests.exceptions.HTTPError as e:
                print(f"Failed to retrieve data from {file_name}: {e}")
            except Exception as e:
                print(f"An error occurred with {file_name}: {e}")
else:
    print(f"Failed to retrieve files from understat folder: {response.status_code}")
