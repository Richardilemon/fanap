from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Define the database connection
database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"
engine = create_engine(database_url)

# Mapping of years to schema names
schemas = {
    "2016-17": "players_2016_17",
    "2017-18": "players_2017_18",
    "2018-19": "players_2018_19",
    "2019-20": "players_2019_20",
    "2020-21": "players_2020_21",
    "2021-22": "players_2021_22",
    "2022-23": "players_2022_23",
}

# Create schemas if they do not exist
with engine.connect() as conn:
    for schema in schemas.values():
        try:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            print(f"Schema '{schema}' created or already exists.")
        except SQLAlchemyError as e:
            print(f"Failed to create schema '{schema}': {e}")

# Get the tables in the database
with engine.connect() as conn:
    try:
        # Fetch all table names in the 'players' schema
        result = conn.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'players'")
        )
        tables = [row[0] for row in result]  # Access by index instead of key
        print(f"Found {len(tables)} tables in the 'players' schema.")

        for table_name in tables:
            # Extract the season from the table name
            for season, schema in schemas.items():
                if season.replace("-", "_") in table_name:
                    print(f"Processing table '{table_name}' for season '{season}'.")

                    try:
                        # Move the table to the new schema
                        conn.execute(
                            text(f'ALTER TABLE players."{table_name}" SET SCHEMA {schema}')
                        )
                        conn.commit()
                        print(f"Table '{table_name}' moved to schema '{schema}' successfully.")

                    except SQLAlchemyError as e:
                        conn.rollback()  # Roll back the transaction for this table
                        print(f"Error processing table '{table_name}': {e}")
                    break  # Exit the inner loop once the season is matched

    except SQLAlchemyError as e:
        print(f"Error fetching tables or processing schema changes: {e}")
