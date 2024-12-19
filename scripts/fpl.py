import pandas as pd
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, Float, Boolean, MetaData, text
import requests
from io import StringIO

print("Script started", flush=True)

# API URLs
github_api_url = "https://api.github.com/repos/vaastav/Fantasy-Premier-League/contents/data/2024-25/players"
github_raw_base_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/2024-25/players"
fpl_api_url = "https://fantasy.premierleague.com/api/fixtures/"

# PostgreSQL database connection
database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"
engine = create_engine(database_url)
schema_name = "players"

# Ensure schema exists
with engine.connect() as connection:
    try:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
        connection.commit()
        print(f"Schema '{schema_name}' created or already exists.")
    except Exception as e:
        print(f"Failed to create schema '{schema_name}': {e}")

# Function to fetch fixture data from FPL API
def fetch_fixture_data():
    print("Fetching fixture data from FPL API...")
    try:
        response = requests.get(fpl_api_url)
        response.raise_for_status()
        fixtures = response.json()

        # Process fixture data into a DataFrame
        fixture_df = pd.DataFrame([
            {
                "fixture_id": f["id"],
                "team_a": f["team_a"],
                "team_h": f["team_h"],
                "team_a_difficulty": f.get("team_a_difficulty"),
                "team_h_difficulty": f.get("team_h_difficulty"),
                "kickoff_time": f["kickoff_time"]
            }
            for f in fixtures
        ])
        print("Fixture data fetched successfully.")
        return fixture_df

    except Exception as e:
        print(f"Failed to fetch fixture data: {e}")
        return pd.DataFrame()

# Fetch fixture data once before processing player data
fixtures_df = fetch_fixture_data()

# Map team IDs to team names (static example; replace with FPL API team data if needed)
team_names = {
    1: "Arsenal", 2: "Aston Villa", 3: "Bournemouth", 4: "Brentford",
    5: "Brighton", 6: "Burnley", 7: "Chelsea", 8: "Crystal Palace",
    9: "Everton", 10: "Fulham", 11: "Liverpool", 12: "Luton",
    13: "Man City", 14: "Man Utd", 15: "Newcastle", 16: "Nottingham Forest",
    17: "Sheffield Utd", 18: "Spurs", 19: "West Ham", 20: "Wolves"
}

# Function to fetch player data from GitHub and merge with fixture data
def process_player_data():
    response = requests.get(github_api_url)
    if response.status_code == 200:
        players = response.json()
        print(f"Found {len(players)} player directories.")

        for player in players:
            if player["type"] == "dir":
                player_name = player["name"]
                player_gw_url = f"{github_raw_base_url}/{player_name}/gw.csv"
                print(f"Processing {player_name}")

                try:
                    # Fetch player gameweek data
                    player_response = requests.get(player_gw_url)
                    player_response.raise_for_status()
                    csv_data = StringIO(player_response.text)
                    player_df = pd.read_csv(csv_data)

                    if player_df.empty:
                        print(f"No data for {player_name}, skipping.")
                        continue

                    # Add fixture details for the next match (first matching fixture)
                    player_team_id = player_df['team'].iloc[0] if 'team' in player_df.columns else None
                    fixture = fixtures_df[fixtures_df["team_h"] == player_team_id].head(1)
                    
                    if not fixture.empty:
                        player_df["fixture_id"] = fixture["fixture_id"].values[0]
                        player_df["team_name"] = team_names.get(player_team_id, "Unknown")
                        player_df["fixture_difficulty"] = fixture["team_h_difficulty"].values[0]
                    else:
                        print(f"No fixtures found for team {player_team_id}, skipping fixture merge.")

                    # Ensure the 'modified' column matches the database schema
                    if 'modified' in player_df.columns:
                        player_df['modified'] = player_df['modified'].astype(float)  # Convert boolean to float


                    # Table name formatting
                    table_name = f"{player_name}_gw".replace(":", "_").replace(",", "_").replace("-", "_").replace(" ", "_")

                    # Create table dynamically if it doesn't exist
                    inspector = inspect(engine)
                    if not inspector.has_table(table_name, schema=schema_name):
                        print(f"Creating table {schema_name}.{table_name}...")
                        metadata = MetaData()
                        columns = [Column(col, Float if pd.api.types.is_numeric_dtype(player_df[col]) else String)
                                   for col in player_df.columns]
                        table = Table(table_name, metadata, *columns, schema=schema_name)
                        table.create(bind=engine)

                    # Insert data into PostgreSQL
                    player_df.to_sql(table_name, engine, schema=schema_name, if_exists="append", index=False)
                    print(f"Data for {player_name} inserted into {table_name}.")

                except Exception as e:
                    print(f"Error processing {player_name}: {e}")
    else:
        print(f"Failed to fetch player data: {response.status_code}")

# Run the player data processing
process_player_data()
print("Script finished.")
