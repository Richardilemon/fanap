import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

# Database configuration
DB_CONFIG = {
    "dbname": "fpl_db",
    "user": "richardilemon",
    "password": "Temitope001",
    "host": "localhost",  # or 'postgres' if running inside Docker
    "port": 5440
}

# FPL API URLs
FPL_FIXTURES_URL = "https://fantasy.premierleague.com/api/fixtures/"
FPL_TEAMS_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

# Function to create schema and table
def setup_database():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Create schema if not exists
        cur.execute("CREATE SCHEMA IF NOT EXISTS fixtures;")
        
        # Create fixtures table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS fixtures.fixtures (
                season VARCHAR(10),
                fixture_id INTEGER PRIMARY KEY,
                team_h_id INTEGER,
                team_h_name VARCHAR(50),
                team_a_id INTEGER,
                team_a_name VARCHAR(50),
                kickoff_time TIMESTAMP,
                team_h_difficulty INTEGER,
                team_a_difficulty INTEGER
            );
        """
        cur.execute(create_table_query)
        print("Table 'fixtures' created or already exists.")
    conn.close()

# Function to fetch team names
def get_team_names():
    print("Fetching team names...")
    try:
        response = requests.get(FPL_TEAMS_URL)
        response.raise_for_status()
        teams_data = response.json()["teams"]
        return {team["id"]: team["name"] for team in teams_data}
    except Exception as e:
        print(f"Error fetching team names: {e}")
        return {}

# Function to fetch fixture data for the current season
def get_current_season_fixtures(team_map, season):
    print(f"Fetching fixtures for season {season}...")
    try:
        response = requests.get(FPL_FIXTURES_URL)
        response.raise_for_status()
        fixtures = response.json()

        # Process fixtures
        fixture_data = []
        for fixture in fixtures:
            fixture_data.append({
                "season": season,
                "fixture_id": fixture["id"],
                "team_h_id": fixture["team_h"],
                "team_h_name": team_map.get(fixture["team_h"], "Unknown"),
                "team_a_id": fixture["team_a"],
                "team_a_name": team_map.get(fixture["team_a"], "Unknown"),
                "kickoff_time": fixture.get("kickoff_time"),
                "team_h_difficulty": fixture.get("team_h_difficulty"),
                "team_a_difficulty": fixture.get("team_a_difficulty")
            })
        return pd.DataFrame(fixture_data)
    except Exception as e:
        print(f"Error fetching fixtures: {e}")
        return pd.DataFrame()

# Function to load data into PostgreSQL
def load_data_to_db(df):
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            insert_query = """
                INSERT INTO fixtures.fixtures (
                    season, fixture_id, team_h_id, team_h_name, team_a_id, team_a_name, 
                    kickoff_time, team_h_difficulty, team_a_difficulty
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fixture_id) DO NOTHING;
            """
            cur.execute(insert_query, (
                row["season"], row["fixture_id"], row["team_h_id"], row["team_h_name"],
                row["team_a_id"], row["team_a_name"], row["kickoff_time"],
                row["team_h_difficulty"], row["team_a_difficulty"]
            ))
        conn.commit()
    conn.close()
    print("Data loaded successfully.")

# Main function
def main():
    current_season = "2024-25"  # Update as necessary
    setup_database()

    # Fetch team names
    team_map = get_team_names()
    if not team_map:
        print("Exiting due to missing team data.")
        return

    # Fetch and load fixture data
    fixture_df = get_current_season_fixtures(team_map, current_season)
    if not fixture_df.empty:
        load_data_to_db(fixture_df)
    else:
        print("No data fetched for the current season.")

if __name__ == "__main__":
    main()
