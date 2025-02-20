import pandas as pd
import requests
from sqlalchemy import create_engine

fpl_api_url = "https://fantasy.premierleague.com/api/fixtures/"
database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"
engine = create_engine(database_url)
schema_name = "fixtures"

def fetch_fixture_data():
    try:
        response = requests.get(fpl_api_url)
        response.raise_for_status()
        fixtures = response.json()
        if not fixtures:
            print("⚠️ No fixtures found. Check if the season has started.")
            return pd.DataFrame()
        
        # Process fixtures into a DataFrame
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
        
        # Write the DataFrame to the database
        table_name = "fixture_data"
        fixture_df.to_sql(table_name, engine, schema=schema_name, if_exists="replace", index=False)
        print(f"Fixture data written to {schema_name}.{table_name} in the database.")
        
        return fixture_df

    except Exception as e:
        print(f"🚨 Failed to fetch fixtures: {e}")
        return pd.DataFrame()

# Example usage
fixture_df = fetch_fixture_data()