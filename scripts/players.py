import requests
import pandas as pd
from sqlalchemy import create_engine

def fetch_player_team_mapping(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        players = data["elements"]  # List of all players

        # Map team IDs to team names
        team_names = {
            1: "Arsenal", 2: "Aston Villa", 3: "Bournemouth", 4: "Brentford",
            5: "Brighton", 6: "Chelsea", 7: "Crystal Palace", 8: "Everton",
            9: "Fulham", 10: "Ipswich Town", 11: "Leicester", 12: "Liverpool",
            13: "Man City", 14: "Man Utd", 15: "Newcastle", 16: "Nottingham Forest",
            17: "Southampton", 18: "Spurs", 19: "West Ham", 20: "Wolves"
        }

        player_team_map = [
            {
                "player_name": player["web_name"].lower(),
                "team_id": player["team"],
                "team_name": team_names.get(player["team"], "Unknown"),
                "full_name": player["web_name"]
            }
            for player in players
        ]
        
        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(player_team_map)
        
        # Write the DataFrame to a CSV file
        csv_file_path = "player_team_mapping.csv"
        df.to_csv(csv_file_path, index=False)
        print(f"Player-Team mapping written to {csv_file_path}")
        
        # Write the DataFrame to the database
        database_url = "postgresql://richardilemon:Temitope001@localhost:5440/fpl_db"
        engine = create_engine(database_url)
        schema_name = "players"
        table_name = "player_team_mapped"
        df.to_sql(table_name, engine, schema=schema_name, if_exists="replace", index=False)
        print(f"Player-Team mapping written to {schema_name}.{table_name} in the database.")
        
        return player_team_map
    except Exception as e:
        print(f"Failed to fetch player-team mappings: {e}")
        return []

# Example usage
url = "https://fantasy.premierleague.com/api/bootstrap-static/"
player_team_map = fetch_player_team_mapping(url)