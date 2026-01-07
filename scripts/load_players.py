import requests
import os
from scripts.utils.db_config import db_connection_wrapper


def fetch_players_data():
    """
    Retrieve player data from the Fantasy Premier League (FPL) API.

    Returns:
        list: A list of player dictionaries containing player attributes 
              (e.g., name,, team, status). Returns an empty list if 
              the API request fails.
    """
    try:
        url = os.getenv("FPL_TEAMS_API_URL", "https://fantasy.premierleague.com/api/bootstrap-static/")
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("elements", [])

    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch players data: {e} (URL: {url})")
        return []


def map_position(element_type):
    """
    Map numeric element_type to position string.
    
    Args:
        element_type (int): Numeric position type from FPL API
        
    Returns:
        str: Position abbreviation (GK, DEF, MID, FWD)
    """
    position_map = {
        1: "GK",
        2: "DEF",
        3: "MID",
        4: "FWD"
    }
    return position_map.get(element_type, "UNKNOWN")


@db_connection_wrapper
def load_players(connection, players):
    """
    Insert or update player data in the 'players' table.

    Args:
        connection: Active database connection provided by the decorator.
        players (list): A list of player dictionaries fetched from the FPL API.

    Behavior:
        - Inserts new player records into the database.
        - Updates existing player records if a conflict occurs on 'code'.
        - Commits all changes once completed.
    """
    _cursor = connection.cursor()
    for player in players:
        position = map_position(player["element_type"])
        _cursor.execute(
            """
            INSERT INTO players (
                code, first_name, second_name, web_name,
                team_code, position, now_cost, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                second_name = EXCLUDED.second_name,
                web_name = EXCLUDED.web_name,
                team_code = EXCLUDED.team_code,
                position = EXCLUDED.position,
                now_cost = EXCLUDED.now_cost,
                status = EXCLUDED.status;
        """,
            (
                player["code"],
                player["first_name"],
                player["second_name"],
                player["web_name"],
                player["team_code"],
                position,
                player["now_cost"],
                player["status"],
            ),
        )
    connection.commit()
    print(f"Inserted/Updated {len(players)} players into database...")