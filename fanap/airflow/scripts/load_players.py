import requests
from airflow.models import Variable
from scripts.utils.db_config import db_connection_wrapper


def fetch_players_data():
    """Fetch player data from the FPL API."""
    try:
        url = Variable.get("FPL_TEAMS_API_URL")
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("elements", [])

    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch players data: {e} (URL: {url})")
        return []


@db_connection_wrapper
def load_players(connection, players):
    """Insert or update player data into the table."""
    _cursor = connection.cursor()
    for player in players:
        _cursor.execute(
            """
            INSERT INTO players (
                code, first_name, second_name, web_name,
                team_code, type_name, now_cost, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                second_name = EXCLUDED.second_name,
                web_name = EXCLUDED.web_name,
                team_code = EXCLUDED.team_code,
                type_name = EXCLUDED.type_name,
                now_cost = EXCLUDED.now_cost,
                status = EXCLUDED.status;
        """,
            (
                player["code"],
                player["first_name"],
                player["second_name"],
                player["web_name"],
                player["team_code"],
                player["element_type"],
                player["now_cost"],
                player["status"],
            ),
        )
    connection.commit()
    print(f"Inserted/Updated {len(players)} players into database...")
