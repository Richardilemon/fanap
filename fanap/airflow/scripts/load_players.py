import requests
import psycopg2
from scripts.db_config import get_db_connection
from airflow.models import Variable


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


def create_players_table(conn):
    """Create the players table if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                code INTEGER PRIMARY KEY,
                first_name TEXT,
                second_name TEXT,
                web_name TEXT,
                team_code INTEGER,
                type_name TEXT,
                now_cost INTEGER,
                status TEXT
            );
        """)
        conn.commit()


def insert_players(conn, players):
    """Insert or update player data into the table."""
    with conn.cursor() as cur:
        for player in players:
            cur.execute(
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
        conn.commit()


def main():
    conn = get_db_connection()
    try:
        print("Creating players table...")
        create_players_table(conn)

        print("Fetching players data...")
        players = fetch_players_data()

        print(f"Inserting {len(players)} players into database...")
        insert_players(conn, players)

        print("✅ Players data loaded successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
