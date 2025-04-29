import requests
import psycopg2
from datetime import datetime, timedelta
from db_config import get_db_connection


SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24"]


def fetch_fixtures_for_season(season):
    """Fetch fixtures from the GitHub archive for a specific season."""
    url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/fixtures.csv"
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def parse_gameweeks(csv_text, season):
    """Extract first fixture time for each gameweek in a season to set deadlines."""
    import csv
    from io import StringIO

    reader = csv.DictReader(StringIO(csv_text))
    gameweek_map = {}

    for row in reader:
        gw = int(row["event"])
        kickoff_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%d %H:%M:%S")
        if gw not in gameweek_map or kickoff_time < gameweek_map[gw]:
            gameweek_map[gw] = kickoff_time

    gameweeks = []
    for gw, kickoff in gameweek_map.items():
        deadline = kickoff - timedelta(hours=1)
        gameweeks.append((season, gw, deadline))

    return gameweeks


def create_gameweeks_table(conn):
    """Create the game_weeks table."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS game_weeks (
                season TEXT,
                gameweek INTEGER,
                deadline TIMESTAMP,
                PRIMARY KEY (season, gameweek)
            );
        """)
        conn.commit()


def insert_gameweeks(conn, gameweeks):
    """Insert gameweek data into the table."""
    with conn.cursor() as cur:
        for season, gw, deadline in gameweeks:
            cur.execute("""
                INSERT INTO game_weeks (season, gameweek, deadline)
                VALUES (%s, %s, %s)
                ON CONFLICT (season, gameweek) DO UPDATE SET
                    deadline = EXCLUDED.deadline;
            """, (season, gw, deadline))
        conn.commit()


def main():
    conn = get_db_connection()
    try:
        print("Creating game_weeks table...")
        create_gameweeks_table(conn)

        for season in SEASONS:
            print(f"Processing season {season}...")
            csv_text = fetch_fixtures_for_season(season)
            gameweeks = parse_gameweeks(csv_text, season)
            insert_gameweeks(conn, gameweeks)

        print("✅ Gameweek deadlines loaded successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
