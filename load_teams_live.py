import requests
import psycopg2
from db_config import get_db_connection
from datetime import datetime

def fetch_teams_data():
    """Fetch team data from the FPL API."""
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("teams", [])

def create_teams_table(conn):
    """Create the teams table if it doesn't already exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                season TEXT,
                code INTEGER,
                id INTEGER,
                name TEXT,
                short_name TEXT,
                strength_overall_home INTEGER,
                strength_overall_away INTEGER,
                strength_attack_home INTEGER,
                strength_attack_away INTEGER,
                strength_defence_home INTEGER,
                strength_defence_away INTEGER,
                strength INTEGER,
                position INTEGER,
                PRIMARY KEY (season, code)
            );
        """)
        conn.commit()

def insert_teams(conn, teams, season):
    """Insert or update team data into the table."""
    with conn.cursor() as cur:
        for team in teams:
            cur.execute("""
                INSERT INTO teams (
                    season, code, id, name, short_name,
                    strength_overall_home, strength_overall_away,
                    strength_attack_home, strength_attack_away,
                    strength_defence_home, strength_defence_away,
                    strength, position
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (season, code) DO UPDATE SET
                    id = EXCLUDED.id,
                    name = EXCLUDED.name,
                    short_name = EXCLUDED.short_name,
                    strength_overall_home = EXCLUDED.strength_overall_home,
                    strength_overall_away = EXCLUDED.strength_overall_away,
                    strength_attack_home = EXCLUDED.strength_attack_home,
                    strength_attack_away = EXCLUDED.strength_attack_away,
                    strength_defence_home = EXCLUDED.strength_defence_home,
                    strength_defence_away = EXCLUDED.strength_defence_away,
                    strength = EXCLUDED.strength,
                    position = EXCLUDED.position;
            """, (
                season, team["code"], team["id"], team["name"], team["short_name"],
                team["strength_overall_home"], team["strength_overall_away"],
                team["strength_attack_home"], team["strength_attack_away"],
                team["strength_defence_home"], team["strength_defence_away"],
                team["strength"], team["position"]
            ))
        conn.commit()

def infer_season():
    """Infer the FPL season based on today's date."""
    today = datetime.today()
    year = today.year
    month = today.month
    if month >= 8:
        start_year = year
        end_year = year + 1
    else:
        start_year = year - 1
        end_year = year
    return f"{start_year}/{str(end_year)[-2:]}"

def main(season=None):
    if not season:
        season = infer_season()

    conn = get_db_connection()
    try:
        print("Creating teams table...")
        create_teams_table(conn)

        print("Fetching teams data...")
        teams = fetch_teams_data()

        print(f"Inserting {len(teams)} teams into database for season {season}...")
        insert_teams(conn, teams, season)

        print("\u2705 Teams data loaded successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    season_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(season=season_arg)
