import sys
sys.path.append('/app')
import requests
import psycopg2
from datetime import datetime
from db_config import get_db_connection
import csv
from io import StringIO

SEASONS = ["2018-19", "2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]

def fetch_fixtures(season):
    url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/fixtures.csv"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch fixtures.csv for {season}: {e} (URL: {url})")
        return None

def parse_fixtures(csv_text, season):
    if not csv_text:
        print(f"⚠️ Skipping {season}: No CSV data.")
        return []

    reader = csv.DictReader(StringIO(csv_text))
    fixtures = []

    for row in reader:
        code = int(row["code"])
        gameweek = int(row["event"]) if row["event"] and row["event"] != '' else None
        kickoff_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%dT%H:%M:%SZ") if row["kickoff_time"] else None
        team_h = int(row["team_h"])
        team_a = int(row["team_a"])
        # Handle float values in scores
        try:
            team_h_score = int(float(row["team_h_score"])) if row["team_h_score"] and row["team_h_score"] != '' else None
        except (ValueError, TypeError):
            team_h_score = None
        try:
            team_a_score = int(float(row["team_a_score"])) if row["team_a_score"] and row["team_a_score"] != '' else None
        except (ValueError, TypeError):
            team_a_score = None
        team_h_difficulty = int(row["team_h_difficulty"]) if row["team_h_difficulty"] else None
        team_a_difficulty = int(row["team_a_difficulty"]) if row["team_a_difficulty"] else None

        fixtures.append((
            code, season, gameweek, kickoff_time,
            team_h, team_a,
            team_h_score, team_a_score,
            team_h_difficulty, team_a_difficulty
        ))

    return fixtures

def create_fixtures_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fixtures (
                code INTEGER PRIMARY KEY,
                season TEXT,
                gameweek INTEGER,
                kickoff_time TIMESTAMP WITH TIME ZONE,
                team_h_code INTEGER,
                team_a_code INTEGER,
                team_h_score INTEGER,
                team_a_score INTEGER,
                team_h_difficulty INTEGER,
                team_a_difficulty INTEGER
            );
        """)
        conn.commit()

def insert_fixtures(conn, fixtures):
    if not fixtures:
        return

    with conn.cursor() as cur:
        for fixture in fixtures:
            cur.execute("""
                INSERT INTO fixtures (
                    code, season, gameweek, kickoff_time,
                    team_h_code, team_a_code,
                    team_h_score, team_a_score,
                    team_h_difficulty, team_a_difficulty
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET
                    season = EXCLUDED.season,
                    gameweek = EXCLUDED.gameweek,
                    kickoff_time = EXCLUDED.kickoff_time,
                    team_h_code = EXCLUDED.team_h_code,
                    team_a_code = EXCLUDED.team_a_code,
                    team_h_score = EXCLUDED.team_h_score,
                    team_a_score = EXCLUDED.team_a_score,
                    team_h_difficulty = EXCLUDED.team_h_difficulty,
                    team_a_difficulty = EXCLUDED.team_a_difficulty;
            """, fixture)
        conn.commit()

def main():
    conn = get_db_connection()
    try:
        print("Creating fixtures table...")
        create_fixtures_table(conn)

        for season in SEASONS:
            print(f"Fetching fixtures for {season}...")
            csv_text = fetch_fixtures(season)
            fixtures = parse_fixtures(csv_text, season)
            print(f"Inserting {len(fixtures)} fixtures...")
            insert_fixtures(conn, fixtures)

        print("✅ Fixtures loaded successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()