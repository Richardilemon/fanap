import requests
import psycopg2
from datetime import datetime
from db_config import get_db_connection
import csv
from io import StringIO

SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24"]


def fetch_fixtures(season):
    url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/fixtures.csv"
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def parse_fixtures(csv_text, season):
    reader = csv.DictReader(StringIO(csv_text))
    fixtures = []

    for row in reader:
        code = int(row["code"])
        event = int(row["event"]) if row["event"] else None
        kickoff_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%d %H:%M:%S") if row["kickoff_time"] else None
        team_h = int(row["team_h"])
        team_a = int(row["team_a"])
        team_h_score = int(row["team_h_score"]) if row["team_h_score"] else None
        team_a_score = int(row["team_a_score"]) if row["team_a_score"] else None
        team_h_difficulty = int(row["team_h_difficulty"]) if row["team_h_difficulty"] else None
        team_a_difficulty = int(row["team_a_difficulty"]) if row["team_a_difficulty"] else None

        fixtures.append((
            code, season, event, kickoff_time,
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
                event INTEGER,
                kickoff_time TIMESTAMP,
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
    with conn.cursor() as cur:
        for fixture in fixtures:
            cur.execute("""
                INSERT INTO fixtures (
                    code, season, event, kickoff_time,
                    team_h_code, team_a_code,
                    team_h_score, team_a_score,
                    team_h_difficulty, team_a_difficulty
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fixture_id) DO UPDATE SET
                    season = EXCLUDED.season,
                    event = EXCLUDED.event,
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
