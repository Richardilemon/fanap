import sys

sys.path.append("/app")
import requests
import pandas as pd
import psycopg2
from io import StringIO
from scripts.db_config import get_db_connection

SEASONS = ["2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]


def create_teams_table(conn):
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


def fetch_teams_csv(base_url, season):
    url = f"{base_url.rstrip('/')}/{season}/teams.csv"  # Remove trailing slash
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch teams.csv for {season}: {e}")
        return None


def load_and_insert_teams_from_csv(conn, csv_text, season_label):
    if not csv_text:
        print(f"⚠️ Skipping {season_label}: No CSV data.")
        return

    df = pd.read_csv(StringIO(csv_text))

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO teams (
                    season, code, id, name, short_name,
                    strength_overall_home, strength_overall_away,
                    strength_attack_home, strength_attack_away,
                    strength_defence_home, strength_defence_away,
                    strength, position
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            """,
                (
                    season_label,
                    row["code"],
                    row["id"],
                    row["name"],
                    row["short_name"],
                    row["strength_overall_home"],
                    row["strength_overall_away"],
                    row["strength_attack_home"],
                    row["strength_attack_away"],
                    row["strength_defence_home"],
                    row["strength_defence_away"],
                    row["strength"],
                    row["position"],
                ),
            )
        conn.commit()
    print(f"✅ Loaded teams for {season_label}")


def main(base_url):
    conn = get_db_connection()
    try:
        print("Creating teams table...")
        create_teams_table(conn)
        for season in SEASONS:
            print(f"Fetching teams for {season}...")
            csv_text = fetch_teams_csv(base_url, season)
            load_and_insert_teams_from_csv(conn, csv_text, season)
        print("✅ Teams history loaded successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    base_url = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    )
    main(base_url)
