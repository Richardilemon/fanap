import os
import pandas as pd
import psycopg2
from db_config import get_db_connection


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


def load_and_insert_teams_from_csv(conn, season_folder_path, season_label):
    teams_csv = os.path.join(season_folder_path, "teams.csv")
    if not os.path.isfile(teams_csv):
        print(f"⚠️ Skipping {season_label}: teams.csv not found.")
        return

    df = pd.read_csv(teams_csv)

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
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
            """, (
                season_label,
                row["code"], row["id"], row["name"], row["short_name"],
                row["strength_overall_home"], row["strength_overall_away"],
                row["strength_attack_home"], row["strength_attack_away"],
                row["strength_defence_home"], row["strength_defence_away"],
                row["strength"], row["position"]
            ))
        conn.commit()
    print(f"✅ Loaded teams for {season_label}")


def normalize_season_folder(folder_name):
    return folder_name.replace('-', '/')


def main(base_data_path):
    conn = get_db_connection()
    try:
        create_teams_table(conn)

        for folder in sorted(os.listdir(base_data_path)):
            season_path = os.path.join(base_data_path, folder)
            if not os.path.isdir(season_path):
                continue
            season = normalize_season_folder(folder)
            load_and_insert_teams_from_csv(conn, season_path, season)

    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    base_path = sys.argv[1] if len(sys.argv) > 1 else "./data"
    main(base_path)
    print("✅ Teams data loaded successfully.")
