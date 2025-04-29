import os
import requests
import csv
import psycopg2
from io import StringIO
from datetime import datetime, timedelta
from db_config import get_db_connection

SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24"]
REPO_BASE = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"


def fetch_csv(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def fetch_fixtures_for_season(season):
    url = f"{REPO_BASE}/{season}/fixtures.csv"
    return list(csv.DictReader(StringIO(fetch_csv(url))))


def fetch_players_list(season):
    url = f"{REPO_BASE}/{season}/players_raw.csv"
    return list(csv.DictReader(StringIO(fetch_csv(url))))


def fetch_player_gameweek(season, player_folder):
    url = f"{REPO_BASE}/{season}/players/{player_folder}/gw.csv"
    try:
        return list(csv.DictReader(StringIO(fetch_csv(url))))
    except Exception as e:
        print(f"❌ Failed to fetch gw.csv for {player_folder} in {season}: {e}")
        return []


def extract_player_name_and_id(folder_name):
    parts = folder_name.split("_")
    if len(parts) >= 3:
        return f"{parts[0]} {parts[1]}", parts[-1]
    return folder_name, ""


def match_team_code(fixtures, opponent_id, kickoff_time_str):
    kickoff_time = datetime.strptime(kickoff_time_str, "%Y-%m-%d %H:%M:%S")
    for row in fixtures:
        fixture_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%d %H:%M:%S")
        if abs((fixture_time - kickoff_time).total_seconds()) <= 60:
            if int(row["team_h"]) == int(opponent_id):
                return int(row["team_a"])
            elif int(row["team_a"]) == int(opponent_id):
                return int(row["team_h"])
    return None


def create_player_gameweek_stats_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_gameweek_stats (
                season TEXT,
                gameweek INTEGER,
                player_name TEXT,
                team_code INTEGER,
                goals_scored INTEGER,
                assists INTEGER,
                clean_sheets INTEGER,
                goals_conceded INTEGER,
                own_goals INTEGER,
                penalties_saved INTEGER,
                penalties_missed INTEGER,
                red_cards INTEGER,
                yellow_cards INTEGER,
                big_chances_missed INTEGER,
                big_chances_created INTEGER,
                clearance_blocks_interceptions INTEGER,
                completed_passes INTEGER,
                dribbles INTEGER,
                errors_leading_to_goal INTEGER,
                fouls INTEGER,
                key_passes INTEGER,
                open_play_crosses INTEGER,
                was_home BOOLEAN,
                winning_goals INTEGER,
                PRIMARY KEY (season, gameweek, player_name)
            );
        """)
        conn.commit()


def insert_player_gameweek_stats(conn, records):
    with conn.cursor() as cur:
        for record in records:
            cur.execute("""
                INSERT INTO player_gameweek_stats (
                    season, gameweek, player_name, team_code, goals_scored, assists, clean_sheets,
                    goals_conceded, own_goals, penalties_saved, penalties_missed, red_cards,
                    yellow_cards, big_chances_missed, big_chances_created, clearance_blocks_interceptions,
                    completed_passes, dribbles, errors_leading_to_goal, fouls, key_passes, open_play_crosses,
                    was_home, winning_goals
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (season, gameweek, player_name) DO UPDATE SET
                    team_code = EXCLUDED.team_code,
                    goals_scored = EXCLUDED.goals_scored,
                    assists = EXCLUDED.assists,
                    clean_sheets = EXCLUDED.clean_sheets,
                    goals_conceded = EXCLUDED.goals_conceded,
                    own_goals = EXCLUDED.own_goals,
                    penalties_saved = EXCLUDED.penalties_saved,
                    penalties_missed = EXCLUDED.penalties_missed,
                    red_cards = EXCLUDED.red_cards,
                    yellow_cards = EXCLUDED.yellow_cards,
                    big_chances_missed = EXCLUDED.big_chances_missed,
                    big_chances_created = EXCLUDED.big_chances_created,
                    clearance_blocks_interceptions = EXCLUDED.clearance_blocks_interceptions,
                    completed_passes = EXCLUDED.completed_passes,
                    dribbles = EXCLUDED.dribbles,
                    errors_leading_to_goal = EXCLUDED.errors_leading_to_goal,
                    fouls = EXCLUDED.fouls,
                    key_passes = EXCLUDED.key_passes,
                    open_play_crosses = EXCLUDED.open_play_crosses,
                    was_home = EXCLUDED.was_home,
                    winning_goals = EXCLUDED.winning_goals;
            """, record)
        conn.commit()


def main():
    conn = get_db_connection()
    try:
        create_player_gameweek_stats_table(conn)
        for season in SEASONS:
            print(f"\n📦 Processing season: {season}")
            fixtures = fetch_fixtures_for_season(season)
            players_list = fetch_players_list(season)
            for player in players_list:
                player_folder = player['first_name'] + "_" + player['second_name'] + "_" + player['id']
                player_name, _ = extract_player_name_and_id(player_folder)
                gw_data = fetch_player_gameweek(season, player_folder)
                records = []
                for row in gw_data:
                    gameweek = int(row["round"])
                    team_code = match_team_code(fixtures, row["opponent_team"], row["kickoff_time"])
                    if team_code is None:
                        print(f"⚠️ Could not determine team code for {player_name} GW{gameweek} in {season}")
                        continue
                    try:
                        record = (
                            season, gameweek, player_name, team_code,
                            int(row["goals_scored"]), int(row["assists"]), int(row["clean_sheets"]),
                            int(row["goals_conceded"]), int(row["own_goals"]), int(row["penalties_saved"]),
                            int(row["penalties_missed"]), int(row["red_cards"]), int(row["yellow_cards"]),
                            int(row["big_chances_missed"]), int(row["big_chances_created"]),
                            int(row["clearances_blocks_interceptions"]), int(row["completed_passes"]),
                            int(row["dribbles"]), int(row["errors_leading_to_goal"]), int(row["fouls"]),
                            int(row["key_passes"]), int(row["open_play_crosses"]),
                            bool(int(row["was_home"])), int(row["winning_goals"])
                        )
                        records.append(record)
                    except Exception as e:
                        print(f"❌ Error parsing row for {player_name} in {season}, GW{gameweek}: {e}")
                if records:
                    insert_player_gameweek_stats(conn, records)
    finally:
        conn.close()
        print("\n✅ Player gameweek stats loaded successfully.")


if __name__ == "__main__":
    main()
