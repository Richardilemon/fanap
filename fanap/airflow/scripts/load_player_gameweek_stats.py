import csv
from io import StringIO
from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
from scripts.utils.infer_season import SEASONS, infer_season

# SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24"]
REPO_BASE = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"  # FIXTURES_BASE_URL


def parse_players_seasons(players_gw_stats):
    all_players_stats = []
    for players_stat in players_gw_stats:
        players_stats = list(csv.DictReader(StringIO(players_stat["csv"])))

        all_players_stats.append(players_stats)
    return all_players_stats[0]


def extract_player_name_and_id(folder_name):
    parts = folder_name.split("_")
    if len(parts) >= 3:
        return f"{parts[0]} {parts[1]}", parts[-1]
    return folder_name, ""


def get_players_folder(players):
    players_lists = []

    for player in players:
        player_folder = (
            player["first_name"] + "_" + player["second_name"] + "_" + player["id"]
        )
        # player_name, _ = extract_player_name_and_id(player_folder)

        players_lists.append(player_folder)

    return players_lists


# https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/2018-19/players/P/gw.csv
# f"{Variable.get('FIXTURES_BASE_URL')}/{season}/players/{player}/{endpoint}"


# def match_team_code(fixtures, opponent_id, kickoff_time_str):
#     kickoff_time = datetime.strptime(kickoff_time_str, "%Y-%m-%d %H:%M:%S")
#     for row in fixtures:
#         fixture_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%d %H:%M:%S")
#         if abs((fixture_time - kickoff_time).total_seconds()) <= 60:
#             if int(row["team_h"]) == int(opponent_id):
#                 return int(row["team_a"])
#             elif int(row["team_a"]) == int(opponent_id):
#                 return int(row["team_h"])
#     return None


def match_team_code(fixtures, opponent_id, kickoff_time_str):
    # Parse kickoff_time with the correct format
    kickoff_time = datetime.strptime(kickoff_time_str, "%Y-%m-%dT%H:%M:%SZ")

    for row in fixtures:
        fixture_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%dT%H:%M:%SZ")

        # Compare timestamps with a 60-second tolerance
        if abs((fixture_time - kickoff_time).total_seconds()) <= 60:
            if int(row["team_h"]) == int(opponent_id):
                return int(row["team_a"])
            elif int(row["team_a"]) == int(opponent_id):
                return int(row["team_h"])

    return None


def parse_gw_stats_table(gw_data, fixtures, players_folder):
    records = []

    if isinstance(gw_data[0], list):
        gw_data = [row for sublist in gw_data for row in sublist]

    for row in gw_data:
        gameweek = int(row["round"])
        team_code = match_team_code(fixtures, row["opponent_team"], row["kickoff_time"])
        season = infer_season(row["kickoff_time"])
        player_name = extract_player_name_and_id(players_folder)
        if team_code is None:
            print(f"⚠️ Could not determine team code for GW{gameweek} in {season}")
            continue
        try:
            record = (
                season,
                gameweek,
                player_name,
                team_code,
                int(row["goals_scored"]),
                int(row["assists"]),
                int(row["clean_sheets"]),
                int(row["goals_conceded"]),
                int(row["own_goals"]),
                int(row["penalties_saved"]),
                int(row["penalties_missed"]),
                int(row["red_cards"]),
                int(row["yellow_cards"]),
                int(row["big_chances_missed"]),
                int(row["big_chances_created"]),
                int(row["clearances_blocks_interceptions"]),
                int(row["completed_passes"]),
                int(row["dribbles"]),
                int(row["errors_leading_to_goal"]),
                int(row["fouls"]),
                int(row["key_passes"]),
                int(row["open_play_crosses"]),
                bool(int(row["was_home"])),
                int(row["winning_goals"]),
            )
            records.append(record)
        except Exception as e:
            print(
                f"❌ Error parsing row for {player_name} in {season}, GW{gameweek}: {e}"
            )


def insert_player_gameweek_stats(conn, records):
    with conn.cursor() as cur:
        for record in records:
            cur.execute(
                """
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
            """,
                record,
            )
        conn.commit()


# def main():
#     conn = db_connection_wrapper()
#     try:
#         # create_player_gameweek_stats_table(conn)
#         for season in SEASONS:
#             print(f"\n📦 Processing season: {season}")
#             fixtures = fetch_fixtures_for_season(season)
#             players_list = fetch_players_list(season)
#             for player in players_list:
#                 player_folder = (
#                     player["first_name"]
#                     + "_"
#                     + player["second_name"]
#                     + "_"
#                     + player["id"]
#                 )
#                 player_name, _ = extract_player_name_and_id(player_folder)
#                 gw_data = fetch_player_gameweek(season, player_folder)
#                 records = []
#                 for row in gw_data:
#                     gameweek = int(row["round"])
#                     team_code = match_team_code(
#                         fixtures, row["opponent_team"], row["kickoff_time"]
#                     )
#                     if team_code is None:
#                         print(
#                             f"⚠️ Could not determine team code for {player_name} GW{gameweek} in {season}"
#                         )
#                         continue
#                     try:
#                         record = (
#                             season,
#                             gameweek,
#                             player_name,
#                             team_code,
#                             int(row["goals_scored"]),
#                             int(row["assists"]),
#                             int(row["clean_sheets"]),
#                             int(row["goals_conceded"]),
#                             int(row["own_goals"]),
#                             int(row["penalties_saved"]),
#                             int(row["penalties_missed"]),
#                             int(row["red_cards"]),
#                             int(row["yellow_cards"]),
#                             int(row["big_chances_missed"]),
#                             int(row["big_chances_created"]),
#                             int(row["clearances_blocks_interceptions"]),
#                             int(row["completed_passes"]),
#                             int(row["dribbles"]),
#                             int(row["errors_leading_to_goal"]),
#                             int(row["fouls"]),
#                             int(row["key_passes"]),
#                             int(row["open_play_crosses"]),
#                             bool(int(row["was_home"])),
#                             int(row["winning_goals"]),
#                         )
#                         records.append(record)
#                     except Exception as e:
#                         print(
#                             f"❌ Error parsing row for {player_name} in {season}, GW{gameweek}: {e}"
#                         )
#                 if records:
#                     insert_player_gameweek_stats(conn, records)
#     finally:
#         conn.close()
#         print("\n✅ Player gameweek stats loaded successfully.")


# if __name__ == "__main__":
#     main()
