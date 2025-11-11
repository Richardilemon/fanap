import csv
from io import StringIO
from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
from scripts.utils.infer_season import SEASONS, infer_season
import logging
from datetime import datetime, timezone
import re
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_fixture_time(fixture_time_str):
    """
    Convert 'datetime.datetime@version=1(timestamp=1599910200.0,tz=UTC)'
    into a datetime object.
    """
    try:
        timestamp_part = fixture_time_str.split("timestamp=")[1].split(",")[0]
        ts = float(timestamp_part)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        print(f"⚠️ Could not parse fixture time '{fixture_time_str}': {e}")
        return None
    
# SEASONS = ["2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]
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


def parse_gw_stats_table(gw_data):
    """
    Parse player gameweek statistics.

    Args:
        gw_data (list): A list (or list of lists) of player gameweek stats dictionaries.

    Returns:
        list[tuple]: A list of tuples representing cleaned and structured player gameweek data.
    """
    # Add null check
    if gw_data is None:
        print("⚠️ Warning: gw_data is None. Returning empty records list.")
        return []
    
    if not gw_data:
        print("⚠️ Warning: gw_data is empty. Returning empty records list.")
        return []
    
    records = []

    # Flatten if gw_data contains sublists
    if gw_data and isinstance(gw_data[0], list):
        gw_data = [row for sublist in gw_data for row in sublist]

    for row in gw_data:

        try:

            gameweek = int(row.get("round", 0))
            kickoff_time = datetime.strptime(row.get("kickoff_time"), "%Y-%m-%dT%H:%M:%SZ")
            season = infer_season(kickoff_time) if kickoff_time else None

            player_name = row.get("player_name", "Unknown Player")
            
            # Extract player cost (value) - convert from FPL format (e.g., 95 = £9.5m)
            player_cost = int(row.get("value", 0)) / 10.0

            record = (
                season,
                gameweek,
                player_name,
                player_cost,  # Added player cost
                int(row.get("fixture")),
                int(row.get("opponent_team")),
                int(row.get("goals_scored", 0)),
                int(row.get("assists", 0)),
                int(row.get("clean_sheets", 0)),
                int(row.get("goals_conceded", 0)),
                int(row.get("own_goals", 0)),
                int(row.get("penalties_saved", 0)),
                int(row.get("penalties_missed", 0)),
                int(row.get("red_cards", 0)),
                int(row.get("yellow_cards", 0)),
                int(row.get("big_chances_missed", 0)),
                int(row.get("big_chances_created", 0)),
                int(row.get("clearances_blocks_interceptions", 0)),
                int(row.get("completed_passes", 0)),
                int(row.get("dribbles", 0)),
                int(row.get("errors_leading_to_goal", 0)),
                int(row.get("fouls", 0)),
                int(row.get("key_passes", 0)),
                int(row.get("open_play_crosses", 0)),
                bool(str(row.get("was_home", "False")).lower() in ["true", "1"]),
                int(row.get("winning_goals", 0)),
            )
            records.append(record)

        except Exception as e:
            print(f"❌ Error parsing record for {row.get('player_name', 'Unknown')} in GW{row.get('round', '?')}: {e}")

    print(f"✅ Parsed {len(records)} player gameweek records successfully.")
    return records


@db_connection_wrapper
def load_player_gameweek_stats(connection, records):
    _cursor = connection.cursor()
    for record in records:
        _cursor.execute(
            """
            INSERT INTO player_gameweek_stats (
                season, gameweek, player_name, player_cost, fixture_id, opponent_team, goals_scored, assists, clean_sheets,
                goals_conceded, own_goals, penalties_saved, penalties_missed, red_cards,
                yellow_cards, big_chances_missed, big_chances_created, clearance_blocks_interceptions,
                completed_passes, dribbles, errors_leading_to_goal, fouls, key_passes, open_play_crosses,
                was_home, winning_goals
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (season, gameweek, player_name) DO UPDATE SET
                player_cost = EXCLUDED.player_cost,
                fixture_id = EXCLUDED.fixture_id,
                opponent_team = EXCLUDED.opponent_team,
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
    connection.commit()