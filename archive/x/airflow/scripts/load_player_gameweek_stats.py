import csv
from io import StringIO
from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
from scripts.utils.infer_season import SEASONS, infer_season
import logging
from datetime import datetime, timezone
import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def validate_player_gameweek_record(season, gameweek, player_name, player_cost):
    """
    Validate a player gameweek record before insertion.
    
    Parameters
    ----------
    season : str
        Season string (e.g., "2024-25")
    gameweek : int
        Gameweek number
    player_name : str
        Player name
    player_cost : float
        Player cost in millions
        
    Returns
    -------
    tuple
        (is_valid: bool, error_message: str or None)
    """
    # Check season format
    if not season or not isinstance(season, str):
        return False, f"Invalid season format: {season}"
    
    # Check gameweek range (Premier League has 38 gameweeks)
    if not isinstance(gameweek, int) or gameweek < 1 or gameweek > 38:
        return False, f"Gameweek must be between 1 and 38, got: {gameweek}"
    
    # Check player name is non-empty
    if not player_name or not isinstance(player_name, str) or len(player_name.strip()) == 0:
        return False, f"Invalid player name: {player_name}"
    
    # Check player cost is reasonable (FPL players typically between £3.5m - £15m)
    if not isinstance(player_cost, (int, float)) or player_cost < 3.0 or player_cost > 20.0:
        return False, f"Player cost out of range: {player_cost}"
    
    return True, None


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
        logger.warning(f"Could not parse fixture time '{fixture_time_str}': {e}")
        return None


REPO_BASE = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"


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
        logger.warning("gw_data is None. Returning empty records list.")
        print("⚠️ Warning: gw_data is None. Returning empty records list.")
        return []
    
    if not gw_data:
        logger.warning("gw_data is empty. Returning empty records list.")
        print("⚠️ Warning: gw_data is empty. Returning empty records list.")
        return []
    
    records = []
    skipped = 0

    # Flatten if gw_data contains sublists
    if gw_data and isinstance(gw_data[0], list):
        gw_data = [row for sublist in gw_data for row in sublist]

    for row_num, row in enumerate(gw_data, start=1):
        try:
            # Validate required fields
            required_fields = ["round", "kickoff_time", "player_name", "value", "fixture", "opponent_team"]
            missing_fields = [f for f in required_fields if f not in row]
            if missing_fields:
                logger.warning(f"Row {row_num}: Missing required fields: {missing_fields}")
                skipped += 1
                continue

            gameweek = int(row.get("round", 0))
            kickoff_time = datetime.strptime(row.get("kickoff_time"), "%Y-%m-%dT%H:%M:%SZ")
            season = infer_season(kickoff_time) if kickoff_time else None

            player_name = row.get("player_name", "Unknown Player")
            
            # Extract player cost (value) - convert from FPL format (e.g., 95 = £9.5m)
            player_cost = int(row.get("value", 0)) / 10.0

            # Validate before creating record
            is_valid, error_msg = validate_player_gameweek_record(
                season, gameweek, player_name, player_cost
            )
            if not is_valid:
                logger.warning(f"Row {row_num}: Validation failed - {error_msg}")
                skipped += 1
                continue

            record = (
                season,
                gameweek,
                player_name,
                player_cost,
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

        except ValueError as e:
            logger.warning(f"Row {row_num}: Invalid data format for {row.get('player_name', 'Unknown')} - {e}")
            skipped += 1
            continue
        except Exception as e:
            logger.error(f"Row {row_num}: Unexpected error for {row.get('player_name', 'Unknown')} - {e}")
            skipped += 1
            continue

    logger.info(f"Parsed {len(records)} player gameweek records ({skipped} skipped)")
    print(f"✅ Parsed {len(records)} player gameweek records successfully ({skipped} skipped).")
    return records


@db_connection_wrapper
def load_player_gameweek_stats(connection, records):
    """
    Load player gameweek statistics into the database.
    
    Parameters
    ----------
    connection : PostgresHook
        Active database connection provided by the decorator.
    records : list of tuple
        List of player gameweek stat tuples to insert.
    
    Returns
    -------
    None
        Commits changes and prints success message.
    """
    if not records:
        logger.warning("No player gameweek stats to load")
        print("⚠️ No player gameweek stats to load.")
        return
    
    # Validate data structure
    if not isinstance(records, list):
        logger.error(f"Expected list of records, got {type(records)}")
        return
    
    _cursor = connection.cursor()
    total_inserted = 0
    total_updated = 0
    total_failed = 0
    
    try:
        for record in records:
            try:
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
                        winning_goals = EXCLUDED.winning_goals
                    RETURNING (xmax = 0) AS inserted;
                """,
                    record,
                )
                
                # Check if it was an insert or update
                result = _cursor.fetchone()
                if result and result[0]:
                    total_inserted += 1
                else:
                    total_updated += 1
                    
            except psycopg2.IntegrityError as e:
                logger.error(f"Integrity error for {record[2]} GW{record[1]}: {e}")
                total_failed += 1
                connection.rollback()
                continue
            except psycopg2.DatabaseError as e:
                logger.error(f"Database error for {record[2]} GW{record[1]}: {e}")
                total_failed += 1
                connection.rollback()
                continue
        
        connection.commit()
        logger.info(f"Player stats loaded: {total_inserted} inserted, {total_updated} updated, {total_failed} failed")
        print(f"✅ Player Stats: {total_inserted} inserted, {total_updated} updated, {total_failed} failed")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Fatal error loading player gameweek stats: {e}")
        print(f"❌ Failed to load player gameweek stats: {e}")
        raise