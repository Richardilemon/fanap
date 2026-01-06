import csv
from io import StringIO
from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
from scripts.utils.infer_season import SEASONS, infer_season
import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2 import extras

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def validate_player_gameweek_record(season, gameweek, player_name, player_cost):
    """Validate a player gameweek record before insertion."""
    if not season or not isinstance(season, str):
        return False, f"Invalid season format: {season}"
    
    max_gw = 47 if season == "2019-20" else 38
    if not isinstance(gameweek, int) or gameweek < 1 or gameweek > max_gw:
        return False, f"Gameweek must be between 1 and {max_gw}, got: {gameweek}"
    
    if not player_name or not isinstance(player_name, str) or len(player_name.strip()) == 0:
        return False, f"Invalid player name: {player_name}"
    
    if not isinstance(player_cost, (int, float)) or player_cost < 3.0 or player_cost > 20.0:
        return False, f"Player cost out of range: {player_cost}"
    
    return True, None


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
            player["first_name"] + "_" + player["second_name"] + "_" + str(player["id"])
        )
        players_lists.append(player_folder)
    return players_lists


def safe_int(value, default=0):
    """Safely convert value to int"""
    try:
        if value is None or value == '':
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        if value is None or value == '':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_bool(value, default=False):
    """Safely convert value to bool"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes']
    return default


def parse_gw_stats_table(gw_data):
    """
    Parse player gameweek statistics - COMPREHENSIVE VERSION
    Collects ALL available fields from Vaastav data
    """
    if gw_data is None or not gw_data:
        logger.warning("gw_data is None or empty. Returning empty records list.")
        print("⚠️ Warning: gw_data is None or empty.")
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

            gameweek = safe_int(row.get("round"))
            kickoff_time = row.get("kickoff_time", "")
            
            # Parse kickoff time for season inference
            try:
                kickoff_dt = datetime.strptime(kickoff_time, "%Y-%m-%dT%H:%M:%SZ")
                season = infer_season(kickoff_dt)
            except:
                season = None

            player_name = row.get("player_name", "Unknown Player")
            player_cost = safe_int(row.get("value")) / 10.0

            # Validate before creating record
            is_valid, error_msg = validate_player_gameweek_record(
                season, gameweek, player_name, player_cost
            )
            if not is_valid:
                logger.warning(f"Row {row_num}: Validation failed - {error_msg}")
                skipped += 1
                continue

            # Build comprehensive record tuple
            record = (
                # Core identification
                season,                                          # season
                gameweek,                                        # gameweek
                player_name,                                     # player_name
                safe_int(row.get("fixture")),                    # fixture_id
                safe_int(row.get("opponent_team")),              # opponent_team
                safe_bool(row.get("was_home")),                  # was_home
                
                # Match context
                safe_int(row.get("team_h_score")),               # team_h_score
                safe_int(row.get("team_a_score")),               # team_a_score
                safe_int(row.get("minutes")),                    # minutes
                safe_bool(row.get("starts")),                    # starts
                kickoff_time,                                    # kickoff_time
                
                # FPL scoring
                safe_int(row.get("total_points")),               # total_points
                safe_int(row.get("bonus")),                      # bonus
                safe_int(row.get("bps")),                        # bps
                
                # Performance stats
                safe_int(row.get("goals_scored")),               # goals_scored
                safe_int(row.get("assists")),                    # assists
                safe_int(row.get("clean_sheets")),               # clean_sheets
                safe_int(row.get("goals_conceded")),             # goals_conceded
                safe_int(row.get("own_goals")),                  # own_goals
                safe_int(row.get("penalties_saved")),            # penalties_saved
                safe_int(row.get("penalties_missed")),           # penalties_missed
                0,  # penalties_scored (not in Vaastav, calculate from goals?)
                safe_int(row.get("red_cards")),                  # red_cards
                safe_int(row.get("yellow_cards")),               # yellow_cards
                safe_int(row.get("saves")),                      # saves
                
                # Expected stats
                safe_float(row.get("expected_goals")),           # expected_goals
                safe_float(row.get("expected_assists")),         # expected_assists
                safe_float(row.get("expected_goal_involvements")), # expected_goal_involvements
                safe_float(row.get("expected_goals_conceded")),  # expected_goals_conceded
                
                # ICT metrics
                safe_float(row.get("influence")),                # influence
                safe_float(row.get("creativity")),               # creativity
                safe_float(row.get("threat")),                   # threat
                safe_float(row.get("ict_index")),                # ict_index
                
                # Detailed performance
                0,  # big_chances_missed (not in Vaastav CSV)
                0,  # big_chances_created (not in Vaastav CSV)
                0,  # clearances_blocks_interceptions (not in Vaastav CSV)
                0,  # completed_passes (not in Vaastav CSV)
                0,  # dribbles (not in Vaastav CSV)
                0,  # errors_leading_to_goal (not in Vaastav CSV)
                0,  # fouls (not in Vaastav CSV)
                0,  # key_passes (not in Vaastav CSV)
                0,  # open_play_crosses (not in Vaastav CSV)
                safe_int(row.get("tackles")),                    # tackles
                safe_int(row.get("recoveries")),                 # recoveries
                0,  # winning_goals (not in Vaastav CSV)
                
                # Ownership/transfers
                safe_int(row.get("selected")),                   # selected
                safe_int(row.get("transfers_in")),               # transfers_in
                safe_int(row.get("transfers_out")),              # transfers_out
                safe_int(row.get("transfers_balance")),          # transfers_balance
                safe_int(row.get("value")),                      # value
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


def load_player_gameweek_stats(records):
    """Load comprehensive player gameweek statistics into the database - OPTIMIZED with execute_batch"""
    from scripts.utils.db_config import get_db_connection
    import time

    if not records:
        logger.warning("No player gameweek stats to load")
        print("⚠️ No player gameweek stats to load.")
        return

    if not isinstance(records, list):
        logger.error(f"Expected list of records, got {type(records)}")
        return

    # Batch size for execute_batch
    BATCH_SIZE = 5000
    MAX_RETRIES = 3

    total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"  💾 Loading {len(records)} records in {total_batches} batches...")

    # SQL for upsert
    upsert_sql = """
        INSERT INTO player_gameweek_stats (
            season, gameweek, player_name, fixture_id, opponent_team, was_home,
            team_h_score, team_a_score, minutes, starts, kickoff_time,
            total_points, bonus, bps,
            goals_scored, assists, clean_sheets, goals_conceded, own_goals,
            penalties_saved, penalties_missed, penalties_scored, red_cards, yellow_cards, saves,
            expected_goals, expected_assists, expected_goal_involvements, expected_goals_conceded,
            influence, creativity, threat, ict_index,
            big_chances_missed, big_chances_created, clearances_blocks_interceptions,
            completed_passes, dribbles, errors_leading_to_goal, fouls, key_passes,
            open_play_crosses, tackles, recoveries, winning_goals,
            selected, transfers_in, transfers_out, transfers_balance, value
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (season, gameweek, player_name) DO UPDATE SET
            fixture_id = EXCLUDED.fixture_id,
            opponent_team = EXCLUDED.opponent_team,
            was_home = EXCLUDED.was_home,
            team_h_score = EXCLUDED.team_h_score,
            team_a_score = EXCLUDED.team_a_score,
            minutes = EXCLUDED.minutes,
            starts = EXCLUDED.starts,
            kickoff_time = EXCLUDED.kickoff_time,
            total_points = EXCLUDED.total_points,
            bonus = EXCLUDED.bonus,
            bps = EXCLUDED.bps,
            goals_scored = EXCLUDED.goals_scored,
            assists = EXCLUDED.assists,
            clean_sheets = EXCLUDED.clean_sheets,
            goals_conceded = EXCLUDED.goals_conceded,
            own_goals = EXCLUDED.own_goals,
            penalties_saved = EXCLUDED.penalties_saved,
            penalties_missed = EXCLUDED.penalties_missed,
            penalties_scored = EXCLUDED.penalties_scored,
            red_cards = EXCLUDED.red_cards,
            yellow_cards = EXCLUDED.yellow_cards,
            saves = EXCLUDED.saves,
            expected_goals = EXCLUDED.expected_goals,
            expected_assists = EXCLUDED.expected_assists,
            expected_goal_involvements = EXCLUDED.expected_goal_involvements,
            expected_goals_conceded = EXCLUDED.expected_goals_conceded,
            influence = EXCLUDED.influence,
            creativity = EXCLUDED.creativity,
            threat = EXCLUDED.threat,
            ict_index = EXCLUDED.ict_index,
            big_chances_missed = EXCLUDED.big_chances_missed,
            big_chances_created = EXCLUDED.big_chances_created,
            clearances_blocks_interceptions = EXCLUDED.clearances_blocks_interceptions,
            completed_passes = EXCLUDED.completed_passes,
            dribbles = EXCLUDED.dribbles,
            errors_leading_to_goal = EXCLUDED.errors_leading_to_goal,
            fouls = EXCLUDED.fouls,
            key_passes = EXCLUDED.key_passes,
            open_play_crosses = EXCLUDED.open_play_crosses,
            tackles = EXCLUDED.tackles,
            recoveries = EXCLUDED.recoveries,
            winning_goals = EXCLUDED.winning_goals,
            selected = EXCLUDED.selected,
            transfers_in = EXCLUDED.transfers_in,
            transfers_out = EXCLUDED.transfers_out,
            transfers_balance = EXCLUDED.transfers_balance,
            value = EXCLUDED.value;
    """

    total_processed = 0
    connection = None

    for retry in range(MAX_RETRIES):
        try:
            # Single connection for all batches
            connection = get_db_connection()
            _cursor = connection.cursor()

            for batch_start in range(0, len(records), BATCH_SIZE):
                batch = records[batch_start:batch_start + BATCH_SIZE]
                batch_num = batch_start // BATCH_SIZE + 1

                # Use execute_batch for bulk insert (much faster than individual inserts)
                extras.execute_batch(_cursor, upsert_sql, batch, page_size=1000)
                connection.commit()

                total_processed += len(batch)
                print(f"     Batch {batch_num}/{total_batches} committed ({total_processed}/{len(records)} records)")

            logger.info(f"Player stats loaded: {total_processed} records processed")
            print(f"✅ Player Stats: {total_processed} records loaded successfully")
            break

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logger.warning(f"Connection error, retry {retry + 1}/{MAX_RETRIES}: {e}")
            if retry < MAX_RETRIES - 1:
                print(f"  ⚠️ Connection lost, retrying in 5 seconds...")
                time.sleep(5)
            else:
                logger.error(f"Failed after {MAX_RETRIES} retries")
                print(f"  ❌ Failed after {MAX_RETRIES} retries")

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            print(f"  ❌ Failed: {e}")
            break

        finally:
            if connection:
                try:
                    connection.close()
                except:
                    pass