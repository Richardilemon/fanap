import csv
from io import StringIO
from datetime import datetime, timedelta
from scripts.utils.db_config import db_connection_wrapper
from scripts.utils.infer_season import infer_season
import logging
import psycopg2

logger = logging.getLogger(__name__)


def validate_gameweek_record(season, gameweek, deadline):
    """
    Validate a single gameweek record before insertion.
    
    Parameters
    ----------
    season : str
        Season string (e.g., "2024-25")
    gameweek : int
        Gameweek number
    deadline : datetime
        Gameweek deadline timestamp
        
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
    
    # Check deadline is a datetime object
    if not isinstance(deadline, datetime):
        return False, f"Deadline must be datetime object, got: {type(deadline)}"
    
    # Check deadline is in reasonable range (not too far in past or future)
    now = datetime.now()
    if deadline < datetime(2016, 1, 1) or deadline > now + timedelta(days=365):
        return False, f"Deadline out of reasonable range: {deadline}"
    
    return True, None


def parse_gameweeks(fixture_data):
    """
    Parse fixture data to determine gameweek deadlines for a given season.

    This function extracts the earliest fixture time for each gameweek from the provided
    fixture dataset and uses it to calculate the gameweek's deadline (1 hour before kickoff).
    The results are returned as a list of tuples suitable for database insertion.

    Parameters
    ----------
    fixture_data : dict
        A dictionary containing fixture data.
        Expected structure: {"csv": "<CSV content as string>"}

    Returns
    -------
    list of tuple
        A list of tuples, where each tuple contains:
        (
            season : str,
            gameweek : int,
            deadline : datetime
        )

    Notes
    -----
    - The function assumes that each CSV row has `event` and `kickoff_time` columns.
    - `infer_season()` is used to determine the season based on the fixture kickoff date.
    - For each gameweek, only the earliest fixture kickoff time is considered.
    - The deadline is set to one hour before the first fixture of each gameweek.
    """
    if not fixture_data:
        logger.warning("No fixture data provided for gameweek parsing")
        return []
    
    if "csv" not in fixture_data:
        logger.error("Fixture data missing 'csv' key")
        return []
    
    try:
        reader = csv.DictReader(StringIO(fixture_data["csv"]))
        gameweeks = []
        gw_map = {}
        
        for row_num, row in enumerate(reader, start=1):
            try:
                # Validate required fields exist
                if "event" not in row or "kickoff_time" not in row:
                    logger.warning(f"Row {row_num}: Missing required fields (event or kickoff_time)")
                    continue
                
                gw = int(row["event"])
                kickoff_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%dT%H:%M:%SZ")
                season = infer_season(kickoff_date=kickoff_time)
                
                if gw not in gw_map or kickoff_time < gw_map[gw]:
                    gw_map[gw] = kickoff_time
                    
            except ValueError as e:
                logger.warning(f"Row {row_num}: Invalid data format - {e}")
                continue
            except Exception as e:
                logger.error(f"Row {row_num}: Unexpected error - {e}")
                continue
        
        # Build final gameweeks list
        for gw, kickoff in gw_map.items():
            deadline = kickoff - timedelta(hours=1)
            gameweeks.append((season, gw, deadline))
        
        logger.info(f"Successfully parsed {len(gameweeks)} gameweeks")
        return gameweeks
        
    except Exception as e:
        logger.error(f"Failed to parse gameweeks: {e}")
        return []


@db_connection_wrapper
def load_gameweeks(connection, gameweeks, total_players=None):
    """
    Insert or update gameweek deadlines into the database.

    This function takes parsed gameweek data and inserts them into the `game_weeks` table.
    If a record with the same `(season, gameweek)` already exists, its `deadline` is updated.

    Parameters
    ----------
    connection : PostgresHook
        Active PostgreSQL connection provided by the `db_connection_wrapper` decorator.
    gameweeks : list of list of tuple
        A list containing lists of tuples, where each tuple represents a gameweek entry.
        Each tuple should follow the structure:
        (season : str, gameweek : int, deadline : datetime)
    total_players : int, optional
        Total number of FPL managers from bootstrap-static API.
        Used for accurate ownership percentage calculations.

    Returns
    -------
    None
        Commits the changes to the database and prints a success message after completion.

    Notes
    -----
    - Uses an UPSERT pattern to prevent duplicate entries.
    - If `gameweeks` is empty, the function exits silently.
    - The expected database table schema:
        CREATE TABLE game_weeks (
            season TEXT,
            gameweek INTEGER,
            deadline TIMESTAMP,
            total_players BIGINT,
            PRIMARY KEY (season, gameweek)
        );
    """
    if not gameweeks:
        logger.warning("No gameweeks data to load")
        print("⚠️ No gameweeks data to load.")
        return
    
    # Validate data structure
    if not isinstance(gameweeks, list):
        logger.error(f"Expected list of gameweeks, got {type(gameweeks)}")
        return
    
    _cursor = connection.cursor()
    total_inserted = 0
    total_updated = 0
    total_failed = 0
    
    try:
        for gw_list in gameweeks:
            if not isinstance(gw_list, list):
                logger.warning(f"Expected list of tuples, got {type(gw_list)}")
                continue
                
            for season, gameweek_num, deadline in gw_list:
                # Validate record before insertion
                is_valid, error_msg = validate_gameweek_record(season, gameweek_num, deadline)
                if not is_valid:
                    logger.warning(f"Validation failed for GW{gameweek_num} ({season}): {error_msg}")
                    total_failed += 1
                    continue
                
                try:
                    _cursor.execute(
                        """
                        INSERT INTO game_weeks (season, gameweek, deadline, total_players)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (season, gameweek) DO UPDATE SET
                            deadline = EXCLUDED.deadline,
                            total_players = COALESCE(EXCLUDED.total_players, game_weeks.total_players)
                        RETURNING (xmax = 0) AS inserted;
                    """,
                        (season, gameweek_num, deadline, total_players),
                    )
                    
                    # Check if it was an insert or update
                    result = _cursor.fetchone()
                    if result and result[0]:
                        total_inserted += 1
                    else:
                        total_updated += 1
                        
                except psycopg2.IntegrityError as e:
                    logger.error(f"Integrity error for GW{gameweek_num} ({season}): {e}")
                    total_failed += 1
                    connection.rollback()
                    continue
                except psycopg2.DatabaseError as e:
                    logger.error(f"Database error for GW{gameweek_num} ({season}): {e}")
                    total_failed += 1
                    connection.rollback()
                    continue
        
        connection.commit()
        logger.info(f"Gameweeks loaded: {total_inserted} inserted, {total_updated} updated, {total_failed} failed")
        print(f"✅ Gameweeks: {total_inserted} inserted, {total_updated} updated, {total_failed} failed")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Fatal error loading gameweeks: {e}")
        print(f"❌ Failed to load gameweeks: {e}")
        raise