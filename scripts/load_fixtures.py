from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
import csv
from io import StringIO
from scripts.utils.infer_season import infer_season
import logging
import psycopg2

logger = logging.getLogger(__name__)


def validate_fixture_record(code, season, team_h, team_a, team_h_score=None, team_a_score=None):
    """
    Validate a fixture record before insertion.
    
    Parameters
    ----------
    code : int
        Unique fixture code
    season : str
        Season string (e.g., "2024-25")
    team_h : int
        Home team code
    team_a : int
        Away team code
    team_h_score : int, optional
        Home team score
    team_a_score : int, optional
        Away team score
        
    Returns
    -------
    tuple
        (is_valid: bool, error_message: str or None)
    """
    # Check fixture code is positive integer
    if not isinstance(code, int) or code <= 0:
        return False, f"Invalid fixture code: {code}"
    
    # Check season format
    if not season or not isinstance(season, str):
        return False, f"Invalid season format: {season}"
    
    # Check team codes are positive integers
    if not isinstance(team_h, int) or team_h <= 0:
        return False, f"Invalid home team code: {team_h}"
    if not isinstance(team_a, int) or team_a <= 0:
        return False, f"Invalid away team code: {team_a}"
    
    # Team cannot play itself
    if team_h == team_a:
        return False, f"Team cannot play itself: {team_h}"
    
    # Validate scores if present
    if team_h_score is not None:
        if not isinstance(team_h_score, int) or team_h_score < 0 or team_h_score > 20:
            return False, f"Invalid home team score: {team_h_score}"
    
    if team_a_score is not None:
        if not isinstance(team_a_score, int) or team_a_score < 0 or team_a_score > 20:
            return False, f"Invalid away team score: {team_a_score}"
    
    return True, None


def parse_fixtures(csv_text):
    """Parse fixture data including the id column"""
    if not csv_text:
        logger.warning("No CSV data provided for fixture parsing")
        print("⚠️ Skipping : No CSV data.")
        return []
    
    if "csv" not in csv_text:
        logger.error("CSV data missing 'csv' key")
        return []

    try:
        reader = csv.DictReader(StringIO(csv_text["csv"]))
        fixtures = []
        skipped = 0

        for row_num, row in enumerate(reader, start=1):
            try:
                # Validate required fields (now including 'id')
                required_fields = ["id", "code", "team_h", "team_a", "kickoff_time"]
                missing_fields = [f for f in required_fields if f not in row or not row[f]]
                if missing_fields:
                    logger.warning(f"Row {row_num}: Missing required fields: {missing_fields}")
                    skipped += 1
                    continue
                
                fixture_id = int(row["id"])  # NEW: Get the id column
                code = int(row["code"])
                gameweek = int(row["event"]) if row.get("event") and row["event"] != "" else None
                
                kickoff_time = (
                    datetime.strptime(row["kickoff_time"], "%Y-%m-%dT%H:%M:%SZ")
                    if row["kickoff_time"]
                    else None
                )
                season = infer_season(kickoff_time)

                team_h = int(row["team_h"])
                team_a = int(row["team_a"])
                
                # Handle scores...
                try:
                    team_h_score = (
                        int(float(row["team_h_score"]))
                        if row.get("team_h_score") and row["team_h_score"] != ""
                        else None
                    )
                except (ValueError, TypeError):
                    team_h_score = None
                    
                try:
                    team_a_score = (
                        int(float(row["team_a_score"]))
                        if row.get("team_a_score") and row["team_a_score"] != ""
                        else None
                    )
                except (ValueError, TypeError):
                    team_a_score = None
                    
                team_h_difficulty = (
                    int(row["team_h_difficulty"]) if row.get("team_h_difficulty") else None
                )
                team_a_difficulty = (
                    int(row["team_a_difficulty"]) if row.get("team_a_difficulty") else None
                )
                
                # Validate
                is_valid, error_msg = validate_fixture_record(
                    code, season, team_h, team_a, team_h_score, team_a_score
                )
                if not is_valid:
                    logger.warning(f"Row {row_num}: Validation failed - {error_msg}")
                    skipped += 1
                    continue

                fixtures.append(
                    (
                        fixture_id,  # NEW: Add id as first column
                        code,
                        season,
                        gameweek,
                        row["kickoff_time"],
                        team_h,
                        team_a,
                        team_h_score,
                        team_a_score,
                        team_h_difficulty,
                        team_a_difficulty,
                    )
                )
                
            except ValueError as e:
                logger.warning(f"Row {row_num}: Invalid data format - {e}")
                skipped += 1
                continue
            except Exception as e:
                logger.error(f"Row {row_num}: Unexpected error - {e}")
                skipped += 1
                continue

        logger.info(f"Parsed {len(fixtures)} fixtures ({skipped} skipped)")
        return fixtures
        
    except Exception as e:
        logger.error(f"Failed to parse fixtures: {e}")
        return []


@db_connection_wrapper
def load_fixtures(connection, fixtures):
    """Insert or update fixture data including id column"""
    if not fixtures:
        logger.warning("No fixtures data to load")
        print("⚠️ No fixtures data to load.")
        return
    
    if not isinstance(fixtures, list):
        logger.error(f"Expected list of fixtures, got {type(fixtures)}")
        return
    
    cursor = connection.cursor()
    total_inserted = 0
    total_updated = 0
    total_failed = 0

    try:
        for fixture_list in fixtures:
            if not isinstance(fixture_list, list):
                logger.warning(f"Expected list of tuples, got {type(fixture_list)}")
                continue
                
            for match in fixture_list:
                try:
                    cursor.execute(
                        """
                        INSERT INTO fixtures (
                            id, code, season, gameweek, kickoff_time,
                            team_h_code, team_a_code,
                            team_h_score, team_a_score,
                            team_h_difficulty, team_a_difficulty
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (code) DO UPDATE SET
                            id = EXCLUDED.id,
                            season = EXCLUDED.season,
                            gameweek = EXCLUDED.gameweek,
                            kickoff_time = EXCLUDED.kickoff_time,
                            team_h_code = EXCLUDED.team_h_code,
                            team_a_code = EXCLUDED.team_a_code,
                            team_h_score = EXCLUDED.team_h_score,
                            team_a_score = EXCLUDED.team_a_score,
                            team_h_difficulty = EXCLUDED.team_h_difficulty,
                            team_a_difficulty = EXCLUDED.team_a_difficulty
                        RETURNING (xmax = 0) AS inserted;
                    """,
                        match,
                    )
                    
                    result = cursor.fetchone()
                    if result and result[0]:
                        total_inserted += 1
                    else:
                        total_updated += 1
                        
                except psycopg2.IntegrityError as e:
                    logger.error(f"Integrity error for fixture {match[1]}: {e}")
                    total_failed += 1
                    connection.rollback()
                    continue
                except psycopg2.DatabaseError as e:
                    logger.error(f"Database error for fixture {match[1]}: {e}")
                    total_failed += 1
                    connection.rollback()
                    continue

        connection.commit()
        logger.info(f"Fixtures loaded: {total_inserted} inserted, {total_updated} updated, {total_failed} failed")
        print(f"✅ Fixtures: {total_inserted} inserted, {total_updated} updated, {total_failed} failed")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Fatal error loading fixtures: {e}")
        print(f"❌ Failed to load fixtures: {e}")
        raise