import requests
from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
from airflow.models import Variable
import logging
from scripts.utils.infer_season import infer_season
import psycopg2

logger = logging.getLogger(__name__)


def validate_team_record(code, team_id, name, short_name):
    """
    Validate a team record before insertion.
    
    Parameters
    ----------
    code : int
        Team code
    team_id : int
        Team ID
    name : str
        Full team name
    short_name : str
        Abbreviated team name
        
    Returns
    -------
    tuple
        (is_valid: bool, error_message: str or None)
    """
    # Check code is positive integer
    if not isinstance(code, int) or code <= 0:
        return False, f"Invalid team code: {code}"
    
    # Check team_id is positive integer
    if not isinstance(team_id, int) or team_id <= 0:
        return False, f"Invalid team ID: {team_id}"
    
    # Check name is non-empty string
    if not name or not isinstance(name, str) or len(name.strip()) == 0:
        return False, f"Invalid team name: {name}"
    
    # Check short_name is non-empty string and reasonable length
    if not short_name or not isinstance(short_name, str) or len(short_name) > 10:
        return False, f"Invalid team short name: {short_name}"
    
    return True, None


def fetch_teams_data():
    """
    Fetch team data from the Fantasy Premier League (FPL) API.

    This function retrieves the list of teams from the official FPL API using the
    endpoint URL stored in the Airflow Variable `FPL_TEAMS_API_URL`. The API returns
    a JSON response containing multiple data categories, from which the `teams` key
    is extracted.

    The function gracefully handles request-related errors (e.g., network issues,
    invalid URLs, or non-2xx HTTP responses) and logs the failure instead of
    interrupting the DAG run.

    Returns
    -------
    list of dict
        A list of dictionaries, where each dictionary represents a team and includes
        details such as:
            - id: Unique team ID
            - name: Full team name
            - short_name: Abbreviated team name
            - code: Numeric code representing the team
            - strength_*: Team strength metrics (overall, attack, defense, etc.)

        Returns an empty list (`[]`) if the request fails or the data cannot be fetched.

    Raises
    ------
    None
        All exceptions are handled internally, so no exceptions are propagated.
    """
    try:
        url = Variable.get("FPL_TEAMS_API_URL")
        logger.info(f"Fetching teams data from {url}")
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        teams = data.get("teams", [])
        
        if not teams:
            logger.warning("No teams data returned from API")
            return []
        
        # Validate we got a reasonable number of teams (EPL has 20 teams)
        if len(teams) < 15 or len(teams) > 25:
            logger.warning(f"Unexpected number of teams: {len(teams)} (expected ~20)")
        
        logger.info(f"Successfully fetched {len(teams)} teams")
        return teams
        
    except requests.Timeout:
        logger.error(f"Timeout fetching teams data from {url}")
        print(f"⚠️ Timeout fetching teams data")
        return []
    except requests.RequestException as e:
        logger.error(f"Failed to fetch teams data: {e}")
        print(f"⚠️ Failed to fetch teams data: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching teams: {e}")
        print(f"⚠️ Unexpected error: {e}")
        return []


@db_connection_wrapper
def load_teams(connection, teams, season):
    """
    Insert or update Fantasy Premier League (FPL) team data into the database.

    This function takes a list of teams retrieved from the FPL API and inserts
    each team's information into the `teams` table. If a record for the given
    season and team code already exists, it performs an update instead of
    creating a duplicate (using the `ON CONFLICT` clause).

    The database connection is automatically established via the
    `get_connection_wrapper` decorator, which provides a managed PostgreSQL
    connection through Airflow's `PostgresHook`.

    Parameters
    ----------
    connection : PostgresHook
        Active database connection object provided by the connection wrapper.
    teams : list[dict]
        A list of dictionaries, where each dictionary represents a team and
        contains keys such as `id`, `code`, `name`, `short_name`, and
        strength attributes (home/away, attack/defence).
    season : str
        The FPL season string in the format 'YYYY-YY', used as a reference
        to differentiate seasonal records.

    Behavior
    --------
    - Inserts team data into the `teams` table if it does not exist.
    - Updates existing team data if the team already exists for the given season.
    - Commits all database operations upon success.
    - Connection is closed by the decorator, not manually.

    Logs
    ----
    - Logs successful database connection establishment.
    - Logs when team data insertion or update is completed.
    - Logs database connection closure (handled by decorator).
    """
    if not teams:
        logger.warning("No teams data to load")
        print("⚠️ No teams data to load.")
        return
    
    # Validate data structure
    if not isinstance(teams, list):
        logger.error(f"Expected list of teams, got {type(teams)}")
        return
    
    _cursor = connection.cursor()
    total_inserted = 0
    total_updated = 0
    total_failed = 0
    
    try:
        for team in teams:
            # Validate required fields exist
            required_fields = ["code", "id", "name", "short_name"]
            missing_fields = [f for f in required_fields if f not in team]
            if missing_fields:
                logger.warning(f"Team missing required fields: {missing_fields}")
                total_failed += 1
                continue
            
            # Validate team record
            is_valid, error_msg = validate_team_record(
                team["code"], team["id"], team["name"], team["short_name"]
            )
            if not is_valid:
                logger.warning(f"Validation failed for team {team.get('name', 'Unknown')}: {error_msg}")
                total_failed += 1
                continue
            
            try:
                _cursor.execute(
                    """
                    INSERT INTO teams (
                        season, code, id, name, short_name,
                        strength_overall_home, strength_overall_away,
                        strength_attack_home, strength_attack_away,
                        strength_defence_home, strength_defence_away,
                        strength
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        strength = EXCLUDED.strength
                    RETURNING (xmax = 0) AS inserted;
                """,
                    (
                        infer_season(),
                        team["code"],
                        team["id"],
                        team["name"],
                        team["short_name"],
                        team.get("strength_overall_home", 0),
                        team.get("strength_overall_away", 0),
                        team.get("strength_attack_home", 0),
                        team.get("strength_attack_away", 0),
                        team.get("strength_defence_home", 0),
                        team.get("strength_defence_away", 0),
                        team.get("strength", 0),
                    ),
                )
                
                # Check if it was an insert or update
                result = _cursor.fetchone()
                if result and result[0]:
                    total_inserted += 1
                else:
                    total_updated += 1
                    
            except psycopg2.IntegrityError as e:
                logger.error(f"Integrity error for team {team.get('name', 'Unknown')}: {e}")
                total_failed += 1
                connection.rollback()
                continue
            except psycopg2.DatabaseError as e:
                logger.error(f"Database error for team {team.get('name', 'Unknown')}: {e}")
                total_failed += 1
                connection.rollback()
                continue
        
        connection.commit()
        logger.info(f"Teams loaded: {total_inserted} inserted, {total_updated} updated, {total_failed} failed")
        print(f"✅ Teams: {total_inserted} inserted, {total_updated} updated, {total_failed} failed (Season: {season})")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Fatal error loading teams: {e}")
        print(f"❌ Failed to load teams: {e}")
        raise