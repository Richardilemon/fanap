import requests
from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
from airflow.models import Variable
import logging
from scripts.utils.infer_season import infer_season


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

    url = Variable.get("FPL_TEAMS_API_URL")
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("teams", [])
    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch teams data: {e} (URL: {url})")
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
    - Closes the database connection after execution.

    Logs
    ----
    - Logs successful database connection establishment.
    - Logs when team data insertion or update is completed.
    - Logs database connection closure.

    """
    # with connection.cursor() as cur:
    _cursor = connection.cursor()
    for team in teams:
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
                strength = EXCLUDED.strength;
        """,
            (
                infer_season(),
                team["code"],
                team["id"],
                team["name"],
                team["short_name"],
                team["strength_overall_home"],
                team["strength_overall_away"],
                team["strength_attack_home"],
                team["strength_attack_away"],
                team["strength_defence_home"],
                team["strength_defence_away"],
                team["strength"],
            ),
        )
    connection.commit()

    logging.info("Teams data inserted/updated successfully.")

    print(f"Inserted {len(teams)} teams into database for season {season}...")

    connection.close()
    logging.info("Database Connection Closed")
