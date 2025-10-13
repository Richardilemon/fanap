import requests
from datetime import datetime
from scripts.db_config import db_connection_wrapper
from airflow.models import Variable
import logging


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


# # @db_connection_wrapper
# # def create_teams_table(conn, **kwargs):
# #     """
# #     Create the `teams` table in the PostgreSQL database if it does not already exist.

# #     This function ensures that the required table structure for storing Fantasy Premier League
# #     (FPL) team data exists in the database. It defines the schema for team attributes, including
# #     identifiers, performance strengths, and other key metrics. The function uses a primary key
# #     composed of `season` and `code` to maintain unique entries for each team across seasons.

# #     The function is decorated with `@db_connection_wrapper`, which automatically manages
# #     database connection setup and teardown using Airflow's `PostgresHook`.

# #     Parameters
# #     ----------
# #     conn : The active PostgreSQL connection object, automatically provided by the decorator.

# #     **kwargs : dict
# #         Additional keyword arguments (not used here but included for compatibility with Airflow’s
# #         operator call signature).

# #     Returns
# #     -------
# #     None
# #         The function does not return a value. It executes SQL statements to create the table
# #         and commits the transaction.

# #     Raises
# #     ------
# #     If any database-related error occurs during table creation, it will be propagated upward
# #     after being logged by the decorator.

# #     """

# #     with conn.cursor() as cur:
# #         cur.execute("""
# #             CREATE TABLE IF NOT EXISTS teams (
# #                 season TEXT,
# #                 code INTEGER,
# #                 id INTEGER,
# #                 name TEXT,
# #                 short_name TEXT,
# #                 strength_overall_home INTEGER,
# #                 strength_overall_away INTEGER,
# #                 strength_attack_home INTEGER,
# #                 strength_attack_away INTEGER,
# #                 strength_defence_home INTEGER,
# #                 strength_defence_away INTEGER,
# #                 strength INTEGER,
# #                 PRIMARY KEY (season, code)
# #             );
# #         """)
# #         conn.commit()

# #         logging.info("Teams table created or already exists.")

# #         conn.close()
# #         logging.info("Database Connection Closed")


def infer_season():
    """Return the current FPL season (e.g., '2024-25') based on today's date."""
    today = datetime.today()
    year = today.year
    month = today.month
    if month >= 8:
        start_year = year
        end_year = year + 1
    else:
        start_year = year - 1
        end_year = year
    return f"{start_year}-{str(end_year)[-2:]}"


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


# # def main(season=None):
# #     if not season:
# #         season = infer_season()

# #     conn = get_db_connection()
# #     try:
# #         print("Creating teams table...")
# #         create_teams_table(conn)

# #         print("Fetching teams data...")
# #         teams = fetch_teams_data()

# #         print(f"Inserting {len(teams)} teams into database for season {season}...")
# #         insert_teams(conn, teams, season)

# #         print("\u2705 Teams data loaded successfully.")
# #     finally:
# #         conn.close()


# # if __name__ == "__main__":
# #     import sys

# #     season_arg = sys.argv[1] if len(sys.argv) > 1 else None
# #     main(season=season_arg)
