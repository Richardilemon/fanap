import csv
from io import StringIO
from datetime import datetime, timedelta
from scripts.utils.db_config import db_connection_wrapper
from scripts.utils.infer_season import infer_season


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
    reader = csv.DictReader(StringIO(fixture_data["csv"]))
    gameweeks = []
    gw_map = {}

    for row in reader:
        gw = int(row["event"])
        kickoff_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%dT%H:%M:%SZ")
        season = infer_season(kickoff_date=kickoff_time)
        if gw not in gw_map or kickoff_time < gw_map[gw]:
            gw_map[gw] = kickoff_time

    for gw, kickoff in gw_map.items():
        deadline = kickoff - timedelta(hours=1)
        gameweeks.append((season, gw, deadline))

    return gameweeks


@db_connection_wrapper
def load_gameweeks(connection, gameweeks):
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
            PRIMARY KEY (season, gameweek)
        );
    """
    _cursor = connection.cursor()
    for gw in gameweeks:
        for season, gw, deadline in gw:
            _cursor.execute(
                """
                INSERT INTO game_weeks (season, gameweek, deadline)
                VALUES (%s, %s, %s)
                ON CONFLICT (season, gameweek) DO UPDATE SET
                    deadline = EXCLUDED.deadline;
            """,
                (season, gw, deadline),
            )
    connection.commit()
    print(f"✅ Inserted/Updated {len(gameweeks) * gw} gameweeks.")
