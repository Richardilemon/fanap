from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
import csv
from io import StringIO
from scripts.utils.infer_season import infer_season


def parse_fixtures(csv_text):
    """
    Parse fixture data from a CSV text object into a structured list of tuples.

    This function processes fixture-related CSV data (as returned by an API or a file),
    infers the season based on the kickoff time, safely handles missing or malformed values,
    and returns a list of tuples ready for database insertion.

    Parameters
    ----------
    csv_text : dict
        A dictionary expected to contain a `"csv"` key with the CSV data as a string.
        Example: {"csv": "<CSV string content>"}

    Returns
    -------
    list of tuple
        A list where each tuple represents a fixture record with the following structure:
        (
            code : int,
            season : str,
            gameweek : int or None,
            kickoff_time : datetime or None,
            team_h : int,
            team_a : int,
            team_h_score : int or None,
            team_a_score : int or None,
            team_h_difficulty : int or None,
            team_a_difficulty : int or None
        )

    Notes
    -----
    - Skips rows without valid data.
    - Uses `infer_season()` to determine the season from kickoff date.
    - Gracefully handles missing, empty, or float values in numeric fields.
    """
    if not csv_text:
        print("⚠️ Skipping : No CSV data.")
        return []

    reader = csv.DictReader(StringIO(csv_text["csv"]))
    fixtures = []

    for row in reader:
        code = int(row["code"])
        gameweek = int(row["event"]) if row["event"] and row["event"] != "" else None
        kickoff_time = (
            datetime.strptime(row["kickoff_time"], "%Y-%m-%dT%H:%M:%SZ")
            if row["kickoff_time"]
            else None
        )
        season = infer_season(kickoff_time)

        team_h = int(row["team_h"])
        team_a = int(row["team_a"])
        # Handle float values in scores
        try:
            team_h_score = (
                int(float(row["team_h_score"]))
                if row["team_h_score"]
                and row["team_h_score"] != ""  # use lambda to check for empty string
                else None
            )
        except (ValueError, TypeError):
            team_h_score = None
        try:
            team_a_score = (
                int(float(row["team_a_score"]))
                if row["team_a_score"]
                and row["team_a_score"] != ""  # use lambda to check for empty string
                else None
            )
        except (ValueError, TypeError):
            team_a_score = None
        team_h_difficulty = (
            int(row["team_h_difficulty"]) if row["team_h_difficulty"] else None
        )
        team_a_difficulty = (
            int(row["team_a_difficulty"]) if row["team_a_difficulty"] else None
        )
        

        print(
            f"Season -> {type(season)}, code -> {type(code)}, gameweek -> {type(gameweek)}, "
            f"kickoff_time -> {type(kickoff_time)}, team_h -> {type(team_h)}, team_a -> {type(team_a)}, "
            f"team_h_score -> {type(team_h_score)}, team_a_score -> {type(team_a_score)}, "
            f"team_h_difficulty -> {type(team_h_difficulty)}, team_a_difficulty -> {type(team_a_difficulty)}"
        )

        fixtures.append(
            (
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

    return fixtures


@db_connection_wrapper
def load_fixtures(connection, fixtures):

    """
    Insert or update fixture data in the PostgreSQL database.

    This function inserts parsed fixture records into the `fixtures` table,
    updating any existing rows with matching `code` values to ensure data consistency.

    Parameters
    ----------
    connection : PostgresHook
        Active PostgreSQL database connection provided by the `db_connection_wrapper` decorator.
    fixtures : list of list of tuple
        List containing lists of fixture tuples as returned by `parse_fixtures()`.

    Returns
    -------
    None
        Commits all changes to the database and prints a success message upon completion.

    Notes
    -----
    - Uses an UPSERT pattern with `ON CONFLICT (code)` to prevent duplicate records.
    - Ignores empty fixture lists safely.
    """
    if not fixtures:
        return
    cursor = connection.cursor()

    for fixture in fixtures:
        for match in fixture:
            cursor.execute(
                """
                INSERT INTO fixtures (
                    code, season, gameweek, kickoff_time,
                    team_h_code, team_a_code,
                    team_h_score, team_a_score,
                    team_h_difficulty, team_a_difficulty
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET
                    season = EXCLUDED.season,
                    gameweek = EXCLUDED.gameweek,
                    kickoff_time = EXCLUDED.kickoff_time,
                    team_h_code = EXCLUDED.team_h_code,
                    team_a_code = EXCLUDED.team_a_code,
                    team_h_score = EXCLUDED.team_h_score,
                    team_a_score = EXCLUDED.team_a_score,
                    team_h_difficulty = EXCLUDED.team_h_difficulty,
                    team_a_difficulty = EXCLUDED.team_a_difficulty;
            """,
                match,
            )

    connection.commit()
    print(f"✅ Inserted/Updated {len(fixture) * len(fixtures)} fixtures.")
