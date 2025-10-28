import csv
from io import StringIO
from datetime import datetime, timedelta
from scripts.utils.db_config import db_connection_wrapper
from scripts.utils.infer_season import infer_season


def parse_gameweeks(fixture_data):
    """Extract first fixture time for each gameweek in a season to set deadlines."""

    # season = fixture_data["season"]
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
    """Insert gameweek data into the table."""
    _cursor = connection.cursor()
    # for season, gw, deadline in gameweeks:
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
