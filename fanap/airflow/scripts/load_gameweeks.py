import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
from scripts.db_config import db_connection_wrapper
from airflow.models import Variable

SEASONS = ["2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]


def fetch_gameweek_for_season(season) -> dict:
    """Fetch fixtures from the GitHub archive for a specific season."""
    try:
        url = f"{Variable.get('FIXTURES_BASE_URL')}/{season}/{Variable.get('FIXTURES_DATASET_PATH')}"
        response = requests.get(url)
        response.raise_for_status()
        return {"season": season, "csv": response.text}

    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch fixtures.csv for {season}: {e} (URL: {url})")
        return {"season": season, "csv": None}


def parse_gameweeks(fixture_data):
    """Extract first fixture time for each gameweek in a season to set deadlines."""

    season = fixture_data["season"]
    reader = csv.DictReader(StringIO(fixture_data["csv"]))
    gameweeks = []
    gw_map = {}

    for row in reader:
        gw = int(row["event"])
        kickoff_time = datetime.strptime(row["kickoff_time"], "%Y-%m-%dT%H:%M:%SZ")
        if gw not in gw_map or kickoff_time < gw_map[gw]:
            gw_map[gw] = kickoff_time

    for gw, kickoff in gw_map.items():
        deadline = kickoff - timedelta(hours=1)
        gameweeks.append((season, gw, deadline))

    return gameweeks


def combine_gameweeks(results):
    """Combine parsed gameweek results from multiple seasons into one dictionary."""
    combined = []
    for r in results:
        if r:
            combined.extend(r)
    return combined


@db_connection_wrapper
def load_gameweeks(connection, gameweeks):
    """Insert gameweek data into the table."""
    _cursor = connection.cursor()
    for season, gw, deadline in gameweeks:
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
    print(f"✅ Inserted/Updated {len(gameweeks)} gameweeks.")
