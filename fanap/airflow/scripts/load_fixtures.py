import sys

sys.path.append("/app")
import requests
from datetime import datetime
from scripts.db_config import db_connection_wrapper
import csv
from io import StringIO
from airflow.models import Variable

SEASONS = ["2018-19", "2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]


def fetch_data_from_api(season) -> dict:
    """Fetch fixtures from the GitHub archive for a specific season."""
    try:
        url = f"{Variable.get('FIXTURES_BASE_URL')}/{season}/{Variable.get('FIXTURES_DATASET_PATH')}"
        response = requests.get(url)
        response.raise_for_status()
        return {"season": season, "csv": response.text}

    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch fixtures.csv for {season}: {e} (URL: {url})")
        return {"season": season, "csv": None}


def parse_fixtures(csv_text, season):
    if not csv_text:
        print(f"⚠️ Skipping {season}: No CSV data.")
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

        fixtures.append(
            (
                code,
                season,
                gameweek,
                kickoff_time,
                team_h,
                team_a,
                team_h_score,
                team_a_score,
                team_h_difficulty,
                team_a_difficulty,
            )
        )

    return fixtures


def combine_data_from_api(results):
    """Combine parsed gameweek results from multiple seasons into one dictionary."""
    combined = []
    for r in results:
        if r:
            combined.extend(r)
    return combined


@db_connection_wrapper
def load_fixtures(connection, fixtures):
    if not fixtures:
        return
    _cursor = connection.cursor()

    for fixture in fixtures:
        _cursor.execute(
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
            fixture,
        )
    connection.commit()
    print(f"✅ Inserted/Updated {len(fixtures)} fixtures.")
