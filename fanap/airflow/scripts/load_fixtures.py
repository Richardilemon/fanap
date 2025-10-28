from datetime import datetime
from scripts.utils.db_config import db_connection_wrapper
import csv
from io import StringIO
from scripts.utils.infer_season import infer_season


def parse_fixtures(csv_text):
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


@db_connection_wrapper
def load_fixtures(connection, fixtures):
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
