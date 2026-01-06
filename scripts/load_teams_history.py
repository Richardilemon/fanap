from io import StringIO
import csv
from scripts.utils.db_config import db_connection_wrapper


SEASONS = ["2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]

def parse_team_history(csv_text):
    """
    Parse multiple season team history CSVs into structured records.

    Parameters
    ----------
    csv_text : list[dict]
        Each dict should have keys:
        - "csv": string containing CSV data
        - "season": string or list of seasons

    Returns
    -------
    list[dict]
        Each record is a dictionary containing all CSV fields plus a "season" key.
    """
    parsed_records = []

    for data in csv_text:
        csv_data = data.get("csv")
        seasons = data.get("season")

        if not csv_data or not seasons:
            continue

        # Ensure 'seasons' is always iterable
        if isinstance(seasons, str):
            seasons = [seasons]

        reader = csv.DictReader(StringIO(csv_data))

        # For each record and each season, append combined data
        for record in reader:
            for season in seasons:
                record_with_season = dict(record)
                record_with_season["season"] = season
                parsed_records.append(record_with_season)

    return parsed_records
    
    
@db_connection_wrapper
def load_teams_history_records(connection, teams_history_records):
    if not teams_history_records:
        print(f"⚠️ Skipping Records: No CSV data.")
        return

    cursor = connection.cursor()
    for row in teams_history_records:
        season = row["season"]
        cursor.execute(
            """
            INSERT INTO teams_history (
                season, code, id, name, short_name,
                strength_overall_home, strength_overall_away,
                strength_attack_home, strength_attack_away,
                strength_defence_home, strength_defence_away,
                strength, position
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                strength = EXCLUDED.strength,
                position = EXCLUDED.position;
        """,
            (
                season,
                row["code"],
                row["id"],
                row["name"],
                row["short_name"],
                row["strength_overall_home"],
                row["strength_overall_away"],
                row["strength_attack_home"],
                row["strength_attack_away"],
                row["strength_defence_home"],
                row["strength_defence_away"],
                row["strength"],
                row["position"],
            ),
        )
    connection.commit()
    print(f"✅ Loaded teams for {season}")

