from datetime import datetime

SEASONS = ["2018-19", "2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]


def infer_season(kickoff_date=None):
    """Return the FPL season (e.g., '2024-25') based on a given or current date."""
    if kickoff_date is None:
        date = datetime.today()
    else:
        date = kickoff_date

    year = date.year
    month = date.month

    if month >= 8:  # Season typically starts in August
        start_year = year
        end_year = year + 1
    else:
        start_year = year - 1
        end_year = year

    return f"{start_year}-{str(end_year)[-2:]}"
