import requests
from airflow.models import Variable
import csv
from io import StringIO
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# def fetch_data_from_api(season) -> dict:
#     """Fetch fixtures from the GitHub archive for a specific season."""
#     try:
#         url = f"{Variable.get('FIXTURES_BASE_URL')}/{season}/{Variable.get('FIXTURES_DATASET_PATH')}"
#         response = requests.get(url)
#         response.raise_for_status()
#         return {"season": season, "csv": response.text}

#     except requests.RequestException as e:
#         print(f"⚠️ Failed to fetch fixtures.csv for {season}: {e} (URL: {url})")
#         return {"season": season, "csv": None}

# https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/players_raw.csv


# def fetch_player_gameweek(season, player_folder):
#     url = f"{REPO_BASE}/{season}/players/{player_folder}/gw.csv"
#     try:
#         return list(csv.DictReader(StringIO(fetch_csv(url))))
#     except Exception as e:
#         print(f"❌ Failed to fetch gw.csv for {player_folder} in {season}: {e}")
#         return []


def fetch_data_from_api(
    season,
    category=None,
    endpoint=Variable.get("FIXTURES_DATASET_PATH"),
    player_folder=None,
) -> dict:
    """Fetch fixtures from the GitHub archive for a specific season."""
    try:
        if (
            (season is not None)
            and (category is None)
            and (endpoint is not None)
            and (player_folder is None)
        ):
            url = f"{Variable.get('FIXTURES_BASE_URL')}/{season}/{endpoint}"
            response = requests.get(url)
            logging.info(f"Requested Url -> {url}")
            return {"season": season, "csv": response.text}

        elif (
            (season is not None)
            and (endpoint is not None)
            and (player_folder is not None)
        ):
            for player in player_folder:
                url = f"{Variable.get('FIXTURES_BASE_URL')}/{season}/players/{player}/{endpoint}"  # PLAYER_GW_DATASET_PATH

                response = requests.get(url)

                # response.text["player name"] = " ".join(player.split("_")[::-1])
                # print(response.text["player name"])
            # logging.info(f"Requested Url -> {url}")
            # return {"season": season, "csv": response.text}
            return list(csv.DictReader(StringIO(response.text)))

        else:
            url = Variable.get("FPL_TEAMS_API_URL")
            response = requests.get(url)
            return response.json().get(str(category), [])

    except requests.RequestException as e:
        print(f"An Error Occured fetching data from : {url} -> {e}")
