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


def fetch_data_from_api(
    season,
    category=None,
    endpoint=Variable.get("FIXTURES_DATASET_PATH"),
    player_folder=None,
) -> dict:
    
    """
    Fetch data from the Fantasy Premier League (FPL) GitHub archive or API 
    based on the provided parameters.

    This function supports multiple data retrieval modes:
      1. **Fixtures Mode** — Fetches the fixtures dataset for a specific season.
      2. **Player Data Mode** — Fetches player-level data for a list of players 
         from the GitHub archive.
      3. **Category Mode** — Fetches live data (e.g., teams, events, etc.) 
         directly from the FPL API.

    Args:
        season (str): The season to fetch data for (e.g., "2023-24").
        category (str, optional): The FPL API category to fetch from 
            (e.g., "elements", "teams"). Defaults to None.
        endpoint (str, optional): The relative path or dataset filename 
            (e.g., "fixtures.csv" or "gw.csv"). Defaults to the Airflow variable 
            `FIXTURES_DATASET_PATH`.
        player_folder (list, optional): A list of player folder names used to 
            build player data URLs. If provided, player-specific data will be fetched. 
            Defaults to None.

    Returns:
        dict or list:
            - If fetching fixtures, returns a dictionary: 
              `{"season": season, "csv": <CSV content>}`.
            - If fetching player data, returns a list of dictionaries containing 
              player gameweek stats with an added `"player_name"` field.
            - If fetching API category data, returns the JSON list for that category.

    Raises:
        requests.RequestException: If a network error or request failure occurs.

    Example:
        >>> fetch_data_from_api("2023-24")
        {'season': '2023-24', 'csv': 'event,team_h,team_a,kickoff_time,...'}

        >>> fetch_data_from_api("2023-24", player_folder=["mohamed_salah_123"])
        [{'player_name': 'mohamed salah', 'goals_scored': '2', ...}]

        >>> fetch_data_from_api("2023-24", category="elements")
        [{'id': 1, 'web_name': 'Haaland', 'team_code': 11, ...}]
    """
    try:
        if (
            (season is not None)
            and (category is None)
            and (endpoint is not None)
            and (player_folder is None)
        ):
            url = f"{Variable.get('FIXTURES_BASE_URL')}{season}/{endpoint}"
            response = requests.get(url)
            logging.info(f"Requested Url -> {url}")
            return {"season": season, "csv": response.text}

        elif (
            (season is not None)
            and (endpoint is not None)
            and (player_folder is not None)
        ):

            all_player_data = []

            for player in player_folder:
                url = f"{Variable.get('FIXTURES_BASE_URL')}{season}/players/{player}/{endpoint}"

                # logging.info(f"Requested URL for {player} --> {url}")
                response = requests.get(url)

                if response.status_code == 200:
                    player_data = list(csv.DictReader(StringIO(response.text)))
                    logging.info(f"Players Data --> {player_data}")
                    for row in player_data:
                        row["player_name"] = " ".join(player.split("_")[:-1])

                    all_player_data.extend(player_data)
                else:
                    logging.warning(f"Failed to fetch data for {player}: {response.status_code}")

            return all_player_data

        else:
            url = Variable.get("FPL_TEAMS_API_URL")
            response = requests.get(url)
            return response.json().get(str(category), [])

    except requests.RequestException as e:
        print(f"An Error Occured fetching data from : {url} -> {e}")
