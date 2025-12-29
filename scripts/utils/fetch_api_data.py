"""
Universal API Data Fetcher
Works with both Airflow (Variables) and standalone (environment variables)
"""
import os
import requests
import csv
from io import StringIO
import logging

logger = logging.getLogger(__name__)


def get_config_variable(key, default=None):
    """
    Get configuration variable from Airflow or environment.
    
    Priority:
    1. Try Airflow Variable (if available)
    2. Fall back to environment variable
    3. Use default if provided
    
    Args:
        key: Configuration key name
        default: Default value if not found
        
    Returns:
        Configuration value or default
        
    Raises:
        KeyError: If variable not found and no default provided
    """
    # Try Airflow Variable first
    try:
        from airflow.models import Variable
        value = Variable.get(key, default_var=default)
        if value is not None:
            logger.debug(f"Got {key} from Airflow Variable")
            return value
    except ImportError:
        pass  # Airflow not available, try environment
    except Exception as e:
        logger.debug(f"Airflow Variable.get failed for {key}: {e}")
    
    # Try environment variable
    value = os.getenv(key, default)
    if value is not None:
        logger.debug(f"Got {key} from environment variable")
        return value
    
    # No value found
    if default is None:
        raise KeyError(
            f"Configuration variable '{key}' not found in Airflow Variables or environment.\n"
            f"Please set it as:\n"
            f"  - Airflow Variable: airflow variables set {key} <value>\n"
            f"  - Environment: export {key}=<value>\n"
            f"  - .env file: {key}=<value>"
        )
    
    return default


def fetch_data_from_api(
    season,
    category=None,
    endpoint=None,
    player_folder=None,
) -> dict:
    """
    Fetch data from the Fantasy Premier League (FPL) GitHub archive or API 
    based on the provided parameters.

    This function supports multiple data retrieval modes:
      1. **Fixtures Mode** – Fetches the fixtures dataset for a specific season.
      2. **Player Data Mode** – Fetches player-level data for a list of players 
         from the GitHub archive.
      3. **Category Mode** – Fetches live data (e.g., teams, events, etc.) 
         directly from the FPL API.

    Args:
        season (str): The season to fetch data for (e.g., "2023-24").
        category (str, optional): The FPL API category to fetch from 
            (e.g., "elements", "teams"). Defaults to None.
        endpoint (str, optional): The relative path or dataset filename 
            (e.g., "fixtures.csv" or "gw.csv"). If None, uses FIXTURES_DATASET_PATH.
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
    url = None
    
    try:
        # Mode 1: Fetch fixtures or historical data for a season
        if (
            (season is not None)
            and (category is None)
            and (endpoint is not None)
            and (player_folder is None)
        ):
            base_url = get_config_variable('FIXTURES_BASE_URL')
            url = f"{base_url}{season}/{endpoint}"
            
            logger.info(f"Fetching season data from: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            return {"season": season, "csv": response.text}

        # Mode 2: Fetch player-specific gameweek data
        elif (
            (season is not None)
            and (endpoint is not None)
            and (player_folder is not None)
        ):
            base_url = get_config_variable('FIXTURES_BASE_URL')
            all_player_data = []
            successful = 0
            failed = 0

            for player in player_folder:
                url = f"{base_url}{season}/players/{player}/{endpoint}"
                
                try:
                    response = requests.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        player_data = list(csv.DictReader(StringIO(response.text)))
                        
                        # Add player name to each row
                        player_name = " ".join(player.split("_")[:-1])
                        for row in player_data:
                            row["player_name"] = player_name
                        
                        all_player_data.extend(player_data)
                        successful += 1
                        logger.debug(f"✅ Fetched data for {player_name} ({len(player_data)} records)")
                    else:
                        failed += 1
                        logger.warning(
                            f"⚠️ Failed to fetch data for {player}: "
                            f"HTTP {response.status_code}"
                        )
                        
                except requests.Timeout:
                    failed += 1
                    logger.warning(f"⚠️ Timeout fetching data for {player}")
                except requests.RequestException as e:
                    failed += 1
                    logger.warning(f"⚠️ Error fetching data for {player}: {e}")

            logger.info(
                f"Player data fetch complete: {successful} successful, {failed} failed, "
                f"{len(all_player_data)} total records"
            )
            return all_player_data

        # Mode 3: Fetch live data from FPL API
        else:
            api_url = get_config_variable('FPL_TEAMS_API_URL')
            url = api_url
            
            logger.info(f"Fetching live data from FPL API: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # If category specified, return that specific data
            if category:
                result = data.get(str(category), [])
                logger.info(f"Retrieved {len(result) if isinstance(result, list) else 'N/A'} items for category '{category}'")
                return result
            
            # Otherwise return full response
            return data

    except requests.Timeout:
        logger.error(f"❌ Request timeout for URL: {url}")
        raise
    except requests.HTTPError as e:
        logger.error(f"❌ HTTP error for URL: {url} - {e}")
        raise
    except requests.RequestException as e:
        logger.error(f"❌ Request failed for URL: {url} - {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching data: {e}")
        raise


# Convenience functions for common use cases
def fetch_fixtures(season):
    """
    Fetch fixtures for a specific season.
    
    Args:
        season: Season string (e.g., "2024-25")
        
    Returns:
        dict: {"season": season, "csv": <fixture data>}
    """
    endpoint = get_config_variable('FIXTURES_DATASET_PATH', 'fixtures.csv')
    return fetch_data_from_api(season, endpoint=endpoint)


def fetch_teams():
    """
    Fetch current teams from FPL API.
    
    Returns:
        list: List of team dictionaries
    """
    return fetch_data_from_api(season=None, category='teams')


def fetch_players():
    """
    Fetch current players from FPL API.
    
    Returns:
        list: List of player dictionaries
    """
    return fetch_data_from_api(season=None, category='elements')


def fetch_gameweeks():
    """
    Fetch current gameweek information from FPL API.
    
    Returns:
        list: List of gameweek dictionaries
    """
    return fetch_data_from_api(season=None, category='events')


# Configuration verification
def verify_config():
    """
    Verify all required configuration variables are set.
    Useful for debugging configuration issues.
    
    Returns:
        dict: Configuration status
    """
    required_vars = {
        'FIXTURES_BASE_URL': 'https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/',
        'FPL_TEAMS_API_URL': 'https://fantasy.premierleague.com/api/bootstrap-static/',
        'FIXTURES_DATASET_PATH': 'fixtures.csv',
        'PLAYERS_DATASET_PATH': 'players_raw.csv',
        'PLAYER_GW_DATASET_PATH': 'gw.csv',
        'FPL_TEAMS_DATAPATH': 'teams.csv',
    }
    
    status = {}
    all_ok = True
    
    for var, default in required_vars.items():
        try:
            value = get_config_variable(var, default)
            status[var] = {'status': 'OK', 'value': value, 'source': 'set'}
        except KeyError:
            status[var] = {'status': 'MISSING', 'value': None, 'source': None}
            all_ok = False
    
    return {'all_ok': all_ok, 'variables': status}


if __name__ == '__main__':
    # Test configuration when run directly
    import sys
    from pprint import pprint

    from dotenv import load_dotenv  
    
    load_dotenv() 
    
    print("🔍 Verifying API configuration...")
    config_status = verify_config()
    
    print("\n📋 Configuration Status:")
    pprint(config_status)
    
    if not config_status['all_ok']:
        print("\n⚠️ Some configuration variables are missing!")
        print("Please set them in your .env file or as environment variables.")
        sys.exit(1)
    
    print("\n✅ All configuration variables are set!")
    
    # Test API connection
    print("\n🌐 Testing API connection...")
    try:
        teams = fetch_teams()
        print(f"✅ Successfully fetched {len(teams)} teams from FPL API")
        
        players = fetch_players()
        print(f"✅ Successfully fetched {len(players)} players from FPL API")
        
        print("\n✅ API connection test successful!")
    except Exception as e:
        print(f"\n❌ API connection test failed: {e}")
        sys.exit(1)