"""
FPL API Client
Fetches data from the official Fantasy Premier League API
"""
import requests
import time
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Base URL for FPL API
FPL_BASE_URL = "https://fantasy.premierleague.com/api"

# Rate limiting - be respectful to the API
REQUEST_DELAY = 0.5  # seconds between requests


class FPLAPIClient:
    """Client for interacting with the FPL API"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'FPL-Data-Pipeline/1.0'
        })
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make a request to the FPL API with error handling and rate limiting
        
        Args:
            endpoint: API endpoint (e.g., 'bootstrap-static')
            params: Optional query parameters
            
        Returns:
            JSON response as dict, or None if request fails
        """
        url = f"{FPL_BASE_URL}/{endpoint}/"
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Rate limiting
            time.sleep(REQUEST_DELAY)
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching {endpoint}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching {endpoint}: {e}")
            return None
        except ValueError as e:
            logger.error(f"JSON decode error for {endpoint}: {e}")
            return None
    
    def get_bootstrap_static(self) -> Optional[Dict]:
        """
        Fetch bootstrap-static data (master data with teams, players, gameweeks)
        
        Returns:
            Dict containing:
            - events: List of gameweeks
            - teams: List of all teams
            - elements: List of all players
            - element_types: Player positions
        """
        logger.info("Fetching bootstrap-static data...")
        data = self._make_request('bootstrap-static')
        
        if data:
            logger.info(f"✅ Fetched {len(data.get('teams', []))} teams, "
                       f"{len(data.get('elements', []))} players, "
                       f"{len(data.get('events', []))} gameweeks")
        
        return data
    
    def get_fixtures(self) -> Optional[List[Dict]]:
        """
        Fetch all fixtures for the current season
        
        Returns:
            List of fixture dictionaries
        """
        logger.info("Fetching fixtures...")
        fixtures = self._make_request('fixtures')
        
        if fixtures:
            logger.info(f"✅ Fetched {len(fixtures)} fixtures")
        
        return fixtures
    
    def get_fixtures_by_gameweek(self, gameweek: int) -> Optional[List[Dict]]:
        """
        Fetch fixtures for a specific gameweek
        
        Args:
            gameweek: Gameweek number (1-38)
            
        Returns:
            List of fixture dictionaries for that gameweek
        """
        logger.info(f"Fetching fixtures for gameweek {gameweek}...")
        fixtures = self._make_request('fixtures', params={'event': gameweek})
        
        if fixtures:
            logger.info(f"✅ Fetched {len(fixtures)} fixtures for GW{gameweek}")
        
        return fixtures
    
    def get_player_summary(self, player_id: int) -> Optional[Dict]:
        """
        Fetch detailed stats for a specific player
        
        Args:
            player_id: FPL player ID
            
        Returns:
            Dict containing:
            - history: This season's gameweek stats
            - history_past: Past seasons summary
            - fixtures: Upcoming fixtures
        """
        data = self._make_request(f'element-summary/{player_id}')
        return data
    
    def get_live_gameweek(self, gameweek: int) -> Optional[Dict]:
        """
        Fetch live stats for a specific gameweek
        
        Args:
            gameweek: Gameweek number (1-38)
            
        Returns:
            Dict with live stats for all players in that gameweek
        """
        logger.info(f"Fetching live data for gameweek {gameweek}...")
        data = self._make_request(f'event/{gameweek}/live')
        
        if data:
            logger.info(f"✅ Fetched live data for GW{gameweek}")
        
        return data
    
    def get_all_player_gameweek_stats(self, player_ids: List[int]) -> List[Dict]:
        """
        Fetch gameweek stats for multiple players
        
        Args:
            player_ids: List of FPL player IDs
            
        Returns:
            List of player data with gameweek history
        """
        logger.info(f"Fetching gameweek stats for {len(player_ids)} players...")
        
        all_stats = []
        failed = 0
        
        for i, player_id in enumerate(player_ids, 1):
            data = self.get_player_summary(player_id)
            
            if data and 'history' in data:
                all_stats.append({
                    'player_id': player_id,
                    'history': data['history']
                })
            else:
                failed += 1
            
            # Progress update every 50 players
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(player_ids)} players fetched")
                print(f"   {i}/{len(player_ids)} - Success: {i-failed}, Failed: {failed}")
        
        logger.info(f"✅ Fetched stats for {len(all_stats)} players ({failed} failed)")
        print(f"✅ Total: {len(all_stats)} successful, {failed} failed")
        
        return all_stats
    
    def get_current_gameweek(self) -> Optional[int]:
        """
        Get the current active gameweek number
        
        Returns:
            Current gameweek number, or None if not found
        """
        data = self.get_bootstrap_static()
        
        if not data or 'events' not in data:
            return None
        
        # Find current gameweek (is_current=True)
        for event in data['events']:
            if event.get('is_current'):
                return event['id']
        
        # If no current gameweek, return the next one
        for event in data['events']:
            if event.get('is_next'):
                return event['id']
        
        return None
    
    def get_finished_gameweeks(self) -> List[int]:
        """
        Get list of finished gameweek numbers
        
        Returns:
            List of gameweek numbers that are finished
        """
        data = self.get_bootstrap_static()
        
        if not data or 'events' not in data:
            return []
        
        finished = [
            event['id'] 
            for event in data['events'] 
            if event.get('finished', False)
        ]
        
        return finished


# Convenience functions for quick access
def fetch_fpl_bootstrap() -> Optional[Dict]:
    """Quick access: Fetch bootstrap-static data"""
    client = FPLAPIClient()
    return client.get_bootstrap_static()


def fetch_fpl_fixtures() -> Optional[List[Dict]]:
    """Quick access: Fetch all fixtures"""
    client = FPLAPIClient()
    return client.get_fixtures()


def fetch_fpl_player_stats(player_ids: List[int]) -> List[Dict]:
    """Quick access: Fetch player gameweek stats"""
    client = FPLAPIClient()
    return client.get_all_player_gameweek_stats(player_ids)


# Test function
if __name__ == '__main__':
    print("=" * 70)
    print("🧪 TESTING FPL API CLIENT")
    print("=" * 70)
    
    client = FPLAPIClient()
    
    # Test 1: Bootstrap
    print("\n1️⃣ Testing bootstrap-static...")
    bootstrap = client.get_bootstrap_static()
    if bootstrap:
        print(f"   ✅ Teams: {len(bootstrap['teams'])}")
        print(f"   ✅ Players: {len(bootstrap['elements'])}")
        print(f"   ✅ Gameweeks: {len(bootstrap['events'])}")
    
    # Test 2: Current gameweek
    print("\n2️⃣ Testing current gameweek...")
    current_gw = client.get_current_gameweek()
    print(f"   ✅ Current gameweek: {current_gw}")
    
    # Test 3: Fixtures
    print("\n3️⃣ Testing fixtures...")
    fixtures = client.get_fixtures()
    if fixtures:
        print(f"   ✅ Total fixtures: {len(fixtures)}")
    
    # Test 4: Single player
    print("\n4️⃣ Testing player summary (Isak - ID 499)...")
    player_data = client.get_player_summary(499)
    if player_data and 'history' in player_data:
        print(f"   ✅ Gameweeks played: {len(player_data['history'])}")
        if player_data['history']:
            gw1 = player_data['history'][0]
            print(f"   ✅ GW1 points: {gw1.get('total_points', 0)}")
    
    print("\n" + "=" * 70)
    print("✅ FPL API CLIENT TEST COMPLETE")
    print("=" * 70)