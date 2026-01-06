"""
Load data from FPL API into database
Integrates FPL API with existing database loaders
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load .env from project root
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

import logging
from datetime import datetime

from fpl_api_client import FPLAPIClient
from fpl_api_parsers import (
    parse_fpl_fixtures,
    create_player_name_map,
    parse_fpl_player_gameweek_stats_with_names
)
from load_teams_live import fetch_teams_data, load_teams
from load_players import fetch_players_data, load_players
from load_fixtures import load_fixtures
from load_player_gameweek_stats import load_player_gameweek_stats
from utils.infer_season import infer_season

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FPLAPILoader:
    """Loads data from FPL API into database"""
    
    def __init__(self):
        self.client = FPLAPIClient()
        self.bootstrap_data = None
        self.current_season = infer_season()
    
    def _ensure_bootstrap(self):
        """Fetch bootstrap data if not already cached"""
        if not self.bootstrap_data:
            logger.info("Fetching bootstrap-static data...")
            self.bootstrap_data = self.client.get_bootstrap_static()
            
            if not self.bootstrap_data:
                raise Exception("Failed to fetch bootstrap data from FPL API")
        
        return self.bootstrap_data
    
    def load_teams(self):
        """Load teams from FPL API using existing loader"""
        print("\n" + "=" * 70)
        print("📥 LOADING TEAMS FROM FPL API")
        print("=" * 70)
        
        # Use existing fetch function (already returns dicts from FPL API)
        teams_data = fetch_teams_data()
        
        if not teams_data:
            print("❌ No teams data to load")
            return False
        
        print(f"✅ Fetched {len(teams_data)} teams from FPL API")
        
        # Use existing load_teams function (expects dicts + season)
        load_teams(teams_data, self.current_season)
        return True
    
    def load_players(self):
        """Load players from FPL API using existing loader"""
        print("\n" + "=" * 70)
        print("👥 LOADING PLAYERS FROM FPL API")
        print("=" * 70)
        
        # Use existing fetch function (already returns dicts from FPL API)
        players_data = fetch_players_data()
        
        if not players_data:
            print("❌ No players data to load")
            return False
        
        print(f"✅ Fetched {len(players_data)} players from FPL API")
        
        # Use existing load_players function (expects dicts)
        load_players(players_data)
        return True
    
    def load_fixtures(self):
        """Load fixtures from FPL API"""
        print("\n" + "=" * 70)
        print("🏟️  LOADING FIXTURES FROM FPL API")
        print("=" * 70)
        
        fixtures_data = self.client.get_fixtures()
        
        if not fixtures_data:
            print("❌ Failed to fetch fixtures from FPL API")
            return False
        
        fixtures_parsed = parse_fpl_fixtures(fixtures_data)
        
        if not fixtures_parsed:
            print("❌ No fixtures data to load")
            return False
        
        print(f"✅ Parsed {len(fixtures_parsed)} fixtures")
        
        # Wrap in list for load_fixtures (expects list of lists)
        fixtures_wrapped = [fixtures_parsed]
        
        load_fixtures(fixtures_wrapped)
        return True
    
    def load_player_gameweek_stats(self, specific_gameweeks=None):
        """
        Load player gameweek stats from FPL API
        
        Args:
            specific_gameweeks: Optional list of gameweeks to load (e.g., [10, 11, 12])
        """
        print("\n" + "=" * 70)
        print("📊 LOADING PLAYER GAMEWEEK STATS FROM FPL API")
        print("=" * 70)
        
        bootstrap = self._ensure_bootstrap()
        
        # Create player name map
        name_map = create_player_name_map(bootstrap)
        print(f"✅ Created name map for {len(name_map)} players")
        
        # Get all player IDs
        player_ids = [p['id'] for p in bootstrap['elements']]
        print(f"📋 Found {len(player_ids)} players to fetch")
        
        # Estimate time
        estimated_time = len(player_ids) * 0.5 / 60
        print(f"⏱️  Estimated time: ~{estimated_time:.1f} minutes\n")
        
        if input("⚠️  This is slow! Continue? (yes/no): ").lower() != "yes":
            print("Cancelled.")
            return False
        
        # Fetch stats for all players
        player_stats = self.client.get_all_player_gameweek_stats(player_ids)
        
        if not player_stats:
            print("❌ Failed to fetch player stats")
            return False
        
        # Parse with names
        stats_parsed = parse_fpl_player_gameweek_stats_with_names(player_stats, name_map)
        
        if not stats_parsed:
            print("❌ No player stats to load")
            return False
        
        # Filter by specific gameweeks if requested
        if specific_gameweeks:
            original_count = len(stats_parsed)
            stats_parsed = [
                stat for stat in stats_parsed 
                if stat[1] in specific_gameweeks
            ]
            print(f"📌 Filtered to gameweeks {specific_gameweeks}: {len(stats_parsed)}/{original_count} records")
        
        print(f"✅ Parsed {len(stats_parsed)} player gameweek records")
        
        # Load into database
        load_player_gameweek_stats(stats_parsed)
        return True
    
    def load_incremental_gameweeks(self, start_gameweek=None):
        """
        Load only new/recent gameweeks incrementally
        
        Args:
            start_gameweek: Load from this gameweek onwards (default: current GW)
        """
        print("\n" + "=" * 70)
        print("📊 INCREMENTAL GAMEWEEK LOAD")
        print("=" * 70)
        
        # Get current gameweek
        current_gw = self.client.get_current_gameweek()
        
        if not current_gw:
            print("❌ Could not determine current gameweek")
            return False
        
        print(f"📍 Current gameweek: {current_gw}")
        
        if start_gameweek is None:
            # Default: load current gameweek only
            gameweeks_to_load = [current_gw]
        else:
            # Load from start_gameweek to current
            gameweeks_to_load = list(range(start_gameweek, current_gw + 1))
        
        print(f"📌 Loading gameweeks: {gameweeks_to_load}")
        
        return self.load_player_gameweek_stats(specific_gameweeks=gameweeks_to_load)
    
    def load_all(self, include_player_stats=False):
        """Load all data from FPL API"""
        print("=" * 70)
        print("🚀 FPL API FULL DATA LOAD")
        print("=" * 70)
        
        start = datetime.now()
        results = {}
        
        # Load teams
        try:
            results['teams'] = self.load_teams()
        except Exception as e:
            logger.error(f"Teams loading failed: {e}")
            results['teams'] = False
        
        # Load players
        try:
            results['players'] = self.load_players()
        except Exception as e:
            logger.error(f"Players loading failed: {e}")
            results['players'] = False
        
        # Load fixtures
        try:
            results['fixtures'] = self.load_fixtures()
        except Exception as e:
            logger.error(f"Fixtures loading failed: {e}")
            results['fixtures'] = False
        
        # Load player stats (optional)
        if include_player_stats:
            try:
                results['player_stats'] = self.load_player_gameweek_stats()
            except Exception as e:
                logger.error(f"Player stats loading failed: {e}")
                results['player_stats'] = False
        else:
            print("\n⏭️  Skipping player gameweek stats (use --include-stats to load)")
            results['player_stats'] = 'Skipped'
        
        # Summary
        duration = (datetime.now() - start).total_seconds()
        
        print("\n" + "=" * 70)
        print("📊 SUMMARY")
        print("=" * 70)
        print(f"Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
        print("\nResults:")
        for key, status in results.items():
            if status == 'Skipped':
                emoji = "⏭️"
            elif status:
                emoji = "✅"
            else:
                emoji = "❌"
            print(f"  {emoji} {key}: {status}")
        print("=" * 70)


def main():
    """CLI for FPL API loader"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Load data from FPL API')
    parser.add_argument('--teams', action='store_true', help='Load teams only')
    parser.add_argument('--players', action='store_true', help='Load players only')
    parser.add_argument('--fixtures', action='store_true', help='Load fixtures only')
    parser.add_argument('--player-stats', action='store_true', help='Load player gameweek stats')
    parser.add_argument('--gameweeks', type=str, help='Specific gameweeks (e.g., "10,11,12")')
    parser.add_argument('--incremental', action='store_true', help='Load current gameweek only')
    parser.add_argument('--from-gw', type=int, help='Load from this gameweek to current')
    parser.add_argument('--all', action='store_true', help='Load all data')
    parser.add_argument('--include-stats', action='store_true', help='Include player stats in --all')
    
    args = parser.parse_args()
    
    loader = FPLAPILoader()
    
    if args.teams:
        loader.load_teams()
    
    elif args.players:
        loader.load_players()
    
    elif args.fixtures:
        loader.load_fixtures()
    
    elif args.player_stats:
        gameweeks = None
        if args.gameweeks:
            gameweeks = [int(gw.strip()) for gw in args.gameweeks.split(',')]
        loader.load_player_gameweek_stats(specific_gameweeks=gameweeks)
    
    elif args.incremental:
        loader.load_incremental_gameweeks()
    
    elif args.from_gw:
        loader.load_incremental_gameweeks(start_gameweek=args.from_gw)
    
    elif args.all:
        loader.load_all(include_player_stats=args.include_stats)
    
    else:
        print("=" * 70)
        print("🔄 FPL API DATA LOADER")
        print("=" * 70)
        print("\nUsage:")
        print("  python scripts/load_fpl_api_data.py --teams")
        print("  python scripts/load_fpl_api_data.py --players")
        print("  python scripts/load_fpl_api_data.py --fixtures")
        print("  python scripts/load_fpl_api_data.py --incremental")
        print("  python scripts/load_fpl_api_data.py --from-gw 10")
        print("  python scripts/load_fpl_api_data.py --player-stats --gameweeks '10,11,12'")
        print("  python scripts/load_fpl_api_data.py --all")
        print("  python scripts/load_fpl_api_data.py --all --include-stats")


if __name__ == '__main__':
    main()