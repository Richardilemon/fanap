"""
Standalone ETL Script for GitHub Actions / Non-Airflow Environments

This script runs the complete FPL ETL pipeline without requiring Airflow.
Perfect for GitHub Actions, cron jobs, or manual execution.

Usage:
    python run_etl_standalone.py [--season SEASON] [--skip-players] [--skip-teams]
    
Examples:
    # Run full ETL for current season
    python run_etl_standalone.py
    
    # Run for specific season
    python run_etl_standalone.py --season 2024-25
    
    # Skip player/team updates (fixtures only)
    python run_etl_standalone.py --skip-players --skip-teams
"""

import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import ETL functions
from scripts.load_fixtures import parse_fixtures, load_fixtures
from scripts.load_gameweeks import parse_gameweeks, load_gameweeks
from scripts.load_players import fetch_players_data, load_players
from scripts.load_teams_live import fetch_teams_data, load_teams
from scripts.utils.infer_season import infer_season, SEASONS
from scripts.utils.fetch_api_data import fetch_data_from_api
from scripts.utils.db_config import get_db_connection


def check_database_connection():
    """Verify database is accessible before starting ETL"""
    print("🔍 Checking database connection...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        print("✅ Database connection successful\n")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}\n")
        return False


def load_fixtures_pipeline(season):
    """Load fixtures for a specific season"""
    print(f"📥 Loading fixtures for {season}...")
    try:
        fixtures_data = fetch_data_from_api(season)
        if not fixtures_data:
            print(f"⚠️ No fixtures data returned for {season}")
            return False
        
        parsed_fixtures = parse_fixtures(fixtures_data)
        if not parsed_fixtures:
            print(f"⚠️ No fixtures parsed for {season}")
            return False
        
        load_fixtures([parsed_fixtures])
        print(f"✅ Fixtures loaded for {season}\n")
        return True
        
    except Exception as e:
        print(f"❌ Error loading fixtures for {season}: {e}\n")
        return False


def load_gameweeks_pipeline(season):
    """Load gameweeks for a specific season"""
    print(f"📥 Loading gameweeks for {season}...")
    try:
        fixtures_data = fetch_data_from_api(season)
        if not fixtures_data:
            print(f"⚠️ No data for gameweeks in {season}")
            return False
        
        gameweeks_data = parse_gameweeks(fixtures_data)
        if not gameweeks_data:
            print(f"⚠️ No gameweeks parsed for {season}")
            return False
        
        load_gameweeks([gameweeks_data])
        print(f"✅ Gameweeks loaded for {season}\n")
        return True
        
    except Exception as e:
        print(f"❌ Error loading gameweeks for {season}: {e}\n")
        return False


def load_teams_pipeline():
    """Load current teams data"""
    print("📥 Loading teams...")
    try:
        current_season = infer_season()
        teams = fetch_teams_data()
        
        if not teams:
            print("⚠️ No teams data returned")
            return False
        
        load_teams(teams, current_season)
        print("✅ Teams loaded\n")
        return True
        
    except Exception as e:
        print(f"❌ Error loading teams: {e}\n")
        return False


def load_players_pipeline():
    """Load current players data"""
    print("📥 Loading players...")
    try:
        players = fetch_players_data()
        
        if not players:
            print("⚠️ No players data returned")
            return False
        
        load_players(players)
        print("✅ Players loaded\n")
        return True
        
    except Exception as e:
        print(f"❌ Error loading players: {e}\n")
        return False


def run_full_etl(season=None, skip_players=False, skip_teams=False):
    """
    Run complete ETL pipeline
    
    Args:
        season: Specific season to process (default: current season)
        skip_players: Skip player data loading
        skip_teams: Skip team data loading
        
    Returns:
        bool: True if all steps succeeded, False otherwise
    """
    start_time = datetime.now()
    print("=" * 70)
    print("🚀 FPL ETL PIPELINE STARTED")
    print("=" * 70)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check database connection first
    if not check_database_connection():
        print("❌ Pipeline aborted: Database connection failed")
        return False
    
    # Determine season(s) to process
    if season:
        seasons_to_process = [season]
        print(f"📅 Processing season: {season}\n")
    else:
        current_season = infer_season()
        seasons_to_process = [current_season]
        print(f"📅 Processing current season: {current_season}\n")
    
    # Track success/failure
    results = {
        'fixtures': [],
        'gameweeks': [],
        'teams': None,
        'players': None
    }
    
    # Process each season
    for season in seasons_to_process:
        print(f"\n{'='*70}")
        print(f"PROCESSING SEASON: {season}")
        print(f"{'='*70}\n")
        
        # 1. Load fixtures
        results['fixtures'].append(load_fixtures_pipeline(season))
        
        # 2. Load gameweeks
        results['gameweeks'].append(load_gameweeks_pipeline(season))
    
    # 3. Load teams (current season only)
    if not skip_teams:
        print(f"\n{'='*70}")
        print("LOADING CURRENT TEAMS")
        print(f"{'='*70}\n")
        results['teams'] = load_teams_pipeline()
    else:
        print("\n⏭️ Skipping teams (--skip-teams flag set)\n")
    
    # 4. Load players (current season only)
    if not skip_players:
        print(f"\n{'='*70}")
        print("LOADING CURRENT PLAYERS")
        print(f"{'='*70}\n")
        results['players'] = load_players_pipeline()
    else:
        print("\n⏭️ Skipping players (--skip-players flag set)\n")
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 70)
    print("📊 PIPELINE SUMMARY")
    print("=" * 70)
    print(f"Duration: {duration:.2f} seconds")
    print(f"\nResults:")
    print(f"  Fixtures:  {sum(results['fixtures'])}/{len(results['fixtures'])} seasons")
    print(f"  Gameweeks: {sum(results['gameweeks'])}/{len(results['gameweeks'])} seasons")
    if not skip_teams:
        print(f"  Teams:     {'✅' if results['teams'] else '❌'}")
    if not skip_players:
        print(f"  Players:   {'✅' if results['players'] else '❌'}")
    
    # Overall success
    all_succeeded = (
        all(results['fixtures']) and
        all(results['gameweeks']) and
        (results['teams'] if not skip_teams else True) and
        (results['players'] if not skip_players else True)
    )
    
    if all_succeeded:
        print("\n✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 70 + "\n")
        return True
    else:
        print("\n⚠️ PIPELINE COMPLETED WITH ERRORS")
        print("=" * 70 + "\n")
        return False


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Run FPL ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_etl_standalone.py                    # Run full ETL for current season
  python run_etl_standalone.py --season 2024-25   # Run for specific season
  python run_etl_standalone.py --skip-players     # Skip player updates
  python run_etl_standalone.py --skip-teams       # Skip team updates
        """
    )
    
    parser.add_argument(
        '--season',
        type=str,
        help='Specific season to process (e.g., 2024-25)',
        default=None
    )
    
    parser.add_argument(
        '--skip-players',
        action='store_true',
        help='Skip loading player data'
    )
    
    parser.add_argument(
        '--skip-teams',
        action='store_true',
        help='Skip loading team data'
    )
    
    return parser.parse_args()


def main():
    args = parse_arguments()
    success = run_full_etl(
        season=args.season,
        skip_players=args.skip_players,
        skip_teams=args.skip_teams
    )
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()