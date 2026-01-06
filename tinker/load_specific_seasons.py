"""
Load Specific Seasons - Player Gameweek Stats
Uses the SAME logic as current season (2025-26)
Usage: python load_specific_seasons.py 2024-25 2023-24 2022-23
"""
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import csv
from io import StringIO

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.load_player_gameweek_stats import (
    parse_gw_stats_table,
    load_player_gameweek_stats
)
from scripts.utils.fetch_api_data import fetch_data_from_api

# Available seasons
AVAILABLE_SEASONS = ["2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]


def fetch_historical_players(season):
    """Fetch player list from Vaastav's repository for a specific season"""
    try:
        # Fetch players_raw.csv from Vaastav for this season
        data = fetch_data_from_api(
            season=season,
            endpoint="players_raw.csv"
        )
        
        if not data:
            print(f"  ⚠️ No data returned from players_raw.csv")
            return None
        
        # data is a list of dicts with 'csv' key
        csv_data = data[0].get('csv', '') if len(data) > 0 else ''
        
        if not csv_data:
            print(f"  ⚠️ Empty CSV data")
            return None
        
        # Parse CSV
        reader = csv.DictReader(StringIO(csv_data))
        players = list(reader)
        
        return players
    except Exception as e:
        print(f"  ❌ Error fetching historical players: {e}")
        import traceback
        traceback.print_exc()
        return None


def get_players_folder_from_historical(players):
    """Generate player folders from historical player data (same format as current)"""
    players_lists = []
    for player in players:
        # Same format as current season
        first_name = player.get('first_name', '').strip()
        second_name = player.get('second_name', '').strip()
        player_id = player.get('id', '').strip()
        
        if first_name and second_name and player_id:
            player_folder = f"{first_name}_{second_name}_{player_id}"
            players_lists.append(player_folder)
    
    return players_lists


def load_player_stats_for_season(season):
    """Load player gameweek stats for a season - SAME LOGIC AS 2025-26"""
    print(f"\n{'='*70}")
    print(f"📅 SEASON: {season}")
    print(f"{'='*70}")
    
    start = datetime.now()
    
    # Step 1: Get players list for this season
    print(f"\n  📥 Fetching player list for {season}...")
    players = fetch_historical_players(season)
    
    if not players:
        print(f"  ❌ Failed to fetch players for {season}")
        return False
    
    print(f"  ✅ Got {len(players)} players from {season}")
    
    # Step 2: Generate player folders
    print("\n  📂 Generating player folders...")
    player_folders = get_players_folder_from_historical(players)
    print(f"  ✅ {len(player_folders)} player folders")
    
    # Step 3: Fetch gameweek data (same as 2025-26)
    print(f"\n  📥 Fetching gameweek data for {len(player_folders)} players...")
    print(f"  ⏱️  Estimated time: {len(player_folders) * 0.5 / 60:.0f} minutes")
    print(f"  💡 Progress updates every 100 players\n")
    
    all_data = []
    success = 0
    failed = 0
    
    for i, folder in enumerate(player_folders, 1):
        try:
            data = fetch_data_from_api(
                season=season,
                endpoint="gw.csv",
                player_folder=[folder]
            )
            if data:
                all_data.extend(data)
                success += 1
        except:
            failed += 1
        
        if i % 100 == 0 or i == len(player_folders):
            elapsed = (datetime.now() - start).total_seconds()
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(player_folders) - i) / rate if rate > 0 else 0
            print(f"     {i}/{len(player_folders)} - Success: {success}, Failed: {failed} - ETA: {remaining/60:.1f}min")
    
    if not all_data:
        print("\n  ❌ No data fetched")
        return False
    
    # Step 4: Parse (same parser as 2025-26)
    print(f"\n  🔄 Parsing {len(all_data)} records...")
    parsed = parse_gw_stats_table(all_data)
    
    if not parsed:
        print("  ❌ Parsing failed")
        return False
    
    # Step 5: Load to database (same loader as 2025-26)
    print(f"\n  💾 Loading to database...")
    load_player_gameweek_stats(parsed)
    
    duration = (datetime.now() - start).total_seconds()
    print(f"\n  ⏱️  Duration: {duration/60:.1f} minutes")
    print(f"  ✅ Season {season} complete!")
    
    return True


def main():
    print("="*70)
    print("🚀 LOAD SPECIFIC SEASONS")
    print("="*70)
    
    # Get seasons from command line args
    if len(sys.argv) < 2:
        print("\n❌ No seasons specified!")
        print(f"\nUsage: python {sys.argv[0]} <season1> [season2] [season3] ...")
        print(f"\nAvailable seasons: {', '.join(AVAILABLE_SEASONS)}")
        print(f"\nExamples:")
        print(f"  python {sys.argv[0]} 2024-25")
        print(f"  python {sys.argv[0]} 2024-25 2023-24")
        print(f"  python {sys.argv[0]} 2019-20 2020-21 2021-22 2022-23 2023-24 2024-25")
        sys.exit(1)
    
    # Parse seasons from args
    seasons_to_load = sys.argv[1:]
    
    # Validate seasons
    invalid_seasons = [s for s in seasons_to_load if s not in AVAILABLE_SEASONS]
    if invalid_seasons:
        print(f"\n❌ Invalid seasons: {', '.join(invalid_seasons)}")
        print(f"Available seasons: {', '.join(AVAILABLE_SEASONS)}")
        sys.exit(1)
    
    print(f"\n📦 Seasons to load: {', '.join(seasons_to_load)}")
    print(f"⏱️  Estimated time: ~{len(seasons_to_load)*30} minutes ({len(seasons_to_load)*30/60:.1f} hours)")
    
    if input("\nProceed? (yes/no): ").lower() != "yes":
        print("Cancelled.")
        sys.exit(0)
    
    print("\n" + "="*70)
    print("🚀 STARTING...")
    print("="*70)
    
    total_start = datetime.now()
    results = {}
    
    for season in seasons_to_load:
        results[season] = load_player_stats_for_season(season)
    
    # Summary
    total = (datetime.now() - total_start).total_seconds()
    print("\n" + "="*70)
    print("📊 SUMMARY")
    print("="*70)
    print(f"⏱️  Total: {total/60:.1f} minutes ({total/3600:.1f} hours)")
    for s, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {s}")
    
    ok_count = sum(results.values())
    print(f"\n✅ Success: {ok_count}/{len(seasons_to_load)}")
    print("="*70)
    
    if ok_count > 0:
        print("\n💡 Next step: Run 'python migrate_add_team_names.py' to populate team names")


if __name__ == '__main__':
    main()