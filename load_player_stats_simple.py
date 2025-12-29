"""
Simple Player Gameweek Stats Loader
Uses your existing scripts/load_player_gameweek_stats.py functions
"""
import os
import sys
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.load_players import fetch_players_data
from scripts.load_player_gameweek_stats import (
    get_players_folder,
    parse_gw_stats_table,
    load_player_gameweek_stats
)
from scripts.utils.fetch_api_data import fetch_data_from_api

# Available seasons
SEASONS = ["2018-19", "2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]


def load_player_stats_for_season(season):
    """Load player gameweek stats for a season"""
    print(f"\n{'='*70}")
    print(f"📅 SEASON: {season}")
    print(f"{'='*70}")
    
    start = datetime.now()
    
    # Step 1: Get current players list
    print("\n  📥 Fetching player list...")
    players = fetch_players_data()
    if not players:
        print("  ❌ Failed to fetch players")
        return False
    print(f"  ✅ Got {len(players)} players")
    
    # Step 2: Generate player folders
    print("\n  📂 Generating player folders...")
    player_folders = get_players_folder(players)
    print(f"  ✅ {len(player_folders)} player folders")
    
    # Step 3: Fetch gameweek data (this is the slow part)
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
            rate = i / elapsed
            remaining = (len(player_folders) - i) / rate if rate > 0 else 0
            print(f"     {i}/{len(player_folders)} - Success: {success}, Failed: {failed} - ETA: {remaining/60:.1f}min")
    
    if not all_data:
        print("\n  ❌ No data fetched")
        return False
    
    # Step 4: Parse
    print(f"\n  🔄 Parsing {len(all_data)} records...")
    parsed = parse_gw_stats_table(all_data)
    
    if not parsed:
        print("  ❌ Parsing failed")
        return False
    
    # Step 5: Load to database
    print(f"\n  💾 Loading to database...")
    load_player_gameweek_stats(parsed)
    
    duration = (datetime.now() - start).total_seconds()
    print(f"\n  ⏱️  Duration: {duration/60:.1f} minutes")
    print(f"  ✅ Season {season} complete!")
    
    return True


def main():
    print("="*70)
    print("🚀 PLAYER GAMEWEEK STATS LOADER")
    print("="*70)
    print("\n⚠️  WARNING: Loading player stats is SLOW!")
    print("Each season takes ~25-35 minutes (fetches data for ~700 players)")
    print("="*70)
    
    print(f"\nAvailable: {', '.join(SEASONS)}")
    print("\n1. Current season only (2025-26) ~30min")
    print("2. Last 2 seasons (2024-25, 2025-26) ~60min")  
    print("3. Last 3 seasons (2023-24, 2024-25, 2025-26) ~90min")
    print("4. Specific season")
    print("5. All seasons (2018-19 to 2025-26) ~4 hours!")
    
    choice = input("\nChoice (1-5): ").strip()
    
    if choice == "1":
        to_load = ["2025-26"]
    elif choice == "2":
        to_load = ["2024-25", "2025-26"]
    elif choice == "3":
        to_load = ["2023-24", "2024-25", "2025-26"]
    elif choice == "4":
        s = input("Season (e.g. 2024-25): ").strip()
        if s not in SEASONS:
            print(f"❌ Invalid. Choose from: {', '.join(SEASONS)}")
            sys.exit(1)
        to_load = [s]
    elif choice == "5":
        if input("⚠️  This takes ~4 hours. Sure? (yes/no): ").lower() != "yes":
            sys.exit(0)
        to_load = SEASONS
    else:
        print("❌ Invalid choice")
        sys.exit(1)
    
    print(f"\n📦 Loading: {', '.join(to_load)}")
    print(f"⏱️  Est. time: ~{len(to_load)*30} minutes")
    
    if input("\nProceed? (yes/no): ").lower() != "yes":
        sys.exit(0)
    
    print("\n" + "="*70)
    print("🚀 STARTING...")
    print("="*70)
    
    total_start = datetime.now()
    results = {}
    
    for season in to_load:
        results[season] = load_player_stats_for_season(season)
    
    # Summary
    total = (datetime.now() - total_start).total_seconds()
    print("\n" + "="*70)
    print("📊 SUMMARY")
    print("="*70)
    print(f"⏱️  Total: {total/60:.1f} minutes")
    for s, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {s}")
    
    ok_count = sum(results.values())
    print(f"\n✅ Success: {ok_count}/{len(to_load)}")
    print("="*70)
    print("\n💡 Run: python check_all_data.py")


if __name__ == '__main__':
    main()