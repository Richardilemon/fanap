"""
Load Historical Seasons - Optimized Version
Fetches the correct player list for each season from Vaastav
Uses parallel HTTP requests for faster data fetching
"""
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import csv
from io import StringIO
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.load_player_gameweek_stats import (
    parse_gw_stats_table,
    load_player_gameweek_stats
)

# Available seasons
AVAILABLE_SEASONS = ["2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25"]


def fetch_players_for_season(season):
    """Fetch the correct player list for a historical season from Vaastav"""
    url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/players_raw.csv"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse CSV
        reader = csv.DictReader(StringIO(response.text))
        players = list(reader)
        
        return players
    except Exception as e:
        print(f"  ❌ Error fetching players for {season}: {e}")
        return None


def get_players_folder(players):
    """Generate player folder names from player data"""
    player_folders = []
    for player in players:
        first_name = player.get('first_name', '').strip()
        second_name = player.get('second_name', '').strip()
        player_id = player.get('id', '').strip()

        if first_name and second_name and player_id:
            folder = f"{first_name}_{second_name}_{player_id}"
            player_folders.append(folder)

    return player_folders


def fetch_single_player_data(args):
    """Fetch gameweek data for a single player - used for parallel fetching"""
    season, folder, base_url = args
    url = f"{base_url}{season}/players/{folder}/gw.csv"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            player_data = list(csv.DictReader(StringIO(response.text)))
            player_name = " ".join(folder.split("_")[:-1])
            for row in player_data:
                row["player_name"] = player_name
            return {"success": True, "data": player_data}
        return {"success": False, "data": []}
    except Exception:
        return {"success": False, "data": []}


def load_player_stats_for_season(season):
    """Load player gameweek stats for a season"""
    print(f"\n{'='*70}")
    print(f"📅 SEASON: {season}")
    print(f"{'='*70}")
    
    start = datetime.now()
    
    # Step 1: Get players list for THIS SPECIFIC SEASON
    print(f"\n  📥 Fetching player list for {season} from Vaastav...")
    players = fetch_players_for_season(season)
    
    if not players:
        print(f"  ❌ Failed to fetch players for {season}")
        return False
    
    print(f"  ✅ Got {len(players)} players who played in {season}")
    
    # Step 2: Generate player folders
    print("\n  📂 Generating player folders...")
    player_folders = get_players_folder(players)
    print(f"  ✅ {len(player_folders)} player folders")
    
    # Step 3: Fetch gameweek data (PARALLEL)
    print(f"\n  📥 Fetching gameweek data for {len(player_folders)} players...")
    print(f"  🚀 Using parallel requests (50 workers)")

    base_url = os.getenv('FIXTURES_BASE_URL', 'https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/')

    all_data = []
    success = 0
    failed = 0

    # Prepare arguments for parallel fetching
    fetch_args = [(season, folder, base_url) for folder in player_folders]

    # Use ThreadPoolExecutor for parallel HTTP requests
    MAX_WORKERS = 50
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_single_player_data, args): args[1] for args in fetch_args}

        for future in as_completed(futures):
            completed += 1
            result = future.result()
            if result["success"]:
                all_data.extend(result["data"])
                success += 1
            else:
                failed += 1

            if completed % 100 == 0 or completed == len(player_folders):
                elapsed = (datetime.now() - start).total_seconds()
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (len(player_folders) - completed) / rate if rate > 0 else 0
                print(f"     {completed}/{len(player_folders)} - Success: {success}, Failed: {failed} - ETA: {remaining/60:.1f}min")
    
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
    if len(sys.argv) < 2:
        print("="*70)
        print("🚀 LOAD HISTORICAL SEASONS")
        print("="*70)
        print(f"\nUsage: python {sys.argv[0]} <season1> [season2] ...")
        print(f"\nAvailable seasons: {', '.join(AVAILABLE_SEASONS)}")
        print(f"\nExamples:")
        print(f"  python {sys.argv[0]} 2024-25")
        print(f"  python {sys.argv[0]} 2023-24 2024-25")
        print(f"  python {sys.argv[0]} 2019-20 2020-21 2021-22 2022-23 2023-24 2024-25")
        sys.exit(1)
    
    seasons_to_load = sys.argv[1:]
    
    # Validate
    invalid = [s for s in seasons_to_load if s not in AVAILABLE_SEASONS]
    if invalid:
        print(f"❌ Invalid seasons: {', '.join(invalid)}")
        print(f"Available: {', '.join(AVAILABLE_SEASONS)}")
        sys.exit(1)
    
    print("="*70)
    print("🚀 LOAD HISTORICAL SEASONS")
    print("="*70)
    print(f"\n📦 Seasons: {', '.join(seasons_to_load)}")
    print(f"⏱️  Est: ~{len(seasons_to_load)*30} min ({len(seasons_to_load)*0.5:.1f} hours)")
    
    if input("\nProceed? (yes/no): ").lower() != "yes":
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
    print(f"⏱️  Total: {total/60:.1f} min ({total/3600:.1f} hours)")
    for s, ok in results.items():
        print(f"  {'✅' if ok else '❌'} {s}")
    
    ok_count = sum(results.values())
    print(f"\n✅ Success: {ok_count}/{len(seasons_to_load)}")
    
    if ok_count > 0:
        print("\n💡 Next: python migrate_add_team_names.py")


if __name__ == '__main__':
    main()