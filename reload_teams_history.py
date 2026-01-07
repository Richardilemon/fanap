"""
Reload teams_history for all historical seasons
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.load_teams_history import parse_team_history, load_teams_history_records, SEASONS
from scripts.utils.fetch_api_data import fetch_data_from_api

print("=" * 70)
print("🔄 RELOADING TEAMS HISTORY")
print("=" * 70)
print(f"\nSeasons to load: {', '.join(SEASONS)}\n")

if input("Proceed? (yes/no): ").lower() != "yes":
    print("Cancelled.")
    sys.exit(0)

all_teams_data = []
successful_seasons = []
failed_seasons = []

for season in SEASONS:
    print(f"\n📥 Fetching teams for {season}...")
    
    try:
        teams_data = fetch_data_from_api(
            season=season,
            endpoint="teams.csv"
        )
        
        if teams_data and len(teams_data) > 0:
            # teams_data is a list of dicts, each with 'csv' key
            # We need to add 'season' key to each dict
            for item in teams_data:
                if isinstance(item, dict) and 'csv' in item:
                    item['season'] = season
            
            all_teams_data.extend(teams_data)
            successful_seasons.append(season)
            print(f"✅ Fetched teams for {season}")
        else:
            print(f"⚠️ No data returned for {season}")
            failed_seasons.append(season)
    
    except Exception as e:
        print(f"❌ Failed to fetch {season}: {e}")
        failed_seasons.append(season)

if not all_teams_data:
    print("\n❌ No teams data to load")
    print(f"Failed seasons: {', '.join(failed_seasons)}")
    sys.exit(1)

print(f"\n✅ Successfully fetched {len(successful_seasons)} seasons: {', '.join(successful_seasons)}")
if failed_seasons:
    print(f"⚠️  Failed seasons: {', '.join(failed_seasons)}")

print(f"\n🔄 Parsing {len(all_teams_data)} CSV files...")
parsed_teams = parse_team_history(all_teams_data)

if not parsed_teams:
    print("❌ Parsing failed")
    sys.exit(1)

print(f"✅ Parsed {len(parsed_teams)} team records")

print("\n💾 Loading to database...")
load_teams_history_records(parsed_teams)

print("\n" + "=" * 70)
print("✅ TEAMS HISTORY RELOADED!")
print("=" * 70)
print(f"Loaded {len(successful_seasons)} seasons: {', '.join(successful_seasons)}")
if failed_seasons:
    print(f"⚠️  Skipped {len(failed_seasons)} seasons (not available): {', '.join(failed_seasons)}")