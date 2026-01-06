"""
Test Weekly Incremental Update Pipeline
Simulates the weekly Monday update process
"""
import sys
import os
from pathlib import Path

# Add project root and scripts to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'scripts'))

from dotenv import load_dotenv
load_dotenv()

from datetime import datetime
from scripts.load_teams_live import fetch_teams_data, load_teams
from scripts.load_players import fetch_players_data, load_players
from scripts.load_fixtures import parse_fixtures, load_fixtures
from scripts.utils.infer_season import infer_season
from scripts.utils.fetch_api_data import fetch_data_from_api
import subprocess

# Import FPL API client and loader
from scripts.fpl_api_client import FPLAPIClient
from scripts.fpl_api_parsers import create_player_name_map, parse_fpl_player_gameweek_stats_with_names
from scripts.load_player_gameweek_stats import load_player_gameweek_stats

print("=" * 70)
print("🔄 WEEKLY INCREMENTAL UPDATE TEST")
print("=" * 70)
print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Season: {infer_season()}")
print("=" * 70)

# Get current gameweek
client = FPLAPIClient()
current_gw = client.get_current_gameweek()

if not current_gw:
    print("❌ Could not determine current gameweek")
    exit(1)

print(f"\n📍 Current Gameweek: {current_gw}")
print(f"\nThis will update:")
print(f"  1. Teams (current status)")
print(f"  2. Players (current status)")
print(f"  3. Fixtures (all fixtures)")
print(f"  4. Player stats for GW{current_gw}")


start_time = datetime.now()
results = {}

# 1. Update Teams
print("\n" + "=" * 70)
print("1️⃣ UPDATING TEAMS")
print("=" * 70)
try:
    teams_data = fetch_teams_data()
    if teams_data:
        load_teams(teams_data, infer_season())
        results['teams'] = True
    else:
        results['teams'] = False
except Exception as e:
    print(f"❌ Teams update failed: {e}")
    results['teams'] = False

# 2. Update Players
print("\n" + "=" * 70)
print("2️⃣ UPDATING PLAYERS")
print("=" * 70)
try:
    players_data = fetch_players_data()
    if players_data:
        load_players(players_data)
        results['players'] = True
    else:
        results['players'] = False
except Exception as e:
    print(f"❌ Players update failed: {e}")
    results['players'] = False

# 3. Update Fixtures
print("\n" + "=" * 70)
print("3️⃣ UPDATING FIXTURES")
print("=" * 70)
try:
    fixtures_data = client.get_fixtures()
    if fixtures_data:
        from scripts.fpl_api_parsers import parse_fpl_fixtures
        fixtures_parsed = parse_fpl_fixtures(fixtures_data)
        if fixtures_parsed:
            from scripts.load_fixtures import load_fixtures
            load_fixtures([fixtures_parsed])
            results['fixtures'] = True
        else:
            results['fixtures'] = False
    else:
        results['fixtures'] = False
except Exception as e:
    print(f"❌ Fixtures update failed: {e}")
    results['fixtures'] = False

# 4. Update Player Stats for Current Gameweek
print("\n" + "=" * 70)
print(f"4️⃣ UPDATING PLAYER STATS FOR GW{current_gw}")
print("=" * 70)
try:
    # Get bootstrap data for player names
    bootstrap = client.get_bootstrap_static()
    if not bootstrap:
        print("❌ Failed to fetch bootstrap data")
        results['player_stats'] = False
    else:
        # Create player name map
        name_map = create_player_name_map(bootstrap)
        print(f"✅ Created name map for {len(name_map)} players")
        
        # Get player IDs
        player_ids = [p['id'] for p in bootstrap['elements']]
        print(f"📋 Fetching stats for {len(player_ids)} players (GW{current_gw} only)...")
        print(f"⏱️  Estimated time: ~{len(player_ids) * 0.5 / 60:.1f} minutes\n")
        
        # Fetch stats
        player_stats = client.get_all_player_gameweek_stats(player_ids)
        
        if player_stats:
            # Parse
            stats_parsed = parse_fpl_player_gameweek_stats_with_names(player_stats, name_map)
            
            # Filter to current gameweek only
            current_gw_stats = [stat for stat in stats_parsed if stat[1] == current_gw]
            print(f"✅ Parsed {len(current_gw_stats)} records for GW{current_gw}")
            
            # Load
            if current_gw_stats:
                load_player_gameweek_stats(current_gw_stats)
                results['player_stats'] = True
            else:
                print("⚠️ No stats for current gameweek")
                results['player_stats'] = False
        else:
            results['player_stats'] = False
            
except Exception as e:
    print(f"❌ Player stats update failed: {e}")
    import traceback
    traceback.print_exc()
    results['player_stats'] = False

# 5. Update Team Names
print("\n" + "=" * 70)
print("5️⃣ UPDATING TEAM NAMES")
print("=" * 70)
try:
    result = subprocess.run(
        ["python", "migrate_add_team_names.py"],
        capture_output=True,
        text=True,
        timeout=300
    )
    results['team_names'] = (result.returncode == 0)
    if result.returncode == 0:
        print("✅ Team names updated")
    else:
        print(f"❌ Team names update failed")
        if result.stderr:
            print(result.stderr[:500])
except Exception as e:
    print(f"❌ Team names update failed: {e}")
    results['team_names'] = False

# Summary
duration = (datetime.now() - start_time).total_seconds()

print("\n" + "=" * 70)
print("📊 WEEKLY UPDATE SUMMARY")
print("=" * 70)
print(f"Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
print(f"\nResults:")
for component, status in results.items():
    emoji = "✅" if status else "❌"
    print(f"  {emoji} {component.replace('_', ' ').title()}: {status}")

all_success = all(results.values())
print(f"\n{'✅ ALL UPDATES SUCCESSFUL' if all_success else '⚠️ SOME UPDATES FAILED'}")
print("=" * 70)

# Verify data
if all_success:
    print("\n" + "=" * 70)
    print("🔍 VERIFYING DATA")
    print("=" * 70)
    
    from scripts.utils.db_config import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check current gameweek data
    cursor.execute(f"""
        SELECT COUNT(*), 
               COUNT(DISTINCT player_name),
               AVG(minutes)
        FROM player_gameweek_stats
        WHERE season = '{infer_season()}'
          AND gameweek = {current_gw};
    """)
    
    gw_stats = cursor.fetchone()
    print(f"\nGameweek {current_gw} Data:")
    print(f"  Total records: {gw_stats[0]:,}")
    print(f"  Unique players: {gw_stats[1]:,}")
    print(f"  Avg minutes: {gw_stats[2]:.1f}")
    
    cursor.close()
    conn.close()
    
    print("\n✅ Weekly update test complete!")
else:
    print("\n⚠️ Review failed components above")