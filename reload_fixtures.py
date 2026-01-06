"""
Reload fixtures for ALL seasons including the id column
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.load_fixtures import parse_fixtures, load_fixtures
from scripts.utils.fetch_api_data import fetch_data_from_api

SEASONS = ["2019-20", "2020-21", "2021-22", "2022-23", "2023-24", "2024-25", "2025-26"]

print("=" * 70)
print("🔄 RELOADING FIXTURES FOR ALL SEASONS")
print("=" * 70)
print(f"\nSeasons: {', '.join(SEASONS)}")

if input("\nProceed? (yes/no): ").lower() != "yes":
    print("Cancelled.")
    sys.exit(0)

results = {}

for season in SEASONS:
    print(f"\n{'='*70}")
    print(f"📥 SEASON: {season}")
    print(f"{'='*70}")
    
    try:
        # Fetch from Vaastav
        print(f"  Fetching fixtures...")
        fixtures_data = fetch_data_from_api(
            season=season,
            endpoint="fixtures.csv"
        )
        
        if not fixtures_data:
            print(f"  ❌ No data for {season}")
            results[season] = False
            continue
        
        # Parse (includes id now)
        print(f"  Parsing...")
        if isinstance(fixtures_data, list) and len(fixtures_data) > 0:
            parsed = parse_fixtures(fixtures_data[0])
        else:
            parsed = parse_fixtures(fixtures_data)
        
        if not parsed:
            print(f"  ❌ Parsing failed for {season}")
            results[season] = False
            continue
        
        print(f"  ✅ Parsed {len(parsed)} fixtures")
        
        # Load to database
        print(f"  Loading to database...")
        load_fixtures([parsed])
        results[season] = True
        print(f"  ✅ {season} complete!")
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        results[season] = False

# Summary
print("\n" + "=" * 70)
print("📊 SUMMARY")
print("=" * 70)

for season, success in results.items():
    emoji = "✅" if success else "❌"
    print(f"  {emoji} {season}")

success_count = sum(results.values())
print(f"\n✅ Success: {success_count}/{len(SEASONS)}")

# Verify
if success_count > 0:
    print("\n" + "=" * 70)
    print("🔍 VERIFICATION")
    print("=" * 70)
    
    from scripts.utils.db_config import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT season,
               COUNT(*) as total,
               COUNT(id) as with_id,
               MIN(id) as min_id,
               MAX(id) as max_id
        FROM fixtures
        GROUP BY season
        ORDER BY season;
    """)
    
    print(f"\n{'Season':<10} {'Total':<8} {'With ID':<8} {'ID Range':<20}")
    print("-" * 55)
    
    for row in cursor.fetchall():
        season, total, with_id, min_id, max_id = row
        id_range = f"{min_id}-{max_id}" if min_id else "NULL"
        print(f"{season:<10} {total:<8} {with_id:<8} {id_range:<20}")
    
    cursor.close()
    conn.close()

print("\n" + "=" * 70)
print("✅ FIXTURES RELOAD COMPLETE!")
print("=" * 70)

if success_count > 0:
    print("\n💡 Next: python migrate_add_team_names.py")