"""
Reload fixtures for current season
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.load_fixtures import parse_fixtures, load_fixtures
from scripts.utils.fetch_api_data import fetch_data_from_api
from scripts.utils.infer_season import infer_season

print("=" * 70)
print("🔄 RELOADING FIXTURES")
print("=" * 70)

season = infer_season()
print(f"\n📅 Season: {season}\n")

print("1️⃣ Fetching fixtures from API...")
try:
    fixtures_data = fetch_data_from_api(
        season=season,
        endpoint="fixtures.csv"
    )
    
    if not fixtures_data:
        print("❌ No fixtures data returned")
        sys.exit(1)
    
    print(f"✅ Fetched fixtures data")
    
except Exception as e:
    print(f"❌ Failed to fetch: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n2️⃣ Parsing fixtures...")
try:
    # fixtures_data should be a list with one dict containing 'csv' key
    if isinstance(fixtures_data, list) and len(fixtures_data) > 0:
        parsed_fixtures = parse_fixtures(fixtures_data[0])
    else:
        parsed_fixtures = parse_fixtures(fixtures_data)
    
    if not parsed_fixtures:
        print("❌ No fixtures parsed")
        sys.exit(1)
    
    print(f"✅ Parsed {len(parsed_fixtures)} fixtures")
    
except Exception as e:
    print(f"❌ Failed to parse: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n3️⃣ Loading to database...")
try:
    # Wrap in list as load_fixtures expects list of lists
    load_fixtures([parsed_fixtures])
    print("✅ Loaded successfully")
    
except Exception as e:
    print(f"❌ Failed to load: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 70)
print("✅ FIXTURES RELOADED!")
print("=" * 70)

# Verify
from scripts.utils.db_config import get_db_connection
conn = get_db_connection()
cursor = conn.cursor()

cursor.execute("""
    SELECT COUNT(*), COUNT(id), MIN(id), MAX(id)
    FROM fixtures
    WHERE season = %s;
""", (season,))

count, id_count, min_id, max_id = cursor.fetchone()

print(f"\n📊 Verification:")
print(f"   Total fixtures: {count}")
print(f"   With id: {id_count}")
print(f"   ID range: {min_id} to {max_id}")

if id_count < count:
    print(f"\n⚠️  Warning: {count - id_count} fixtures missing id!")
else:
    print(f"\n✅ All fixtures have id column populated!")

cursor.close()
conn.close()