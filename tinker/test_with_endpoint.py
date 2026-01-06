from dotenv import load_dotenv
load_dotenv()

from scripts.utils.fetch_api_data import fetch_data_from_api

season = "2025-26"

print(f"Fetching fixtures for {season} WITH endpoint...")
result = fetch_data_from_api(
    season=season,
    endpoint="fixtures.csv"
)

print(f"\nResult type: {type(result)}")
if isinstance(result, dict) and 'csv' in result:
    print(f"✅ CSV found! Length: {len(result['csv'])} characters")
    print(f"First 200 chars:\n{result['csv'][:200]}")
else:
    print(f"❌ Still no CSV. Keys: {result.keys() if isinstance(result, dict) else 'N/A'}")
