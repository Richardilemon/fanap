from dotenv import load_dotenv
load_dotenv()

from scripts.utils.fetch_api_data import fetch_data_from_api
import json

season = "2025-26"

print(f"Fetching fixtures for {season}...")
result = fetch_data_from_api(season)

print(f"\nResult type: {type(result)}")
print(f"Result keys: {result.keys() if isinstance(result, dict) else 'Not a dict'}")

if isinstance(result, dict):
    if 'csv' in result:
        print(f"CSV length: {len(result['csv'])} characters")
        print(f"First 200 chars: {result['csv'][:200]}")
    else:
        print("ERROR: 'csv' key missing!")
        print(f"Available keys: {list(result.keys())}")
else:
    print(f"ERROR: Result is not a dict, it's {type(result)}")
    print(f"Result: {result}")
