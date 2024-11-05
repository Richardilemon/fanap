import requests

# API endpoint for regions data
url = "https://fantasy.premierleague.com/api/regions/"

try:
    # Send a GET request to the endpoint
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    # Parse the JSON response
    data = response.json()

    # Print the full JSON data (formatted for readability)
    print("Regions Data:", data)

    # Example: Print each region name and its details
    for region in data:
        print(f"Region Name: {region.get('name')}")
        print(f"Country Code: {region.get('country_code')}")
        print(f"Region ID: {region.get('id')}")
        print()  # Blank line for readability

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
