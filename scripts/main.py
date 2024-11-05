import requests
import json

# FPL API Endpoint
url = "https://fantasy.premierleague.com/api/bootstrap-static/"

try:
    # Send a GET request to the API endpoint
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    # Parse the JSON response
    data = response.json()

    # Example: Extract players data
    players = data.get('elements', [])
    teams = data.get('teams', [])
    gameweeks = data.get('events', [])

    # Print some sample data
    print("Total Players:", len(players))
    print("Sample Player:", players[0])  # Sample player data
    print("Total Teams:", len(teams))
    for team in teams:
        print(team['name'])
    print("Sample Team:", teams[5])  # Sample team data
    print("Total Gameweeks:", len(gameweeks))
    print("Current Gameweek:", gameweeks[0])  # Sample gameweek data

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
