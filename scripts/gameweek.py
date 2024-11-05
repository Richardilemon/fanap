import requests

# Set the gameweek you want to fetch data for
gameweek = 1  # Change this to the desired gameweek number

# API endpoint with the gameweek inserted
url = f"https://fantasy.premierleague.com/api/event/{gameweek}/live/"

try:
    # Send a GET request to the endpoint
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    # Parse the JSON response
    data = response.json()

    # Print the full JSON data (formatted for readability)
    print("Live Data for Gameweek", gameweek)
    
    # Sample: Print player stats for the gameweek
    elements = data.get("elements", [])
    for player in elements:
        player_id = player.get("id")
        stats = player.get("stats", {})
        print(f"Player ID: {player_id}")
        print(f"Total Points: {stats.get('total_points')}")
        print(f"Goals Scored: {stats.get('goals_scored')}")
        print(f"Assists: {stats.get('assists')}")
        print(f"Yellow Cards: {stats.get('yellow_cards')}")
        print()  # Blank line for readability

except requests.exceptions.RequestException as e:
    print(f"Error fetching data: {e}")
