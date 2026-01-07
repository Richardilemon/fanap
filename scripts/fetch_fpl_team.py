"""
Fetch FPL Team Data
Connect user's FPL team to Fanap for personalized analysis
"""
import requests
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()

FPL_BASE_URL = "https://fantasy.premierleague.com/api"


def fetch_team_info(team_id):
    """Get team basic info"""
    url = f"{FPL_BASE_URL}/entry/{team_id}/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def fetch_team_picks(team_id, gameweek):
    """Get team picks for a specific gameweek"""
    url = f"{FPL_BASE_URL}/entry/{team_id}/event/{gameweek}/picks/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def fetch_team_transfers(team_id):
    """Get all transfers made by team"""
    url = f"{FPL_BASE_URL}/entry/{team_id}/transfers/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def fetch_team_history(team_id):
    """Get team's season history"""
    url = f"{FPL_BASE_URL}/entry/{team_id}/history/"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def save_user_team(team_id, user_email=None):
    """Save FPL team to database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get team info
    team_data = fetch_team_info(team_id)
    
    # Create user_teams table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_teams (
            team_id INTEGER PRIMARY KEY,
            team_name TEXT,
            player_name TEXT,
            user_email TEXT,
            overall_rank INTEGER,
            overall_points INTEGER,
            last_synced TIMESTAMP DEFAULT NOW()
        );
    """)
    
    # Insert/update team
    cursor.execute("""
        INSERT INTO user_teams (
            team_id, team_name, player_name, user_email,
            overall_rank, overall_points, last_synced
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (team_id) DO UPDATE SET
            team_name = EXCLUDED.team_name,
            overall_rank = EXCLUDED.overall_rank,
            overall_points = EXCLUDED.overall_points,
            last_synced = NOW();
    """, (
        team_id,
        team_data['name'],
        team_data['player_first_name'] + ' ' + team_data['player_last_name'],
        user_email,
        team_data['summary_overall_rank'],
        team_data['summary_overall_points']
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return team_data


def get_current_squad(team_id, gameweek):
    """Get user's current squad with player details"""
    picks_data = fetch_team_picks(team_id, gameweek)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    squad = []
    for pick in picks_data['picks']:
        cursor.execute("""
            SELECT 
                web_name,
                first_name || ' ' || second_name as full_name,
                position,
                now_cost / 10.0 as price,
                t.name as team
            FROM players p
            JOIN teams t ON p.team_code = t.code AND t.season = '2025-26'
            WHERE p.code = %s;
        """, (pick['element'],))
        
        player = cursor.fetchone()
        if player:
            squad.append({
                'web_name': player[0],
                'full_name': player[1],
                'position': player[2],
                'price': player[3],
                'team': player[4],
                'is_captain': pick['is_captain'],
                'is_vice_captain': pick['is_vice_captain'],
                'multiplier': pick['multiplier']
            })
    
    cursor.close()
    conn.close()
    
    return squad


if __name__ == '__main__':
    # Test with your team ID
    team_id = input("Enter your FPL team ID: ")
    team_info = fetch_team_info(int(team_id))
    print(f"\n✅ Team: {team_info['name']}")
    print(f"   Manager: {team_info['player_first_name']} {team_info['player_last_name']}")
    print(f"   Rank: {team_info['summary_overall_rank']:,}")
    print(f"   Points: {team_info['summary_overall_points']}")