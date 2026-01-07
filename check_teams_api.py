"""
Check teams data from FPL API
Shows all available team fields and their current values
"""
import requests
import json
from pprint import pprint

def check_teams_api():
    """Fetch and display teams data from FPL API"""
    
    print("=" * 80)
    print("🔍 CHECKING FPL API TEAMS DATA")
    print("=" * 80)
    
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    
    print(f"\n📡 Fetching from: {url}\n")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        teams = data.get('teams', [])
        
        print(f"✅ Found {len(teams)} teams\n")
        print("=" * 80)
        
        # Show first team in detail to see all fields
        if teams:
            print("\n📋 AVAILABLE FIELDS (showing first team as example):")
            print("=" * 80)
            
            first_team = teams[0]
            
            for key, value in first_team.items():
                print(f"{key:<30} = {value}")
            
            print("\n" + "=" * 80)
            print("📊 ALL TEAMS SUMMARY:")
            print("=" * 80)
            
            # Show key fields for all teams
            print(f"\n{'ID':<4} {'Code':<6} {'Name':<20} {'Short':<6} {'Str':<4} {'Att_H':<6} {'Def_H':<6} {'Att_A':<6} {'Def_A':<6}")
            print("-" * 90)
            
            for team in teams:
                print(f"{team['id']:<4} "
                      f"{team['code']:<6} "
                      f"{team['name']:<20} "
                      f"{team['short_name']:<6} "
                      f"{team.get('strength', 0):<4} "
                      f"{team.get('strength_attack_home', 0):<6} "
                      f"{team.get('strength_defence_home', 0):<6} "
                      f"{team.get('strength_attack_away', 0):<6} "
                      f"{team.get('strength_defence_away', 0):<6}")
            
            # Export to JSON for inspection
            print("\n" + "=" * 80)
            print("💾 EXPORTING TO teams_api_data.json")
            print("=" * 80)
            
            with open('teams_api_data.json', 'w') as f:
                json.dump(teams, f, indent=2)
            
            print("\n✅ Exported to teams_api_data.json")
            print("📄 You can open this file to see all available fields")
            
            # Show summary stats
            print("\n" + "=" * 80)
            print("📈 STRENGTH ANALYSIS:")
            print("=" * 80)
            
            # Sort by overall strength
            sorted_teams = sorted(teams, key=lambda x: x.get('strength', 0), reverse=True)
            
            print("\n🏆 Strongest Teams (Overall):")
            for i, team in enumerate(sorted_teams[:5], 1):
                print(f"  {i}. {team['name']:<20} - Strength: {team.get('strength', 0)}")
            
            print("\n⚔️ Best Attack (Home):")
            sorted_by_attack_h = sorted(teams, key=lambda x: x.get('strength_attack_home', 0), reverse=True)
            for i, team in enumerate(sorted_by_attack_h[:5], 1):
                print(f"  {i}. {team['name']:<20} - Attack (H): {team.get('strength_attack_home', 0)}")
            
            print("\n🛡️ Best Defence (Home):")
            sorted_by_def_h = sorted(teams, key=lambda x: x.get('strength_defence_home', 0), reverse=True)
            for i, team in enumerate(sorted_by_def_h[:5], 1):
                print(f"  {i}. {team['name']:<20} - Defence (H): {team.get('strength_defence_home', 0)}")
            
        print("\n" + "=" * 80)
        print("✅ CHECK COMPLETE!")
        print("=" * 80)
        
    except requests.RequestException as e:
        print(f"❌ Error fetching data: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    check_teams_api()