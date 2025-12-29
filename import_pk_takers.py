"""
Import Penalty Takers from FPL Scout Page
Guided input tool to manually enter data from:
https://fantasy.premierleague.com/the-scout/set-piece-takers
"""
import os
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()

# Team codes mapping (from FPL API)
TEAM_CODES = {
    'Arsenal': 3,
    'Aston Villa': 7,
    'Bournemouth': 91,
    'Brentford': 94,
    'Brighton': 36,
    'Burnley': 90,
    'Chelsea': 8,
    'Crystal Palace': 31,
    'Everton': 11,
    'Fulham': 54,
    'Leeds': 2,
    'Liverpool': 14,
    'Man City': 43,
    'Man Utd': 1,
    'Newcastle': 4,
    "Nott'm Forest": 17,
    'Spurs': 6,
    'Sunderland': 56,
    'West Ham': 21,
    'Wolves': 39
}


def clear_season_data(season='2025-26'):
    """Clear existing penalty taker data for a season"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM penalty_takers WHERE season = %s", (season,))
    deleted = cursor.rowcount
    conn.commit()
    
    print(f"🗑️  Cleared {deleted} existing records for {season}")
    
    cursor.close()
    conn.close()


def add_penalty_taker(team_name, player_name, season='2025-26', 
                     priority=1, confidence='confirmed'):
    """Add a penalty taker to the database"""
    
    if team_name not in TEAM_CODES:
        print(f"❌ Unknown team: {team_name}")
        print(f"Available teams: {', '.join(TEAM_CODES.keys())}")
        return False
    
    team_code = TEAM_CODES[team_name]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO penalty_takers 
                (team_code, team_name, player_name, season, priority, confidence)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_code, season, priority) DO UPDATE SET
                player_name = EXCLUDED.player_name,
                team_name = EXCLUDED.team_name,
                confidence = EXCLUDED.confidence,
                last_updated = NOW();
        """, (team_code, team_name, player_name, season, priority, confidence))
        
        conn.commit()
        print(f"  ✅ {team_name}: {player_name} (priority {priority}, {confidence})")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        cursor.close()
        conn.close()
        return False


def bulk_import_wizard():
    """Interactive wizard to import all penalty takers"""
    
    print("="*80)
    print("⚽ FPL PENALTY TAKERS IMPORT WIZARD")
    print("="*80)
    print("\nGo to: https://fantasy.premierleague.com/the-scout/set-piece-takers")
    print("Then enter the penalty taker for each team as shown on that page.")
    print("="*80)
    
    season = input("\nSeason (default: 2025-26): ").strip() or "2025-26"
    
    # Ask if should clear existing data
    clear = input(f"\nClear existing data for {season}? (yes/no): ").strip().lower()
    if clear == 'yes':
        clear_season_data(season)
    
    print(f"\n📝 Enter penalty takers for {season}")
    print("Format: FirstName LastName (or leave blank if uncertain)")
    print("-"*80)
    
    for team_name in sorted(TEAM_CODES.keys()):
        print(f"\n{team_name}:")
        
        # First choice
        first = input(f"  1st choice: ").strip()
        if first:
            conf = input(f"  Confidence (confirmed/likely/uncertain) [confirmed]: ").strip() or "confirmed"
            add_penalty_taker(team_name, first, season, priority=1, confidence=conf)
        
        # Second choice (optional)
        second = input(f"  2nd choice (optional, press Enter to skip): ").strip()
        if second:
            conf = input(f"  Confidence (likely/uncertain) [likely]: ").strip() or "likely"
            add_penalty_taker(team_name, second, season, priority=2, confidence=conf)
    
    print("\n" + "="*80)
    print("✅ Import complete!")
    print("="*80)
    print("\nRun 'python manage_penalty_takers.py' to view the data")


def quick_update_single():
    """Quick update for a single team"""
    print("\n🔄 Quick Update - Single Team")
    print("-"*80)
    
    print("\nAvailable teams:")
    for i, team in enumerate(sorted(TEAM_CODES.keys()), 1):
        print(f"{i:2d}. {team}")
    
    team_input = input("\nEnter team name or number: ").strip()
    
    # Handle numeric input
    if team_input.isdigit():
        idx = int(team_input) - 1
        teams = sorted(TEAM_CODES.keys())
        if 0 <= idx < len(teams):
            team_name = teams[idx]
        else:
            print("❌ Invalid number")
            return
    else:
        team_name = team_input
    
    if team_name not in TEAM_CODES:
        print(f"❌ Unknown team: {team_name}")
        return
    
    season = input("Season (default: 2025-26): ").strip() or "2025-26"
    
    print(f"\n{team_name} penalty taker:")
    player = input("  Player name: ").strip()
    if not player:
        print("❌ Player name required")
        return
    
    priority = input("  Priority (1=first, 2=second) [1]: ").strip() or "1"
    confidence = input("  Confidence (confirmed/likely/uncertain) [confirmed]: ").strip() or "confirmed"
    
    add_penalty_taker(team_name, player, season, int(priority), confidence)
    print("\n✅ Updated!")


def main():
    print("="*80)
    print("⚽ PENALTY TAKERS IMPORTER")
    print("="*80)
    print("\nSource: https://fantasy.premierleague.com/the-scout/set-piece-takers")
    print("="*80)
    
    print("\nOptions:")
    print("1. Bulk import all teams (guided wizard)")
    print("2. Quick update single team")
    print("3. Exit")
    
    choice = input("\nChoice (1-3): ").strip()
    
    if choice == "1":
        bulk_import_wizard()
    elif choice == "2":
        quick_update_single()
    elif choice == "3":
        print("Goodbye!")
    else:
        print("Invalid choice")


if __name__ == '__main__':
    main()