"""
Manage Penalty Takers Data
Easy CLI tool to view and update penalty taker information
"""
import os
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()


def view_penalty_takers(season='2025-26'):
    """Display current penalty takers"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            team_name,
            player_name,
            priority,
            confidence,
            notes,
            last_updated
        FROM penalty_takers
        WHERE season = %s
        ORDER BY team_name, priority;
    """, (season,))
    
    results = cursor.fetchall()
    
    if not results:
        print(f"⚠️ No penalty takers found for season {season}")
        cursor.close()
        conn.close()
        return
    
    print(f"\n{'='*80}")
    print(f"⚽ PENALTY TAKERS - {season}")
    print(f"{'='*80}\n")
    
    current_team = None
    for team, player, priority, confidence, notes, updated in results:
        if team != current_team:
            if current_team is not None:
                print()
            current_team = team
            print(f"📌 {team}")
        
        priority_label = "1st" if priority == 1 else "2nd" if priority == 2 else f"{priority}th"
        conf_emoji = "✅" if confidence == "confirmed" else "🟡" if confidence == "likely" else "❓"
        
        print(f"   {conf_emoji} {priority_label}: {player} ({confidence})")
        if notes:
            print(f"      Note: {notes}")
    
    print(f"\n{'='*80}\n")
    
    cursor.close()
    conn.close()


def get_player_penalty_status(player_name, season='2025-26'):
    """Check if a player is on penalties"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            team_name,
            priority,
            confidence,
            notes
        FROM penalty_takers
        WHERE player_name ILIKE %s AND season = %s;
    """, (f"%{player_name}%", season))
    
    result = cursor.fetchone()
    
    if result:
        team, priority, confidence, notes = result
        priority_label = "1st choice" if priority == 1 else "2nd choice" if priority == 2 else f"{priority}th choice"
        
        print(f"\n✅ {player_name} - Penalty taker for {team}")
        print(f"   Priority: {priority_label}")
        print(f"   Confidence: {confidence}")
        if notes:
            print(f"   Notes: {notes}")
    else:
        print(f"\n❌ {player_name} - Not listed as a penalty taker")
    
    cursor.close()
    conn.close()


def update_penalty_taker(team_code, team_name, player_name, season='2025-26', 
                         priority=1, confidence='confirmed', notes=None):
    """Update or add a penalty taker"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO penalty_takers 
            (team_code, team_name, player_name, season, priority, confidence, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (team_code, season, priority) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            team_name = EXCLUDED.team_name,
            confidence = EXCLUDED.confidence,
            notes = EXCLUDED.notes,
            last_updated = NOW();
    """, (team_code, team_name, player_name, season, priority, confidence, notes))
    
    conn.commit()
    print(f"✅ Updated: {player_name} ({team_name}) - {priority}st choice, {confidence}")
    
    cursor.close()
    conn.close()


def get_penalty_taker_recommendations():
    """Show players who likely take penalties based on stats"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("\n🔍 Analyzing player stats to find likely penalty takers...\n")
    
    # Find players who have taken penalties (missed or scored)
    cursor.execute("""
        WITH penalty_stats AS (
            SELECT 
                player_name,
                COUNT(DISTINCT gameweek) as games_played,
                SUM(penalties_missed) as total_pen_missed,
                SUM(CASE WHEN penalties_missed > 0 THEN 1 ELSE 0 END) as games_with_pen_miss
            FROM player_gameweek_stats
            WHERE season = '2025-26'
            GROUP BY player_name
        )
        SELECT 
            ps.player_name,
            ps.total_pen_missed,
            ps.games_with_pen_miss,
            CASE 
                WHEN pt.player_name IS NOT NULL THEN '✅ Already tracked'
                ELSE '❓ Not tracked yet'
            END as status
        FROM penalty_stats ps
        LEFT JOIN penalty_takers pt ON ps.player_name = pt.player_name 
            AND pt.season = '2025-26'
        WHERE ps.total_pen_missed > 0 OR ps.games_with_pen_miss > 0
        ORDER BY ps.total_pen_missed DESC, ps.games_with_pen_miss DESC
        LIMIT 20;
    """)
    
    results = cursor.fetchall()
    
    if results:
        print("Players who have taken penalties this season:")
        print(f"{'Player':<25} {'Pens Missed':<12} {'Status'}")
        print("-" * 60)
        for player, missed, games, status in results:
            print(f"{player:<25} {missed:<12} {status}")
    else:
        print("No penalty data found in player stats yet.")
    
    print()
    
    cursor.close()
    conn.close()


def export_to_csv(filename='penalty_takers.csv', season='2025-26'):
    """Export penalty takers to CSV"""
    import csv
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            team_code,
            team_name,
            player_name,
            season,
            priority,
            confidence,
            notes,
            last_updated
        FROM penalty_takers
        WHERE season = %s
        ORDER BY team_name, priority;
    """, (season,))
    
    results = cursor.fetchall()
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['team_code', 'team_name', 'player_name', 'season', 
                        'priority', 'confidence', 'notes', 'last_updated'])
        writer.writerows(results)
    
    print(f"✅ Exported {len(results)} records to {filename}")
    
    cursor.close()
    conn.close()


def main():
    print("="*80)
    print("⚽ PENALTY TAKERS MANAGER")
    print("="*80)
    
    while True:
        print("\nOptions:")
        print("1. View all penalty takers")
        print("2. Check specific player")
        print("3. Update penalty taker")
        print("4. Find penalty takers from stats")
        print("5. Export to CSV")
        print("6. Exit")
        
        choice = input("\nChoice (1-6): ").strip()
        
        if choice == "1":
            season = input("Season (default: 2025-26): ").strip() or "2025-26"
            view_penalty_takers(season)
            
        elif choice == "2":
            player = input("Player name: ").strip()
            if player:
                get_player_penalty_status(player)
            
        elif choice == "3":
            print("\nUpdate penalty taker:")
            team_code = int(input("  Team code: "))
            team_name = input("  Team name: ")
            player_name = input("  Player name: ")
            priority = int(input("  Priority (1=first, 2=second): ") or "1")
            confidence = input("  Confidence (confirmed/likely/uncertain): ") or "confirmed"
            notes = input("  Notes (optional): ") or None
            
            update_penalty_taker(team_code, team_name, player_name, 
                               priority=priority, confidence=confidence, notes=notes)
            
        elif choice == "4":
            get_penalty_taker_recommendations()
            
        elif choice == "5":
            filename = input("Filename (default: penalty_takers.csv): ").strip() or "penalty_takers.csv"
            export_to_csv(filename)
            
        elif choice == "6":
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice")


if __name__ == '__main__':
    main()