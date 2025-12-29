"""
Check all data in FPL database
Shows what's loaded and what's missing
"""
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("📊 FPL DATABASE DATA SUMMARY")
    print("=" * 70)
    
    # 1. Fixtures by season
    print("\n🏟️ FIXTURES BY SEASON:")
    cursor.execute("""
        SELECT season, COUNT(*) as count
        FROM fixtures
        GROUP BY season
        ORDER BY season;
    """)
    
    fixtures_data = cursor.fetchall()
    if fixtures_data:
        for season, count in fixtures_data:
            print(f"   {season}: {count:4d} fixtures")
    else:
        print("   ❌ No fixtures loaded")
    
    # 2. Gameweeks by season
    print("\n📅 GAMEWEEKS BY SEASON:")
    cursor.execute("""
        SELECT season, COUNT(*) as count
        FROM game_weeks
        GROUP BY season
        ORDER BY season;
    """)
    
    gameweeks_data = cursor.fetchall()
    if gameweeks_data:
        for season, count in gameweeks_data:
            print(f"   {season}: {count:2d} gameweeks")
    else:
        print("   ❌ No gameweeks loaded")
    
    # 3. Teams (current season)
    print("\n⚽ CURRENT TEAMS:")
    cursor.execute("""
        SELECT season, COUNT(*) as count
        FROM teams
        GROUP BY season
        ORDER BY season DESC
        LIMIT 1;
    """)
    
    teams_data = cursor.fetchone()
    if teams_data:
        print(f"   {teams_data[0]}: {teams_data[1]} teams")
    else:
        print("   ❌ No current teams loaded")
    
    # 4. Teams history
    print("\n📚 TEAMS HISTORY:")
    cursor.execute("""
        SELECT season, COUNT(*) as count
        FROM teams_history
        GROUP BY season
        ORDER BY season;
    """)
    
    teams_history_data = cursor.fetchall()
    if teams_history_data:
        for season, count in teams_history_data:
            print(f"   {season}: {count:2d} teams")
    else:
        print("   ❌ No teams history loaded")
    
    # 5. Players (current)
    print("\n👥 CURRENT PLAYERS:")
    cursor.execute("SELECT COUNT(*) FROM players;")
    player_count = cursor.fetchone()[0]
    if player_count > 0:
        # Breakdown by position
        cursor.execute("""
            SELECT position, COUNT(*) as count
            FROM players
            GROUP BY position
            ORDER BY position;
        """)
        print(f"   Total: {player_count} players")
        for pos, count in cursor.fetchall():
            print(f"     {pos}: {count}")
    else:
        print("   ❌ No players loaded")
    
    # 6. Player gameweek stats
    print("\n📈 PLAYER GAMEWEEK STATS:")
    cursor.execute("""
        SELECT season, COUNT(*) as count
        FROM player_gameweek_stats
        GROUP BY season
        ORDER BY season;
    """)
    
    stats_data = cursor.fetchall()
    if stats_data:
        for season, count in stats_data:
            print(f"   {season}: {count:6d} records")
    else:
        print("   ❌ No player gameweek stats loaded")
        print("   💡 This is normal - player stats require separate loading")
    
    # 7. Overall totals
    print("\n" + "=" * 70)
    print("📊 OVERALL TOTALS:")
    print("=" * 70)
    
    cursor.execute("""
        SELECT 
            (SELECT COUNT(*) FROM fixtures) as fixtures,
            (SELECT COUNT(*) FROM game_weeks) as gameweeks,
            (SELECT COUNT(*) FROM teams) as current_teams,
            (SELECT COUNT(*) FROM teams_history) as teams_history,
            (SELECT COUNT(*) FROM players) as players,
            (SELECT COUNT(*) FROM player_gameweek_stats) as player_stats;
    """)
    
    totals = cursor.fetchone()
    print(f"   Fixtures:            {totals[0]:6d}")
    print(f"   Gameweeks:           {totals[1]:6d}")
    print(f"   Current Teams:       {totals[2]:6d}")
    print(f"   Teams History:       {totals[3]:6d}")
    print(f"   Current Players:     {totals[4]:6d}")
    print(f"   Player GW Stats:     {totals[5]:6d}")
    
    # 8. Database size estimate
    cursor.execute("""
        SELECT 
            pg_size_pretty(pg_database_size(current_database())) as db_size;
    """)
    db_size = cursor.fetchone()[0]
    print(f"\n💾 Database Size: {db_size}")
    
    print("=" * 70)
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()