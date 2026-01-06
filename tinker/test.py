"""
Diagnose and fix opponent_team_name issues
"""
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection
import csv

load_dotenv()

def diagnose_opponent_teams():
    """Diagnose what opponent_team represents"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 80)
    print("🔍 DIAGNOSING OPPONENT_TEAM VALUES")
    print("=" * 80)
    
    # Check teams table
    print("\n1️⃣ Teams in database (2025-26):")
    cursor.execute("""
        SELECT id, code, name
        FROM teams
        WHERE season = '2025-26'
        ORDER BY id;
    """)
    
    print(f"\n{'ID':<5} {'Code':<6} {'Name':<20}")
    print("-" * 35)
    teams = cursor.fetchall()
    for row in teams:
        print(f"{row[0]:<5} {row[1]:<6} {row[2]:<20}")
    
    # Check opponent_team values
    print("\n\n2️⃣ Unique opponent_team values in player_gameweek_stats:")
    cursor.execute("""
        SELECT DISTINCT opponent_team
        FROM player_gameweek_stats
        WHERE season = '2025-26'
        ORDER BY opponent_team;
    """)
    
    opponent_values = [row[0] for row in cursor.fetchall()]
    print(f"Opponent team values: {opponent_values}")
    
    # Check which join works better
    print("\n\n3️⃣ Testing joins:")
    
    # Test by ID
    cursor.execute("""
        SELECT COUNT(DISTINCT pgs.opponent_team)
        FROM player_gameweek_stats pgs
        INNER JOIN teams t ON pgs.opponent_team = t.id AND pgs.season = t.season
        WHERE pgs.season = '2025-26';
    """)
    matches_by_id = cursor.fetchone()[0]
    
    # Test by code
    cursor.execute("""
        SELECT COUNT(DISTINCT pgs.opponent_team)
        FROM player_gameweek_stats pgs
        INNER JOIN teams t ON pgs.opponent_team = t.code AND pgs.season = t.season
        WHERE pgs.season = '2025-26';
    """)
    matches_by_code = cursor.fetchone()[0]
    
    print(f"Unique opponent_team values that match:")
    print(f"  - By teams.id: {matches_by_id}/{len(opponent_values)}")
    print(f"  - By teams.code: {matches_by_code}/{len(opponent_values)}")
    
    # Show specific examples
    print("\n\n4️⃣ Sample mappings:")
    cursor.execute("""
        SELECT 
            pgs.opponent_team,
            pgs.opponent_team_name as current_name,
            t_by_id.name as name_if_id,
            t_by_code.name as name_if_code,
            COUNT(*) as record_count
        FROM player_gameweek_stats pgs
        LEFT JOIN teams t_by_id ON pgs.opponent_team = t_by_id.id AND pgs.season = t_by_id.season
        LEFT JOIN teams t_by_code ON pgs.opponent_team = t_by_code.code AND pgs.season = t_by_code.season
        WHERE pgs.season = '2025-26'
        GROUP BY pgs.opponent_team, pgs.opponent_team_name, t_by_id.name, t_by_code.name
        ORDER BY pgs.opponent_team
        LIMIT 20;
    """)
    
    print(f"\n{'Opp Val':<8} {'Current Name':<18} {'Name (by ID)':<18} {'Name (by code)':<18} {'Count':<8}")
    print("-" * 80)
    for row in cursor.fetchall():
        opp_val, current, by_id, by_code, count = row
        print(f"{opp_val:<8} {current or 'NULL':<18} {by_id or 'NULL':<18} {by_code or 'NULL':<18} {count:<8}")
    
    # Determine correct approach
    print("\n\n5️⃣ Recommendation:")
    if matches_by_id >= matches_by_code:
        print(f"✅ Use teams.ID (matches {matches_by_id} values)")
        recommendation = "id"
    else:
        print(f"✅ Use teams.CODE (matches {matches_by_code} values)")
        recommendation = "code"
    
    cursor.close()
    conn.close()
    
    return recommendation


def fix_opponent_team_names(use_column="id"):
    """Fix opponent_team_name using the correct column"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("\n" + "=" * 80)
    print(f"🔧 FIXING opponent_team_name USING teams.{use_column}")
    print("=" * 80)
    
    # Clear existing values
    print("\n1️⃣ Clearing existing opponent_team_name...")
    cursor.execute("""
        UPDATE player_gameweek_stats
        SET opponent_team_name = NULL;
    """)
    cleared = cursor.rowcount
    conn.commit()
    print(f"✅ Cleared {cleared} records")
    
    # Populate correctly
    print(f"\n2️⃣ Populating opponent_team_name using teams.{use_column}...")
    cursor.execute(f"""
        UPDATE player_gameweek_stats pgs
        SET opponent_team_name = t.name
        FROM teams t
        WHERE pgs.opponent_team = t.{use_column}
          AND pgs.season = t.season;
    """)
    updated = cursor.rowcount
    conn.commit()
    print(f"✅ Updated {updated} records")
    
    # Verify
    print("\n3️⃣ Verifying fix...")
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(opponent_team_name) as with_name,
            COUNT(DISTINCT opponent_team_name) as unique_names
        FROM player_gameweek_stats
        WHERE season = '2025-26';
    """)
    
    total, with_name, unique = cursor.fetchone()
    print(f"\n📊 Results:")
    print(f"   Total records: {total}")
    print(f"   With opponent name: {with_name} ({with_name/total*100:.1f}%)")
    print(f"   Unique opponent names: {unique}")
    
    # Show examples
    print("\n4️⃣ Sample corrected data:")
    cursor.execute("""
        SELECT 
            player_name,
            player_team_name,
            opponent_team,
            opponent_team_name,
            gameweek
        FROM player_gameweek_stats
        WHERE season = '2025-26'
          AND player_name IN ('Aaron Hickey', 'Aaron Ramsdale', 'Antoine Semenyo', 'Alex Iwobi')
        ORDER BY player_name;
    """)
    
    print(f"\n{'Player':<22} {'Player Team':<15} {'Opp ID':<8} {'Opponent':<18} {'GW'}")
    print("-" * 80)
    for row in cursor.fetchall():
        print(f"{row[0]:<22} {row[1] or 'NULL':<15} {row[2]:<8} {row[3] or 'NULL':<18} {row[4]}")
    
    print("\n" + "=" * 80)
    print("✅ FIX COMPLETE!")
    print("=" * 80)
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    # First diagnose
    recommended_column = diagnose_opponent_teams()
    
    # Then ask user if they want to fix
    print("\n" + "=" * 80)
    response = input(f"\nProceed with fix using teams.{recommended_column}? (yes/no): ")
    
    if response.lower() == 'yes':
        fix_opponent_team_names(recommended_column)
    else:
        print("Cancelled.")