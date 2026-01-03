"""
Debug and fix team names migration
"""
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()

def debug_fixture_match():
    """Debug why fixtures aren't matching"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("🔍 DEBUGGING FIXTURE MATCHING")
    print("=" * 70)
    
    # Check Isak's data
    print("\n1️⃣ Isak's player_gameweek_stats data:")
    cursor.execute("""
        SELECT 
            gameweek,
            fixture_id,
            was_home,
            season
        FROM player_gameweek_stats
        WHERE player_name = 'Alexander Isak'
          AND season = '2025-26'
        ORDER BY gameweek
        LIMIT 5;
    """)
    
    print(f"\n{'GW':<4} {'Fixture ID':<12} {'Was Home':<10} {'Season'}")
    print("-" * 40)
    for row in cursor.fetchall():
        print(f"{row[0]:<4} {row[1]:<12} {row[2]:<10} {row[3]}")
    
    # Check if fixtures exist
    print("\n\n2️⃣ Checking if fixtures table has matching records:")
    cursor.execute("""
        SELECT 
            code,
            gameweek,
            season,
            team_h_code,
            team_a_code
        FROM fixtures
        WHERE season = '2025-26'
        ORDER BY gameweek
        LIMIT 5;
    """)
    
    print(f"\n{'Code':<8} {'GW':<4} {'Season':<10} {'Home Code':<12} {'Away Code'}")
    print("-" * 50)
    for row in cursor.fetchall():
        print(f"{row[0]:<8} {row[1]:<4} {row[2]:<10} {row[3]:<12} {row[4]}")
    
    # Check for matching fixture_id
    print("\n\n3️⃣ Trying to match Isak's fixture_id with fixtures.code:")
    cursor.execute("""
        SELECT 
            pgs.fixture_id as pgs_fixture_id,
            f.code as fixture_code,
            pgs.season as pgs_season,
            f.season as fixture_season,
            f.team_h_code,
            f.team_a_code
        FROM player_gameweek_stats pgs
        LEFT JOIN fixtures f ON pgs.fixture_id = f.code AND pgs.season = f.season
        WHERE pgs.player_name = 'Alexander Isak'
          AND pgs.season = '2025-26'
        ORDER BY pgs.gameweek
        LIMIT 5;
    """)
    
    print(f"\n{'PGS Fixture':<12} {'Fixtures Code':<14} {'PGS Season':<12} {'Fix Season':<12} {'Match?'}")
    print("-" * 70)
    for row in cursor.fetchall():
        match = "✅" if row[1] is not None else "❌"
        print(f"{row[0]:<12} {row[1] or 'NULL':<14} {row[2]:<12} {row[3] or 'NULL':<12} {match}")
    
    # Check column types
    print("\n\n4️⃣ Checking column types:")
    cursor.execute("""
        SELECT 
            column_name, 
            data_type 
        FROM information_schema.columns 
        WHERE table_name = 'player_gameweek_stats' 
          AND column_name IN ('fixture_id', 'season', 'was_home')
        ORDER BY column_name;
    """)
    
    print(f"\n{'Column':<20} {'Type'}")
    print("-" * 30)
    for row in cursor.fetchall():
        print(f"{row[0]:<20} {row[1]}")
    
    cursor.execute("""
        SELECT 
            column_name, 
            data_type 
        FROM information_schema.columns 
        WHERE table_name = 'fixtures' 
          AND column_name IN ('code', 'season')
        ORDER BY column_name;
    """)
    
    print(f"\n{'Column':<20} {'Type'}")
    print("-" * 30)
    for row in cursor.fetchall():
        print(f"{row[0]:<20} {row[1]}")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    debug_fixture_match()