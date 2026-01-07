"""
Add team name columns to player_gameweek_stats
FINAL CORRECT VERSION using fixture_id matching

Logic:
1. opponent_team (id) → teams.id → teams.name → opponent_team_name ✅
2. fixture_id → fixtures.id → team_h_code/team_a_code → teams.id → teams.name → player_team_name ✅
"""
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()

def add_team_name_columns():
    """Add team name columns with fixture_id matching"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("🔧 ADDING TEAM NAME COLUMNS TO player_gameweek_stats")
    print("=" * 70)
    
    # Step 1: Drop and recreate columns
    print("\n1️⃣ Resetting columns...")
    
    cursor.execute("""
        ALTER TABLE player_gameweek_stats 
        DROP COLUMN IF EXISTS player_team_name CASCADE;
    """)
    
    cursor.execute("""
        ALTER TABLE player_gameweek_stats 
        DROP COLUMN IF EXISTS opponent_team_name CASCADE;
    """)
    
    cursor.execute("""
        ALTER TABLE player_gameweek_stats 
        DROP COLUMN IF EXISTS actual_team_code CASCADE;
    """)
    
    cursor.execute("""
        ALTER TABLE player_gameweek_stats 
        ADD COLUMN player_team_name TEXT;
    """)
    
    cursor.execute("""
        ALTER TABLE player_gameweek_stats 
        ADD COLUMN opponent_team_name TEXT;
    """)
    
    conn.commit()
    print("✅ Columns reset")
    
    # Step 2: Populate opponent_team_name
    print("\n2️⃣ Populating opponent_team_name...")
    print("   Logic: opponent_team (id) → teams.id → teams.name")
    
    cursor.execute("""
        UPDATE player_gameweek_stats pgs
        SET opponent_team_name = t.name
        FROM teams t
        WHERE pgs.opponent_team = t.id
          AND pgs.season = t.season;
    """)
    
    updated = cursor.rowcount
    conn.commit()
    print(f"✅ Updated {updated} records")
    
    # Step 3: Populate player_team_name using fixture_id + was_home
    # CRITICAL FIX: fixtures.team_h_code references teams.id (NOT teams.code!)
    print("\n3️⃣ Populating player_team_name...")
    print("   Logic: fixture_id + was_home → fixtures → team_h/a_code → teams.id → teams.name")
    
    cursor.execute("""
        UPDATE player_gameweek_stats
        SET player_team_name = subquery.team_name
        FROM (
            SELECT 
                pgs.season,
                pgs.gameweek,
                pgs.player_name,
                CASE 
                    -- If player was home, get home team name
                    WHEN pgs.was_home THEN th.name
                    -- If player was away, get away team name
                    ELSE ta.name
                END as team_name
            FROM player_gameweek_stats pgs
            JOIN fixtures f ON pgs.fixture_id = f.id AND pgs.season = f.season
            -- FIXED: team_h_code/team_a_code reference teams.id, not teams.code
            JOIN teams th ON f.team_h_code = th.id AND f.season = th.season
            JOIN teams ta ON f.team_a_code = ta.id AND f.season = ta.season
        ) AS subquery
        WHERE player_gameweek_stats.season = subquery.season
          AND player_gameweek_stats.gameweek = subquery.gameweek
          AND player_gameweek_stats.player_name = subquery.player_name;
    """)
    
    updated = cursor.rowcount
    conn.commit()
    print(f"✅ Updated {updated} records")
    
    # Step 4: Add indexes
    print("\n4️⃣ Adding indexes...")
    
    cursor.execute("""
        DROP INDEX IF EXISTS idx_player_gw_stats_player_team;
        DROP INDEX IF EXISTS idx_player_gw_stats_opponent_team;
    """)
    
    cursor.execute("""
        CREATE INDEX idx_player_gw_stats_player_team 
        ON player_gameweek_stats(player_team_name, season);
    """)
    
    cursor.execute("""
        CREATE INDEX idx_player_gw_stats_opponent_team 
        ON player_gameweek_stats(opponent_team_name, season);
    """)
    
    conn.commit()
    print("✅ Indexes created")
    
    # Step 5: Verify ALL SEASONS
    print("\n5️⃣ Verifying all seasons...")

    cursor.execute("""
        SELECT 
            season,
            COUNT(*) as total,
            COUNT(player_team_name) as with_player,
            COUNT(opponent_team_name) as with_opponent
        FROM player_gameweek_stats
        GROUP BY season
        ORDER BY season;
    """)

    print(f"\n📊 Results by season:")
    print(f"{'Season':<10} {'Total':<8} {'Player Team':<12} {'Opponent':<12} {'Coverage'}")
    print("-" * 60)

    for row in cursor.fetchall():
        season, total, player, opponent = row
        coverage = min(player/total*100, opponent/total*100) if total > 0 else 0
        print(f"{season:<10} {total:<8} {player:<12} {opponent:<12} {coverage:.1f}%")
    
    # Step 6: Check for NULLs
    cursor.execute("""
        SELECT COUNT(*)
        FROM player_gameweek_stats
        WHERE season = '2025-26'
          AND (player_team_name IS NULL OR opponent_team_name IS NULL);
    """)
    
    null_count = cursor.fetchone()[0]
    
    if null_count > 0:
        print(f"\n⚠️  Found {null_count} records with NULL values")
        
        cursor.execute("""
            SELECT 
                player_name,
                gameweek,
                fixture_id,
                player_team_name,
                opponent_team_name
            FROM player_gameweek_stats
            WHERE season = '2025-26'
              AND (player_team_name IS NULL OR opponent_team_name IS NULL)
            LIMIT 10;
        """)
        
        print(f"\n{'Player':<25} {'GW':<4} {'FixID':<7} {'Player Team':<15} {'Opponent':<15}")
        print("-" * 75)
        for row in cursor.fetchall():
            player, gw, fix_id, p_team, opp = row
            print(f"{player:<25} {gw:<4} {fix_id:<7} {p_team or 'NULL':<15} {opp or 'NULL':<15}")
    else:
        print("\n✅ No NULL values found!")
    
    # Step 7: Verify Isak
    print("\n6️⃣ Checking Alexander Isak...")
    
    cursor.execute("""
        SELECT 
            pgs.gameweek,
            pgs.fixture_id,
            pgs.was_home,
            pgs.player_team_name,
            pgs.opponent_team_name,
            f.team_h_code,
            f.team_a_code,
            th.name as fixture_home,
            ta.name as fixture_away
        FROM player_gameweek_stats pgs
        LEFT JOIN fixtures f ON pgs.fixture_id = f.id AND pgs.season = f.season
        LEFT JOIN teams th ON f.team_h_code = th.id AND f.season = th.season
        LEFT JOIN teams ta ON f.team_a_code = ta.id AND f.season = ta.season
        WHERE pgs.player_name = 'Alexander Isak'
          AND pgs.season = '2025-26'
        ORDER BY pgs.gameweek
        LIMIT 10;
    """)
    
    print(f"\n{'GW':<4} {'FixID':<6} {'V':<3} {'Played For':<15} {'vs':<3} {'Opponent':<15} {'Actual Fixture':<30}")
    print("-" * 95)
    
    for row in cursor.fetchall():
        gw, fix_id, was_home, played, opp, h_code, a_code, fix_h, fix_a = row
        venue = "🏠" if was_home else "✈️"
        fixture = f"{fix_h or 'NULL'} vs {fix_a or 'NULL'}"
        print(f"{gw:<4} {fix_id:<6} {venue:<3} {played or 'NULL':<15} {'vs':<3} {opp or 'NULL':<15} {fixture:<30}")
    
    print("\n💡 'Played For' should match home or away in 'Actual Fixture'")
    
    # Step 8: Check transferred players
    print("\n7️⃣ Checking transferred players...")
    
    cursor.execute("""
        SELECT 
            pgs.player_name,
            pgs.gameweek,
            pgs.player_team_name,
            pgs.opponent_team_name,
            t_curr.name as current_team,
            CASE 
                WHEN pgs.player_team_name = t_curr.name THEN '✅ Same'
                ELSE '🔄 Transferred'
            END as status
        FROM player_gameweek_stats pgs
        JOIN players p ON pgs.player_name = p.first_name || ' ' || p.second_name
        JOIN teams t_curr ON p.team_code = t_curr.code AND t_curr.season = '2025-26'
        WHERE pgs.season = '2025-26'
          AND pgs.player_name IN ('Alexander Isak', 'Alejandro Garnacho Ferreyra')
        ORDER BY pgs.player_name, pgs.gameweek;
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(f"\n{'Player':<30} {'GW':<4} {'Played For':<15} {'vs':<3} {'Opponent':<15} {'Current':<15} {'Status'}")
        print("-" * 110)
        
        last = None
        for row in rows:
            player, gw, played, opp, curr, status = row
            if last != player and last is not None:
                print("-" * 110)
            last = player
            print(f"{player:<30} {gw:<4} {played or 'NULL':<15} {'vs':<3} {opp or 'NULL':<15} {curr:<15} {status}")
    
    print("\n" + "=" * 70)
    print("✅ MIGRATION COMPLETE!")
    print("=" * 70)
    print("\n💡 How it works:")
    print("   1. opponent_team_name: opponent_team (id) → teams.id → name")
    print("   2. player_team_name: fixture_id → fixtures.team_h/a_code → teams.id → name")
    print("   3. CRITICAL: fixtures.team_h_code references teams.id (NOT teams.code!)")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    add_team_name_columns()