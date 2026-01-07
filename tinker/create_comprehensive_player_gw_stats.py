"""
Create comprehensive player_gameweek_stats table with ALL fields
Backs up existing table first
"""
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection
from datetime import datetime

load_dotenv()

def create_comprehensive_schema():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("=" * 70)
    print("🔧 CREATING COMPREHENSIVE PLAYER GAMEWEEK STATS SCHEMA")
    print("=" * 70)
    
    # Step 1: Backup existing table
    print("\n1️⃣ Backing up existing table...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_table = f"player_gameweek_stats_backup_{timestamp}"
    
    cursor.execute(f"""
        CREATE TABLE {backup_table} AS 
        SELECT * FROM player_gameweek_stats;
    """)
    
    cursor.execute(f"SELECT COUNT(*) FROM {backup_table};")
    backup_count = cursor.fetchone()[0]
    print(f"✅ Backed up {backup_count} records to {backup_table}")
    
    # Step 2: Drop existing table
    print("\n2️⃣ Dropping old table...")
    cursor.execute("DROP TABLE IF EXISTS player_gameweek_stats CASCADE;")
    print("✅ Dropped old table")
    
    # Step 3: Create new comprehensive table
    print("\n3️⃣ Creating new comprehensive table...")
    
    cursor.execute("""
        CREATE TABLE player_gameweek_stats (
            -- Core identification
            season TEXT NOT NULL,
            gameweek INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            fixture_id INTEGER,
            opponent_team INTEGER,
            was_home BOOLEAN,
            
            -- Match context
            team_h_score INTEGER,
            team_a_score INTEGER,
            minutes INTEGER DEFAULT 0,
            starts BOOLEAN DEFAULT FALSE,
            kickoff_time TEXT,
            
            -- FPL scoring
            total_points INTEGER DEFAULT 0,
            bonus INTEGER DEFAULT 0,
            bps INTEGER DEFAULT 0,
            
            -- Performance stats
            goals_scored INTEGER DEFAULT 0,
            assists INTEGER DEFAULT 0,
            clean_sheets INTEGER DEFAULT 0,
            goals_conceded INTEGER DEFAULT 0,
            own_goals INTEGER DEFAULT 0,
            penalties_saved INTEGER DEFAULT 0,
            penalties_missed INTEGER DEFAULT 0,
            penalties_scored INTEGER DEFAULT 0,
            red_cards INTEGER DEFAULT 0,
            yellow_cards INTEGER DEFAULT 0,
            saves INTEGER DEFAULT 0,
            
            -- Expected stats (xG family)
            expected_goals DECIMAL(10,2) DEFAULT 0,
            expected_assists DECIMAL(10,2) DEFAULT 0,
            expected_goal_involvements DECIMAL(10,2) DEFAULT 0,
            expected_goals_conceded DECIMAL(10,2) DEFAULT 0,
            
            -- ICT metrics
            influence DECIMAL(10,1) DEFAULT 0,
            creativity DECIMAL(10,1) DEFAULT 0,
            threat DECIMAL(10,1) DEFAULT 0,
            ict_index DECIMAL(10,1) DEFAULT 0,
            
            -- Detailed performance (from Vaastav when available)
            big_chances_missed INTEGER DEFAULT 0,
            big_chances_created INTEGER DEFAULT 0,
            clearances_blocks_interceptions INTEGER DEFAULT 0,
            completed_passes INTEGER DEFAULT 0,
            dribbles INTEGER DEFAULT 0,
            errors_leading_to_goal INTEGER DEFAULT 0,
            fouls INTEGER DEFAULT 0,
            key_passes INTEGER DEFAULT 0,
            open_play_crosses INTEGER DEFAULT 0,
            tackles INTEGER DEFAULT 0,
            recoveries INTEGER DEFAULT 0,
            winning_goals INTEGER DEFAULT 0,
            
            -- Ownership/transfers
            selected INTEGER DEFAULT 0,
            transfers_in INTEGER DEFAULT 0,
            transfers_out INTEGER DEFAULT 0,
            transfers_balance INTEGER DEFAULT 0,
            value INTEGER DEFAULT 0,
            
            -- Team names (denormalized for convenience)
            player_team_name TEXT,
            opponent_team_name TEXT,
            
            -- Constraints
            PRIMARY KEY (season, gameweek, player_name)
        );
    """)
    
    print("✅ Created comprehensive table")
    
    # Step 4: Create indexes
    print("\n4️⃣ Creating indexes...")
    
    indexes = [
        ("idx_pgs_season_gameweek", "season, gameweek"),
        ("idx_pgs_player_name", "player_name"),
        ("idx_pgs_total_points", "total_points DESC"),
        ("idx_pgs_fixture_id", "fixture_id"),
        ("idx_pgs_player_team", "player_team_name, season"),
        ("idx_pgs_opponent_team", "opponent_team_name, season"),
        ("idx_pgs_goals", "goals_scored DESC"),
        ("idx_pgs_assists", "assists DESC"),
        ("idx_pgs_minutes", "minutes DESC"),
        ("idx_pgs_bonus", "bonus DESC"),
    ]
    
    for idx_name, idx_columns in indexes:
        cursor.execute(f"""
            CREATE INDEX {idx_name} ON player_gameweek_stats({idx_columns});
        """)
        print(f"   ✅ {idx_name}")
    
    conn.commit()
    
    # Step 5: Verify
    print("\n5️⃣ Verifying...")
    
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'player_gameweek_stats'
        ORDER BY ordinal_position;
    """)
    
    columns = cursor.fetchall()
    print(f"\n✅ Table created with {len(columns)} columns:")
    print(f"\n{'Column':<35} {'Type':<20}")
    print("-" * 55)
    for col_name, col_type in columns[:10]:  # Show first 10
        print(f"{col_name:<35} {col_type:<20}")
    print(f"... and {len(columns) - 10} more columns")
    
    print("\n" + "=" * 70)
    print("✅ COMPREHENSIVE SCHEMA CREATED!")
    print("=" * 70)
    print(f"\n💾 Backup table: {backup_table}")
    print(f"📊 Backed up: {backup_count} records")
    print("\n🔄 Next steps:")
    print("   1. Update parsers to include all fields")
    print("   2. Reload data from Vaastav (GW1-9)")
    print("   3. Reload data from FPL API (GW10-19)")
    print("   4. Run migration to populate team names")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    print("\n⚠️  WARNING: This will:")
    print("   - Backup existing player_gameweek_stats table")
    print("   - Drop and recreate with new comprehensive schema")
    print("   - You'll need to reload all data after this\n")
    
    confirm = input("Continue? (yes/no): ").lower()
    
    if confirm == 'yes':
        create_comprehensive_schema()
    else:
        print("Cancelled.")