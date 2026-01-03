from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'), sslmode='require')
cursor = conn.cursor()

print("Checking fixture_id matching...\n")

# Check Isak's fixture_ids
cursor.execute("""
    SELECT 
        pgs.gameweek,
        pgs.fixture_id,
        pgs.opponent_team,
        f.id as fixture_id_in_fixtures,
        f.team_h_code,
        f.team_a_code,
        th.name as home_team,
        ta.name as away_team
    FROM player_gameweek_stats pgs
    LEFT JOIN fixtures f ON pgs.fixture_id = f.id AND pgs.season = f.season
    LEFT JOIN teams th ON f.team_h_code = th.code AND f.season = th.season
    LEFT JOIN teams ta ON f.team_a_code = ta.code AND f.season = ta.season
    WHERE pgs.player_name = 'Alexander Isak'
      AND pgs.season = '2025-26'
    ORDER BY pgs.gameweek
    LIMIT 10;
""")

print(f"{'GW':<4} {'PGS FixID':<10} {'OppID':<7} {'Fix FixID':<10} {'Fixture':<30} {'Match?'}")
print("-" * 80)

for row in cursor.fetchall():
    gw, pgs_fix_id, opp_id, fix_id, h_code, a_code, home, away = row
    fixture = f"{home or 'NULL'} vs {away or 'NULL'}" if home and away else "NO MATCH"
    match = "✅" if fix_id else "❌"
    print(f"{gw:<4} {pgs_fix_id:<10} {opp_id:<7} {str(fix_id) if fix_id else 'NULL':<10} {fixture:<30} {match}")

# Check matching stats
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(f.id) as matched
    FROM player_gameweek_stats pgs
    LEFT JOIN fixtures f ON pgs.fixture_id = f.id AND pgs.season = f.season
    WHERE pgs.season = '2025-26';
""")

total, matched = cursor.fetchone()
print(f"\nTotal player_gameweek_stats: {total}")
print(f"Matched with fixtures: {matched} ({matched/total*100:.1f}%)")

if matched < total:
    print(f"\n⚠️  {total - matched} records don't match fixtures!")
    
    cursor.execute("""
        SELECT DISTINCT pgs.fixture_id
        FROM player_gameweek_stats pgs
        LEFT JOIN fixtures f ON pgs.fixture_id = f.id AND pgs.season = f.season
        WHERE pgs.season = '2025-26'
          AND f.id IS NULL
        LIMIT 10;
    """)
    
    print("\nUnmatched fixture_ids in player_gameweek_stats:")
    for row in cursor.fetchall():
        print(f"  - {row[0]}")
else:
    print("\n✅ All records match! Ready to run migration!")

cursor.close()
conn.close()