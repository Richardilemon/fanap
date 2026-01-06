"""
Copy historical team data from teams_history to teams table
So the migration can find all seasons
"""
from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()

conn = get_db_connection()
cursor = conn.cursor()

print("=" * 70)
print("📋 COPYING TEAMS_HISTORY → TEAMS")
print("=" * 70)

# Check what's in teams_history
cursor.execute("""
    SELECT season, COUNT(*) 
    FROM teams_history 
    GROUP BY season 
    ORDER BY season;
""")

print("\n🔍 teams_history contents:")
for season, count in cursor.fetchall():
    print(f"   {season}: {count} teams")

# Check what's in teams
cursor.execute("""
    SELECT season, COUNT(*) 
    FROM teams 
    GROUP BY season 
    ORDER BY season;
""")

print("\n🔍 teams contents (before):")
for season, count in cursor.fetchall():
    print(f"   {season}: {count} teams")

print("\n📥 Copying historical seasons to teams table...")

# Insert from teams_history, skip conflicts (current season already in teams)
cursor.execute("""
    INSERT INTO teams (
        season, code, id, name, short_name,
        strength_overall_home, strength_overall_away,
        strength_attack_home, strength_attack_away,
        strength_defence_home, strength_defence_away,
        strength
    )
    SELECT 
        season, code, id, name, short_name,
        strength_overall_home, strength_overall_away,
        strength_attack_home, strength_attack_away,
        strength_defence_home, strength_defence_away,
        strength
    FROM teams_history
    ON CONFLICT (season, code) DO NOTHING;
""")

inserted = cursor.rowcount
conn.commit()

print(f"✅ Inserted {inserted} historical team records")

# Verify
cursor.execute("""
    SELECT season, COUNT(*) 
    FROM teams 
    GROUP BY season 
    ORDER BY season;
""")

print("\n🔍 teams contents (after):")
for season, count in cursor.fetchall():
    print(f"   {season}: {count} teams")

print("\n" + "=" * 70)
print("✅ DONE! Now run: python migrate_add_team_names.py")
print("=" * 70)

cursor.close()
conn.close()