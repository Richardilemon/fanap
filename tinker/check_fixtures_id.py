from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'), sslmode='require')
cursor = conn.cursor()

print("Checking fixtures id column...\n")

cursor.execute("""
    SELECT id, code, season, gameweek, team_h_code, team_a_code
    FROM fixtures
    WHERE season = '2025-26'
    ORDER BY gameweek
    LIMIT 50;
""")

print(f"{'ID':<8} {'Code':<10} {'Season':<10} {'GW':<5} {'Home':<5} {'Away':<5}")
print("-" * 50)

for row in cursor.fetchall():
    fixture_id, code, season, gw, h, a = row
    print(f"{str(fixture_id):<8} {code:<10} {season:<10} {gw or 'NULL':<5} {h:<5} {a:<5}")

cursor.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(id) as with_id,
        SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_count
    FROM fixtures
    WHERE season = '2025-26';
""")

total, with_id, nulls = cursor.fetchone()
print(f"\nTotal: {total}, With ID: {with_id}, NULL IDs: {nulls}")

cursor.close()
conn.close()