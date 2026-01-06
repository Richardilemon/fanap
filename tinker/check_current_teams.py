from dotenv import load_dotenv
from scripts.utils.db_config import get_db_connection

load_dotenv()

conn = get_db_connection()
cursor = conn.cursor()

cursor.execute("""
    SELECT code, name, short_name
    FROM teams
    WHERE season = '2025-26'
    ORDER BY name;
""")

print("Current teams in your database (2025-26):")
print("-" * 60)
for code, name, short_name in cursor.fetchall():
    print(f"{code:3d}  {name:<25} ({short_name})")

cursor.close()
conn.close()
