"""
Verify comprehensive player gameweek stats data
"""
from dotenv import load_dotenv
load_dotenv()

from scripts.utils.db_config import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

cursor.execute('''
    SELECT 
        gameweek,
        player_team_name,
        opponent_team_name,
        total_points,
        minutes,
        goals_scored,
        assists,
        bonus,
        expected_goals,
        influence,
        creativity,
        threat,
        selected,
        transfers_in,
        value
    FROM player_gameweek_stats 
    WHERE player_name = 'Alexander Isak' 
      AND season = '2025-26'
    ORDER BY gameweek;
''')

print('='*120)
print('ALEXANDER ISAK - 2025-26 SEASON - COMPREHENSIVE DATA')
print('='*120)
print(f'{"GW":<4} {"Team":<12} {"vs":<12} {"Pts":<4} {"Min":<4} {"G":<2} {"A":<2} {"Bon":<4} {"xG":<5} {"Inf":<5} {"Cre":<5} {"Thr":<5} {"Own":<8} {"TI":<7} {"Val":<5}')
print('-'*120)

for row in cursor.fetchall():
    gw, team, opp, pts, mins, g, a, bon, xg, inf, cre, thr, sel, ti, val = row
    print(f'{gw:<4} {(team or "?"):<12} {(opp or "?"):<12} {pts:<4} {mins:<4} {g:<2} {a:<2} {bon:<4} {xg:<5.2f} {inf:<5.1f} {cre:<5.1f} {thr:<5.1f} {sel:<8} {ti:<7} {val/10.0:<5.1f}')

print('='*120)

# Check data coverage
cursor.execute('''
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT gameweek) as gameweeks,
        MIN(gameweek) as first_gw,
        MAX(gameweek) as last_gw,
        COUNT(CASE WHEN total_points > 0 THEN 1 END) as with_points,
        COUNT(CASE WHEN player_team_name IS NOT NULL THEN 1 END) as with_team_names
    FROM player_gameweek_stats
    WHERE season = '2025-26';
''')

stats = cursor.fetchone()
print(f'\nDATA COVERAGE FOR 2025-26:')
print(f'  Total Records: {stats[0]:,}')
print(f'  Gameweeks: {stats[1]} (GW{stats[2]}-{stats[3]})')
print(f'  Records with points: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)')
print(f'  Records with team names: {stats[5]:,} ({stats[5]/stats[0]*100:.1f}%)')

cursor.close()
conn.close()

print('\n✅ Verification complete!')