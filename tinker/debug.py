"""Debug script to check tuple size"""
import sys
sys.path.insert(0, '.')

from scripts.load_player_gameweek_stats import parse_gw_stats_table

# Sample data
sample_data = [{
    'round': '1',
    'kickoff_time': '2025-08-16T11:30:00Z',
    'player_name': 'Test Player',
    'value': '100',
    'fixture': '1',
    'opponent_team': '2',
    'was_home': 'False',
    'team_h_score': '2',
    'team_a_score': '1',
    'minutes': '90',
    'starts': 'True',
    'total_points': '6',
    'bonus': '0',
    'bps': '25',
    'goals_scored': '1',
    'assists': '0',
    'clean_sheets': '0',
    'goals_conceded': '1',
    'own_goals': '0',
    'penalties_saved': '0',
    'penalties_missed': '0',
    'red_cards': '0',
    'yellow_cards': '0',
    'saves': '0',
    'expected_goals': '0.5',
    'expected_assists': '0.2',
    'expected_goal_involvements': '0.7',
    'expected_goals_conceded': '1.0',
    'influence': '50.0',
    'creativity': '20.0',
    'threat': '30.0',
    'ict_index': '10.0',
    'tackles': '2',
    'recoveries': '5',
    'selected': '1000000',
    'transfers_in': '50000',
    'transfers_out': '30000',
    'transfers_balance': '20000',
}]

parsed = parse_gw_stats_table(sample_data)

if parsed:
    print(f"✅ Tuple created with {len(parsed[0])} values")
    print("\nTuple values:")
    for i, val in enumerate(parsed[0], 1):
        print(f"  {i}. {val}")
    
    # Count placeholders in SQL
    sql_columns = """
    season, gameweek, player_name, fixture_id, opponent_team, was_home,
    team_h_score, team_a_score, minutes, starts, kickoff_time,
    total_points, bonus, bps,
    goals_scored, assists, clean_sheets, goals_conceded, own_goals,
    penalties_saved, penalties_missed, penalties_scored, red_cards, yellow_cards, saves,
    expected_goals, expected_assists, expected_goal_involvements, expected_goals_conceded,
    influence, creativity, threat, ict_index,
    big_chances_missed, big_chances_created, clearances_blocks_interceptions,
    completed_passes, dribbles, errors_leading_to_goal, fouls, key_passes,
    open_play_crosses, tackles, recoveries, winning_goals,
    selected, transfers_in, transfers_out, transfers_balance, value
    """
    
    column_count = len([c.strip() for c in sql_columns.split(',') if c.strip()])
    print(f"\n📊 SQL expects {column_count} columns")
    print(f"📦 Tuple has {len(parsed[0])} values")
    
    if column_count != len(parsed[0]):
        print(f"\n❌ MISMATCH! Difference: {column_count - len(parsed[0])}")
    else:
        print(f"\n✅ MATCH!")
else:
    print("❌ No parsed data")