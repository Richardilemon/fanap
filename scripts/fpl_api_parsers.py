"""
FPL API Parsers - COMPREHENSIVE VERSION
Transform FPL API JSON responses into database-ready formats with ALL available fields
"""
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def parse_fpl_teams(bootstrap_data: Dict) -> List[Tuple]:
    """Parse teams from bootstrap-static data"""
    if not bootstrap_data or 'teams' not in bootstrap_data:
        logger.warning("No teams data in bootstrap")
        return []
    
    teams = []
    season = "2025-26"
    
    for team in bootstrap_data['teams']:
        teams.append((
            team['id'],
            team['code'],
            team['name'],
            team.get('short_name', team['name'][:3].upper()),
            season
        ))
    
    logger.info(f"Parsed {len(teams)} teams")
    return teams


def parse_fpl_players(bootstrap_data: Dict) -> List[Tuple]:
    """Parse players from bootstrap-static data"""
    if not bootstrap_data or 'elements' not in bootstrap_data:
        logger.warning("No players data in bootstrap")
        return []
    
    players = []
    season = "2025-26"
    
    for player in bootstrap_data['elements']:
        cost = player['now_cost'] / 10.0
        
        players.append((
            player['id'],
            player['first_name'],
            player['second_name'],
            player['web_name'],
            player['team'],
            player['element_type'],
            cost,
            player['total_points'],
            season
        ))
    
    logger.info(f"Parsed {len(players)} players")
    return players


def parse_fpl_fixtures(fixtures_data: List[Dict]) -> List[Tuple]:
    """Parse fixtures from fixtures endpoint"""
    if not fixtures_data:
        logger.warning("No fixtures data provided")
        return []
    
    fixtures = []
    season = "2025-26"
    
    for fixture in fixtures_data:
        kickoff = fixture.get('kickoff_time')
        team_h_score = fixture.get('team_h_score')
        team_a_score = fixture.get('team_a_score')
        
        fixtures.append((
            fixture['id'],
            fixture['code'],
            season,
            fixture.get('event'),
            kickoff,
            fixture['team_h'],
            fixture['team_a'],
            team_h_score,
            team_a_score,
            fixture.get('team_h_difficulty'),
            fixture.get('team_a_difficulty')
        ))
    
    logger.info(f"Parsed {len(fixtures)} fixtures")
    return fixtures


def safe_int(value, default=0):
    """Safely convert value to int"""
    try:
        if value is None or value == '':
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        if value is None or value == '':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def parse_fpl_player_gameweek_stats_with_names(
    player_stats: List[Dict], 
    player_name_map: Dict[int, str]
) -> List[Tuple]:
    """
    Parse player gameweek stats with proper player names - COMPREHENSIVE VERSION
    Collects ALL available fields from FPL API
    """
    if not player_stats:
        logger.warning("No player stats data provided")
        return []
    
    all_records = []
    season = "2025-26"
    skipped = 0
    
    for player_data in player_stats:
        player_id = player_data.get('player_id')
        history = player_data.get('history', [])
        
        player_name = player_name_map.get(player_id)
        
        if not player_name:
            logger.warning(f"No name found for player_id {player_id}, skipping")
            skipped += 1
            continue
        
        for gw_stat in history:
            # Convert cost from FPL format
            cost = safe_int(gw_stat.get('value')) / 10.0
            
            # Build comprehensive record tuple
            record = (
                # Core identification
                season,                                          # season
                safe_int(gw_stat.get('round')),                  # gameweek
                player_name,                                     # player_name
                safe_int(gw_stat.get('fixture')),                # fixture_id
                safe_int(gw_stat.get('opponent_team')),          # opponent_team
                gw_stat.get('was_home', False),                  # was_home
                
                # Match context
                safe_int(gw_stat.get('team_h_score')),           # team_h_score
                safe_int(gw_stat.get('team_a_score')),           # team_a_score
                safe_int(gw_stat.get('minutes')),                # minutes
                bool(gw_stat.get('starts', 0)),                    # starts (boolean in API)
                gw_stat.get('kickoff_time', ''),                 # kickoff_time
                
                # FPL scoring
                safe_int(gw_stat.get('total_points')),           # total_points ✅
                safe_int(gw_stat.get('bonus')),                  # bonus ✅
                safe_int(gw_stat.get('bps')),                    # bps ✅
                
                # Performance stats
                safe_int(gw_stat.get('goals_scored')),           # goals_scored
                safe_int(gw_stat.get('assists')),                # assists
                safe_int(gw_stat.get('clean_sheets')),           # clean_sheets
                safe_int(gw_stat.get('goals_conceded')),         # goals_conceded
                safe_int(gw_stat.get('own_goals')),              # own_goals
                safe_int(gw_stat.get('penalties_saved')),        # penalties_saved
                safe_int(gw_stat.get('penalties_missed')),       # penalties_missed
                0,  # penalties_scored (not directly in API)
                safe_int(gw_stat.get('red_cards')),              # red_cards
                safe_int(gw_stat.get('yellow_cards')),           # yellow_cards
                safe_int(gw_stat.get('saves')),                  # saves ✅
                
                # Expected stats
                safe_float(gw_stat.get('expected_goals')),       # expected_goals ✅
                safe_float(gw_stat.get('expected_assists')),     # expected_assists ✅
                safe_float(gw_stat.get('expected_goal_involvements')),  # expected_goal_involvements ✅
                safe_float(gw_stat.get('expected_goals_conceded')),     # expected_goals_conceded ✅
                
                # ICT metrics
                safe_float(gw_stat.get('influence')),            # influence ✅
                safe_float(gw_stat.get('creativity')),           # creativity ✅
                safe_float(gw_stat.get('threat')),               # threat ✅
                safe_float(gw_stat.get('ict_index')),            # ict_index ✅
                
                # Detailed performance (NOT in basic FPL API)
                0,  # big_chances_missed
                0,  # big_chances_created
                0,  # clearances_blocks_interceptions
                0,  # completed_passes
                0,  # dribbles
                0,  # errors_leading_to_goal
                0,  # fouls
                0,  # key_passes
                0,  # open_play_crosses
                0,  # tackles
                0,  # recoveries
                0,  # winning_goals
                
                # Ownership/transfers
                safe_int(gw_stat.get('selected')),               # selected ✅
                safe_int(gw_stat.get('transfers_in')),           # transfers_in ✅
                safe_int(gw_stat.get('transfers_out')),          # transfers_out ✅
                safe_int(gw_stat.get('transfers_balance')),      # transfers_balance ✅
                safe_int(gw_stat.get('value')),                  # value ✅
            )
            
            all_records.append(record)
    
    logger.info(f"Parsed {len(all_records)} player gameweek records ({skipped} players skipped)")
    return all_records


def create_player_name_map(bootstrap_data: Dict) -> Dict[int, str]:
    """Create a mapping of player_id to full name from bootstrap data"""
    if not bootstrap_data or 'elements' not in bootstrap_data:
        return {}
    
    name_map = {}
    for player in bootstrap_data['elements']:
        full_name = f"{player['first_name']} {player['second_name']}"
        name_map[player['id']] = full_name
    
    logger.info(f"Created name map for {len(name_map)} players")
    return name_map


# Test function
if __name__ == '__main__':
    print("=" * 70)
    print("🧪 TESTING FPL API PARSERS (COMPREHENSIVE)")
    print("=" * 70)
    
    from fpl_api_client import FPLAPIClient
    
    client = FPLAPIClient()
    
    print("\n1️⃣ Testing team parser...")
    bootstrap = client.get_bootstrap_static()
    if bootstrap:
        teams = parse_fpl_teams(bootstrap)
        print(f"   ✅ Parsed {len(teams)} teams")
    
    print("\n2️⃣ Testing player parser...")
    if bootstrap:
        players = parse_fpl_players(bootstrap)
        print(f"   ✅ Parsed {len(players)} players")
    
    print("\n3️⃣ Testing player name map...")
    if bootstrap:
        name_map = create_player_name_map(bootstrap)
        print(f"   ✅ Created map for {len(name_map)} players")
        print(f"   Sample: ID 499 = {name_map.get(499)}")
    
    print("\n4️⃣ Testing player gameweek stats parser...")
    print("   Fetching stats for player ID 499 (Isak)...")
    player_summary = client.get_player_summary(499)
    if player_summary and 'history' in player_summary:
        test_stats = [{'player_id': 499, 'history': player_summary['history']}]
        parsed = parse_fpl_player_gameweek_stats_with_names(test_stats, name_map)
        print(f"   ✅ Parsed {len(parsed)} gameweek records")
        if parsed:
            print(f"   Sample record has {len(parsed[0])} fields")
            print(f"   GW1 total_points: {parsed[0][11]}")  # total_points is 12th field
            print(f"   GW1 minutes: {parsed[0][8]}")        # minutes is 9th field
    
    print("\n" + "=" * 70)
    print("✅ COMPREHENSIVE PARSERS TEST COMPLETE")
    print("=" * 70)