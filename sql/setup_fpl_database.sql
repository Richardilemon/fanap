-- ============================================================================
-- FPL DATABASE SCHEMA FOR NEON
-- ============================================================================

-- Create fixtures table
CREATE TABLE IF NOT EXISTS fixtures (
    code INTEGER PRIMARY KEY,
    season TEXT,
    gameweek INTEGER,
    kickoff_time TIMESTAMP WITH TIME ZONE,
    team_h_code INTEGER,
    team_a_code INTEGER,
    team_h_score INTEGER,
    team_a_score INTEGER,
    team_h_difficulty INTEGER,
    team_a_difficulty INTEGER
);

-- Create game_weeks table
CREATE TABLE IF NOT EXISTS game_weeks (
    season TEXT,
    gameweek INTEGER,
    deadline TIMESTAMP,
    PRIMARY KEY (season, gameweek)
);

-- Create players table
CREATE TABLE IF NOT EXISTS players (
    code INTEGER PRIMARY KEY,
    first_name TEXT,
    second_name TEXT,
    web_name TEXT,
    team_code INTEGER,
    position TEXT,
    now_cost INTEGER,
    status TEXT
);

-- Create player_gameweek_stats table
CREATE TABLE IF NOT EXISTS player_gameweek_stats (
    season TEXT,
    gameweek INTEGER,
    player_name TEXT,
    player_cost DECIMAL(4,1),
    fixture_id INTEGER,
    opponent_team INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    red_cards INTEGER,
    yellow_cards INTEGER,
    big_chances_missed INTEGER,
    big_chances_created INTEGER,
    clearance_blocks_interceptions INTEGER,
    completed_passes INTEGER,
    dribbles INTEGER,
    errors_leading_to_goal INTEGER,
    fouls INTEGER,
    key_passes INTEGER,
    open_play_crosses INTEGER,
    was_home BOOLEAN,
    winning_goals INTEGER,
    PRIMARY KEY (season, gameweek, player_name)
);

-- Create teams table
CREATE TABLE IF NOT EXISTS teams (
    season TEXT,
    code INTEGER,
    id INTEGER,
    name TEXT,
    short_name TEXT,
    strength_overall_home INTEGER,
    strength_overall_away INTEGER,
    strength_attack_home INTEGER,
    strength_attack_away INTEGER,
    strength_defence_home INTEGER,
    strength_defence_away INTEGER,
    strength INTEGER,
    PRIMARY KEY (season, code)
);

-- Create teams_history table
CREATE TABLE IF NOT EXISTS teams_history(
    season TEXT,
    code INTEGER,
    id INTEGER,
    name TEXT,
    short_name TEXT,
    strength_overall_home INTEGER,
    strength_overall_away INTEGER,
    strength_attack_home INTEGER,
    strength_attack_away INTEGER,
    strength_defence_home INTEGER,
    strength_defence_away INTEGER,
    strength INTEGER,
    position INTEGER,
    PRIMARY KEY (season, code)
);