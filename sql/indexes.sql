-- ============================================================================
-- CRITICAL INDEXES FOR FPL DATABASE
-- Run this after initial schema setup
-- ============================================================================

-- FIXTURES TABLE
-- Most common: Filter by season, team, gameweek
CREATE INDEX IF NOT EXISTS idx_fixtures_season_gameweek 
ON fixtures(season, gameweek);

CREATE INDEX IF NOT EXISTS idx_fixtures_team_h 
ON fixtures(team_h_code, season);

CREATE INDEX IF NOT EXISTS idx_fixtures_team_a 
ON fixtures(team_a_code, season);

CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff 
ON fixtures(kickoff_time);

-- Composite index for upcoming fixtures
CREATE INDEX IF NOT EXISTS idx_fixtures_upcoming 
ON fixtures(season, kickoff_time) 
WHERE team_h_score IS NULL;  -- Only unplayed fixtures


-- GAME_WEEKS TABLE
-- Most common: Filter by season, find current/next gameweek
CREATE INDEX IF NOT EXISTS idx_gameweeks_season 
ON game_weeks(season);

CREATE INDEX IF NOT EXISTS idx_gameweeks_deadline 
ON game_weeks(deadline);

-- Find current gameweek (deadline just passed)
CREATE INDEX IF NOT EXISTS idx_gameweeks_current 
ON game_weeks(season, deadline DESC);


-- TEAMS TABLE
CREATE INDEX IF NOT EXISTS idx_teams_season 
ON teams(season);

CREATE INDEX IF NOT EXISTS idx_teams_code_season 
ON teams(code, season);

CREATE INDEX IF NOT EXISTS idx_teams_name 
ON teams(name);


-- PLAYERS TABLE
-- Most common: Filter by team, position, price
CREATE INDEX IF NOT EXISTS idx_players_team 
ON players(team_code);

CREATE INDEX IF NOT EXISTS idx_players_position 
ON players(position);

CREATE INDEX IF NOT EXISTS idx_players_price 
ON players(now_cost);

CREATE INDEX IF NOT EXISTS idx_players_status 
ON players(status);

-- Search by name
CREATE INDEX IF NOT EXISTS idx_players_name 
ON players(LOWER(first_name || ' ' || second_name));

-- Find available players in price range
CREATE INDEX IF NOT EXISTS idx_players_available 
ON players(position, now_cost) 
WHERE status = 'a';


-- PLAYER_GAMEWEEK_STATS TABLE (MOST IMPORTANT!)
-- This table gets queried heavily - needs good indexes

-- Primary lookups
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_season_player 
ON player_gameweek_stats(season, player_name);

CREATE INDEX IF NOT EXISTS idx_player_gw_stats_season_gw 
ON player_gameweek_stats(season, gameweek);

-- Player performance queries
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_goals 
ON player_gameweek_stats(season, goals_scored DESC) 
WHERE goals_scored > 0;

CREATE INDEX IF NOT EXISTS idx_player_gw_stats_assists 
ON player_gameweek_stats(season, assists DESC) 
WHERE assists > 0;

CREATE INDEX IF NOT EXISTS idx_player_gw_stats_clean_sheets 
ON player_gameweek_stats(season, clean_sheets) 
WHERE clean_sheets > 0;

-- Penalty takers (after adding penalties_scored column)
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_penalties 
ON player_gameweek_stats(season, player_name) 
WHERE penalties_scored > 0 OR penalties_missed > 0;

-- Recent form (last 5 gameweeks)
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_recent 
ON player_gameweek_stats(player_name, season, gameweek DESC);


-- PENALTY_TAKERS TABLE
CREATE INDEX IF NOT EXISTS idx_penalty_takers_team_season 
ON penalty_takers(team_code, season);

CREATE INDEX IF NOT EXISTS idx_penalty_takers_player 
ON penalty_takers(player_name, season);

CREATE INDEX IF NOT EXISTS idx_penalty_takers_priority 
ON penalty_takers(season, priority);


-- TEAMS_HISTORY TABLE
CREATE INDEX IF NOT EXISTS idx_teams_history_season 
ON teams_history(season);

CREATE INDEX IF NOT EXISTS idx_teams_history_code_season 
ON teams_history(code, season);


-- ============================================================================
-- ANALYZE TABLES (Update statistics for query planner)
-- ============================================================================

ANALYZE fixtures;
ANALYZE game_weeks;
ANALYZE teams;
ANALYZE players;
ANALYZE player_gameweek_stats;
ANALYZE penalty_takers;
ANALYZE teams_history;

-- ============================================================================
-- VERIFY INDEXES
-- ============================================================================

-- Check all indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;