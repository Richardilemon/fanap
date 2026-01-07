-- ============================================================================
-- FPL DATABASE INDEXES FOR QUERY OPTIMIZATION
-- ============================================================================
-- Purpose: Speed up common queries for the FPL Assistant application
-- Run this after all tables are created
-- ============================================================================

-- ============================================================================
-- FIXTURES TABLE INDEXES
-- ============================================================================

-- Index for querying fixtures by season and gameweek (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_fixtures_season_gameweek 
ON fixtures(season, gameweek);

-- Index for querying fixtures by team (home or away)
CREATE INDEX IF NOT EXISTS idx_fixtures_team_h 
ON fixtures(team_h_code);

CREATE INDEX IF NOT EXISTS idx_fixtures_team_a 
ON fixtures(team_a_code);

-- Composite index for team fixtures by season
CREATE INDEX IF NOT EXISTS idx_fixtures_season_team_h 
ON fixtures(season, team_h_code);

CREATE INDEX IF NOT EXISTS idx_fixtures_season_team_a 
ON fixtures(season, team_a_code);

-- Index for filtering fixtures by kickoff time (for upcoming fixtures queries)
CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff 
ON fixtures(kickoff_time);

-- Index for completed fixtures (where scores exist)
CREATE INDEX IF NOT EXISTS idx_fixtures_completed 
ON fixtures(season, gameweek) 
WHERE team_h_score IS NOT NULL AND team_a_score IS NOT NULL;


-- ============================================================================
-- GAME_WEEKS TABLE INDEXES
-- ============================================================================

-- Primary key already creates index on (season, gameweek)
-- Additional index for deadline-based queries
CREATE INDEX IF NOT EXISTS idx_gameweeks_deadline 
ON game_weeks(deadline);

-- Index for finding current/upcoming gameweeks
CREATE INDEX IF NOT EXISTS idx_gameweeks_season_deadline 
ON game_weeks(season, deadline);


-- ============================================================================
-- PLAYERS TABLE INDEXES
-- ============================================================================

-- Index for player name searches (case-insensitive)
CREATE INDEX IF NOT EXISTS idx_players_web_name 
ON players(LOWER(web_name));

CREATE INDEX IF NOT EXISTS idx_players_second_name 
ON players(LOWER(second_name));

-- Index for filtering by team
CREATE INDEX IF NOT EXISTS idx_players_team 
ON players(team_code);

-- Index for filtering by position
CREATE INDEX IF NOT EXISTS idx_players_position 
ON players(position);

-- Composite index for position + cost queries (e.g., "defenders under £5m")
CREATE INDEX IF NOT EXISTS idx_players_position_cost 
ON players(position, now_cost);

-- Index for cost-based filtering
CREATE INDEX IF NOT EXISTS idx_players_cost 
ON players(now_cost);

-- Index for player status (to filter available players)
CREATE INDEX IF NOT EXISTS idx_players_status 
ON players(status);


-- ============================================================================
-- PLAYER_GAMEWEEK_STATS TABLE INDEXES
-- ============================================================================

-- Primary key already creates index on (season, gameweek, player_name)

-- Index for player performance queries across gameweeks
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_player_season 
ON player_gameweek_stats(player_name, season, gameweek);

-- Index for finding top scorers in a specific gameweek
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_gw_goals 
ON player_gameweek_stats(season, gameweek, goals_scored DESC);

-- Index for finding top assisters in a specific gameweek
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_gw_assists 
ON player_gameweek_stats(season, gameweek, assists DESC);

-- Index for player form (recent gameweeks)
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_player_recent 
ON player_gameweek_stats(player_name, season, gameweek DESC);

-- Index for fixture-based queries
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_fixture 
ON player_gameweek_stats(fixture_id);

-- Index for opponent-based analysis
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_opponent 
ON player_gameweek_stats(opponent_team);

-- Index for home/away performance
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_home_away 
ON player_gameweek_stats(was_home);

-- Composite index for value-based queries (e.g., "best performers under £6m")
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_cost_goals 
ON player_gameweek_stats(player_cost, goals_scored DESC);

-- Index for clean sheet analysis
CREATE INDEX IF NOT EXISTS idx_player_gw_stats_clean_sheets 
ON player_gameweek_stats(season, gameweek, clean_sheets DESC);


-- ============================================================================
-- TEAMS TABLE INDEXES
-- ============================================================================

-- Primary key already creates index on (season, code)

-- Index for team name searches
CREATE INDEX IF NOT EXISTS idx_teams_name 
ON teams(LOWER(name));

CREATE INDEX IF NOT EXISTS idx_teams_short_name 
ON teams(LOWER(short_name));

-- Index for team strength queries
CREATE INDEX IF NOT EXISTS idx_teams_strength_overall 
ON teams(season, strength);

-- Composite indexes for attack/defense strength
CREATE INDEX IF NOT EXISTS idx_teams_attack_strength 
ON teams(season, strength_attack_home, strength_attack_away);

CREATE INDEX IF NOT EXISTS idx_teams_defence_strength 
ON teams(season, strength_defence_home, strength_defence_away);


-- ============================================================================
-- TEAMS_HISTORY TABLE INDEXES
-- ============================================================================

-- Primary key already creates index on (season, code)

-- Index for league position queries
CREATE INDEX IF NOT EXISTS idx_teams_history_position 
ON teams_history(season, position);

-- Index for team historical performance
CREATE INDEX IF NOT EXISTS idx_teams_history_code_season 
ON teams_history(code, season);


-- ============================================================================
-- MATERIALIZED VIEWS FOR COMMON AGGREGATIONS (Optional but Recommended)
-- ============================================================================

-- Top scorers by season (refresh daily)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_scorers AS
SELECT 
    season,
    player_name,
    SUM(goals_scored) as total_goals,
    SUM(assists) as total_assists,
    COUNT(*) as appearances,
    AVG(player_cost) as avg_cost
FROM player_gameweek_stats
GROUP BY season, player_name
ORDER BY season DESC, total_goals DESC;

CREATE INDEX IF NOT EXISTS idx_mv_top_scorers_season 
ON mv_top_scorers(season, total_goals DESC);

-- Player form (last 5 gameweeks) - refresh after each gameweek
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_form AS
SELECT 
    pgs.season,
    pgs.player_name,
    SUM(pgs.goals_scored) as goals_last_5,
    SUM(pgs.assists) as assists_last_5,
    SUM(pgs.clean_sheets) as clean_sheets_last_5,
    AVG(pgs.player_cost) as avg_cost
FROM player_gameweek_stats pgs
INNER JOIN (
    SELECT season, MAX(gameweek) as max_gw
    FROM player_gameweek_stats
    GROUP BY season
) latest ON pgs.season = latest.season
WHERE pgs.gameweek > latest.max_gw - 5
GROUP BY pgs.season, pgs.player_name;

CREATE INDEX IF NOT EXISTS idx_mv_player_form_season 
ON mv_player_form(season, goals_last_5 DESC);

-- Team fixture difficulty
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_team_fixture_difficulty AS
SELECT 
    f.season,
    f.gameweek,
    f.team_h_code,
    f.team_a_code,
    f.team_h_difficulty,
    f.team_a_difficulty,
    t_h.name as home_team_name,
    t_a.name as away_team_name
FROM fixtures f
LEFT JOIN teams t_h ON f.team_h_code = t_h.code AND f.season = t_h.season
LEFT JOIN teams t_a ON f.team_a_code = t_a.code AND f.season = t_a.season;

CREATE INDEX IF NOT EXISTS idx_mv_fixture_difficulty_season_gw 
ON mv_team_fixture_difficulty(season, gameweek);


-- ============================================================================
-- MAINTENANCE COMMANDS (Run periodically)
-- ============================================================================

-- Refresh materialized views (run after each gameweek completes)
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_scorers;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_form;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_team_fixture_difficulty;

-- Analyze tables to update statistics (run weekly)
-- ANALYZE fixtures;
-- ANALYZE game_weeks;
-- ANALYZE players;
-- ANALYZE player_gameweek_stats;
-- ANALYZE teams;
-- ANALYZE teams_history;

-- Vacuum tables to reclaim space (run monthly)
-- VACUUM ANALYZE fixtures;
-- VACUUM ANALYZE game_weeks;
-- VACUUM ANALYZE players;
-- VACUUM ANALYZE player_gameweek_stats;
-- VACUUM ANALYZE teams;
-- VACUUM ANALYZE teams_history;


-- ============================================================================
-- QUERY EXAMPLES TO TEST INDEXES
-- ============================================================================

-- Example 1: Top scorers in last 5 gameweeks under £8m
-- EXPLAIN ANALYZE
-- SELECT player_name, SUM(goals_scored) as goals
-- FROM player_gameweek_stats
-- WHERE season = '2024-25' 
--   AND gameweek >= 15 
--   AND player_cost < 8.0
-- GROUP BY player_name
-- ORDER BY goals DESC
-- LIMIT 20;

-- Example 2: Team's upcoming fixtures
-- EXPLAIN ANALYZE
-- SELECT f.*, t_a.name as opponent
-- FROM fixtures f
-- JOIN teams t_a ON f.team_a_code = t_a.code AND f.season = t_a.season
-- WHERE f.season = '2024-25'
--   AND f.team_h_code = 3  -- Arsenal
--   AND f.kickoff_time > NOW()
-- ORDER BY f.kickoff_time
-- LIMIT 5;

-- Example 3: Players with most clean sheets (defenders)
-- EXPLAIN ANALYZE
-- SELECT p.web_name, SUM(pgs.clean_sheets) as total_cs
-- FROM player_gameweek_stats pgs
-- JOIN players p ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
-- WHERE pgs.season = '2024-25'
--   AND p.position = 'DEF'
-- GROUP BY p.web_name
-- ORDER BY total_cs DESC
-- LIMIT 20;