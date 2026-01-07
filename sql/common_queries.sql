-- ============================================================================
-- FPL ASSISTANT - COMMON QUERIES REFERENCE
-- ============================================================================
-- Quick reference for the most commonly needed queries in your FPL Assistant
-- All queries are optimized with the indexes we created
-- ============================================================================

-- ============================================================================
-- PLAYER DISCOVERY QUERIES
-- ============================================================================

-- 1. Top scorers under a specific price in last N gameweeks
SELECT 
    player_name,
    AVG(player_cost) as avg_cost,
    SUM(goals_scored) as total_goals,
    SUM(assists) as total_assists,
    COUNT(*) as games_played,
    ROUND(SUM(goals_scored)::numeric / COUNT(*), 2) as goals_per_game
FROM player_gameweek_stats
WHERE season = '2024-25' 
  AND gameweek >= (SELECT MAX(gameweek) - 5 FROM game_weeks WHERE season = '2024-25')
  AND player_cost < 8.0
GROUP BY player_name
HAVING SUM(goals_scored) > 0
ORDER BY total_goals DESC
LIMIT 20;

-- 2. Best value players (most points per million)
SELECT 
    player_name,
    AVG(player_cost) as cost,
    SUM(goals_scored * 6 + assists * 3 + clean_sheets * 4) as total_points,
    ROUND(SUM(goals_scored * 6 + assists * 3 + clean_sheets * 4)::numeric / AVG(player_cost), 2) as points_per_million
FROM player_gameweek_stats
WHERE season = '2024-25'
GROUP BY player_name
ORDER BY points_per_million DESC
LIMIT 20;

-- 3. In-form players (best last 5 gameweeks)
SELECT * 
FROM mv_player_form
WHERE season = '2024-25'
ORDER BY (goals_last_5 * 6 + assists_last_5 * 3 + clean_sheets_last_5 * 4) DESC
LIMIT 20;

-- 4. Consistent performers (scored in most gameweeks)
SELECT 
    player_name,
    COUNT(*) as total_games,
    COUNT(CASE WHEN goals_scored > 0 THEN 1 END) as games_scored,
    SUM(goals_scored) as total_goals,
    ROUND(COUNT(CASE WHEN goals_scored > 0 THEN 1 END)::numeric / COUNT(*) * 100, 1) as scoring_percentage
FROM player_gameweek_stats
WHERE season = '2024-25'
GROUP BY player_name
HAVING COUNT(*) >= 10  -- Played at least 10 games
ORDER BY scoring_percentage DESC, total_goals DESC
LIMIT 20;

-- ============================================================================
-- POSITION-SPECIFIC QUERIES
-- ============================================================================

-- 5. Best budget defenders (under £5m with most clean sheets)
SELECT 
    p.web_name,
    p.now_cost / 10.0 as cost,
    SUM(pgs.clean_sheets) as total_clean_sheets,
    COUNT(*) as games_played,
    SUM(pgs.goals_scored) as bonus_goals,
    SUM(pgs.assists) as bonus_assists
FROM player_gameweek_stats pgs
JOIN players p ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
WHERE pgs.season = '2024-25'
  AND p.position = 'DEF'
  AND p.now_cost < 50  -- Under £5m (stored as 50 = £5.0m)
  AND p.status = 'a'   -- Available
GROUP BY p.web_name, p.now_cost
HAVING COUNT(*) >= 5  -- Played at least 5 games
ORDER BY total_clean_sheets DESC, bonus_goals DESC
LIMIT 15;

-- 6. Premium midfielders with goals + assists
SELECT 
    p.web_name,
    p.now_cost / 10.0 as cost,
    SUM(pgs.goals_scored) as goals,
    SUM(pgs.assists) as assists,
    SUM(pgs.goals_scored) + SUM(pgs.assists) as goal_contributions,
    COUNT(*) as games_played
FROM player_gameweek_stats pgs
JOIN players p ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
WHERE pgs.season = '2024-25'
  AND p.position = 'MID'
  AND p.now_cost >= 90  -- Premium (£9m+)
  AND p.status = 'a'
GROUP BY p.web_name, p.now_cost
HAVING COUNT(*) >= 5
ORDER BY goal_contributions DESC
LIMIT 15;

-- 7. Forward options with best goals-per-game
SELECT 
    p.web_name,
    p.now_cost / 10.0 as cost,
    COUNT(*) as games_played,
    SUM(pgs.goals_scored) as total_goals,
    ROUND(SUM(pgs.goals_scored)::numeric / COUNT(*), 2) as goals_per_game,
    SUM(pgs.big_chances_missed) as big_chances_missed
FROM player_gameweek_stats pgs
JOIN players p ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
WHERE pgs.season = '2024-25'
  AND p.position = 'FWD'
  AND p.status = 'a'
GROUP BY p.web_name, p.now_cost
HAVING COUNT(*) >= 5
ORDER BY goals_per_game DESC, total_goals DESC
LIMIT 15;

-- ============================================================================
-- TEAM ANALYSIS QUERIES
-- ============================================================================

-- 8. Team form (goals scored/conceded in last 5 games)
SELECT 
    t.name,
    COUNT(DISTINCT f.gameweek) as games_played,
    SUM(CASE WHEN f.team_h_code = t.code THEN f.team_h_score ELSE f.team_a_score END) as goals_for,
    SUM(CASE WHEN f.team_h_code = t.code THEN f.team_a_score ELSE f.team_h_score END) as goals_against,
    ROUND(AVG(CASE WHEN f.team_h_code = t.code THEN f.team_h_score ELSE f.team_a_score END), 2) as avg_goals_for,
    ROUND(AVG(CASE WHEN f.team_h_code = t.code THEN f.team_a_score ELSE f.team_h_score END), 2) as avg_goals_against
FROM fixtures f
JOIN teams t ON (f.team_h_code = t.code OR f.team_a_code = t.code) AND f.season = t.season
WHERE f.season = '2024-25'
  AND f.gameweek >= (SELECT MAX(gameweek) - 5 FROM fixtures WHERE season = '2024-25')
  AND f.team_h_score IS NOT NULL  -- Only completed fixtures
GROUP BY t.name
ORDER BY avg_goals_for DESC;

-- 9. Upcoming fixtures difficulty (next 5 gameweeks)
SELECT 
    t.short_name,
    STRING_AGG(
        opp.short_name || ' (' || 
        CASE WHEN f.team_h_code = t.code THEN 'H' ELSE 'A' END || 
        ', GW' || f.gameweek || ')',
        ', ' ORDER BY f.gameweek
    ) as next_5_fixtures,
    AVG(CASE WHEN f.team_h_code = t.code THEN f.team_h_difficulty ELSE f.team_a_difficulty END) as avg_difficulty
FROM fixtures f
JOIN teams t ON (f.team_h_code = t.code OR f.team_a_code = t.code) AND f.season = t.season
JOIN teams opp ON (
    CASE WHEN f.team_h_code = t.code THEN f.team_a_code ELSE f.team_h_code END = opp.code
    AND f.season = opp.season
)
WHERE f.season = '2024-25'
  AND f.gameweek BETWEEN 
    (SELECT MIN(gameweek) FROM fixtures WHERE season = '2024-25' AND team_h_score IS NULL) 
    AND 
    (SELECT MIN(gameweek) FROM fixtures WHERE season = '2024-25' AND team_h_score IS NULL) + 4
GROUP BY t.short_name
ORDER BY avg_difficulty ASC;

-- 10. Best defensive teams (clean sheets + goals conceded)
SELECT 
    t.name,
    COUNT(*) as games_played,
    SUM(CASE 
        WHEN (f.team_h_code = t.code AND f.team_a_score = 0) OR 
             (f.team_a_code = t.code AND f.team_h_score = 0) 
        THEN 1 ELSE 0 
    END) as clean_sheets,
    SUM(CASE WHEN f.team_h_code = t.code THEN f.team_a_score ELSE f.team_h_score END) as goals_conceded,
    ROUND(AVG(CASE WHEN f.team_h_code = t.code THEN f.team_a_score ELSE f.team_h_score END), 2) as avg_goals_conceded
FROM fixtures f
JOIN teams t ON (f.team_h_code = t.code OR f.team_a_code = t.code) AND f.season = t.season
WHERE f.season = '2024-25'
  AND f.team_h_score IS NOT NULL
GROUP BY t.name
ORDER BY clean_sheets DESC, avg_goals_conceded ASC;

-- ============================================================================
-- HEAD-TO-HEAD & DIFFERENTIAL QUERIES
-- ============================================================================

-- 11. Differential picks (low ownership but high points)
-- NOTE: Ownership data would come from a separate table if you track it
-- This query shows players with high points from non-top-6 teams
SELECT 
    pgs.player_name,
    p.team_code,
    t.name as team_name,
    AVG(pgs.player_cost) as cost,
    SUM(pgs.goals_scored * 6 + pgs.assists * 3 + pgs.clean_sheets * 4) as total_points
FROM player_gameweek_stats pgs
JOIN players p ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
JOIN teams t ON p.team_code = t.code AND pgs.season = t.season
WHERE pgs.season = '2024-25'
  AND t.code NOT IN (SELECT code FROM teams WHERE season = '2024-25' AND position <= 6)  -- Not top 6 teams
GROUP BY pgs.player_name, p.team_code, t.name
HAVING SUM(pgs.goals_scored * 6 + pgs.assists * 3 + pgs.clean_sheets * 4) >= 30  -- At least 30 points
ORDER BY total_points DESC
LIMIT 20;

-- 12. Player vs specific opponent (historical performance)
SELECT 
    pgs.season,
    pgs.gameweek,
    pgs.player_name,
    t.name as opponent,
    pgs.goals_scored,
    pgs.assists,
    pgs.was_home,
    CASE WHEN pgs.was_home THEN 'Home' ELSE 'Away' END as venue
FROM player_gameweek_stats pgs
JOIN teams t ON pgs.opponent_team = t.code AND pgs.season = t.season
WHERE pgs.player_name = 'Mohamed Salah'  -- Replace with desired player
  AND t.short_name = 'MUN'  -- Replace with desired opponent
ORDER BY pgs.season DESC, pgs.gameweek DESC;

-- ============================================================================
-- CAPTAINCY & FIXTURE ANALYSIS
-- ============================================================================

-- 13. Best captaincy options for next gameweek
WITH next_gw AS (
    SELECT MIN(gameweek) as gw
    FROM fixtures 
    WHERE season = '2024-25' AND team_h_score IS NULL
)
SELECT 
    pgs.player_name,
    AVG(pgs.player_cost) as cost,
    -- Recent form (last 5 GWs)
    SUM(CASE WHEN pgs.gameweek >= (SELECT gw FROM next_gw) - 5 
        THEN pgs.goals_scored ELSE 0 END) as goals_last_5,
    -- Upcoming fixture difficulty
    MIN(f.team_h_difficulty) as fixture_difficulty,
    -- Home/Away
    CASE WHEN MIN(f.team_h_code) = MAX(p.team_code) THEN 'Home' ELSE 'Away' END as venue,
    -- Opponent
    STRING_AGG(DISTINCT t_opp.short_name, ', ') as opponent
FROM player_gameweek_stats pgs
JOIN players p ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
JOIN fixtures f ON f.season = '2024-25' 
    AND f.gameweek = (SELECT gw FROM next_gw)
    AND (f.team_h_code = p.team_code OR f.team_a_code = p.team_code)
JOIN teams t_opp ON (
    CASE WHEN f.team_h_code = p.team_code THEN f.team_a_code ELSE f.team_h_code END = t_opp.code
    AND f.season = t_opp.season
)
WHERE pgs.season = '2024-25'
  AND p.status = 'a'
GROUP BY pgs.player_name, p.team_code
HAVING SUM(CASE WHEN pgs.gameweek >= (SELECT gw FROM next_gw) - 5 
    THEN pgs.goals_scored ELSE 0 END) >= 2  -- At least 2 goals in last 5
ORDER BY goals_last_5 DESC, fixture_difficulty ASC
LIMIT 15;

-- 14. Double gameweek players
-- (Identify players with 2+ fixtures in a gameweek)
SELECT 
    t.name as team_name,
    p.web_name,
    p.position,
    f.gameweek,
    COUNT(*) as num_fixtures,
    STRING_AGG(t_opp.short_name, ' & ' ORDER BY f.kickoff_time) as opponents
FROM fixtures f
JOIN teams t ON (f.team_h_code = t.code OR f.team_a_code = t.code) AND f.season = t.season
JOIN players p ON p.team_code = t.code AND p.status = 'a'
JOIN teams t_opp ON (
    CASE WHEN f.team_h_code = t.code THEN f.team_a_code ELSE f.team_h_code END = t_opp.code
    AND f.season = t_opp.season
)
WHERE f.season = '2024-25'
  AND f.team_h_score IS NULL  -- Upcoming fixtures
  AND f.gameweek = 25  -- Replace with target gameweek
GROUP BY t.name, p.web_name, p.position, f.gameweek
HAVING COUNT(*) > 1  -- Double (or more) gameweek
ORDER BY t.name, p.position;

-- ============================================================================
-- INJURY & AVAILABILITY TRACKING
-- ============================================================================

-- 15. Check player availability
SELECT 
    web_name,
    position,
    t.name as team,
    now_cost / 10.0 as cost,
    status,
    CASE 
        WHEN status = 'a' THEN 'Available'
        WHEN status = 'd' THEN 'Doubtful'
        WHEN status = 'i' THEN 'Injured'
        WHEN status = 'u' THEN 'Unavailable'
        WHEN status = 's' THEN 'Suspended'
        ELSE status
    END as status_description
FROM players p
JOIN teams t ON p.team_code = t.code AND t.season = '2024-25'
WHERE p.status != 'a'  -- Not available
ORDER BY p.position, t.name;

-- ============================================================================
-- ADVANCED ANALYTICS
-- ============================================================================

-- 16. Players overperforming expected (high goals despite low big chances)
SELECT 
    player_name,
    SUM(goals_scored) as total_goals,
    SUM(big_chances_missed) as big_chances_missed,
    ROUND(SUM(goals_scored)::numeric / NULLIF(SUM(big_chances_missed), 0), 2) as conversion_rate
FROM player_gameweek_stats
WHERE season = '2024-25'
GROUP BY player_name
HAVING SUM(goals_scored) >= 5 AND SUM(big_chances_missed) > 0
ORDER BY conversion_rate DESC
LIMIT 20;

-- 17. Assist providers (key passes + assists correlation)
SELECT 
    player_name,
    AVG(player_cost) as cost,
    SUM(assists) as total_assists,
    SUM(key_passes) as total_key_passes,
    SUM(big_chances_created) as big_chances_created,
    ROUND(SUM(key_passes)::numeric / COUNT(*), 1) as key_passes_per_game
FROM player_gameweek_stats
WHERE season = '2024-25'
GROUP BY player_name
HAVING COUNT(*) >= 5
ORDER BY total_assists DESC, big_chances_created DESC
LIMIT 20;

-- 18. Rotation risk (players who don't start every game)
SELECT 
    p.web_name,
    p.position,
    t.name as team,
    COUNT(pgs.gameweek) as games_appeared,
    (SELECT COUNT(*) FROM fixtures WHERE season = '2024-25' AND team_h_score IS NOT NULL 
     AND (team_h_code = p.team_code OR team_a_code = p.team_code)) as team_games,
    ROUND(COUNT(pgs.gameweek)::numeric / 
        NULLIF((SELECT COUNT(*) FROM fixtures WHERE season = '2024-25' AND team_h_score IS NOT NULL 
         AND (team_h_code = p.team_code OR team_a_code = p.team_code)), 0) * 100, 1) as appearance_percentage
FROM players p
JOIN teams t ON p.team_code = t.code AND t.season = '2024-25'
LEFT JOIN player_gameweek_stats pgs ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
    AND pgs.season = '2024-25'
WHERE p.status = 'a'
  AND p.now_cost >= 60  -- Focus on premium players (£6m+)
GROUP BY p.web_name, p.position, t.name, p.team_code
HAVING COUNT(pgs.gameweek) > 0
ORDER BY appearance_percentage ASC, games_appeared DESC;

-- ============================================================================
-- VERIFICATION & DEBUGGING QUERIES
-- ============================================================================

-- 19. Check data freshness
SELECT 
    'fixtures' as table_name,
    COUNT(*) as total_records,
    MAX(gameweek) as latest_gameweek,
    MAX(kickoff_time) as latest_kickoff
FROM fixtures
WHERE season = '2024-25'
UNION ALL
SELECT 
    'player_gameweek_stats',
    COUNT(*),
    MAX(gameweek),
    NULL
FROM player_gameweek_stats
WHERE season = '2024-25';

-- 20. Identify data quality issues
SELECT 
    'Players with no gameweek stats' as issue,
    COUNT(*) as count
FROM players p
LEFT JOIN player_gameweek_stats pgs 
    ON pgs.player_name = CONCAT(p.first_name, ' ', p.second_name)
    AND pgs.season = '2024-25'
WHERE pgs.player_name IS NULL AND p.status = 'a'
UNION ALL
SELECT 
    'Fixtures with NULL scores in past gameweeks',
    COUNT(*)
FROM fixtures
WHERE season = '2024-25'
  AND gameweek < (SELECT MAX(gameweek) - 1 FROM fixtures WHERE season = '2024-25')
  AND team_h_score IS NULL;