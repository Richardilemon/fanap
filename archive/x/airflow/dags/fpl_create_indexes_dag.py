from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

"""
DAG: fpl_create_indexes

Purpose:
    Creates database indexes and materialized views for optimal query performance.
    This should be run ONCE after all tables are created and populated with initial data.
    
Schedule:
    Manual trigger only (no schedule_interval)
    Re-run if you make schema changes or add new tables.
    
What it does:
    1. Creates indexes on all frequently queried columns
    2. Creates materialized views for common aggregations
    3. Optimizes query performance for the FPL Assistant
"""

default_args = {
    "owner": "richardilemon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_create_indexes",
    default_args=default_args,
    description="Create database indexes for FPL query optimization",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["fpl", "database", "optimization"],
)
def fpl_create_indexes():
    """
    Creates all database indexes and materialized views for optimal performance.
    Run this DAG after initial data load is complete.
    """

    # ========================================================================
    # FIXTURES INDEXES
    # ========================================================================
    
    create_fixtures_indexes = PostgresOperator(
        task_id="create_fixtures_indexes",
        postgres_conn_id="fpl_db_conn",
        sql="""
            -- Season and gameweek queries
            CREATE INDEX IF NOT EXISTS idx_fixtures_season_gameweek 
            ON fixtures(season, gameweek);
            
            -- Team-based queries
            CREATE INDEX IF NOT EXISTS idx_fixtures_team_h 
            ON fixtures(team_h_code);
            
            CREATE INDEX IF NOT EXISTS idx_fixtures_team_a 
            ON fixtures(team_a_code);
            
            -- Composite indexes for team fixtures by season
            CREATE INDEX IF NOT EXISTS idx_fixtures_season_team_h 
            ON fixtures(season, team_h_code);
            
            CREATE INDEX IF NOT EXISTS idx_fixtures_season_team_a 
            ON fixtures(season, team_a_code);
            
            -- Kickoff time queries
            CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff 
            ON fixtures(kickoff_time);
            
            -- Completed fixtures
            CREATE INDEX IF NOT EXISTS idx_fixtures_completed 
            ON fixtures(season, gameweek) 
            WHERE team_h_score IS NOT NULL AND team_a_score IS NOT NULL;
        """,
    )

    # ========================================================================
    # GAMEWEEKS INDEXES
    # ========================================================================
    
    create_gameweeks_indexes = PostgresOperator(
        task_id="create_gameweeks_indexes",
        postgres_conn_id="fpl_db_conn",
        sql="""
            CREATE INDEX IF NOT EXISTS idx_gameweeks_deadline 
            ON game_weeks(deadline);
            
            CREATE INDEX IF NOT EXISTS idx_gameweeks_season_deadline 
            ON game_weeks(season, deadline);
        """,
    )

    # ========================================================================
    # PLAYERS INDEXES
    # ========================================================================
    
    create_players_indexes = PostgresOperator(
        task_id="create_players_indexes",
        postgres_conn_id="fpl_db_conn",
        sql="""
            -- Name searches (case-insensitive)
            CREATE INDEX IF NOT EXISTS idx_players_web_name 
            ON players(LOWER(web_name));
            
            CREATE INDEX IF NOT EXISTS idx_players_second_name 
            ON players(LOWER(second_name));
            
            -- Team filtering
            CREATE INDEX IF NOT EXISTS idx_players_team 
            ON players(team_code);
            
            -- Position filtering
            CREATE INDEX IF NOT EXISTS idx_players_position 
            ON players(position);
            
            -- Position + cost queries
            CREATE INDEX IF NOT EXISTS idx_players_position_cost 
            ON players(position, now_cost);
            
            -- Cost filtering
            CREATE INDEX IF NOT EXISTS idx_players_cost 
            ON players(now_cost);
            
            -- Status filtering
            CREATE INDEX IF NOT EXISTS idx_players_status 
            ON players(status);
        """,
    )

    # ========================================================================
    # PLAYER GAMEWEEK STATS INDEXES
    # ========================================================================
    
    create_player_stats_indexes = PostgresOperator(
        task_id="create_player_stats_indexes",
        postgres_conn_id="fpl_db_conn",
        sql="""
            -- Player performance across gameweeks
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_player_season 
            ON player_gameweek_stats(player_name, season, gameweek);
            
            -- Top scorers in gameweek
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_gw_goals 
            ON player_gameweek_stats(season, gameweek, goals_scored DESC);
            
            -- Top assisters in gameweek
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_gw_assists 
            ON player_gameweek_stats(season, gameweek, assists DESC);
            
            -- Player form (recent gameweeks)
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_player_recent 
            ON player_gameweek_stats(player_name, season, gameweek DESC);
            
            -- Fixture-based queries
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_fixture 
            ON player_gameweek_stats(fixture_id);
            
            -- Opponent analysis
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_opponent 
            ON player_gameweek_stats(opponent_team);
            
            -- Home/away performance
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_home_away 
            ON player_gameweek_stats(was_home);
            
            -- Value-based queries
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_cost_goals 
            ON player_gameweek_stats(player_cost, goals_scored DESC);
            
            -- Clean sheet analysis
            CREATE INDEX IF NOT EXISTS idx_player_gw_stats_clean_sheets 
            ON player_gameweek_stats(season, gameweek, clean_sheets DESC);
        """,
    )

    # ========================================================================
    # TEAMS INDEXES
    # ========================================================================
    
    create_teams_indexes = PostgresOperator(
        task_id="create_teams_indexes",
        postgres_conn_id="fpl_db_conn",
        sql="""
            -- Name searches
            CREATE INDEX IF NOT EXISTS idx_teams_name 
            ON teams(LOWER(name));
            
            CREATE INDEX IF NOT EXISTS idx_teams_short_name 
            ON teams(LOWER(short_name));
            
            -- Strength queries
            CREATE INDEX IF NOT EXISTS idx_teams_strength_overall 
            ON teams(season, strength);
            
            -- Attack strength
            CREATE INDEX IF NOT EXISTS idx_teams_attack_strength 
            ON teams(season, strength_attack_home, strength_attack_away);
            
            -- Defence strength
            CREATE INDEX IF NOT EXISTS idx_teams_defence_strength 
            ON teams(season, strength_defence_home, strength_defence_away);
        """,
    )

    # ========================================================================
    # TEAMS HISTORY INDEXES
    # ========================================================================
    
    create_teams_history_indexes = PostgresOperator(
        task_id="create_teams_history_indexes",
        postgres_conn_id="fpl_db_conn",
        sql="""
            -- League position queries
            CREATE INDEX IF NOT EXISTS idx_teams_history_position 
            ON teams_history(season, position);
            
            -- Historical performance
            CREATE INDEX IF NOT EXISTS idx_teams_history_code_season 
            ON teams_history(code, season);
        """,
    )

    # ========================================================================
    # MATERIALIZED VIEWS
    # ========================================================================
    
    create_top_scorers_view = PostgresOperator(
        task_id="create_top_scorers_view",
        postgres_conn_id="fpl_db_conn",
        sql="""
            DROP MATERIALIZED VIEW IF EXISTS mv_top_scorers CASCADE;
            
            CREATE MATERIALIZED VIEW mv_top_scorers AS
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
            
            CREATE INDEX idx_mv_top_scorers_season 
            ON mv_top_scorers(season, total_goals DESC);
        """,
    )

    create_player_form_view = PostgresOperator(
        task_id="create_player_form_view",
        postgres_conn_id="fpl_db_conn",
        sql="""
            DROP MATERIALIZED VIEW IF EXISTS mv_player_form CASCADE;
            
            CREATE MATERIALIZED VIEW mv_player_form AS
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
            
            CREATE INDEX idx_mv_player_form_season 
            ON mv_player_form(season, goals_last_5 DESC);
        """,
    )

    create_fixture_difficulty_view = PostgresOperator(
        task_id="create_fixture_difficulty_view",
        postgres_conn_id="fpl_db_conn",
        sql="""
            DROP MATERIALIZED VIEW IF EXISTS mv_team_fixture_difficulty CASCADE;
            
            CREATE MATERIALIZED VIEW mv_team_fixture_difficulty AS
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
            
            CREATE INDEX idx_mv_fixture_difficulty_season_gw 
            ON mv_team_fixture_difficulty(season, gameweek);
        """,
    )

    # Define task dependencies
    (
        [
            create_fixtures_indexes,
            create_gameweeks_indexes,
            create_players_indexes,
            create_player_stats_indexes,
            create_teams_indexes,
            create_teams_history_indexes,
        ]
        >> create_top_scorers_view
        >> create_player_form_view
        >> create_fixture_difficulty_view
    )


fpl_create_indexes()