from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

"""
DAG: fpl_refresh_materialized_views

Purpose:
    Refreshes materialized views to keep aggregated data up-to-date.
    This ensures fast query performance for common queries.
    
Schedule:
    Runs daily after gameweek data is loaded (suggested: 4 AM daily)
    
What it does:
    1. Refreshes mv_top_scorers (season-long stats)
    2. Refreshes mv_player_form (last 5 gameweeks)
    3. Refreshes mv_team_fixture_difficulty (fixture difficulty ratings)
    4. Runs ANALYZE to update query planner statistics
"""

default_args = {
    "owner": "richardilemon",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_refresh_materialized_views",
    default_args=default_args,
    description="Refresh materialized views for optimal query performance",
    schedule_interval="0 4 * * *",  # Run daily at 4 AM
    catchup=False,
    tags=["fpl", "database", "maintenance"],
)
def fpl_refresh_materialized_views():
    """
    Refreshes all materialized views to keep aggregated data current.
    Run this daily after your ETL pipelines complete.
    """

    # Refresh top scorers view
    refresh_top_scorers = PostgresOperator(
        task_id="refresh_top_scorers",
        postgres_conn_id="fpl_db_conn",
        sql="""
            REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_scorers;
        """,
    )

    # Refresh player form view
    refresh_player_form = PostgresOperator(
        task_id="refresh_player_form",
        postgres_conn_id="fpl_db_conn",
        sql="""
            REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_form;
        """,
    )

    # Refresh fixture difficulty view
    refresh_fixture_difficulty = PostgresOperator(
        task_id="refresh_fixture_difficulty",
        postgres_conn_id="fpl_db_conn",
        sql="""
            REFRESH MATERIALIZED VIEW CONCURRENTLY mv_team_fixture_difficulty;
        """,
    )

    # Update table statistics for query planner
    analyze_tables = PostgresOperator(
        task_id="analyze_tables",
        postgres_conn_id="fpl_db_conn",
        sql="""
            ANALYZE fixtures;
            ANALYZE game_weeks;
            ANALYZE players;
            ANALYZE player_gameweek_stats;
            ANALYZE teams;
            ANALYZE teams_history;
        """,
    )

    # Define task dependencies - refresh views in parallel, then analyze
    [refresh_top_scorers, refresh_player_form, refresh_fixture_difficulty] >> analyze_tables


fpl_refresh_materialized_views()