# from airflow import DAG
# from airflow.decorators import task, dag
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from datetime import datetime, timedelta
# from scripts.load_gameweeks import (
#     load_gameweeks,
#     parse_gameweeks,
# )
# from scripts.utils.fetch_api_data import fetch_data_from_api
# from scripts.utils.infer_season import infer_season, SEASONS
# from airflow.models import Variable

# CURRENT_SEASON = infer_season()


# # Default arguments
# default_args = {
#     "owner": "richardilemon",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
#     "start_date": datetime(2025, 4, 24),
# }


# @dag(
#     dag_id="fpl_player_gw_stats_etl",
#     default_args=default_args,
#     description="ETL Pipeline for Fantasy Premier League Data",
#     schedule_interval="@daily",
#     catchup=False,
# )
# def fpl_player_gw_stats_etl():
#     """ETL Pipeline for Fantasy Premier League Gameweeks Data"""

#     @task()
#     def fetch_players_task(season, endpoint=Variable.get("PLAYERS_DATASET_PATH")):
#         fetch_data_from_api(season)

#     """Create the game_weeks table."""
#     create_gameweeks_table = PostgresOperator(
#         task_id="create_gameweeks_table",
#         postgres_conn_id="fpl_db_conn",
#         sql="""
#             CREATE TABLE IF NOT EXISTS game_weeks (
#                 season TEXT,
#                 gameweek INTEGER,
#                 deadline TIMESTAMP,
#                 PRIMARY KEY (season, gameweek)
#             );
#         """,
#     )

#     @task()
#     def fetch_gameweek_task(season):
#         return fetch_data_from_api(season)

#     @task()
#     def parse_gameweeks_task(gameweeks_data):
#         return parse_gameweeks(gameweeks_data)

#     @task
#     def load_gameweeks_task(gameweeks):
#         return load_gameweeks(gameweeks)

#     gameweeks_fn = fetch_gameweek_task.expand(season=SEASONS)
#     teams_gameweeks_parsed = parse_gameweeks_task.expand(gameweeks_data=gameweeks_fn)
#     load_parsed_gw = load_gameweeks_task(teams_gameweeks_parsed)

#     (create_gameweeks_table >> gameweeks_fn >> teams_gameweeks_parsed >> load_parsed_gw)


# fpl_etl_pipeline = fpl_gameweeks_etl()
