from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from scripts.load_gameweeks import (
    combine_gameweeks,
    fetch_gameweek_for_season,
    load_gameweeks,
    parse_gameweeks,
)
from scripts.load_teams_live import fetch_teams_data, load_teams
from scripts.load_teams_live import infer_season
from airflow.sensors.http_sensor import HttpSensor
import logging
from airflow.models import Variable
from scripts.load_gameweeks import SEASONS

CURRENT_SEASON = infer_season()


# Default arguments
default_args = {
    "owner": "richardilemon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_etl_pipeline",
    default_args=default_args,
    description="ETL Pipeline for Fantasy Premier League Data",
    schedule_interval="@daily",
    catchup=False,
)
def fpl_etl_pipeline():
    """ETL Pipeline for Fantasy Premier League Data"""

    # check_teams_api = HttpSensor(
    #     task_id="check_teams_api",
    #     http_conn_id="fpl_base_api",
    #     endpoint="api/bootstrap-static/",
    #     poke_interval=5,
    #     timeout=20,
    #     mode="reschedule",
    # )

    # Create Teams Table
    create_teams_table = PostgresOperator(
        task_id="create_teams_table",
        postgres_conn_id="fpl_db_conn",
        sql="""
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
        """,
    )

    # Fetch Teams Task (returns list of teams)
    @task()
    def fetch_teams_task():
        teams = fetch_teams_data()
        return teams

    # Insert Teams Task (receives teams from previous)
    @task()
    def load_teams_task(teams):
        load_teams(teams, CURRENT_SEASON)

    # create load_fixtures table
    # load_fixtures_table

    check_fixtures_data_url = HttpSensor(
        task_id="check_fixtures_data_url",
        http_conn_id="fixtures_base_path",
        endpoint=f"{CURRENT_SEASON}/{Variable.get('FIXTURES_DATASET_PATH')}",
        poke_interval=5,
        timeout=20,
        mode="reschedule",
    )

    """Create the game_weeks table."""
    create_gameweeks_table = PostgresOperator(
        task_id="create_gameweeks_table",
        postgres_conn_id="fpl_db_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS game_weeks (
                season TEXT,
                gameweek INTEGER,
                deadline TIMESTAMP,
                PRIMARY KEY (season, gameweek)
            );
        """,
    )

    @task()
    def load_gameweek_task(season):
        return fetch_gameweek_for_season(season)

    @task()
    def parse_gameweeks_task(gameweeks_data):
        return parse_gameweeks(gameweeks_data)

    @task()
    def combine_gameweeks_task(results):
        return combine_gameweeks(results)

    @task
    def populate_gameweeks(gameweeks):
        return load_gameweeks(gameweeks)

    # create_fixtures_table = PostgresOperator(
    #     task_id="create_fixtures_table",
    #     postgres_conn_id="fpl_db_conn",
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS fixtures (
    #             code INTEGER PRIMARY KEY,
    #             season TEXT,
    #             gameweek INTEGER,
    #             kickoff_time TIMESTAMP WITH TIME ZONE,
    #             team_h_code INTEGER,
    #             team_a_code INTEGER,
    #             team_h_score INTEGER,
    #             team_a_score INTEGER,
    #             team_h_difficulty INTEGER,
    #             team_a_difficulty INTEGER
    #         );
    #     """,
    # )

    # Define dependencies using TaskFlow API chaining

    teams = fetch_teams_task()

    gameweeks_fn = load_gameweek_task.expand(season=SEASONS)
    teams_gameweeks_parsed = parse_gameweeks_task.expand(gameweeks_data=gameweeks_fn)
    combined_gameweeks = combine_gameweeks_task(teams_gameweeks_parsed)
    populate_gameweeks(combined_gameweeks)

    (
        [
            (create_teams_table >> load_teams_task(teams))
            # (create_teams_table >> check_teams_api >> load_teams_task(teams))
            >> check_fixtures_data_url
            >> create_gameweeks_table
            >> gameweeks_fn
            # >> teams_gameweeks_parsed
            # >> combine_gameweeks_task
            # >> populate_gameweeks(combined_fixtures)
        ]
        # >> create_fixtures_table
        # >>
        # >> create_gameweeks_table
        # >> gameweeks
        # >> teams_gameweeks_parsed
        # >> combined_fixtures
        # >> populate_gameweeks(combined_fixtures)
    )


fpl_etl_pipeline = fpl_etl_pipeline()

# Define tasks
# load_teams = PythonOperator(
#     task_id="load_teams",
#     python_callable=load_teams_task,
#     dag=dag,
# )

# load_fixtures = PythonOperator(
#     task_id="load_fixtures",
#     python_callable=load_fixtures_main,
#     dag=dag,
# )

# load_gameweeks = PythonOperator(
#     task_id="load_gameweeks",
#     python_callable=load_gameweeks_main,
#     dag=dag,
# )

# load_players = PythonOperator(
#     task_id="load_players",
#     python_callable=load_players_main,
#     dag=dag,
# )

# load_player_gameweek_stats = PythonOperator(
#     task_id="load_player_gameweek_stats",
#     python_callable=load_player_gameweek_stats_main,
#     dag=dag,
# )


# db_conn = PythonOperator(
#     task_id="db_connection_test",
#     python_callable=get_db_connection,
#     dag=dag,
# )


# create_teams_table = PostgresOperator(
#     task_id="create_teams_table",
#     postgres_conn_id="fpl_db_conn",
#     sql="./scripts/sql/create_teams_table.sql",
#     dag=dag,
# )

# create_team_table = PythonOperator(
#     task_id="create_teams_table",
#     python_callable=create_teams_table,
#     dag=dag,
# )


# insert_teams(conn, teams, season)


# Set task dependencies
# load_teams >> load_fixtures >> load_gameweeks >> load_players >> load_player_gameweek_stats
