from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from scripts.load_gameweeks import (
    load_gameweeks,
    parse_gameweeks,
)
from scripts.load_teams_live import load_teams, fetch_teams_data

# from scripts.load_teams_live import fetch_teams_data, load_teams
from scripts.load_teams_live import infer_season
from airflow.sensors.http_sensor import HttpSensor
import logging
from airflow.models import Variable
from scripts.load_fixtures import SEASONS
from scripts.load_fixtures import (
    fetch_data_from_api,
    parse_fixtures,
    load_fixtures,
    combine_data_from_api,
)
from scripts.load_players import fetch_players_data, load_players

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
    #     timeout=10,
    #     mode="poke",
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

    # check_fixtures_data_url = HttpSensor(
    #     task_id="check_fixtures_data_url",
    #     http_conn_id="fixtures_base_path",
    #     endpoint=f"{CURRENT_SEASON}/{Variable.get('FIXTURES_DATASET_PATH')}",
    #     poke_interval=5,
    #     timeout=20,
    #     mode="reschedule",
    # )

    create_fixtures_table = PostgresOperator(
        task_id="create_fixtures_table",
        postgres_conn_id="fpl_db_conn",
        sql="""
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
            );""",
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
    def fetch_fixtures_task(season):
        return fetch_data_from_api(season)

    @task()
    def parse_fixtures_task(fixtures_data):
        return parse_fixtures(fixtures_data, season=SEASONS)

    @task()
    def populate_fixtures(fixtures):
        return load_fixtures(fixtures)

    @task()
    def combine_fixtures_task(results):
        return combine_data_from_api(results)

    @task()
    def load_gameweek_task(season):
        return fetch_data_from_api(season)

    @task()
    def parse_gameweeks_task(gameweeks_data):
        return parse_gameweeks(gameweeks_data)

    @task()
    def combine_gameweeks_task(results):
        return combine_data_from_api(results)

    @task
    def populate_gameweeks(gameweeks):
        return load_gameweeks(gameweeks)

    create_players_table = PostgresOperator(
        task_id="create_players_table",
        postgres_conn_id="fpl_db_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS players (
                code INTEGER PRIMARY KEY,
                first_name TEXT,
                second_name TEXT,
                web_name TEXT,
                team_code INTEGER,
                type_name TEXT,
                now_cost INTEGER,
                status TEXT
            );
        """,
    )

    @task()
    def fetch_players_task():
        return fetch_players_data()

    @task()
    def load_players_task(players):
        return load_players(players)

    @task()
    def combine_players_task(results):
        return combine_data_from_api(results)

    # Define dependencies using TaskFlow API chaining

    teams = fetch_teams_task()

    fixtures_fn = fetch_fixtures_task.expand(season=SEASONS)
    parsed_fixtures = parse_fixtures_task.expand(fixtures_data=fixtures_fn)
    combined_fixtures = combine_fixtures_task(parsed_fixtures)
    populate_fixtures_fn = populate_fixtures(combined_fixtures)

    gameweeks_fn = load_gameweek_task.expand(season=SEASONS)
    teams_gameweeks_parsed = parse_gameweeks_task.expand(gameweeks_data=gameweeks_fn)
    combined_gameweeks = combine_gameweeks_task(teams_gameweeks_parsed)
    populate_gameweeks_fn = populate_gameweeks(combined_gameweeks)

    players_info_fn = fetch_players_task()
    load_players_info_fn = load_players_task(players=players_info_fn)
    # combined_gameweeks = combine_gameweeks_task(teams_gameweeks_parsed)
    # populate_gameweeks_fn = populate_gameweeks(combined_gameweeks)

    (
        [
            # (check_teams_api >> create_teams_table >> load_teams_task(teams))
            (create_teams_table >> load_teams_task(teams))
            >> (create_fixtures_table)
            # >> (check_fixtures_data_url >> create_fixtures_table)
            >> fixtures_fn
            >> parsed_fixtures
            >> combined_fixtures
            >> populate_fixtures_fn
            >> create_gameweeks_table
            >> gameweeks_fn
            >> teams_gameweeks_parsed
            >> combined_gameweeks
            >> populate_gameweeks_fn
            >> create_players_table
            >> load_players_info_fn
        ]
    )


# load_teams >> load_fixtures >> load_gameweeks >> load_players >> load_player_gameweek_stats


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
