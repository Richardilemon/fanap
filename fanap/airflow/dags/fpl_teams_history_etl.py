from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

from scripts.load_teams_history import SEASONS, parse_team_history, load_teams_history_records
# from airflow.sensors.http_sensor import HttpSensor
from scripts.utils.fetch_api_data import fetch_data_from_api
from airflow.models import Variable
# CURRENT_SEASON = infer_season()

# Default arguments
default_args = {
    "owner": "richardilemon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_teams_history_etl",
    default_args=default_args,
    description="ETL Pipeline for Fantasy Premier League Teams History",
    schedule_interval="@daily",
    catchup=False,
)

def fpl_teams_history_etl():
    
    
    create_teams_history_table = PostgresOperator(
        task_id="create_teams_history_table",
        postgres_conn_id="fpl_db_conn",
        sql="""
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
        """
    )
    
    @task()
    def fetch_teams_history_task(season):
        return fetch_data_from_api(season, category=None, endpoint=Variable.get("FPL_TEAMS_DATAPATH"), player_folder=None)

    
    @task()
    def parse_teams_history_task(teams_history):
        return parse_team_history(teams_history)
    
    @task()
    def load_parsed_teams_history_task(parsed_teams_history):
        return load_teams_history_records(parsed_teams_history)
    
    fetch_teams_history_fn = fetch_teams_history_task.expand(season=SEASONS) 
    parse_teams_history_fn = parse_teams_history_task(fetch_teams_history_fn)
    load_teams_history_records_fn = load_parsed_teams_history_task(parse_teams_history_fn)
    
    create_teams_history_table >> load_teams_history_records_fn
    
fpl_teams_history_etl()