from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from scripts.utils.fetch_api_data import fetch_data_from_api
from scripts.load_players import fetch_players_data, load_players


# Default arguments
default_args = {
    "owner": "richardilemon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_players_etl",
    default_args=default_args,
    description="ETL Pipeline for Fantasy Premier League Data",
    schedule_interval="@daily",
    catchup=False,
)
def fpl_players_etl():
    check_players_api = HttpSensor(
        task_id="check_players_api",
        http_conn_id="fpl_base_api",
        endpoint="api/bootstrap-static/",
        headers={"User-Agent": "Mozilla/5.0"},
        poke_interval=5,
        timeout=10,
        mode="poke",
    )

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
                position TEXT,
                now_cost INTEGER,
                status TEXT
            );
        """,
    )

    @task()
    def fetch_players_task(season, category):
        return fetch_data_from_api(season, category)

    @task()
    def load_players_task(players):
        return load_players(players)

    # Define dependencies using TaskFlow API chaining

    players_info_fn = fetch_players_task(season=None, category="elements")
    load_players_info_fn = load_players_task(players=players_info_fn)

    (check_players_api >> create_players_table >> load_players_info_fn)


fpl_etl_pipeline = fpl_players_etl()