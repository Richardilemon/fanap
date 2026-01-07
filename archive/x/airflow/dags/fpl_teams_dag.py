from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

from scripts.load_teams_live import load_teams, fetch_teams_data
from scripts.load_teams_live import infer_season
from airflow.sensors.http_sensor import HttpSensor
from scripts.utils.fetch_api_data import fetch_data_from_api

CURRENT_SEASON = infer_season()

# Default arguments
default_args = {
    "owner": "richardilemon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_teams_etl",
    default_args=default_args,
    description="ETL Pipeline for Fantasy Premier League Teams",
    schedule_interval="@daily",
    catchup=False,
)
def fpl_teams_etl():
    """
    DAG: fpl_teams_dag

    Description:
        This Airflow DAG performs a daily ETL (Extract, Transform, Load) process
        for Fantasy Premier League (FPL) teams data. It connects to the FPL API,
        validates API availability, fetches live team data, and loads it into a
        PostgreSQL database.

    Workflow:
        1. Check if the FPL teams API endpoint is accessible.
        2. Create the `teams` table in PostgreSQL if it doesn't exist.
        3. Fetch team data from the FPL API.
        4. Insert or update the fetched data in the database.

    Dependencies:
        - Airflow PostgreSQL Provider
        - Airflow HTTP Provider
        - Custom scripts in `scripts/load_teams_live.py`
    """

    # Verify the FPL teams API is available before proceeding
    check_teams_api = HttpSensor(
        task_id="check_teams_api",
        http_conn_id="fpl_base_api",
        endpoint="api/bootstrap-static/",
        poke_interval=5,
        timeout=10,
        mode="poke",
    )

    # Create the 'teams' table in PostgreSQL if it doesn't already exist
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

    # Fetch live team data from the FPL API
    @task()
    def fetch_teams_task(season, category):
        """
        Extract data from the FPL teams API.

        Returns:
            list: A list of dictionaries containing team data
        """
        return fetch_data_from_api(season, category)

    # Load the fetched team data into the PostgreSQL database
    @task()
    def load_teams_task(teams):
        """
        Load teams data into the PostgreSQL database.

        Args:
            teams (list): List of team records fetched from the FPL API.
        """
        return load_teams(teams, CURRENT_SEASON)

    fetch_teams = fetch_teams_task(season=None, category="teams")
    load_teams_fn = load_teams_task(teams=fetch_teams)

    check_teams_api >> create_teams_table >> load_teams_fn


fpl_teams_etl()
