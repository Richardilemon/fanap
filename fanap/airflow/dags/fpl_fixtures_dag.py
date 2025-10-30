from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from scripts.utils.infer_season import infer_season, SEASONS
from airflow.sensors.http_sensor import HttpSensor
from scripts.load_fixtures import (
    parse_fixtures,
    load_fixtures,
)

from scripts.utils.fetch_api_data import fetch_data_from_api
from airflow.models import Variable


""" 
This DAG automates the end-to-end ETL process for ingesting Fantasy Premier League
fixtures data into a PostgreSQL database. It ensures that the fixtures data for
the current and upcoming seasons are fetched, parsed, and stored daily.

**Pipeline Overview**
1. ✅ **Check API Availability** – Confirms that the FPL fixtures endpoint is reachable.
2. 🧱 **Create Table (if not exists)** – Ensures the target `fixtures` table exists in Postgres.
3. 🌐 **Fetch Fixtures Data** – Downloads raw fixture data for all seasons defined in `SEASONS`.
4. 🧩 **Parse Fixtures Data** – Cleans, transforms, and formats data into a structured list of tuples.
5. 💾 **Load Fixtures into Postgres** – Inserts the parsed fixtures data into the `fixtures` table.

**Key Features**
- Uses Airflow's TaskFlow API for readability and maintainability.
- Employs dynamic task mapping to process multiple FPL seasons concurrently.
- Includes retry logic and failure resilience for production readiness.

"""
# Determine the current FPL season dynamically
CURRENT_SEASON = infer_season()


# Default arguments
# These arguments apply to all tasks unless overridden individually.

default_args = {
    "owner": "richardilemon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_fixtures_pipeline",
    default_args=default_args,
    description="ETL Pipeline for Fantasy Premier League Data",
    schedule_interval="@daily",
    catchup=False,
)
def fpl_fixtures_pipeline():
    """DAG that orchestrates the ETL process for loading FPL fixtures."""

    # Check if the Fixtures API endpoint is available
    check_fixtures_data_url = HttpSensor(
        task_id="check_fixtures_data_url",
        http_conn_id="fixtures_base_path",
        endpoint=f"{CURRENT_SEASON}/{Variable.get('FIXTURES_DATASET_PATH')}",
        poke_interval=5,
        timeout=20,
        mode="reschedule",
    )

    # Create the 'fixtures' table in Postgres (if it doesn't exist)
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

    # Fetch fixtures data for each season dynamically
    @task()
    def fetch_fixtures_task(season):
        return fetch_data_from_api(season)

    # Parse and clean fetched fixtures data
    @task()
    def parse_fixtures_task(fixtures_data):
        return parse_fixtures(fixtures_data)

    # Load parsed data into the PostgreSQL 'fixtures' table
    @task()
    def load_fixtures_task(fixtures):
        return load_fixtures(fixtures)

    # DAG Dependency
    fixtures_fn = fetch_fixtures_task.expand(season=SEASONS)
    parsed_fixtures = parse_fixtures_task.expand(fixtures_data=fixtures_fn)
    load_fixtures_to_db = load_fixtures_task(parsed_fixtures)

    # Define the overall pipeline flow

    (
        check_fixtures_data_url
        >> create_fixtures_table
        >> fixtures_fn
        >> parsed_fixtures
        >> load_fixtures_to_db
    )

# invoke the pipeline
fpl_fixtures_pipeline()
