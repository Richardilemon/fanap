from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
import sys
sys.path.append('/app')

# Import main functions from scripts
from load_teams_history import main as load_teams_history_main
from load_teams_live import main as load_teams_live_main
from load_fixtures import main as load_fixtures_main
from load_gameweeks import main as load_gameweeks_main
from load_players import main as load_players_main
from load_player_gameweek_stats import main as load_player_gameweek_stats_main

def load_teams_task():
    """Wrapper to run both load_teams_history and load_teams_live."""
    base_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    load_teams_history_main(base_url)
    load_teams_live_main()  # No season parameter needed (uses default)

default_args = {
    'owner': 'richardilemon',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 24),
}

dag = DAG(
    'fpl_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Fantasy Premier League Data',
    schedule_interval='@daily',
    catchup=False,
)

# Define tasks
load_teams = PythonOperator(
    task_id='load_teams',
    python_callable=load_teams_task,
    dag=dag,
)

load_fixtures = PythonOperator(
    task_id='load_fixtures',
    python_callable=load_fixtures_main,
    dag=dag,
)

load_gameweeks = PythonOperator(
    task_id='load_gameweeks',
    python_callable=load_gameweeks_main,
    dag=dag,
)

load_players = PythonOperator(
    task_id='load_players',
    python_callable=load_players_main,
    dag=dag,
)

load_player_gameweek_stats = PythonOperator(
    task_id='load_player_gameweek_stats',
    python_callable=load_player_gameweek_stats_main,
    dag=dag,
)

# Set task dependencies
load_teams >> load_fixtures >> load_gameweeks >> load_players >> load_player_gameweek_stats