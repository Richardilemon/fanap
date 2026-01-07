from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import the functions from your scripts
from load_teams import load_teams_data
from load_fixtures import load_fixtures_data
from load_gameweeks import load_gameweeks_data
from load_players import load_players_data
from load_player_gameweek_stats import load_player_gameweek_stats_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 24),  # Change this to your desired start date
}

# Define the DAG
dag = DAG(
    'fpl_etl_pipeline', 
    default_args=default_args, 
    description='ETL Pipeline for Fantasy Premier League Data',
    schedule_interval='@daily',  # Can change based on your needs
    catchup=False,  # Avoid running historical tasks
)

# Define the tasks
load_teams = PythonOperator(
    task_id='load_teams',
    python_callable=load_teams_data,
    dag=dag,
)

load_fixtures = PythonOperator(
    task_id='load_fixtures',
    python_callable=load_fixtures_data,
    dag=dag,
)

load_gameweeks = PythonOperator(
    task_id='load_gameweeks',
    python_callable=load_gameweeks_data,
    dag=dag,
)

load_players = PythonOperator(
    task_id='load_players',
    python_callable=load_players_data,
    dag=dag,
)

load_player_gameweek_stats = PythonOperator(
    task_id='load_player_gameweek_stats',
    python_callable=load_player_gameweek_stats_data,
    dag=dag,
)

# Set task dependencies
load_teams >> load_fixtures >> load_gameweeks >> load_players >> load_player_gameweek_stats
