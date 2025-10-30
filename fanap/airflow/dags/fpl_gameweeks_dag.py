from airflow.decorators import task, dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from scripts.load_fixtures import parse_fixtures
from scripts.load_gameweeks import (
    load_gameweeks,
    parse_gameweeks,
)
from scripts.load_player_gameweek_stats import get_players_folder
from airflow.models import Variable
from scripts.utils.fetch_api_data import fetch_data_from_api
from scripts.utils.infer_season import infer_season, SEASONS
from scripts.load_player_gameweek_stats import (
    parse_players_seasons,
    parse_gw_stats_table,
    load_player_gameweek_stats
)


CURRENT_SEASON = infer_season()


# Default arguments
default_args = {
    "owner": "richardilemon",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 4, 24),
}


@dag(
    dag_id="fpl_gameweeks_etl",
    default_args=default_args,
    description="ETL Pipeline for Fantasy Premier League Data",
    schedule_interval="@daily",
    catchup=False,
)
def fpl_gameweeks_etl():
    """ETL Pipeline for Fantasy Premier League Gameweeks Data"""

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
    def fetch_gameweek_task(season):
        return fetch_data_from_api(season)

    create_gw_stats_table = PostgresOperator(
        task_id="create_gw_stats_table",
        postgres_conn_id="fpl_db_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS player_gameweek_stats (
                season TEXT,
                gameweek INTEGER,
                player_name TEXT,
                # team_code INTEGER,
                fixture INTEGER,
                opponent_team INTEGER,
                goals_scored INTEGER,
                assists INTEGER,
                clean_sheets INTEGER,
                goals_conceded INTEGER,
                own_goals INTEGER,
                penalties_saved INTEGER,
                penalties_missed INTEGER,
                red_cards INTEGER,
                yellow_cards INTEGER,
                big_chances_missed INTEGER,
                big_chances_created INTEGER,
                clearance_blocks_interceptions INTEGER,
                completed_passes INTEGER,
                dribbles INTEGER,
                errors_leading_to_goal INTEGER,
                fouls INTEGER,
                key_passes INTEGER,
                open_play_crosses INTEGER,
                was_home BOOLEAN,
                winning_goals INTEGER,
                PRIMARY KEY (season, gameweek, player_name)
            );
        """,
    )

    @task()
    def fetch_players_stats_task(
        season, endpoint=Variable.get("PLAYERS_DATASET_PATH"), player_folder=None
    ):
        return fetch_data_from_api(
            season, category=None, endpoint=endpoint, player_folder=player_folder
        )

    @task()
    def extract_players_folder(players):
        return get_players_folder(players)

    @task()
    def fetch_players_gw_task(
        season, player_folder, endpoint=Variable.get("PLAYER_GW_DATASET_PATH")
    ):
        return fetch_data_from_api(
            season, category=None, endpoint=endpoint, player_folder=player_folder
        )

    @task()
    def parse_players_seasons_stats(players_gw_stats):
        return parse_players_seasons(players_gw_stats)

    @task()
    def fetch_fixtures_task(season):
        return fetch_data_from_api(season)

    @task()
    def parse_fixtures_task(fixtures_data):
        return parse_fixtures(fixtures_data)

    @task()
    def parse_gw_stats_table_task(gw_data, fixtures):
        return parse_gw_stats_table(gw_data, fixtures)

    @task()
    def load_player_gameweek_stats_task(records):
        return load_player_gameweek_stats(records)

    @task()
    def parse_gameweeks_task(gameweeks_data):
        return parse_gameweeks(gameweeks_data)

    @task
    def load_gameweeks_task(gameweeks):
        return load_gameweeks(gameweeks)
    

    gameweeks_fn = fetch_gameweek_task.expand(season=SEASONS)
    teams_gameweeks_parsed = parse_gameweeks_task.expand(gameweeks_data=gameweeks_fn)
    load_parsed_gw = load_gameweeks_task(teams_gameweeks_parsed)

    players_task_fn = fetch_players_stats_task.expand(season=SEASONS)
    parse_players_season_stats_fn = parse_players_seasons_stats(players_task_fn)

    get_players_folder_fn = extract_players_folder(
        players=parse_players_season_stats_fn
    )

    fetch_players_gw_fn = fetch_players_gw_task.partial(
        player_folder=get_players_folder_fn
    ).expand(
        season=SEASONS
    )  # NOTE: ONLY THE 2018-19 SEASON DATA (for Players) IS FULLY AVAILABLE ON THE API PROVIDED, AND SOME ACROSS THE OTHER SEASONS.

    fixtures_fn = fetch_fixtures_task.expand(season=SEASONS)
    # get_match_code = match_code_task(fixtures_fn)
    parsed_fixtures = parse_fixtures_task.expand(fixtures_data=fixtures_fn)

    parse_gw_stats_table_task_fn = parse_gw_stats_table_task(
        fetch_players_gw_fn, parsed_fixtures
    )

    # parse_gw_stats_table_task_fn = parse_gw_stats_table_task(
    #     gw_data=fetch_players_gw_fn, fixtures=fixtures_fn
    # )
    load_player_gameweek_stats_task_fn = load_player_gameweek_stats_task(parse_gw_stats_table_task_fn)


    (
    create_gameweeks_table
    >> gameweeks_fn
    >> create_gw_stats_table
    >> players_task_fn
    >> parse_players_season_stats_fn
    >> get_players_folder_fn
    >> parse_gw_stats_table_task_fn
    )

    # Continue remaining links as needed
    parse_gw_stats_table_task_fn >> load_player_gameweek_stats_task_fn

# invoke the pipeline
gameweeks = fpl_gameweeks_etl()
