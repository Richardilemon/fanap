import logging
import requests
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# def get_db_connection():  # fetch_teams_data
#     try:
#         pg_hook = PostgresHook(postgres_conn_id="fpl_db_conn")
#         _connection = pg_hook.get_conn()
#         # _cursor = _connection.cursor()

#         logging.info("Database Connection Established")

#     except Exception as e:
#         logging.error(f"Failed to connect to database: {e}")
#         raise


def db_connection_wrapper(func):
    """
    Decorator to manage PostgreSQL database connections using Airflow's PostgresHook.

    This decorator simplifies database interaction by automatically handling the
    creation and closure of PostgreSQL connections for any function that interacts
    with the database. It ensures that a valid connection object is passed to the
    wrapped function and that any connection-related exceptions are properly logged.

    The decorator retrieves the connection information from the Airflow
    connection identified by `fpl_db_conn`.

    Parameters
    ----------
    func : function
        The target function that requires a PostgreSQL connection.
        It must accept a `connection` parameter as its first argument.

    Returns
    -------
    function
        A wrapped version of the input function that:
            1. Establishes a PostgreSQL connection before execution.
            2. Passes the connection object to the wrapped function.
            3. Logs connection establishment and closure events.
            4. Handles and logs connection-related exceptions gracefully.

    Raises
    ------
    Exception
        Any exception raised during connection establishment or function execution
        will be logged and re-raised to ensure Airflow retries or fails the task properly.
    """

    def wrapper(*args, **kwargs):  # fetch_teams_data
        try:
            pg_hook = PostgresHook(postgres_conn_id="fpl_db_conn")
            connection = pg_hook.get_conn()
            # _cursor = _connection.cursor()

            logging.info("Database Connection Established")

            result = func(connection, *args, **kwargs)  # fetch_teams_data(_connection)

            return result

            connection.close()
            logging.info("Database Connection Closed")

        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    return wrapper
