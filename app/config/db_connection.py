import psycopg2
from ..config.db_config import Config

class DBConnection:
    _connection = None

    @classmethod
    def get_connection(cls):
        if cls._connection is None:
            credentials = Config.get_db_credentials()
            cls._connection = psycopg2.connect(
                user=credentials['user'],
                password=credentials['password'],
                host=credentials['host'],
                port=credentials['port'],
                database=credentials['database']
            )
        return cls._connection

    @classmethod
    def close_connection(cls):
        if cls._connection:
            cls._connection.close()
            cls._connection = None
