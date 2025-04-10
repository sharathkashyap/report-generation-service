import psycopg2
from ..config.db_config import Config

class DBConnection:
    _connection = None
    _is_revoked = True  # Flag to track if the connection is revoked

    @classmethod
    def get_connection(cls):
        if cls._connection is None or cls._is_revoked:
            credentials = Config.get_db_credentials()
            cls._connection = psycopg2.connect(
                user=credentials['user'],
                password=credentials['password'],
                host=credentials['host'],
                port=credentials['port'],
                database=credentials['database']
            )
            cls._is_revoked = False
        return cls._connection

    @classmethod
    def close_connection(cls):
        if cls._connection:
            cls._connection.close()
            cls._connection = None
            cls._is_revoked = True  # Set the revoked flag when the connection is closed
