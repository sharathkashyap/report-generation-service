import psycopg2.pool
from ..config.db_config import Config

class DBConnection:
    _connection_pool = None

    @classmethod
    def initialize_pool(cls, minconn=1, maxconn=10):
        if cls._connection_pool is None:
            credentials = Config.get_db_credentials()
            cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn,
                maxconn,
                user=credentials['user'],
                password=credentials['password'],
                host=credentials['host'],
                port=credentials['port'],
                database=credentials['database']
            )

    @classmethod
    def get_connection(cls):
        if cls._connection_pool is None:
            raise Exception("Connection pool is not initialized. Call initialize_pool first.")
        return cls._connection_pool.getconn()

    @classmethod
    def release_connection(cls, connection):
        if cls._connection_pool and connection:
            cls._connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls):
        if cls._connection_pool:
            cls._connection_pool.closeall()
