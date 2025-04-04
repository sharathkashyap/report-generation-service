from constants import postgres_db_port, postgres_db_user, postgres_db_password, postgres_db_host, postgres_db_name
class Config:
    #SQLALCHEMY_DATABASE_URI = postgres_db_url
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # @staticmethod
    # def get_db_credentials():
    #     return {
    #         'user': 'wingspan',  
    #         'password': 'wing@123',
    #         'host': 'localhost',
    #         'port': 5433,
    #         'database': 'wingspan'
    #     }

    @staticmethod
    def get_db_credentials():
        return {
            'user': postgres_db_user,
            'password': postgres_db_password,
            'host': postgres_db_host,
            'port': postgres_db_port,
            'database': postgres_db_name
        }

