class Config:
    SQLALCHEMY_DATABASE_URI = 'jdbc:postgresql://localhost:5431/warehouse'
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
            'user': 'postgres',
            'password': 'password123',
            'host': 'localhost',
            'port': 5431,
            'database': 'warehouse'
        }

