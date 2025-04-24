# from constants import postgres_db_port, postgres_db_user, postgres_db_password, postgres_db_host, postgres_db_name, postgres_db_url
# from google.cloud import bigquery
# import os

# class Config:
#     SQLALCHEMY_DATABASE_URI = postgres_db_url
#     SQLALCHEMY_TRACK_MODIFICATIONS = False

#     @staticmethod
#     def get_db_credentials():
#         return {
#             'user': postgres_db_user,
#             'password': postgres_db_password,
#             'host': postgres_db_host,
#             'port': postgres_db_port,
#             'database': postgres_db_name
#         }

# def initialize_bigquery():
#     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/sahilchaudhary/Downloads/prj-kb-nprd-uat-gcp-1006-c691f3c2615b.json"
#     return bigquery.Client()

