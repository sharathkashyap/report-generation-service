import pandas as pd
from io import BytesIO
from ..config.db_connection import DBConnection
import logging
import time

# Setup logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
class DataFetcher:
    logger = logging.getLogger(__name__)

    def __init__(self):
        # Shared database connection
        self.connection = DBConnection.get_connection()

    def fetch_data_as_map(self, table_name):
        try:
            cursor = self.connection.cursor()

            # Fetch all data from the table
            query = f"SELECT * FROM {table_name};"
            cursor.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Convert to list of dictionaries
            data_map = [dict(zip(column_names, row)) for row in rows]

            DataFetcher.logger.info(f"Data fetched successfully. Total records: {len(data_map)}")
            return data_map
        except Exception as e:
            DataFetcher.logger.error(f"Error: {e}")
            return []

    def fetch_data_as_csv_stream(self, table_name, org_ids):
        try:
            # Ensure org_ids is a list or tuple
            if not isinstance(org_ids, (list, tuple)):
                org_ids = [org_ids]
            cursor = self.connection.cursor()

            # Fetch filtered user data
            query = f"""
                SELECT * 
                FROM {table_name} 
                WHERE mdo_id IN %s;
            """
            cursor.execute(query, (tuple(org_ids),))
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=column_names)

            required_columns = ['user_id', 'mdo_id', 'full_name', 'email']
            df = df[required_columns]

            user_ids = df['user_id'].tolist()

            if not user_ids:
                DataFetcher.logger.warning("No users found for given org_ids.")
                return None

            # Fetch enrolment data
            query_user_enrolments = """
                SELECT * 
                FROM user_enrolments 
                WHERE user_id IN %s;
            """
            cursor.execute(query_user_enrolments, (tuple(user_ids),))
            enrolment_rows = cursor.fetchall()
            enrolment_column_names = [desc[0] for desc in cursor.description]

            enrolments_df = pd.DataFrame(enrolment_rows, columns=enrolment_column_names)

            # enrolments_required_columns = ['user_id', 'batch_id', 'content_id', 'content_progress_percentage', 'enrolled_on']
            # enrolments_df = enrolments_df[enrolments_required_columns]

            # Convert to CSV stream
            csv_stream = BytesIO()
            enrolments_df.to_csv(csv_stream, index=False)
            csv_stream.seek(0)

            DataFetcher.logger.info(f"CSV stream created for user enrolments. Records: {len(enrolments_df)}")
            return csv_stream

        except Exception as e:
            DataFetcher.logger.error(f"Error: {e}")
            return None

    def fetch_data_as_dataframe(self, table_name, filters=None, columns=None):
        try:
            start_time = time.time()
            DataFetcher.logger.info(f"[{table_name}] - Fetching records.")

            cursor = self.connection.cursor()
            col_clause = ", ".join(columns) if columns else "*"
            query = f"SELECT {col_clause} FROM {table_name}"
            values = []

            if filters:
                conditions = []
                for key, value in filters.items():
                    if "__" in key:
                        col, op = key.split("__")
                        if op == "in" and isinstance(value, list):
                            placeholders = ','.join(['%s'] * len(value))
                            conditions.append(f"{col} IN ({placeholders})")
                            values.extend(value)
                        elif op == "gte":
                            conditions.append(f"{col} >= %s")
                            values.append(value)
                        elif op == "lte":
                            conditions.append(f"{col} <= %s")
                            values.append(value)
                        # Add more ops as needed
                    else:
                        conditions.append(f"{key} = %s")
                        values.append(value)

                query += " WHERE " + " AND ".join(conditions)

            cursor.execute(query, values)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=columns)

            elapsed_time = time.time() - start_time
            DataFetcher.logger.info(f"[{table_name}] - Records fetched: {len(df)} | Time taken: {elapsed_time:.2f} seconds")
            return df
        except Exception as e:
            DataFetcher.logger.error(f"Error fetching data from {table_name}: {e}")
            return pd.DataFrame()

    @staticmethod
    def close_connection(connection):
        if connection:
            connection.close()

    def close(self):
        # Close the shared connection properly
        self.close_connection(self.connection)
