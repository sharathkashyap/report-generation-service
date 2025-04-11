import pandas as pd
from io import BytesIO
from ..config.db_connection import DBConnection
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class DataFetcher:
    logger = logging.getLogger(__name__)

    def __init__(self):
        # Initialize a shared database connection
        self.connection = DBConnection.get_connection()

    def fetch_data_as_map(self, table_name):
        try:
            with self.connection.cursor() as cursor:  # Use context manager for cursor
                # Fetch data from the table
                query = f"SELECT * FROM {table_name};"
                cursor.execute(query)
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]

                # Create a list of dictionaries (map) for the response
                data_map = [dict(zip(column_names, row)) for row in rows]

                DataFetcher.logger.info(f"Data fetched successfully. Total records: {len(data_map)}")
                return data_map
        except Exception as e:
            DataFetcher.logger.error(f"Error: {e}")
            return []

    def fetch_data_as_csv_stream(self, table_name, org_id):
        try:
            with self.connection.cursor() as cursor:  # Use context manager for cursor
                # Fetch all data from the table with filtering by orgId
                query = f"""
                    SELECT * 
                    FROM {table_name} 
                    WHERE mdo_id = %s;
                """
                cursor.execute(query, (org_id,))
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]

                # Load data into a pandas DataFrame
                df = pd.DataFrame(rows, columns=column_names)

                # Filter DataFrame to include only the required columns
                required_columns = ['user_id', 'mdo_id', 'full_name', 'email']
                df = df[required_columns]

                # Process user_enrolments in chunks if needed
                user_ids = df['user_id'].tolist()
                chunk_size = 1000  # Process in chunks of 1000
                enrolments_data = []
                for i in range(0, len(user_ids), chunk_size):
                    chunk_ids = tuple(user_ids[i:i + chunk_size])
                    query_user_enrolments = """
                        SELECT * 
                        FROM user_enrolments 
                        WHERE user_id IN %s;
                    """
                    cursor.execute(query_user_enrolments, (chunk_ids,))
                    enrolments_data.extend(cursor.fetchall())

                enrolment_column_names = [desc[0] for desc in cursor.description]
                enrolments_df = pd.DataFrame(enrolments_data, columns=enrolment_column_names)

                # Filter DataFrame to include only the required columns
                enrolments_required_columns = ['user_id', 'batch_id', 'content_id', 'content_progress_percentage', 'enrolled_on']  # Replace with actual column names
                enrolments_df = enrolments_df[enrolments_required_columns]

                # Export enrolments DataFrame to a CSV byte stream
                csv_stream = BytesIO()
                enrolments_df.to_csv(csv_stream, index=False)
                csv_stream.seek(0)  # Reset stream position to the beginning

                DataFetcher.logger.info(f"Data fetched and converted to CSV stream successfully for user enrolments. Total records: {len(enrolments_df)}")
                return csv_stream
        except Exception as e:
            DataFetcher.logger.error(f"Error: {e}")
            return None

    def fetch_data_as_dataframe(self, table_name, filters=None, columns=None):
        try:
            start_time = time.time()
            DataFetcher.logger.info(f"[{table_name}] - Fetching records.")
            with self.connection.cursor() as cursor:  # Use context manager for cursor
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
                            # Add other operations if needed
                        else:
                            conditions.append(f"{key} = %s")
                            values.append(value)

                    query += " WHERE " + " AND ".join(conditions)

                cursor.execute(query, values)
                rows = []
                chunk_size = 1000  # Fetch in chunks of 1000 rows
                while True:
                    chunk = cursor.fetchmany(chunk_size)
                    if not chunk:
                        break
                    rows.extend(chunk)

                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)

                elapsed_time = time.time() - start_time 
                DataFetcher.logger.info(f"[{table_name}] - Records fetched: {len(df)} | Time taken: {elapsed_time:.2f} seconds")
                return df
        except Exception as e:
            DataFetcher.logger.error(f"Error fetching data from {table_name}: {e}")
            return pd.DataFrame()

    def close_connection(connection):
        if connection:
            connection.close()

    def close(self):
        # Use the standalone close_connection function to close the shared connection
        self.close_connection(self.connection)

