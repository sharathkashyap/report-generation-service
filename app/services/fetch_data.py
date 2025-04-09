import pandas as pd
from io import BytesIO
from ..config.db_connection import DBConnection
import logging 
class DataFetcher:
    logger = logging.getLogger(__name__)
    def __init__(self):# Class-level shared connection
        # Initialize a shared database connection
        self.connection = DBConnection.get_connection()

    def fetch_data_as_map(self, table_name):
        try:
            cursor = self.connection.cursor()

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
            cursor = self.connection.cursor()

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

            # Fetch data from user_enrolments table based on user_id
            user_ids = df['user_id'].tolist()
            query_user_enrolments = """
                SELECT * 
                FROM user_enrolments 
                WHERE user_id IN %s;
            """
            cursor.execute(query_user_enrolments, (tuple(user_ids),))
            enrolment_rows = cursor.fetchall()
            enrolment_column_names = [desc[0] for desc in cursor.description]

            # Load user_enrolments data into a pandas DataFrame
            enrolments_df = pd.DataFrame(enrolment_rows, columns=enrolment_column_names)

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

    def fetch_data_as_dataframe(self, table_name, filters=None,columns=None):
        try:
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
                            conditions.append(f"CAST({col} AS timestamp) >= %s")
                            values.append(value)
                        elif op == "lte":
                            conditions.append(f"CAST({col} AS timestamp) <= %s")
                            values.append(value)
                        # Add other operations if needed
                    else:
                        conditions.append(f"{key} = %s")
                        values.append(value)

                query += " WHERE " + " AND ".join(conditions)

            cursor.execute(query, values)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=columns)
            DataFetcher.logger.info(f"[{table_name}] - Records fetched: {len(df)}")
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

