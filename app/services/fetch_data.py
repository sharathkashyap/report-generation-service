import pandas as pd
from io import BytesIO
from ..config.db_connection import DBConnection
import logging
import time
import gc

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

                # Load data into a pandas DataFrame in chunks
                chunk_size = 1000
                csv_stream = BytesIO()
                first_chunk = True

                for i in range(0, len(rows), chunk_size):
                    chunk = rows[i:i + chunk_size]
                    df = pd.DataFrame(chunk, columns=column_names)

                    # Filter DataFrame to include only the required columns
                    required_columns = ['user_id', 'mdo_id', 'full_name', 'email']
                    df = df[required_columns]

                    # Append to CSV stream
                    df.to_csv(csv_stream, index=False, header=first_chunk, mode='a')
                    first_chunk = False

                    # Cleanup chunk DataFrame
                    del df  # Explicitly delete DataFrame
                    gc.collect()  # Force garbage collection

                csv_stream.seek(0)  # Reset stream position to the beginning
                DataFetcher.logger.info("Data fetched and converted to CSV stream successfully.")

                return csv_stream

        except Exception as e:
            DataFetcher.logger.error(f"Error: {e}")
            return None
        finally:
            gc.collect()  # Ensure garbage collection after method execution


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
                chunk_size = 1000  # Fetch in chunks of 1000 rows
                rows = []
                columns = [desc[0] for desc in cursor.description]

                while True:
                    chunk = cursor.fetchmany(chunk_size)
                    if not chunk:
                        break
                    rows.extend(chunk)

                df = pd.DataFrame(rows, columns=columns)

                elapsed_time = time.time() - start_time 
                DataFetcher.logger.info(f"[{table_name}] - Records fetched: {len(df)} | Time taken: {elapsed_time:.2f} seconds")

                # Return the DataFrame and ensure cleanup
                return df
        except Exception as e:
            DataFetcher.logger.error(f"Error fetching data from {table_name}: {e}")
            return pd.DataFrame()
        finally:
            # Explicitly delete the DataFrame and trigger garbage collection
            if 'df' in locals():
                del df
            gc.collect()

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def close(self):
        self.close_connection()

