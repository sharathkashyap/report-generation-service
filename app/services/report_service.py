import os
import csv
import io 
import logging 
from cryptography.fernet import Fernet
from app.models.report_model import ReportData
from app.services.fetch_data import DataFetcher 
from constants import USER_DETAILS_TABLE,CONTENT_TABLE, USER_ENROLMENTS_TABLE
import pandas as pd
from io import BytesIO

class ReportService:
    @staticmethod
    def generate_csv(org_id):
        try:
            # Fetch data as a CSV stream
            csv_stream = DataFetcher.fetch_data_as_csv_stream(DataFetcher(), USER_DETAILS_TABLE, org_id)

            print("Data fetched successfully when generating CSV.")

            if not csv_stream:
                return b""  # Return empty bytes if no data

            # Ensure the data is returned as a byte-stream
            csv_data = csv_stream.read()
            return csv_data 
        except Exception as e:
            print(f"Error occurred while generating CSV: {e}")
            return b""  # Return empty bytes in case of an error

    @staticmethod
    def encrypt_csv(csv_data: bytes, encryption_key: bytes) -> bytes:
        """
        Encrypts the given CSV data using the provided encryption key.

        :param csv_data: The CSV data to encrypt as bytes.
        :param encryption_key: A 32-byte base64-encoded encryption key.
        :return: Encrypted data as bytes.
        """
        try:
            fernet = Fernet(encryption_key)
            encrypted_data = fernet.encrypt(csv_data)
            return encrypted_data
        except Exception as e:
            logging.error(f"Error occurred while encrypting CSV: {e}")
            raise

    @staticmethod
    def get_total_learning_hours_csv_stream(mdo_id, required_columns=None):
        """
        Generate a CSV stream with total learning hours for each user/content based on completed courses.

        :param mdo_id: MDO ID to filter users
        :param required_columns: List of columns to include in the final CSV (optional)
        :return: Bytes stream of the CSV file
        """
        try:
            fetcher = DataFetcher()

            user_df = fetcher.fetch_data_as_dataframe(USER_DETAILS_TABLE, {"mdo_id": mdo_id})
            enrollment_df = fetcher.fetch_data_as_dataframe(USER_ENROLMENTS_TABLE)
            content_df = fetcher.fetch_data_as_dataframe(CONTENT_TABLE)

            if user_df.empty or enrollment_df.empty or content_df.empty:
                print("One or more tables returned no data.")
                return None

            # Filter only completed courses
            enrollment_df = enrollment_df[enrollment_df["content_progress_percentage"] == 100]

            # Merge all three DataFrames
            merged_df = user_df.merge(enrollment_df, on="user_id", how="inner") \
                               .merge(content_df, on=["content_id", "batch_id"], how="inner")

            # Convert content_duration to numeric to avoid issues during aggregation
            merged_df["content_duration"] = pd.to_numeric(merged_df["content_duration"], errors="coerce").fillna(0)

            # Add total_learning_hours per user (can modify to be per user+content if needed)
            merged_df["total_learning_hours"] = merged_df.groupby("user_id")["content_duration"].transform("sum")

            # Keep all columns by default
            final_df = merged_df

            # If specific columns were passed, filter them
            if required_columns:
                missing_cols = [col for col in required_columns if col not in final_df.columns]
                if missing_cols:
                    print(f"Warning: These columns were not found and will be ignored: {missing_cols}")
                final_df = final_df[[col for col in required_columns if col in final_df.columns]]

            # Convert to CSV stream
            csv_stream = BytesIO()
            final_df.to_csv(csv_stream, index=False)
            csv_stream.seek(0)

            print("CSV stream generated successfully.")
            return csv_stream.getvalue()

        except Exception as e:
            print(f"Error generating CSV stream: {e}")
            return None
