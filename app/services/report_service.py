import os
import csv
import io 
import logging 
from cryptography.fernet import Fernet
from app.models.report_model import ReportData
from app.services.fetch_data import DataFetcher 
from constants import USER_DETAILS_TABLE

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
            # Ensure csv_data is already in bytes; no need to encode
            encrypted_data = fernet.encrypt(csv_data)
            return encrypted_data
        except Exception as e:
            logging.error(f"Error occurred while encrypting CSV: {e}")
            raise
