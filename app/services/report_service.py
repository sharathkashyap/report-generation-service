import logging
from io import BytesIO
from cryptography.fernet import Fernet
import pandas as pd
from app.models.report_model import ReportData
from app.services.fetch_data import DataFetcher
from constants import USER_DETAILS_TABLE, CONTENT_TABLE, USER_ENROLMENTS_TABLE

class ReportService:
    logger = logging.getLogger(__name__)

    @staticmethod
    def generate_csv(org_id):
        try:
            csv_stream = DataFetcher.fetch_data_as_csv_stream(DataFetcher(), USER_DETAILS_TABLE, org_id)
            ReportService.logger.info("Data fetched successfully for CSV generation.")

            if not csv_stream:
                return b""

            return csv_stream.read()

        except Exception as e:
            ReportService.logger.error(f"Error generating CSV: {e}")
            return b""

    @staticmethod
    def encrypt_csv(csv_data: bytes, encryption_key: bytes) -> bytes:
        try:
            fernet = Fernet(encryption_key)
            return fernet.encrypt(csv_data)
        except Exception as e:
            logging.error(f"Error encrypting CSV: {e}")
            raise

    @staticmethod
    def get_total_learning_hours_csv_stream(start_date, end_date, mdo_id, required_columns=None):
        try:
            fetcher = DataFetcher()

            # Fetch filtered user data
            user_df = fetcher.fetch_data_as_dataframe(
                USER_DETAILS_TABLE,
                {"mdo_id": mdo_id},
                columns=["user_id", "mdo_id", "full_name"]
            )

            if user_df.empty:
                ReportService.logger.info("No users found for given mdo_id.")
                return None

            user_ids = user_df["user_id"].tolist()
            ReportService.logger.info(f"Fetched {len(user_ids)} users.")

            # Fetch filtered enrollment data
            enrollment_filters = {
                "first_completed_on__gte": start_date,
                "first_completed_on__lte": end_date
            }

            enrollment_df = fetcher.fetch_data_as_dataframe(
                USER_ENROLMENTS_TABLE,
                enrollment_filters,
                columns=["user_id", "certificate_generated", "content_id", "enrolled_on", "first_completed_on", "last_completed_on"]
            )

            if enrollment_df.empty:
                ReportService.logger.info("No enrollment data found for the given date range.")
                return None

            # Filter enrollment to only matching user_ids
            enrollment_df = enrollment_df[enrollment_df["user_id"].isin(user_ids)]
            if enrollment_df.empty:
                ReportService.logger.info("No enrollments matched the filtered user IDs.")
                return None

            # Fetch content data (consider filtering by content_id list if needed)
            content_df = fetcher.fetch_data_as_dataframe(
                CONTENT_TABLE,
                columns=["content_id", "content_duration", "content_name"]
            )

            if content_df.empty:
                ReportService.logger.info("No content data found.")
                return None

            # Merge all three datasets
            merged_df = (
                user_df
                .merge(enrollment_df, on="user_id", how="inner")
                .merge(content_df, on="content_id", how="inner")
            )

            if merged_df.empty:
                ReportService.logger.info("Merged dataset is empty.")
                return None

            # Convert content_duration to numeric
            #merged_df["content_duration"] = pd.to_numeric(merged_df.get("content_duration", 0), errors="coerce").fillna(0)

            # Optional: calculate total learning hours per user
            # merged_df["total_learning_hours"] = merged_df.groupby("user_id")["content_duration"].transform("sum")

            # Filter columns if specified
            if required_columns:
                existing_columns = [col for col in required_columns if col in merged_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                merged_df = merged_df[existing_columns]

            # Convert to CSV
            csv_stream = BytesIO()
            merged_df.to_csv(csv_stream, index=False)
            csv_stream.seek(0)
            ReportService.logger.info(f"CSV stream generated with {len(merged_df)} rows.")
            return csv_stream.getvalue()

        except Exception as e:
            ReportService.logger.error(f"Error generating CSV stream: {e}")
            return None
