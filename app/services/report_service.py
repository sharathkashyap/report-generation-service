import logging
from io import BytesIO
from cryptography.fernet import Fernet
import pandas as pd
from app.models.report_model import ReportData
from app.services.fetch_data import DataFetcher
from constants import USER_DETAILS_TABLE, CONTENT_TABLE, USER_ENROLMENTS_TABLE,USER_CONSUMPTION_TABLE
import gc


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
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
    def get_total_learning_hours_csv_stream(start_date, end_date, org_ids, required_columns=None):
        try:
            fetcher = DataFetcher()

            if not isinstance(org_ids, list) or not org_ids:
                ReportService.logger.warning("Invalid or empty org_ids provided.")
                return None

            
            mdo_id=org_ids[0]
            print(mdo_id)
            # Fetch filtered enrollment data for all orgs
            enrollment_filters = {
                "mdo_id": mdo_id,
                "enrolled_on__gte": start_date,
                "enrolled_on__lte": end_date
            }

            enrollment_df = fetcher.fetch_data_as_dataframe(
                USER_CONSUMPTION_TABLE,
                enrollment_filters
            )

            if enrollment_df.empty:
                ReportService.logger.info("No enrollment data found for the given org_ids and date range.")
                return None

            # Filter columns if specified
            if required_columns:
                existing_columns = [col for col in required_columns if col in enrollment_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                enrollment_df = enrollment_df[existing_columns]

            # Convert to CSV
            csv_stream = BytesIO()
            enrollment_df.to_csv(csv_stream, index=False)
            csv_stream.seek(0)
            ReportService.logger.info(f"CSV stream generated with {len(enrollment_df)} rows for org_ids: {org_ids}")         
            del enrollment_df
            gc.collect()
            return csv_stream.getvalue()

        except Exception as e:
            ReportService.logger.error(f"Error generating CSV stream: {e}")
            return None
