import logging
from app.services.fetch_data_bigQuery import BigQueryService
from constants import MASTER_ENROLMENTS_TABLE, MASTER_USER_TABLE, odisha_child_org_id, odisha_only_enable
import gc


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
class ReportService:
    logger = logging.getLogger(__name__)

    @staticmethod
    def fetch_user_cumulative_report(email=None, phone=None, ehrms_id=None, start_date=None, end_date=None, orgId=None, required_columns=None):
        try:
            bigquery_service = BigQueryService()

            # Build filters for user details
            user_filters = []
            if email:
                user_filters.append(f"email = '{email}'")
            if phone:
                user_filters.append(f"phone_number = '{phone}'")
            if ehrms_id:
                user_filters.append(f"external_system_id = '{ehrms_id}'")

            if not user_filters:
                ReportService.logger.info("No filters provided for fetching user data.")
                return None

            # Construct the query for fetching user data
            user_filter_query = ' AND '.join(user_filters)
            user_query = f"""
                SELECT user_id, mdo_id
                FROM `{MASTER_USER_TABLE}`
                WHERE {user_filter_query}
            """

            ReportService.logger.info(f"Executing user query: {user_query}")
            user_df = bigquery_service.run_query(user_query)

            if user_df.empty:
                ReportService.logger.info("No users found matching the provided filters.")
                return None

            user_ids = user_df["user_id"].tolist()
            ReportService.logger.info(f"Fetched {len(user_ids)} users.")

            # Construct the query for fetching enrollment data
            enrollment_query = f"""
                SELECT *
                FROM `{MASTER_ENROLMENTS_TABLE}`
                WHERE user_id IN ({', '.join([f"'{uid}'" for uid in user_ids])})
            """

            if start_date and end_date:
                enrollment_query += f" AND enrolled_on BETWEEN '{start_date}' AND '{end_date}'"

            ReportService.logger.info(f"Executing enrollment query: {enrollment_query}")
            enrollment_df = bigquery_service.run_query(enrollment_query)

            if enrollment_df.empty:
                ReportService.logger.info("No enrollment data found for the given user.")
                return None

            # Filter columns if specified
            if required_columns:
                existing_columns = [col for col in required_columns if col in enrollment_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                merged_df = enrollment_df[existing_columns]

            def generate_csv_stream(df, cols):
                try:
                    yield ','.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        yield ','.join(map(str, row)) + '\n'
                finally:
                    # Safe cleanup after generator is fully consumed
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportService.logger.info("Cleaned up DataFrame after streaming.")

            ReportService.logger.info(f"CSV stream generated with {len(merged_df)} rows.")

            # Return CSV content without closing the stream
            return generate_csv_stream(merged_df, merged_df.columns.tolist())

        except MemoryError as me:
            ReportService.logger.error("MemoryError encountered. Consider processing data in smaller chunks.")
            raise
        except Exception as e:
            ReportService.logger.error(f"Error generating cumulative report: {e}")
            return None

    @staticmethod
    def fetch_master_enrolments_data(start_date, end_date, mdo_id,  is_full_report_required, required_columns):
        try:
            bigquery_service = BigQueryService()

            # Add date filtering to the query if start_date and end_date are provided
            date_filter = ""
            if start_date and end_date:
                date_filter = f" AND enrolled_on BETWEEN '{start_date}' AND '{end_date}'"
            if is_full_report_required:
                if (odisha_only_enable.lower() == 'true'):
                    mdo_id_org_list = eval(odisha_child_org_id)
                    mdo_id_org_list.append(mdo_id)
            else: 
                mdo_id_org_list = [mdo_id]    
            
            mdo_id_list = [f"'{mid}'" for mid in mdo_id_org_list]  # Quote each ID
            mdo_id_str = ', '.join(mdo_id_list)  # Join them with commas
            query = f"""
                SELECT * 
                FROM `{MASTER_ENROLMENTS_TABLE}`
                WHERE mdo_id in ({mdo_id_str}){date_filter}
            """

            ReportService.logger.info(f"Executing query: {query}")

            # Update to use run_query instead of execute_query
            result_df = bigquery_service.run_query(query)

            if result_df.empty:
                ReportService.logger.info("No data found for the given mdo_id and date range.")
                return None

            ReportService.logger.info(f"Fetched {len(result_df)} rows from master_enrolments_data.")

            # Filter the result DataFrame to include only the required columns
            if required_columns:
                existing_columns = [col for col in required_columns if col in result_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                result_df = result_df[existing_columns]

            # Generate CSV stream from the result DataFrame
            def generate_csv_stream(df, cols):
                try:
                    yield ','.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        yield ','.join(map(str, row)) + '\n'
                finally:
                    # Safe cleanup after generator is fully consumed
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportService.logger.info("Cleaned up DataFrame after streaming.")

            ReportService.logger.info(f"CSV stream generated with {len(result_df)} rows.")

            # Return CSV content without closing the stream
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportService.logger.error(f"Error fetching master enrolments data: {e}")
            return None

    @staticmethod
    def fetch_master_user_data(mdo_id,  is_full_report_required, required_columns=None, user_creation_start_date=None, user_creation_end_date=None):
        try:
            bigquery_service = BigQueryService()

            # Add date filtering to the query if start_date and end_date are provided
            date_filter = ""
            if user_creation_start_date and user_creation_end_date:
                date_filter = f" AND user_registration_date BETWEEN '{user_creation_start_date}' AND '{user_creation_end_date}'"
            if is_full_report_required:
                if (odisha_only_enable.lower() == 'true'):
                    mdo_id_org_list = eval(odisha_child_org_id)
                    mdo_id_org_list.append(mdo_id)
            else: 
                mdo_id_org_list = [mdo_id]   
            
            mdo_id_list = [f"'{mid}'" for mid in mdo_id_org_list]  # Quote each ID
            mdo_id_str = ', '.join(mdo_id_list)  # Join them with commas
            query = f"""
                SELECT * 
                FROM `{MASTER_USER_TABLE}`
                WHERE mdo_id in ({mdo_id_str}){date_filter}
            """

            ReportService.logger.info(f"Executing query: {query}")

            # Update to use run_query instead of execute_query
            result_df = bigquery_service.run_query(query)

            if result_df.empty:
                ReportService.logger.info("No data found for user the given mdo_id and date range.")
                return None

            ReportService.logger.info(f"Fetched {len(result_df)} rows from master_enrolments_data.")

            # Filter the result DataFrame to include only the required columns
            if required_columns:
                existing_columns = [col for col in required_columns if col in result_df.columns]
                missing_columns = list(set(required_columns) - set(existing_columns))
                if missing_columns:
                    ReportService.logger.info(f"Warning: Missing columns skipped: {missing_columns}")
                result_df = result_df[existing_columns]

            # Generate CSV stream from the result DataFrame
            def generate_csv_stream(df, cols):
                try:
                    yield ','.join(cols) + '\n'
                    for row in df.itertuples(index=False, name=None):
                        row_dict = dict(zip(cols, row))
                        
                        # Mask email
                        if 'email' in row_dict and row_dict['email']:
                            parts = row_dict['email'].split('@')
                            if len(parts) == 2:
                                domain_parts = parts[1].split('.')
                                masked_domain = '.'.join(['*' * len(part) for part in domain_parts])
                                row_dict['email'] = f"{parts[0]}@{masked_domain}"
                            else:
                                row_dict['email'] = parts[0]

                        # Mask phone number: e.g., ******2245
                        if 'phone_number' in row_dict and row_dict['phone_number']:
                            phone = str(row_dict['phone_number'])
                            if len(phone) >= 4:
                                row_dict['phone_number'] = '*' * (len(phone) - 4) + phone[-4:]
                            else:
                                row_dict['phone_number'] = '*' * len(phone)

                        # Convert back to row and yield
                        yield ','.join([str(row_dict.get(col, '')) for col in cols]) + '\n'
                finally:
                    df.drop(df.index, inplace=True)
                    del df
                    gc.collect()
                    ReportService.logger.info("Cleaned up DataFrame after streaming.")
            ReportService.logger.info(f"CSV stream generated with {len(result_df)} rows.")

            # Return CSV content without closing the stream
            return generate_csv_stream(result_df, result_df.columns.tolist())

        except Exception as e:
            ReportService.logger.error(f"Error fetching master user data: {e}")
            return None

