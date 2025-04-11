from flask import Blueprint, request, jsonify, Response
from app.services.report_service import ReportService
from datetime import datetime, time
import logging
import io
import time as time_module  # To avoid conflict with datetime.time
from app.authentication.AccessTokenValidator import AccessTokenValidator
from constants import X_AUTHENTICATED_USER_TOKEN, IS_VALIDATION_ENABLED, REQUIRED_COLUMNS_FOR_ENROLLMENTS

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

report_controller = Blueprint('report_controller', __name__)

@report_controller.route('/report/orgs', methods=['POST'])
def get_report():
    start_timer = time_module.time()
    try:
        logger.info("Received request to generate report for multiple org_ids.")

        if IS_VALIDATION_ENABLED.lower() == 'true':
            # Extract and validate user token
            user_token = request.headers.get(X_AUTHENTICATED_USER_TOKEN)
            if not user_token:
                logger.error("Missing 'x-authenticated-user-token' in headers.")
                return jsonify({'error': 'Authentication token is required.'}), 401
            
            user_org_id = AccessTokenValidator.verify_user_token_get_org(user_token, True)
            if not user_org_id:
                logger.error("Invalid or expired authentication token.")
                return jsonify({'error': 'Invalid or expired authentication token.'}), 401

        # Parse and validate request body
        data = request.get_json()
        if not data or 'start_date' not in data or 'end_date' not in data or 'org_ids' not in data:
            raise KeyError("Missing 'start_date', 'end_date', or 'org_ids' in request body.")

        org_ids = data['org_ids']
        if not isinstance(org_ids, list) or not org_ids:
            return jsonify({'error': "'org_ids' must be a non-empty list."}), 400

        if IS_VALIDATION_ENABLED.lower() == 'true':
            # Optional: Check if the user has access to all org_ids
            # For now, we just log the first org_id match
            if user_org_id not in org_ids:
                logger.error(f"User does not have access to the requested org_ids: {org_ids}")
                return jsonify({'error': f'Access denied for the requested organization IDs.'}), 403

        # Parse and format date range
        start_date = datetime.strptime(data['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(data['end_date'], '%Y-%m-%d')

        start_date = datetime.combine(start_date.date(), time.min)  # 00:00:00
        end_date = datetime.combine(end_date.date(), time.max)      # 23:59:59.999999

        logger.info(f"Generating report for org_ids={org_ids} from {start_date} to {end_date}")

        # Validate date range
        if (end_date - start_date).days > 365:
            logger.warning(f"Date range exceeds 1 year: start_date={start_date}, end_date={end_date}")
            return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        try:
            csv_data = ReportService.get_total_learning_hours_csv_stream(
                start_date, end_date, org_ids, required_columns=REQUIRED_COLUMNS_FOR_ENROLLMENTS
            )

            if not csv_data:
                logger.warning(f"No data found for org_ids={org_ids} within given date range.")
                return jsonify({'error': 'No data found for the given organization IDs.'}), 404

            # Convert CSV data to BytesIO
            if isinstance(csv_data, str):
                csv_data = csv_data.encode('utf-8')

            csv_stream = io.BytesIO()
            csv_stream.write(csv_data)
            csv_stream.seek(0)

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for org_ids={org_ids}: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Report generated successfully for org_ids={org_ids} in {time_taken} seconds")

        return Response(
            csv_stream.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="report_multiple_orgs.csv"'
            }
        )

    except KeyError as e:
        error_message = str(e)
        logger.error(f"Missing required fields in request: {error_message}")
        return jsonify({'error': 'Invalid input. Please provide start_date, end_date, and org_ids.', 'details': error_message}), 400

    except ValueError as e:
        error_message = str(e)
        logger.error(f"Invalid date format in request: {error_message}")
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.', 'details': error_message}), 400

    except FileNotFoundError as e:
        error_message = str(e)
        logger.error(f"File not found during report generation: {error_message}")
        return jsonify({'error': 'Report file could not be generated.', 'details': error_message}), 500

    except Exception as e:
        error_message = str(e)
        logger.exception(f"Unexpected error occurred: {error_message}")
        return jsonify({'error': 'An unexpected error occurred. Please try again later.', 'details': error_message}), 500
