from flask import Blueprint, request, jsonify, Response
from app.services.report_service import ReportService
from datetime import datetime
import logging
import io
from app.authentication.AccessTokenValidator import AccessTokenValidator
from constants import X_AUTHENTICATED_USER_TOKEN, IS_VALIDATION_ENABLED,REQUIRED_COLUMNS_FOR_ENROLLMENTS
from datetime import datetime, time

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

report_controller = Blueprint('report_controller', __name__)

@report_controller.route('/report/org/<org_id>', methods=['POST'])
def get_report(org_id):
    try:
        if IS_VALIDATION_ENABLED.lower() == 'true' :
            # Extract and validate user token
            user_token = request.headers.get(X_AUTHENTICATED_USER_TOKEN)
            if not user_token:
                logger.error("Missing 'x-authenticated-user-token' in headers.")
                return jsonify({'error': 'Authentication token is required.'}), 401
            user_org_id = AccessTokenValidator.verify_user_token_get_org(user_token, True)
            if not user_org_id:
                logger.error("Invalid or expired authentication token.")
                return jsonify({'error': 'Invalid or expired authentication token.'}), 401

            logger.info(f"Authenticated user with user_org_id={user_org_id}")
            if user_org_id != org_id:
                logger.error(f"User does not have access to organization ID {org_id}.")
                return jsonify({'error': 'Access denied for the specified organization ID {org_id}.'}), 403
            
        # Parse date range from request JSON
        data = request.get_json()
        if not data or 'start_date' not in data or 'end_date' not in data:
            raise KeyError("Missing 'start_date' or 'end_date' in request body.")

        start_date = datetime.strptime(data['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(data['end_date'], '%Y-%m-%d')

        start_date = datetime.combine(start_date.date(), time.min)        # 00:00:00
        end_date = datetime.combine(end_date.date(), time.max)  

        # Validate date range
        if (end_date - start_date).days > 365:
            logger.warning(f"Date range exceeds 1 year: start_date={start_date}, end_date={end_date}")
            return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        # Generate and process report
        logger.info(f"Generating report for org_id={org_id} with date range {start_date} to {end_date}")
        #csv_data = ReportService.generate_csv(org_id)
        required_cols = ["user_id", "full_name", "content_id", "total_learning_hours"]
        csv_data = ReportService.get_total_learning_hours_csv_stream(start_date,end_date,org_id, required_columns=REQUIRED_COLUMNS_FOR_ENROLLMENTS)

        if not csv_data:
            logger.error(f"No data found for org_id={org_id}")
            return jsonify({'error': 'No data found for the given organization ID.'}), 404

        # Convert CSV data to a BytesIO stream
        if isinstance(csv_data, str):
            csv_data = csv_data.encode('utf-8')

        csv_stream = io.BytesIO()
        csv_stream.write(csv_data)
        csv_stream.seek(0)

        # Return data as a downloadable file
        logger.info(f"Report generated successfully for org_id={org_id}")
        return Response(
            csv_stream.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="report_{org_id}.csv"'
            }
        )

    except KeyError as e:
        logger.error(f"Missing required fields in request: {e}")
        return jsonify({'error': 'Invalid input. Please provide start_date and end_date.'}), 400

    except ValueError as e:
        logger.error(f"Invalid date format in request: {e}")
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400

    except FileNotFoundError as e:
        logger.error(f"File not found during report generation: {e}")
        return jsonify({'error': 'Report file could not be generated.'}), 500

    except Exception as e:
        logger.exception(f"Unexpected error occurred: {e}")
        return jsonify({'error': 'An unexpected error occurred. Please try again later.'}), 500
