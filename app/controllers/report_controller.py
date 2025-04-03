from flask import Blueprint, request, jsonify, Response
from app.services.report_service import ReportService
from datetime import datetime
import logging
from cryptography.fernet import Fernet  # Ensure this import is present
import io  # Ensure this import is present

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

report_controller = Blueprint('report_controller', __name__)

@report_controller.route('/report/org/<org_id>', methods=['POST'])
def get_report(org_id):
    try:
        # Parse date range from request JSON
        data = request.get_json()
        if not data or 'start_date' not in data or 'end_date' not in data:
            raise KeyError("Missing 'start_date' or 'end_date' in request body.")

        start_date = datetime.strptime(data['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(data['end_date'], '%Y-%m-%d')

        # Validate date range
        if (end_date - start_date).days > 365:
            logger.warning(f"Date range exceeds 1 year: start_date={start_date}, end_date={end_date}")
            return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        # Generate and process report
        logger.info(f"Generating report for org_id={org_id} with date range {start_date} to {end_date}")
        csv_data = ReportService.generate_csv(org_id)

        if not csv_data:
            logger.error(f"No data found for org_id={org_id}")
            return jsonify({'error': 'No data found for the given organization ID.'}), 404

        # Convert CSV data to a BytesIO stream
        if isinstance(csv_data, str):  # Ensure csv_data is encoded as bytes
            csv_data = csv_data.encode('utf-8')

        csv_stream = io.BytesIO()
        csv_stream.write(csv_data)
        csv_stream.seek(0)

        # Return data as a downloadable file
        logger.info(f"Report generated successfully for org_id={org_id}")
        return Response(
            csv_stream.getvalue(),  # Retrieve the content of the stream
            mimetype="text/csv",  # Use 'text/csv' for CSV files
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
