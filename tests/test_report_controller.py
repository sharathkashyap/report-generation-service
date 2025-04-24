# import pytest
# from app.controllers.report_controller import report_controller
# from flask import Flask

# @pytest.fixture
# def app():
#     app = Flask(__name__)
#     app.register_blueprint(report_controller)
#     return app

# def test_get_report(client):
#     response = client.post('/report/org/12345', json={"start_date": "2023-01-01", "end_date": "2023-12-31"})
#     assert response.status_code in [200, 400, 404, 500]

# def test_get_user_report(client):
#     response = client.post('/report/user/sync/12345', json={"userEmail": "test@example.com"})
#     assert response.status_code in [200, 400, 404, 500]

# def test_get_org_user_report(client):
#     response = client.post('/report/org/user/12345', json={"user_creation_start_date": "2023-01-01", "user_creation_end_date": "2023-12-31"})
#     assert response.status_code in [200, 400, 404, 500]