# import pytest
# from app.controllers.health_controller import health_controller
# from flask import Flask

# @pytest.fixture
# def app():
#     app = Flask(__name__)
#     app.register_blueprint(health_controller)
#     app.config['TESTING'] = True
#     return app

# @pytest.fixture
# def client(app):
#     return app.test_client()

# def test_health_check(client):
#     response = client.get('/health')
#     assert response.status_code in [200, 500]

# def test_liveness_check(client):
#     response = client.get('/liveness')
#     assert response.status_code == 200
#     assert response.json == {"status": "OK"}