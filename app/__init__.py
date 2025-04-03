from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    app.config.from_object('app.config.db_config.Config')
    
    try:
        db.init_app(app)
    except Exception as e:
        app.logger.error(f"Database initialization failed: {e}")
        raise
    
    try:
        from app.controllers.report_controller import report_controller
        app.register_blueprint(report_controller)
    except Exception as e:
        app.logger.error(f"Blueprint registration failed: {e}")
        raise

    return app
