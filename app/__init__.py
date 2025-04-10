from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import logging
import os
from app.authentication.KeyManager import KeyManager
from constants import ACCESS_TOKEN_PUBLICKEY_BASEPATH, IS_VALIDATION_ENABLED

db = SQLAlchemy()

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app():
    app = Flask(__name__)
    app.config.from_object('app.config.db_config.Config')
    
    try:
        db.init_app(app)
        with app.app_context():
            # Test the database connection
            db.session.execute('SELECT 1')
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise RuntimeError("Application startup aborted due to database connection failure.")

    if IS_VALIDATION_ENABLED.lower() == 'true' : 
        # Initialize KeyManager
        base_path = ACCESS_TOKEN_PUBLICKEY_BASEPATH
        if not base_path:
            logger.error("ACCESS_TOKEN_PUBLICKEY_BASEPATH is not set.")
            raise ValueError("ACCESS_TOKEN_PUBLICKEY_BASEPATH is not set.")
        KeyManager.init(base_path)
        logger.info("KeyManager initialized successfully.")

    try:
        from app.controllers.report_controller import report_controller
        app.register_blueprint(report_controller)
        
        # Register health_controller
        from app.controllers.health_controller import health_controller
        app.register_blueprint(health_controller)
    except Exception as e:
        app.logger.error(f"Blueprint registration failed: {e}")
        raise

    return app
