from flask import Flask
from celery import Celery
import os
from App import config
from App.utils import Utils
import logging

# Global variables
celery = None

def make_celery(app: Flask) -> Celery:
    celery = Celery(app.import_name,
                    broker=config.CELERY_BROKER_URL,
                    backend=config.CELERY_RESULT_BACKEND)
    celery.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
    )
    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
    celery.Task = ContextTask
    logging.info("Celery initialized successfully.")
    return celery

def create_app(config_class=config.ProductionConfig) -> Flask:
    # Initialize Flask app
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
    app.config.from_object(config_class)

    # Initialize global celery
    global celery
    celery = make_celery(app)

    # Register blueprints
    from .routes import main_bp
    app.register_blueprint(main_bp)

    # Import tasks to ensure they are registered with Celery
    from .tasks import fetch_and_cache_data

    # Setup logging
    Utils.setup_logging(config_class.LOG_LEVEL)

    return app
