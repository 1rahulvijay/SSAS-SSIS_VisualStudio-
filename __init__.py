from flask import Flask
import os
from App import config
from App.utils import Utils

def create_app(config_class=config.ProductionConfig) -> Flask:
    # Initialize Flask app
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
    app.config.from_object(config_class)

    # Register blueprints
    from .routes import main_bp
    app.register_blueprint(main_bp)

    # Setup logging
    Utils.setup_logging(config_class.LOG_LEVEL)

    return app
