"""Main application file - Production Monitoring System (Flask)"""
from flask import Flask
from flask_cors import CORS
from web_routes import web_bp
from api_routes import api_bp

def create_app():
    """Create and configure the Flask application."""
    app = Flask(__name__, static_folder="static", template_folder="templates")
    # Allow all origins by default; update as needed
    CORS(app, resources={r"/*": {"origins": ["*"]}}, supports_credentials=True)

    # Register blueprints
    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp)
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5002)