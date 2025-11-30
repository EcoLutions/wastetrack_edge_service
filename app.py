import logging
import signal
import sys

from flask import Flask, jsonify
from flask_cors import CORS

# Setup paths
sys.path.insert(0, './src')

# Import configuration and setup
from src.shared.infrastructure.logging import setup_logging
from config.app_config import AppConfig
from src.container import Container

# Setup logging first
setup_logging()
logger = logging.getLogger(__name__)


def create_flask_app(container: Container) -> Flask:
    """
    Create and configure a Flask application

    Args:
        container: Dependency injection container

    Returns:
        Configured Flask app
    """
    app = Flask(__name__)

    # Enable CORS
    CORS(app)

    # Register blueprints
    app.register_blueprint(container.sensor_reading_controller.get_blueprint())

    # Health check endpoint
    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint"""
        mqtt_status = container.mqtt_manager.is_connected()

        status = {
            'status': 'healthy' if mqtt_status else 'degraded',
            'mqtt_connected': mqtt_status,
            'pending_sync_count': container.sensor_reading_service.get_pending_sync_count()
        }

        status_code = 200 if status['status'] == 'healthy' else 503
        return jsonify(status), status_code

    # Info endpoint
    @app.route('/info', methods=['GET'])
    def info():
        """Application info endpoint"""
        return jsonify({
            'name': 'Edge Service - Container Monitoring',
            'version': '1.0.0',
            'mode': 'immediate_publish',  # No background workers
            'mqtt': {
                'connected': container.mqtt_manager.is_connected(),
                'subscribed': container.mqtt_subscriber.is_subscribed()
            },
            'database': {
                'devices_count': container.device_repository.count(),
                'configs_count': container.container_config_repository.count(),
                'pending_sync': container.sensor_reading_service.get_pending_sync_count()
            }
        }), 200

    logger.info("Flask app created")
    return app


def main():
    """Main application entry point"""
    logger.info("=" * 80)
    logger.info("EDGE SERVICE - CONTAINER MONITORING")
    logger.info("Mode: IMMEDIATE PUBLISH (No Background Workers)")
    logger.info("=" * 80)

    # Create container
    container = Container()

    # Create Flask app
    app = create_flask_app(container)

    # Set up a graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"\nReceived signal {sig}, initiating shutdown...")
        container.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start MQTT
    try:
        container.start_mqtt()
    except Exception as e:
        logger.error(f"Failed to start MQTT: {e}")
        logger.warning("Continuing without MQTT (degraded mode)")

    # Start Flask
    logger.info("=" * 80)
    logger.info(f"Starting Flask server on {AppConfig.FLASK_HOST}:{AppConfig.FLASK_PORT}")
    logger.info(f"Debug mode: {AppConfig.FLASK_DEBUG}")
    logger.info("=" * 80)

    try:
        app.run(
            host=AppConfig.FLASK_HOST,
            port=AppConfig.FLASK_PORT,
            debug=AppConfig.FLASK_DEBUG,
            use_reloader=False
        )
    except Exception as e:
        logger.error(f"Flask server error: {e}", exc_info=True)
    finally:
        container.shutdown()


if __name__ == '__main__':
    main()