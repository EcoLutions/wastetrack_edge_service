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
from config.bluetooth_config import BluetoothConfig
from src.container import Container

# Setup logging first
setup_logging()
logger = logging.getLogger(__name__)


def create_flask_app(container: Container) -> Flask:
    """
    Create and configure Flask application

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
        bluetooth_worker_status = container.bluetooth_polling_worker.is_running()

        status = {
            'status': 'healthy' if (mqtt_status and bluetooth_worker_status) else 'degraded',
            'mqtt_connected': mqtt_status,
            'bluetooth_worker_running': bluetooth_worker_status,
            'pending_sync_count': container.sensor_reading_service.get_pending_sync_count()
        }

        status_code = 200 if status['status'] == 'healthy' else 503
        return jsonify(status), status_code

    # Info endpoint
    @app.route('/info', methods=['GET'])
    def info():
        """Application info endpoint"""
        # Get device count from config loader
        devices = container.device_config_loader.load_devices()

        return jsonify({
            'name': 'Edge Service - Container Monitoring',
            'version': '1.0.0',
            'mode': 'bluetooth_polling',
            'mqtt': {
                'connected': container.mqtt_manager.is_connected(),
                'subscribed': container.mqtt_subscriber.is_subscribed()
            },
            'bluetooth': {
                'polling_interval': BluetoothConfig.POLLING_INTERVAL,
                'devices_configured': len(devices),
                'worker_running': container.bluetooth_polling_worker.is_running(),
                'cycle_count': container.bluetooth_polling_worker.cycle_count
            },
            'database': {
                'devices_count': container.device_repository.count(),
                'configs_count': container.container_config_repository.count(),
                'pending_sync': container.sensor_reading_service.get_pending_sync_count()
            }
        }), 200

    # Bluetooth devices endpoint
    @app.route('/bluetooth/devices', methods=['GET'])
    def bluetooth_devices():
        """Get configured Bluetooth devices"""
        devices = container.device_config_loader.load_devices()

        devices_info = [
            {
                'deviceIdentifier': device.device_identifier,
                'port': device.port,
                'isOnline': device.is_online,
                'lastSeen': device.last_seen.isoformat() if device.last_seen else None,
                'consecutiveFailures': device.consecutive_failures
            }
            for device in devices
        ]

        return jsonify({
            'devices': devices_info,
            'count': len(devices_info)
        }), 200

    logger.info("Flask app created")
    return app


def main():
    """Main application entry point"""
    logger.info("=" * 80)
    logger.info("EDGE SERVICE - CONTAINER MONITORING (BLUETOOTH)")
    logger.info("=" * 80)

    # Create container
    container = Container()

    # Create Flask app
    app = create_flask_app(container)

    # Set up graceful shutdown
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

    # Start Bluetooth worker
    try:
        container.start_bluetooth_worker()
    except Exception as e:
        logger.error(f"Failed to start Bluetooth worker: {e}")
        logger.warning("Continuing without Bluetooth worker")

    # Start Flask
    logger.info("=" * 80)
    logger.info(f"Starting Flask server on {AppConfig.FLASK_HOST}:{AppConfig.FLASK_PORT}")
    logger.info(f"Debug mode: {AppConfig.FLASK_DEBUG}")
    logger.info("=" * 80)
    logger.info("")
    logger.info("Available endpoints:")
    logger.info("  - POST   /api/v1/sensor-readings (legacy REST API)")
    logger.info("  - GET    /api/v1/sensor-readings/pending")
    logger.info("  - GET    /api/v1/sensor-readings/alerts")
    logger.info("  - GET    /health")
    logger.info("  - GET    /info")
    logger.info("  - GET    /bluetooth/devices")
    logger.info("")
    logger.info("Bluetooth Configuration:")
    logger.info(f"  - Polling Interval: {BluetoothConfig.POLLING_INTERVAL}s")
    logger.info(f"  - Baud Rate: {BluetoothConfig.BAUD_RATE}")
    logger.info(f"  - Timeout: {BluetoothConfig.TIMEOUT}s")
    logger.info(f"  - Max Retries: {BluetoothConfig.MAX_RETRIES}")
    logger.info(f"  - Config File: {BluetoothConfig.DEVICES_CONFIG_FILE}")
    logger.info("")
    logger.info("Mode: Bluetooth Polling")
    logger.info("  - Polls devices every 10 seconds")
    logger.info("  - Publishes readings immediately to MQTT")
    logger.info("  - Publishes device online/offline events")
    logger.info("  - Hot reloads bluetooth_devices.json every 5 cycles")
    logger.info("")
    logger.info("=" * 80)

    try:
        app.run(
            host=AppConfig.FLASK_HOST,
            port=AppConfig.FLASK_PORT,
            debug=AppConfig.FLASK_DEBUG,
            use_reloader=False  # Disable reloader to avoid duplicate workers
        )
    except Exception as e:
        logger.error(f"Flask server error: {e}", exc_info=True)
    finally:
        container.shutdown()


if __name__ == '__main__':
    main()