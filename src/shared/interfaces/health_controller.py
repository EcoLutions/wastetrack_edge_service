import logging
from flask import Blueprint, jsonify

logger = logging.getLogger(__name__)


class HealthController:
    """
    Controller for health and info endpoints
    """

    def __init__(self, container):
        """
        Initialize controller with container

        Args:
            container: DI container with all dependencies
        """
        self.container = container
        self.blueprint = Blueprint('health', __name__)
        self._register_routes()

    def _register_routes(self):
        """Register all routes"""
        self.blueprint.add_url_rule(
            '/health',
            'health_check',
            self.health_check,
            methods=['GET']
        )

        self.blueprint.add_url_rule(
            '/info',
            'info',
            self.info,
            methods=['GET']
        )

    def health_check(self):
        """
        GET /health

        Health check endpoint
        """
        try:
            mqtt_status = self.container.mqtt_manager.is_connected()
            worker_status = self.container.sync_worker.is_running()

            status = {
                'status': 'healthy' if (mqtt_status and worker_status) else 'degraded',
                'mqtt_connected': mqtt_status,
                'sync_worker_running': worker_status,
                'pending_sync_count': self.container.sensor_reading_service.get_pending_sync_count()
            }

            status_code = 200 if status['status'] == 'healthy' else 503
            return jsonify(status), status_code

        except Exception as e:
            logger.error(f"Error in health check: {e}", exc_info=True)
            return jsonify({'error': 'Internal server error'}), 500

    def info(self):
        """
        GET /info

        Application info endpoint
        """
        try:
            return jsonify({
                'name': 'Edge Service - Container Monitoring',
                'version': '1.0.0',
                'mqtt': {
                    'connected': self.container.mqtt_manager.is_connected(),
                    'subscribed': self.container.mqtt_subscriber.is_subscribed()
                },
                'workers': {
                    'sync_worker': {
                        'running': self.container.sync_worker.is_running(),
                        'interval_seconds': self.container.sync_worker.interval_seconds
                    }
                },
                'database': {
                    'devices_count': self.container.device_repository.count(),
                    'configs_count': self.container.container_config_repository.count(),
                    'pending_sync': self.container.sensor_reading_service.get_pending_sync_count()
                }
            }), 200

        except Exception as e:
            logger.error(f"Error in info endpoint: {e}", exc_info=True)
            return jsonify({'error': 'Internal server error'}), 500

    def get_blueprint(self):
        """Get Flask Blueprint"""
        return self.blueprint