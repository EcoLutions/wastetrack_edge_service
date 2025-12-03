import logging
from datetime import datetime

from flask import Blueprint, request, jsonify

from src.containermonitoring.application.services import SensorReadingService
from src.containermonitoring.infrastructure.messaging import MqttPublisher

logger = logging.getLogger(__name__)


class SensorReadingController:
    """
    REST API Controller for Sensor Readings

    Provides endpoints for IoT devices to submit sensor data.
    Publishes ALL readings immediately to MQTT (no batch worker).

    Endpoints:
    - POST /api/v1/sensor-readings - Submit a new sensor reading
    - GET /api/v1/sensor-readings/pending - Get pending sync count (debugging)
    - GET /api/v1/sensor-readings/alerts - Get recent alerts (debugging)
    """

    def __init__(
            self,
            sensor_reading_service: SensorReadingService,
            mqtt_publisher: MqttPublisher
    ):
        """
        Initialize controller with dependencies

        Args:
            sensor_reading_service: Service for processing readings
            mqtt_publisher: Publisher for immediate MQTT publishing
        """
        self.sensor_reading_service = sensor_reading_service
        self.mqtt_publisher = mqtt_publisher
        self.blueprint = Blueprint('sensor_readings', __name__)
        self._register_routes()

    def _register_routes(self):
        """Register all routes for this controller"""
        self.blueprint.add_url_rule(
            '/api/v1/sensor-readings',
            'create_reading',
            self.create_reading,
            methods=['POST']
        )

        self.blueprint.add_url_rule(
            '/api/v1/sensor-readings/pending',
            'get_pending_count',
            self.get_pending_count,
            methods=['GET']
        )

        self.blueprint.add_url_rule(
            '/api/v1/sensor-readings/alerts',
            'get_recent_alerts',
            self.get_recent_alerts,
            methods=['GET']
        )

    def create_reading(self):
        """
        POST /api/v1/sensor-readings

        Submit a new sensor reading from IoT device
        Publishes IMMEDIATELY to MQTT (alert or normal)

        Request Headers:
            X-Device-Identifier: string (required) - Device identifier for auth

        Request Body (JSON):
        {
            "deviceIdentifier": "SENSOR-001",
            "containerId": "660e8400-e29b-41d4-a716-446,655,440,000",
            "fillLevelPercentage": 85.5,
            "temperatureCelsius": 23.0,
            "batteryLevelPercentage": 78.0,
            "recordedAt": "2025-01-15T10:30:00"
        }

        Response:
        - 201 Created: Reading processed and published to MQTT
        - 400 Bad Request: Invalid request data
        - 401 Unauthorized: Device not authenticated
        - 500 Internal Server Error: Processing failed
        """
        try:
            # Get device identifier from the header (for authentication)
            device_identifier_header = request.headers.get('X-Device-Identifier')

            # Get JSON body
            if not request.is_json:
                return jsonify({
                    'error': 'Content-Type must be application/json'
                }), 400

            data = request.get_json()

            # Validate required fields
            required_fields = [
                'deviceIdentifier',
                'containerId',
                'fillLevelPercentage',
                'temperatureCelsius',
                'batteryLevelPercentage'
            ]

            missing_fields = [f for f in required_fields if f not in data]
            if missing_fields:
                return jsonify({
                    'error': f'Missing required fields: {missing_fields}'
                }), 400

            # Verify header matches body (security check)
            if device_identifier_header and device_identifier_header != data['deviceIdentifier']:
                logger.warning(
                    f"Device identifier mismatch: "
                    f"header={device_identifier_header}, "
                    f"body={data['deviceIdentifier']}"
                )
                return jsonify({
                    'error': 'Device identifier mismatch between header and body'
                }), 400

            # Parse recordedAt (optional, defaults to now)
            if 'recordedAt' in data:
                try:
                    recorded_at = datetime.fromisoformat(
                        data['recordedAt'].replace('Z', '+00:00')
                    )
                except (ValueError, AttributeError) as e:
                    return jsonify({
                        'error': f'Invalid recordedAt format: {e}'
                    }), 400
            else:
                recorded_at = datetime.now()

            # Validate data ranges
            try:
                fill_level = float(data['fillLevelPercentage'])
                temperature = float(data['temperatureCelsius'])
                battery_level = float(data['batteryLevelPercentage'])

                if not (0 <= fill_level <= 100):
                    return jsonify({
                        'error': 'fillLevelPercentage must be between 0 and 100'
                    }), 400

                if not (0 <= battery_level <= 100):
                    return jsonify({
                        'error': 'batteryLevelPercentage must be between 0 and 100'
                    }), 400

            except (ValueError, TypeError) as e:
                return jsonify({
                    'error': f'Invalid numeric values: {e}'
                }), 400

            # Process reading via service
            reading, message = self.sensor_reading_service.process_iot_reading(
                device_identifier=data['deviceIdentifier'],
                container_id=data['containerId'],
                fill_level_percentage=fill_level,
                temperature_celsius=temperature,
                battery_level_percentage=battery_level,
                recorded_at=recorded_at
            )

            # Check if processing failed
            if reading is None:
                # Check if it's an authentication error
                if 'not authenticated' in message.lower():
                    return jsonify({
                        'error': message
                    }), 401
                else:
                    return jsonify({
                        'error': message
                    }), 500

            # Publish IMMEDIATELY to MQTT (alert or normal)
            mqtt_published = False
            if reading.is_alert:
                # Publish alert
                mqtt_published = self.mqtt_publisher.publish_alert(reading)
                logger.info(f"Alert published to MQTT: {mqtt_published}")
            else:
                # Publish normal reading (immediately, not batched)
                mqtt_published = self.mqtt_publisher.publish_reading_batch([reading])
                logger.info(f"Normal reading published to MQTT: {mqtt_published}")

            # Mark as synced if MQTT publish was successful
            if mqtt_published:
                self.sensor_reading_service.mark_reading_as_synced(reading)

            # Success response
            return jsonify({
                'message': 'Reading processed successfully',
                'mqtt_published': mqtt_published,
                'reading': {
                    'id': reading.id,
                    'deviceId': reading.device_id,
                    'containerId': reading.container_id,
                    'fillLevelPercentage': reading.fill_level_percentage,
                    'temperatureCelsius': reading.temperature_celsius,
                    'batteryLevelPercentage': reading.battery_level_percentage,
                    'recordedAt': reading.recorded_at.isoformat(),
                    'receivedAt': reading.received_at.isoformat(),
                    'isAlert': reading.is_alert,
                    'alertType': reading.alert_type.value
                }
            }), 201

        except Exception as e:
            logger.error(f"Unexpected error in create_reading: {e}", exc_info=True)
            return jsonify({
                'error': 'Internal server error'
            }), 500

    def get_pending_count(self):
        """
        GET /api/v1/sensor-readings/pending

        Get count of readings pending sync to Backend

        Note: Without a background worker, this shows readings that failed MQTT to publish

        Response:
        {
            "pendingCount": 0
        }
        """
        try:
            count = self.sensor_reading_service.get_pending_sync_count()

            return jsonify({
                'pendingCount': count
            }), 200

        except Exception as e:
            logger.error(f"Error getting pending count: {e}", exc_info=True)
            return jsonify({
                'error': 'Internal server error'
            }), 500

    def get_recent_alerts(self):
        """
        GET /api/v1/sensor-readings/alerts

        Get recent alert readings

        Query Parameters:
            limit: int (optional) - Maximum number of alerts (default: 20)

        Response:
        {
            "alerts": [...],
            "count": 5
        }
        """
        try:
            # Get limit from query params
            limit = request.args.get('limit', default=20, type=int)

            # Validate limit
            if limit < 1 or limit > 1000:
                return jsonify({
                    'error': 'limit must be between 1 and 1000'
                }), 400

            # Get alerts from service
            alerts = self.sensor_reading_service.get_recent_alerts(limit)

            # Serialize to JSON
            alerts_json = [
                {
                    'id': alert.id,
                    'deviceId': alert.device_id,
                    'containerId': alert.container_id,
                    'fillLevelPercentage': alert.fill_level_percentage,
                    'temperatureCelsius': alert.temperature_celsius,
                    'batteryLevelPercentage': alert.battery_level_percentage,
                    'recordedAt': alert.recorded_at.isoformat(),
                    'receivedAt': alert.received_at.isoformat(),
                    'isAlert': alert.is_alert,
                    'alertType': alert.alert_type.value
                }
                for alert in alerts
            ]

            return jsonify({
                'alerts': alerts_json,
                'count': len(alerts_json)
            }), 200

        except Exception as e:
            logger.error(f"Error getting recent alerts: {e}", exc_info=True)
            return jsonify({
                'error': 'Internal server error'
            }), 500

    def get_blueprint(self):
        """Get Flask Blueprint for registration"""
        return self.blueprint