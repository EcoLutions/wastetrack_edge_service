import logging
from typing import List
from datetime import datetime
from src.shared.infrastructure.mqtt import MqttConnectionManager
from config.mqtt_config import MqttConfig
from src.containermonitoring.domain.model.aggregates import SensorReading

logger = logging.getLogger(__name__)


class MqttPublisher:
    """
    MQTT Publisher for sending sensor data to Backend

    Publishes to topics:
    - cm/sensors/alerts/full - Immediate alerts (FULL_CONTAINER)
    - cm/sensors/readings/batch - Batch of normal readings

    Responsibilities:
    - Publish individual alert readings immediately
    - Publish batches of normal readings periodically
    - Format payloads according to Backend expectations
    - Handle publishes failures gracefully
    """

    def __init__(self, mqtt_manager: MqttConnectionManager):
        """
        Initialize publisher with MQTT connection

        Args:
            mqtt_manager: Shared MQTT connection manager
        """
        self.mqtt_manager = mqtt_manager

    def publish_alert(self, reading: SensorReading) -> bool:
        """
        Publish an alert reading immediately to Backend

        Topic: cm/sensors/alerts/full

        Payload format:
        {
            "eventType": "SENSOR_ALERT",
            "alertType": "FULL_CONTAINER",
            "reading": {
                "deviceId": "...",
                "containerId": "...",
                "fillLevelPercentage": 95.0,
                "temperatureCelsius": 23.0,
                "batteryLevelPercentage": 80.0,
                "recordedAt": "2025-01-15T10:30:00",
                "receivedAt": "2025-01-15T10:30:05",
                "isAlert": true,
                "alertType": "FULL_CONTAINER"
            },
            "publishedAt": "2025-01-15T10:30:06"
        }

        Args:
            reading: SensorReading aggregate with is_alert=True

        Returns:
            True if published successfully, False otherwise
        """
        if not reading.is_alert:
            logger.warning(
                f"Attempted to publish non-alert reading as alert: {reading.id}"
            )
            return False

        try:
            # Build payload
            payload = {
                "eventType": "SENSOR_ALERT",
                "alertType": reading.alert_type.value,
                "reading": reading.to_mqtt_payload(),
                "publishedAt": datetime.now().isoformat()
            }

            # Publish to MQTT
            success = self.mqtt_manager.publish(
                topic=MqttConfig.TOPIC_SENSOR_ALERT,
                payload=payload,
                retain=False  # Don't retain alerts
            )

            if success:
                logger.info(
                    f"Alert published: reading_id={reading.id}, "
                    f"container={reading.container_id}, "
                    f"fill={reading.fill_level_percentage}%"
                )
            else:
                logger.error(
                    f"Failed to publish alert: reading_id={reading.id}"
                )

            return success

        except Exception as e:
            logger.error(
                f"Error publishing alert for reading {reading.id}: {e}",
                exc_info=True
            )
            return False

    def publish_reading_batch(self, readings: List[SensorReading]) -> bool:
        """
        Publish a batch of sensor readings to Backend

        Topic: cm/sensors/readings/batch

        Payload format:
        {
            "eventType": "SENSOR_READING_BATCH",
            "count": 120,
            "readings": [
                {
                    "deviceId": "...",
                    "containerId": "...",
                    "fillLevelPercentage": 65.0,
                },
            ],
            "publishedAt": "2025-01-15T11:00:00"
        }

        Args:
            readings: List of SensorReading aggregates to publish

        Returns:
            True if published successfully, False otherwise
        """
        if not readings:
            logger.warning("Attempted to publish empty batch")
            return False

        try:
            # Build payload
            payload = {
                "eventType": "SENSOR_READING_BATCH",
                "count": len(readings),
                "readings": [
                    reading.to_mqtt_payload()
                    for reading in readings
                ],
                "publishedAt": datetime.now().isoformat()
            }

            # Publish to MQTT
            success = self.mqtt_manager.publish(
                topic=MqttConfig.TOPIC_SENSOR_READING_BATCH,
                payload=payload,
                retain=False  # Don't retain batches
            )

            if success:
                logger.info(
                    f"Batch published: {len(readings)} readings "
                    f"(IDs: {readings[0].id} - {readings[-1].id})"
                )
            else:
                logger.error(
                    f"Failed to publish batch of {len(readings)} readings"
                )

            return success

        except Exception as e:
            logger.error(
                f"Error publishing batch of {len(readings)} readings: {e}",
                exc_info=True
            )
            return False

    def publish_readings(self, readings: List[SensorReading]) -> tuple[int, int]:
        """
        Publish multiple readings (alerts immediately, others in batch)

        This is a convenience method that:
        1. Separates alerts from normal readings
        2. Publishes alerts individually
        3. Publishes normal readings as a batch

        Args:
            readings: List of SensorReading aggregates

        Returns:
            Tuple of (successful_alerts, successful_batch_count)
        """
        if not readings:
            return 0, 0

        # Separate alerts from normal readings
        alerts = [r for r in readings if r.is_alert]
        normal_readings = [r for r in readings if not r.is_alert]

        # Publish alerts individually
        successful_alerts = 0
        for alert in alerts:
            if self.publish_alert(alert):
                successful_alerts += 1

        # Publish normal readings as a batch
        successful_batch_count = 0
        if normal_readings:
            if self.publish_reading_batch(normal_readings):
                successful_batch_count = len(normal_readings)

        logger.info(
            f"Published: {successful_alerts}/{len(alerts)} alerts, "
            f"{successful_batch_count}/{len(normal_readings)} normal readings"
        )

        return successful_alerts, successful_batch_count

    def is_connected(self) -> bool:
        """
        Check if the MQTT connection is active

        Returns:
            True if connected to broker, False otherwise
        """
        return self.mqtt_manager.is_connected()