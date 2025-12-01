import logging
from datetime import datetime

from src.containermonitoring.domain.model.events import (
    DeviceOnlineEvent,
    DeviceOfflineEvent
)
from src.shared.infrastructure.mqtt import MqttConnectionManager

logger = logging.getLogger(__name__)


class DeviceStatusPublisher:
    """
    Publishes device status events to Backend via MQTT

    Topics:
    - cm/devices/events/online - Device came online
    - cm/devices/events/offline - Device went offline

    Responsibilities:
    - Publish device connection state changes
    - Format events for Backend consumption
    - Handle publishes failures gracefully
    """

    # MQTT Topics
    TOPIC_DEVICE_ONLINE = 'cm/devices/events/online'
    TOPIC_DEVICE_OFFLINE = 'cm/devices/events/offline'

    def __init__(self, mqtt_manager: MqttConnectionManager):
        """
        Initialize publisher

        Args:
            mqtt_manager: Shared MQTT connection manager
        """
        self.mqtt_manager = mqtt_manager

    def publish_device_online(
            self,
            device_id: str,
            device_identifier: str
    ) -> bool:
        """
        Publish device online event

        Args:
            device_id: UUID of the device
            device_identifier: Device identifier (e.g., "SENSOR-001")

        Returns:
            True if published successfully, False otherwise
        """
        try:
            event = DeviceOnlineEvent(
                device_id=device_id,
                device_identifier=device_identifier,
                occurred_at=datetime.now()
            )

            payload = event.to_mqtt_payload()

            success = self.mqtt_manager.publish(
                topic=self.TOPIC_DEVICE_ONLINE,
                payload=payload,
                retain=False
            )

            if success:
                logger.info(
                    f" Device ONLINE event published: {device_identifier}"
                )
            else:
                logger.warning(
                    f" Failed to publish device ONLINE event: {device_identifier}"
                )

            return success

        except Exception as e:
            logger.error(
                f"Error publishing device online event for {device_identifier}: {e}",
                exc_info=True
            )
            return False

    def publish_device_offline(
            self,
            device_id: str,
            device_identifier: str,
            reason: str = "CONNECTION_TIMEOUT",
            consecutive_failures: int = 1
    ) -> bool:
        """
        Publish device offline event

        Args:
            device_id: UUID of the device
            device_identifier: Device identifier (e.g., "SENSOR-001")
            reason: Reason for offline status
            consecutive_failures: Number of consecutive failures

        Returns:
            True if published successfully, False otherwise
        """
        try:
            event = DeviceOfflineEvent(
                device_id=device_id,
                device_identifier=device_identifier,
                occurred_at=datetime.now(),
                reason=reason,
                consecutive_failures=consecutive_failures
            )

            payload = event.to_mqtt_payload()

            success = self.mqtt_manager.publish(
                topic=self.TOPIC_DEVICE_OFFLINE,
                payload=payload,
                retain=False
            )

            if success:
                logger.warning(
                    f"⚠  Device OFFLINE event published: {device_identifier} "
                    f"(failures: {consecutive_failures})"
                )
            else:
                logger.error(
                    f" Failed to publish device OFFLINE event: {device_identifier}"
                )

            return success

        except Exception as e:
            logger.error(
                f"Error publishing device offline event for {device_identifier}: {e}",
                exc_info=True
            )
            return False

    def is_connected(self) -> bool:
        """
        Check if the MQTT connection is active

        Returns:
            True if connected to broker, False otherwise
        """
        return self.mqtt_manager.is_connected()