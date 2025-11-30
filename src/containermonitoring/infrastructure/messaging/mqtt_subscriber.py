import logging
from src.shared.infrastructure.mqtt import MqttConnectionManager
from config.mqtt_config import MqttConfig
from src.containermonitoring.application.event_handlers import (
    DeviceEventHandler,
    ContainerConfigHandler
)

logger = logging.getLogger(__name__)


class MqttSubscriber:
    """
    MQTT Subscriber for receiving events from Backend

    Subscribes to topics:
    - cm/devices/events/created - Device registration events
    - cm/devices/events/updated - Device update events
    - cm/containers/events/config/updated - Container config updates

    Responsibilities:
    - Subscribe to Backend event topics
    - Route incoming messages to appropriate handlers
    - Manage subscription lifecycle
    - Handle connection/reconnection
    """

    def __init__(
            self,
            mqtt_manager: MqttConnectionManager,
            device_event_handler: DeviceEventHandler,
            container_config_handler: ContainerConfigHandler
    ):
        """
        Initialize subscriber with dependencies

        Args:
            mqtt_manager: Shared MQTT connection manager
            device_event_handler: Handler for device events
            container_config_handler: Handler for container config events
        """
        self.mqtt_manager = mqtt_manager
        self.device_event_handler = device_event_handler
        self.container_config_handler = container_config_handler
        self.subscriptions_active = False

    def subscribe_to_backend_events(self):
        """
        Subscribe to all Backend event topics

        Sets up subscriptions for:
        - Device events (created, updated)
        - Container config events (updated)

        Note: Subscriptions persist across reconnections due to
        QoS 1 and clean_session=False in MqttConnectionManager
        """
        try:
            logger.info("Subscribing to Backend event topics...")

            # Subscribe to device-created events
            self.mqtt_manager.subscribe(
                topic=MqttConfig.TOPIC_DEVICE_CREATED,
                handler=self.device_event_handler.handle_device_created
            )
            logger.info(f"{MqttConfig.TOPIC_DEVICE_CREATED}")

            # Subscribe to device updated events
            self.mqtt_manager.subscribe(
                topic=MqttConfig.TOPIC_DEVICE_UPDATED,
                handler=self.device_event_handler.handle_device_updated
            )
            logger.info(f"{MqttConfig.TOPIC_DEVICE_UPDATED}")

            # Subscribe to container config updated events
            self.mqtt_manager.subscribe(
                topic=MqttConfig.TOPIC_CONTAINER_CONFIG_UPDATED,
                handler=self.container_config_handler.handle_config_updated
            )
            logger.info(f"{MqttConfig.TOPIC_CONTAINER_CONFIG_UPDATED}")

            self.subscriptions_active = True
            logger.info("All Backend event subscriptions active")

        except Exception as e:
            logger.error(f"Error subscribing to Backend events: {e}", exc_info=True)
            self.subscriptions_active = False

    def subscribe_with_wildcard(self):
        """
        Alternative: Subscribe using a wildcard pattern

        Subscribes to: cm/# (all topics under cm/)

        This is useful for debugging but less specific than
        individual topic subscriptions. Use with caution in production.
        """
        try:
            logger.info("Subscribing to wildcard pattern: cm/#")

            def wildcard_handler(topic: str, payload: dict):
                """Route wildcard messages to appropriate handlers"""
                logger.debug(f"Wildcard received: {topic}")

                # Route to specific handlers based on a topic
                if topic == MqttConfig.TOPIC_DEVICE_CREATED:
                    self.device_event_handler.handle_device_created(topic, payload)
                elif topic == MqttConfig.TOPIC_DEVICE_UPDATED:
                    self.device_event_handler.handle_device_updated(topic, payload)
                elif topic == MqttConfig.TOPIC_CONTAINER_CONFIG_UPDATED:
                    self.container_config_handler.handle_config_updated(topic, payload)
                else:
                    logger.warning(f"Unhandled topic in wildcard: {topic}")

            self.mqtt_manager.subscribe(
                topic="cm/#",
                handler=wildcard_handler
            )

            self.subscriptions_active = True
            logger.info("Wildcard subscription active")

        except Exception as e:
            logger.error(f"Error subscribing with wildcard: {e}", exc_info=True)
            self.subscriptions_active = False

    def is_subscribed(self) -> bool:
        """
        Check if subscriptions are active

        Returns:
            True if subscribed to Backend events, False otherwise
        """
        return self.subscriptions_active and self.mqtt_manager.is_connected()

    def get_subscription_status(self) -> dict:
        """
        Get detailed subscription status

        Returns:
            Dictionary with subscription details
        """
        return {
            'subscriptions_active': self.subscriptions_active,
            'mqtt_connected': self.mqtt_manager.is_connected(),
            'topics': [
                MqttConfig.TOPIC_DEVICE_CREATED,
                MqttConfig.TOPIC_DEVICE_UPDATED,
                MqttConfig.TOPIC_CONTAINER_CONFIG_UPDATED
            ]
        }