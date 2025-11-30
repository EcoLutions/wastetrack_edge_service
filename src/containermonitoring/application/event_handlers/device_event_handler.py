import logging
from typing import Callable
from src.containermonitoring.domain.model.events import DeviceCreatedEvent
from src.containermonitoring.domain.model.aggregates import Device
from src.containermonitoring.infrastructure.persistence import DeviceRepository

logger = logging.getLogger(__name__)


class DeviceEventHandler:
    """
    Handler for Device-related events from Backend

    Subscribes to:
    - cm/devices/events/created
    - cm/devices/events/updated (future)

    Responsibilities:
    - Parse MQTT payload to domain event
    - Create/update Device aggregate
    - Persist to SQLite via repository
    - Handle errors gracefully
    """

    def __init__(self, device_repository: DeviceRepository):
        """
        Initialize handler with dependencies

        Args:
            device_repository: Repository for device persistence
        """
        self.device_repository = device_repository

    def handle_device_created(self, topic: str, payload: dict) -> None:
        """
        Handle DeviceCreatedEvent from Backend

        Flow:
        1. Parse MQTT payload → DeviceCreatedEvent
        2. Create Device aggregate from event
        3. Check if a device already exists (idempotency)
        4. Save to SQLite

        Args:
            topic: MQTT topic (should be cm/devices/events/created)
            payload: MQTT message payload as dict
        """
        try:
            logger.info(f"Processing DeviceCreatedEvent from topic: {topic}")

            # Parse payload to domain event
            event = DeviceCreatedEvent.from_mqtt_payload(payload)
            logger.debug(f"Parsed event: {event}")

            # Check if a device already exists (idempotency)
            existing_device = self.device_repository.find_by_id(event.device_id)

            if existing_device:
                logger.info(
                    f"Device already exists: {event.device_identifier} "
                    f"(idempotent operation)"
                )
                # Update in case identifier changed
                updated_device = Device.from_backend_event(
                    device_id=event.device_id,
                    device_identifier=event.device_identifier,
                    occurred_at=event.occurred_at
                )
                self.device_repository.save(updated_device)
                logger.info(f"Device updated: {event.device_identifier}")
            else:
                # Create a new device aggregate
                device = Device.from_backend_event(
                    device_id=event.device_id,
                    device_identifier=event.device_identifier,
                    occurred_at=event.occurred_at
                )

                # Persist to SQLite
                self.device_repository.save(device)

                logger.info(
                    f"✅ Device created and saved: {device.device_identifier} "
                    f"(ID: {device.device_id})"
                )

        except ValueError as e:
            # Validation error in event parsing
            logger.error(f"Invalid DeviceCreatedEvent payload: {e}")
            logger.error(f"Payload: {payload}")

        except Exception as e:
            # Unexpected error
            logger.error(
                f"Error handling DeviceCreatedEvent: {e}",
                exc_info=True
            )
            logger.error(f"Payload: {payload}")

    def handle_device_updated(self, topic: str, payload: dict) -> None:
        """
        Handle DeviceUpdatedEvent from Backend (future implementation)

        Currently just logs the event.
        In the future, this could update device properties.

        Args:
            topic: MQTT topic
            payload: MQTT message payload as dict
        """
        try:
            logger.info(f"Processing DeviceUpdatedEvent from topic: {topic}")
            logger.debug(f"Payload: {payload}")

            # TODO: Implement DeviceUpdatedEvent parsing and handling
            logger.warning("DeviceUpdatedEvent handling not yet implemented")

        except Exception as e:
            logger.error(
                f"Error handling DeviceUpdatedEvent: {e}",
                exc_info=True
            )

    def get_handler_for_topic(self, topic: str) -> Callable[[str, dict], None]:
        """
        Get the appropriate handler function for a topic

        Args:
            topic: MQTT topic pattern

        Returns:
            Handler function that accepts (topic, payload)

        Raises:
            ValueError: If a topic is not supported
        """
        handlers = {
            'cm/devices/events/created': self.handle_device_created,
            'cm/devices/events/updated': self.handle_device_updated
        }

        if topic not in handlers:
            raise ValueError(f"Unsupported topic: {topic}")

        return handlers[topic]