import logging
from typing import Callable
from src.containermonitoring.domain.model.events import ContainerConfigUpdatedEvent
from src.containermonitoring.domain.model.aggregates import ContainerConfig
from src.containermonitoring.infrastructure.persistence import ContainerConfigRepository

logger = logging.getLogger(__name__)


class ContainerConfigHandler:
    """
    Handler for ContainerConfig-related events from Backend

    Subscribes to:
    - cm/containers/events/config/updated

    Responsibilities:
    - Parse MQTT payload to domain event
    - Create/update ContainerConfig aggregate
    - Persist to SQLite via repository
    - Handle errors gracefully
    """

    def __init__(self, container_config_repository: ContainerConfigRepository):
        """
        Initialize handler with dependencies

        Args:
            container_config_repository: Repository for container config persistence
        """
        self.container_config_repository = container_config_repository

    def handle_config_updated(self, topic: str, payload: dict) -> None:
        """
        Handle ContainerConfigUpdatedEvent from Backend

        Flow:
        1. Parse MQTT payload -> ContainerConfigUpdatedEvent
        2. Check if config already exists
        3. If exists -> Update a threshold
        4. If not exists -> Create new config
        5. Save to SQLite

        Args:
            topic: MQTT topic (should be cm/containers/events/config/updated)
            payload: MQTT message payload as dict
        """
        try:
            logger.info(f"Processing ContainerConfigUpdatedEvent from topic: {topic}")

            # Parse payload to domain event
            event = ContainerConfigUpdatedEvent.from_mqtt_payload(payload)
            logger.debug(f"Parsed event: {event}")

            # Check if config already exists
            existing_config = self.container_config_repository.find_by_id(
                event.container_id
            )

            if existing_config:
                # Update existing config
                logger.info(
                    f"Updating existing config for container: {event.container_id}"
                )
                logger.info(
                    f"  Old threshold: {existing_config.max_fill_level_threshold}%"
                )
                logger.info(
                    f"  New threshold: {event.max_fill_level_threshold}%"
                )

                existing_config.update_threshold(
                    new_threshold=event.max_fill_level_threshold,
                    occurred_at=event.occurred_at
                )

                # Update sensor_id if changed
                if existing_config.sensor_id != event.sensor_id:
                    logger.info(
                        f" Sensor ID changed: {existing_config.sensor_id} -> "
                        f"{event.sensor_id}"
                    )
                    # Create a new config with updated sensor_id
                    updated_config = ContainerConfig.from_backend_event(
                        container_id=event.container_id,
                        max_fill_level_threshold=event.max_fill_level_threshold,
                        sensor_id=event.sensor_id,
                        occurred_at=event.occurred_at
                    )
                    self.container_config_repository.save(updated_config)
                else:
                    self.container_config_repository.save(existing_config)

                logger.info(
                    f"ContainerConfig updated: {event.container_id} "
                    f"(threshold={event.max_fill_level_threshold}%)"
                )
            else:
                # Create a new config
                logger.info(
                    f"Creating new config for container: {event.container_id}"
                )

                config = ContainerConfig.from_backend_event(
                    container_id=event.container_id,
                    max_fill_level_threshold=event.max_fill_level_threshold,
                    sensor_id=event.sensor_id,
                    occurred_at=event.occurred_at
                )

                self.container_config_repository.save(config)

                logger.info(
                    f"ContainerConfig created: {config.container_id} "
                    f"(threshold={config.max_fill_level_threshold}%, "
                    f"sensor={config.sensor_id})"
                )

        except ValueError as e:
            # Validation error in event parsing
            logger.error(f"Invalid ContainerConfigUpdatedEvent payload: {e}")
            logger.error(f"Payload: {payload}")

        except Exception as e:
            # Unexpected error
            logger.error(
                f"Error handling ContainerConfigUpdatedEvent: {e}",
                exc_info=True
            )
            logger.error(f"Payload: {payload}")

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
            'cm/containers/events/config/updated': self.handle_config_updated
        }

        if topic not in handlers:
            raise ValueError(f"Unsupported topic: {topic}")

        return handlers[topic]