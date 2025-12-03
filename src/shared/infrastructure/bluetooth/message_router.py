import logging
from typing import Dict, Callable

logger = logging.getLogger(__name__)


class BluetoothMessageRouter:
    """
    Routes incoming Bluetooth messages by topic to appropriate handlers

    Implements topic-based message routing similar to MQTT
    """

    def __init__(self):
        """Initialize router with empty handler registry"""
        self.handlers: Dict[str, Callable[[str, Dict], None]] = {}
        logger.info("Bluetooth Message Router initialized")

    def register_handler(
            self,
            topic: str,
            handler: Callable[[str, Dict], None]
    ):
        """
        Register a handler for a specific topic

        Args:
            topic: Topic to handle (e.g., "sensor/reading")
            handler: Function to call when message with this topic arrives
                     Signature: handler(device_identifier: str, data: dict)
        """
        self.handlers[topic] = handler
        logger.info(f"Registered handler for topic: {topic}")

    def route_message(self, device_identifier: str, message: Dict):
        """
        Route a message to the appropriate handler

        Args:
            device_identifier: Identifier of device that sent message
            message: Message dict with 'topic' and 'data' keys
        """
        if not isinstance(message, dict):
            logger.warning(
                f"Invalid message from {device_identifier}: not a dict"
            )
            return

        topic = message.get('topic')
        data = message.get('data', {})

        if not topic:
            logger.warning(
                f"Message from {device_identifier} missing 'topic' field"
            )
            return

        # Find handler
        handler = self.handlers.get(topic)

        if handler:
            try:
                logger.debug(
                    f"Routing message from {device_identifier}: "
                    f"topic={topic}"
                )
                handler(device_identifier, data)
            except Exception as e:
                logger.error(
                    f"Error in handler for topic '{topic}': {e}",
                    exc_info=True
                )
        else:
            logger.warning(
                f"No handler registered for topic: {topic} "
                f"(from {device_identifier})"
            )

    def get_registered_topics(self) -> list:
        """Get list of registered topics"""
        return list(self.handlers.keys())