import logging
from typing import Dict

logger = logging.getLogger(__name__)


class SensorReadingHandler:
    """
    Handler for sensor/reading topic from ESP32

    This is for when ESP32 pushes data unsolicited
    (vs request/response pattern)
    """

    def __init__(self, bluetooth_polling_service):
        """
        Initialize handler

        Args:
            bluetooth_polling_service: Service for processing readings
        """
        self.bluetooth_polling_service = bluetooth_polling_service

    def handle(self, device_identifier: str, data: Dict):
        """
        Handle sensor reading message

        Args:
            device_identifier: Device that sent the reading
            data: Reading data from ESP32
        """
        logger.info(f"Received sensor reading from {device_identifier}")

        try:
            # Process the reading
            success = self.bluetooth_polling_service._process_reading(
                device_identifier,
                data
            )

            if success:
                logger.info(f"✅ Processed reading from {device_identifier}")
            else:
                logger.warning(f"❌ Failed to process reading from {device_identifier}")

        except Exception as e:
            logger.error(
                f"Error handling sensor reading from {device_identifier}: {e}",
                exc_info=True
            )