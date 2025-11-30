import logging
from typing import Dict

logger = logging.getLogger(__name__)

class SensorStatusHandler:
    """
    Handler for sensor/status topic from ESP32

    Receives health/status information from device
    """

    def handle(self, device_identifier: str, data: Dict):
        """
        Handle sensor status message

        Args:
            device_identifier: Device that sent the status
            data: Status data from ESP32
                  e.g., {"battery": 85.0, "signal": "strong", "uptime": 36000}
        """
        logger.info(f"Received sensor status from {device_identifier}")
        logger.debug(f"Status data: {data}")

        # Extract status info
        battery = data.get('battery')
        signal = data.get('signal')
        uptime = data.get('uptime')

        # Log status
        if battery is not None:
            logger.info(f"  Battery: {battery}%")

        if signal:
            logger.info(f"  Signal: {signal}")

        if uptime is not None:
            logger.info(f"  Uptime: {uptime}s")

        # TODO: Could save this to database if needed
        # For now, just logging is sufficient