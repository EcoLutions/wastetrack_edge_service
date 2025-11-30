import logging
from typing import Optional, Dict
from .serial_client import SerialClient
from config.bluetooth_config import BluetoothConfig

logger = logging.getLogger(__name__)


class BluetoothCommandSender:
    """
    Sends commands to ESP32 devices via Bluetooth

    Provides high-level methods for common commands
    """

    def __init__(self, serial_client: SerialClient):
        """
        Initialize command sender

        Args:
            serial_client: Serial client for communication
        """
        self.serial_client = serial_client

    def ping(self) -> bool:
        """
        Send ping command and wait for pong response

        Returns:
            True if device responded with pong, False otherwise
        """
        message = {
            "topic": BluetoothConfig.TOPIC_COMMAND_PING,
            "data": {}
        }

        response = self.serial_client.send_and_wait_response(
            message=message,
            expected_topic=BluetoothConfig.TOPIC_COMMAND_PONG,
            timeout=BluetoothConfig.TIMEOUT
        )

        return response is not None

    def request_current_reading(self) -> Optional[Dict]:
        """
        Request current sensor reading from device

        Returns:
            Sensor reading data or None if timeout/error
        """
        message = {
            "topic": BluetoothConfig.TOPIC_REQUEST_CURRENT_READING,
            "data": {}
        }

        response = self.serial_client.send_and_wait_response(
            message=message,
            expected_topic=BluetoothConfig.TOPIC_RESPONSE_CURRENT_READING,
            timeout=BluetoothConfig.TIMEOUT
        )

        if response:
            return response.get('data')

        return None

    def set_threshold(self, threshold: float) -> bool:
        """
        Set fill level threshold on device

        Args:
            threshold: Threshold percentage (0-100)

        Returns:
            True if sent successfully
        """
        if not (0 <= threshold <= 100):
            logger.error(f"Invalid threshold: {threshold} (must be 0-100)")
            return False

        message = {
            "topic": BluetoothConfig.TOPIC_CONFIG_THRESHOLD,
            "data": {
                "value": threshold
            }
        }

        return self.serial_client.send_message(message)

    def set_interval(self, seconds: int) -> bool:
        """
        Set data sending interval on device

        Args:
            seconds: Interval in seconds

        Returns:
            True if sent successfully
        """
        if seconds < 1:
            logger.error(f"Invalid interval: {seconds} (must be >= 1)")
            return False

        message = {
            "topic": BluetoothConfig.TOPIC_CONFIG_INTERVAL,
            "data": {
                "seconds": seconds
            }
        }

        return self.serial_client.send_message(message)

    def reset_device(self) -> bool:
        """
        Send reset command to device

        Returns:
            True if sent successfully
        """
        message = {
            "topic": BluetoothConfig.TOPIC_COMMAND_RESET,
            "data": {}
        }

        return self.serial_client.send_message(message)