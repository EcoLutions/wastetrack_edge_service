import serial
import json
import logging
import time
from typing import Optional, Dict
from config.bluetooth_config import BluetoothConfig

logger = logging.getLogger(__name__)


class SerialClient:
    """
    Bluetooth Serial Client with retry logic

    Handles serial communication with ESP32 devices
    """

    def __init__(self, port: str):
        """
        Initialize serial client

        Args:
            port: Serial port (e.g., COM4, /dev/ttyUSB0)
        """
        self.port = port
        self.connection: Optional[serial.Serial] = None
        self.is_connected = False

    def connect(self) -> bool:
        """
        Connect to serial port with retry logic

        Returns:
            True if connected, False otherwise
        """
        for attempt in range(BluetoothConfig.MAX_RETRIES):
            try:
                logger.info(
                    f"Connecting to {self.port} "
                    f"(attempt {attempt + 1}/{BluetoothConfig.MAX_RETRIES})..."
                )

                self.connection = serial.Serial(
                    port=self.port,
                    baudrate=BluetoothConfig.BAUD_RATE,
                    timeout=BluetoothConfig.TIMEOUT,
                    write_timeout=BluetoothConfig.WRITE_TIMEOUT
                )

                self.is_connected = True
                logger.info(f"✅ Connected to {self.port}")
                return True

            except serial.SerialException as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")

                if attempt < BluetoothConfig.MAX_RETRIES - 1:
                    logger.info(f"Retrying in {BluetoothConfig.RETRY_DELAY}s...")
                    time.sleep(BluetoothConfig.RETRY_DELAY)
                else:
                    logger.error(f"❌ Failed to connect to {self.port} after {BluetoothConfig.MAX_RETRIES} attempts")

            except Exception as e:
                logger.error(f"Unexpected error connecting to {self.port}: {e}", exc_info=True)
                break

        self.is_connected = False
        return False

    def disconnect(self):
        """Disconnect from serial port"""
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
                logger.info(f"Disconnected from {self.port}")
        except Exception as e:
            logger.error(f"Error disconnecting from {self.port}: {e}")
        finally:
            self.is_connected = False
            self.connection = None

    def send_message(self, message: Dict) -> bool:
        """
        Send JSON message to ESP32

        Args:
            message: Dictionary to send as JSON

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.is_connected or not self.connection:
            logger.warning(f"Cannot send message: not connected to {self.port}")
            return False

        try:
            # Convert to JSON and add newline
            json_str = json.dumps(message) + '\n'

            # Send
            self.connection.write(json_str.encode('utf-8'))
            self.connection.flush()

            logger.debug(f"Sent to {self.port}: {json_str.strip()}")
            return True

        except serial.SerialTimeoutException:
            logger.error(f"Timeout sending message to {self.port}")
            return False

        except Exception as e:
            logger.error(f"Error sending message to {self.port}: {e}", exc_info=True)
            return False

    def read_message(self, timeout: Optional[float] = None) -> Optional[Dict]:
        """
        Read JSON message from ESP32

        Args:
            timeout: Read timeout in seconds (uses default if None)

        Returns:
            Parsed JSON dict or None if error/timeout
        """
        if not self.is_connected or not self.connection:
            logger.warning(f"Cannot read message: not connected to {self.port}")
            return None

        try:
            # Set timeout if provided
            if timeout is not None:
                original_timeout = self.connection.timeout
                self.connection.timeout = timeout

            # Read line
            raw_line = self.connection.readline()

            # Restore original timeout
            if timeout is not None:
                self.connection.timeout = original_timeout

            if not raw_line:
                return None

            # Decode
            line = raw_line.decode('utf-8').strip()

            if not line:
                return None

            # Parse JSON
            if line.startswith('{'):
                try:
                    message = json.loads(line)
                    logger.debug(f"Received from {self.port}: {line}")
                    return message
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON from {self.port}: {line[:100]}")
                    return None
            else:
                # Non-JSON message (debug/info from ESP32)
                logger.info(f"[{self.port}] {line}")
                return None

        except serial.SerialException as e:
            logger.error(f"Serial error reading from {self.port}: {e}")
            return None

        except Exception as e:
            logger.error(f"Error reading from {self.port}: {e}", exc_info=True)
            return None

    def send_and_wait_response(
            self,
            message: Dict,
            expected_topic: str,
            timeout: float = 5.0
    ) -> Optional[Dict]:
        """
        Send message and wait for response with specific topic

        Used for request/response patterns (e.g., ping/pong)

        Args:
            message: Message to send
            expected_topic: Expected response topic
            timeout: Max time to wait for response

        Returns:
            Response message or None if timeout/error
        """
        if not self.send_message(message):
            return None

        start_time = time.time()

        while time.time() - start_time < timeout:
            response = self.read_message(timeout=1.0)

            if response and response.get('topic') == expected_topic:
                return response

            # Small delay to avoid busy waiting
            time.sleep(0.1)

        logger.warning(
            f"Timeout waiting for response topic '{expected_topic}' from {self.port}"
        )
        return None

    def __repr__(self) -> str:
        status = "CONNECTED" if self.is_connected else "DISCONNECTED"
        return f"SerialClient({self.port}, {status})"