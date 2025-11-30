import os

from dotenv import load_dotenv

load_dotenv()


class BluetoothConfig:
    """
    Bluetooth-specific configuration

    Centralizes all Bluetooth serial communication settings
    """

    # Serial communication
    BAUD_RATE = int(os.getenv('BLUETOOTH_BAUD_RATE', 115200))
    TIMEOUT = int(os.getenv('BLUETOOTH_TIMEOUT', 5))
    WRITE_TIMEOUT = 2

    # Polling
    POLLING_INTERVAL = int(os.getenv('BLUETOOTH_POLLING_INTERVAL', 10))
    CONFIG_RELOAD_CYCLES = int(os.getenv('BLUETOOTH_CONFIG_RELOAD_CYCLES', 5))

    # Retry logic
    MAX_RETRIES = int(os.getenv('BLUETOOTH_MAX_RETRIES', 3))
    RETRY_DELAY = int(os.getenv('BLUETOOTH_RETRY_DELAY', 2))

    # Device config file
    DEVICES_CONFIG_FILE = './config/bluetooth_devices.json'

    # Protocol topics
    # ESP32 → Edge
    TOPIC_SENSOR_READING = 'sensor/reading'
    TOPIC_SENSOR_STATUS = 'sensor/status'
    TOPIC_RESPONSE_CURRENT_READING = 'response/current_reading'
    TOPIC_COMMAND_PONG = 'command/pong'

    # Edge → ESP32
    TOPIC_REQUEST_CURRENT_READING = 'request/current_reading'
    TOPIC_COMMAND_PING = 'command/ping'
    TOPIC_CONFIG_THRESHOLD = 'config/threshold'
    TOPIC_CONFIG_INTERVAL = 'config/interval'
    TOPIC_COMMAND_RESET = 'command/reset'