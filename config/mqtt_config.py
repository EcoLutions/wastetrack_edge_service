import os
from dotenv import load_dotenv

load_dotenv()


class MqttConfig:
    """
    Configuration MQTT to set connection Edge ↔ Backend

    Topics Structure:
    - cm/devices/events/created           → Backend publish device created
    - cm/containers/events/config/updated → Backend publish container config updated
    - cm/sensors/alerts/full              → Edge publish full sensor alerts
    - cm/sensors/readings/batch           → Edge publish batch sensor readings
    """

    # ========================================
    # Broker Configuration
    # ========================================
    BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
    BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))

    # Client ID (unique per Edge instance)
    CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'edge-service-001')

    # Authentication (empty for no auth)
    USERNAME = os.getenv('MQTT_USERNAME', '')
    PASSWORD = os.getenv('MQTT_PASSWORD', '')

    # ========================================
    # Topics - SUBSCRIBE (Edge listen)
    # ========================================
    TOPIC_DEVICE_CREATED = 'cm/devices/events/created'
    TOPIC_DEVICE_UPDATED = 'cm/devices/events/updated'
    TOPIC_CONTAINER_CONFIG_UPDATED = 'cm/containers/events/config/updated'

    # ========================================
    # Topics - PUBLISH (Edge send)
    # ========================================
    TOPIC_SENSOR_ALERT = 'cm/sensors/alerts/full'
    TOPIC_SENSOR_READING_BATCH = 'cm/sensors/readings/batch'

    # ========================================
    # QoS Levels
    # ========================================
    QOS_SUBSCRIBE = 1  # At least once
    QOS_PUBLISH = 1  # At least once

    # ========================================
    # Connection Settings
    # ========================================
    KEEP_ALIVE = 60  # Seconds
    RECONNECT_DELAY = 5  # Seconds to retry

    @classmethod
    def has_authentication(cls) -> bool:
        """Check if MQTT authentication is configured."""
        return bool(cls.USERNAME and cls.PASSWORD)