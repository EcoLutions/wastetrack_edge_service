import logging
from shared.infrastructure.mqtt import MqttConnectionManager
from shared.infrastructure.database import database

from containermonitoring.infrastructure.persistence import (
    DeviceRepository,
    ContainerConfigRepository,
    SensorReadingRepository
)

from containermonitoring.application.services import (
    DeviceService,
    ContainerConfigService,
    SensorReadingService
)

from containermonitoring.application.event_handlers import (
    DeviceEventHandler,
    ContainerConfigHandler
)

from containermonitoring.infrastructure.messaging import (
    MqttPublisher,
    MqttSubscriber
)

from containermonitoring.interfaces.rest import SensorReadingController

logger = logging.getLogger(__name__)


class Container:
    """
    Dependency Injection Container

    Manages all application dependencies and their lifecycle.
    No background workers - all readings published immediately.
    """

    def __init__(self):
        logger.info("Initializing application container...")

        # Infrastructure - Database
        self._database = database
        self._ensure_database_connected()

        # Infrastructure - MQTT
        self.mqtt_manager = MqttConnectionManager()

        # Repositories
        self.device_repository = DeviceRepository()
        self.container_config_repository = ContainerConfigRepository()
        self.sensor_reading_repository = SensorReadingRepository()

        # Application Services
        self.device_service = DeviceService(self.device_repository)
        self.container_config_service = ContainerConfigService(
            self.container_config_repository
        )
        self.sensor_reading_service = SensorReadingService(
            self.sensor_reading_repository,
            self.device_service,
            self.container_config_service
        )

        # Event Handlers
        self.device_event_handler = DeviceEventHandler(self.device_repository)
        self.container_config_handler = ContainerConfigHandler(
            self.container_config_repository
        )

        # MQTT Infrastructure
        self.mqtt_publisher = MqttPublisher(self.mqtt_manager)
        self.mqtt_subscriber = MqttSubscriber(
            self.mqtt_manager,
            self.device_event_handler,
            self.container_config_handler
        )

        # REST Controllers (now includes mqtt_publisher for immediate publishing)
        self.sensor_reading_controller = SensorReadingController(
            self.sensor_reading_service,
            self.mqtt_publisher
        )

        logger.info("Application container initialized")

    def _ensure_database_connected(self):
        """Ensure database is connected and tables exist"""
        try:
            if self._database.is_closed():
                self._database.connect()
            logger.info("Database connected")
        except Exception as e:
            logger.error(f"Database connection failed: {e}", exc_info=True)
            raise

    def start_mqtt(self):
        """Start MQTT connection and subscriptions"""
        logger.info("Starting MQTT...")

        # Connect to broker
        self.mqtt_manager.connect()

        # Wait a moment for the connection to establish
        import time
        time.sleep(2)

        if not self.mqtt_manager.is_connected():
            logger.error("Failed to connect to MQTT broker")
            raise RuntimeError("MQTT connection failed")

        # Subscribe to Backend events
        self.mqtt_subscriber.subscribe_to_backend_events()

        logger.info("MQTT started and subscribed")

    def shutdown(self):
        """Gracefully shutdown all components"""
        logger.info("Shutting down application...")

        # Disconnect MQTT
        logger.info("Disconnecting MQTT...")
        self.mqtt_manager.disconnect()

        # Close database
        logger.info("Closing database...")
        if not self._database.is_closed():
            self._database.close()

        logger.info("Application shutdown complete")