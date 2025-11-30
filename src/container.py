import logging

from config.bluetooth_config import BluetoothConfig
from containermonitoring.application.event_handlers import (
    DeviceEventHandler,
    ContainerConfigHandler
)
from containermonitoring.application.handlers import (
    SensorReadingHandler,
    SensorStatusHandler
)
from containermonitoring.application.services import (
    DeviceService,
    ContainerConfigService,
    SensorReadingService,
    BluetoothPollingService
)
from containermonitoring.application.workers import BluetoothPollingWorker
from containermonitoring.infrastructure.messaging import (
    MqttPublisher,
    MqttSubscriber,
    DeviceStatusPublisher
)
from containermonitoring.infrastructure.persistence import (
    DeviceRepository,
    ContainerConfigRepository,
    SensorReadingRepository
)
from containermonitoring.interfaces.rest import SensorReadingController
from shared.infrastructure.bluetooth import (
    DeviceConfigLoader,
    BluetoothMessageRouter
)
from shared.infrastructure.database import database
from shared.infrastructure.mqtt import MqttConnectionManager

logger = logging.getLogger(__name__)


class Container:
    """
    Dependency Injection Container

    Manages all application dependencies and their lifecycle.
    Now includes Bluetooth infrastructure for IoT communication.
    """

    def __init__(self):
        logger.info("Initializing application container...")

        # Infrastructure - Database
        self._database = database
        self._ensure_database_connected()

        # Infrastructure - MQTT
        self.mqtt_manager = MqttConnectionManager()

        # Infrastructure - Bluetooth
        self.device_config_loader = DeviceConfigLoader(
            BluetoothConfig.DEVICES_CONFIG_FILE
        )
        self.bluetooth_message_router = BluetoothMessageRouter()

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

        # Event Handlers (for MQTT Backend events)
        self.device_event_handler = DeviceEventHandler(self.device_repository)
        self.container_config_handler = ContainerConfigHandler(
            self.container_config_repository
        )

        # MQTT Infrastructure
        self.mqtt_publisher = MqttPublisher(self.mqtt_manager)
        self.device_status_publisher = DeviceStatusPublisher(self.mqtt_manager)
        self.mqtt_subscriber = MqttSubscriber(
            self.mqtt_manager,
            self.device_event_handler,
            self.container_config_handler
        )

        # Bluetooth Polling Service
        self.bluetooth_polling_service = BluetoothPollingService(
            self.sensor_reading_repository,
            self.device_service,
            self.container_config_service,
            self.mqtt_publisher,
            self.device_status_publisher
        )

        # Bluetooth Message Handlers (for unsolicited ESP32 messages)
        self.sensor_reading_handler = SensorReadingHandler(
            self.bluetooth_polling_service
        )
        self.sensor_status_handler = SensorStatusHandler()

        # Register Bluetooth topic handlers
        self._register_bluetooth_handlers()

        # REST Controllers
        self.sensor_reading_controller = SensorReadingController(
            self.sensor_reading_service,
            self.mqtt_publisher
        )

        # Background Workers
        self.bluetooth_polling_worker = BluetoothPollingWorker(
            self.bluetooth_polling_service,
            self.device_config_loader
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

    def _register_bluetooth_handlers(self):
        """Register handlers for Bluetooth message topics"""
        self.bluetooth_message_router.register_handler(
            BluetoothConfig.TOPIC_SENSOR_READING,
            self.sensor_reading_handler.handle
        )

        self.bluetooth_message_router.register_handler(
            BluetoothConfig.TOPIC_SENSOR_STATUS,
            self.sensor_status_handler.handle
        )

        logger.info("Bluetooth message handlers registered")

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

    def start_bluetooth_worker(self):
        """Start Bluetooth polling worker"""
        logger.info("Starting Bluetooth polling worker...")

        self.bluetooth_polling_worker.start()

        logger.info("Bluetooth polling worker started")

    def shutdown(self):
        """Gracefully shutdown all components"""
        logger.info("Shutting down application...")

        # Stop Bluetooth worker
        logger.info("Stopping Bluetooth polling worker...")
        self.bluetooth_polling_worker.stop()

        # Disconnect MQTT
        logger.info("Disconnecting MQTT...")
        self.mqtt_manager.disconnect()

        # Close database
        logger.info("Closing database...")
        if not self._database.is_closed():
            self._database.close()

        logger.info("Application shutdown complete")