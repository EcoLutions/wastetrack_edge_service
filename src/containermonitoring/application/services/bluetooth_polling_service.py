import logging
from datetime import datetime
from typing import Dict

from src.containermonitoring.domain.model.aggregates import SensorReading, AlertType
from src.containermonitoring.infrastructure.messaging import (
    MqttPublisher,
    DeviceStatusPublisher
)
from src.containermonitoring.infrastructure.persistence import SensorReadingRepository
from src.shared.infrastructure.bluetooth import (
    BluetoothDevice,
    SerialClient,
    BluetoothCommandSender
)
from src.shared.infrastructure.bluetooth.serial_port_manager import SerialPortManager
from .container_config_service import ContainerConfigService
from .device_service import DeviceService

logger = logging.getLogger(__name__)


class BluetoothPollingService:
    """
    Service for polling ESP32 devices via Bluetooth

    Responsibilities:
    - Connect to devices via Bluetooth serial
    - Request sensor readings
    - Map ESP32 data to domain model
    - Determine alert status
    - Save readings to SQLite
    - Publish to MQTT immediately
    - Manage device online/offline status
    - Publish status events
    """

    def __init__(
            self,
            sensor_reading_repository: SensorReadingRepository,
            device_service: DeviceService,
            container_config_service: ContainerConfigService,
            mqtt_publisher: MqttPublisher,
            device_status_publisher: DeviceStatusPublisher
    ):
        """
        Initialize service with dependencies

        Args:
            sensor_reading_repository: Repository for reading persistence
            device_service: Service for device operations
            container_config_service: Service for config operations
            mqtt_publisher: Publisher for sensor data
            device_status_publisher: Publisher for device status events
        """
        self.sensor_reading_repository = sensor_reading_repository
        self.device_service = device_service
        self.container_config_service = container_config_service
        self.mqtt_publisher = mqtt_publisher
        self.device_status_publisher = device_status_publisher

    def poll_device(self, bluetooth_device: BluetoothDevice) -> bool:
        """
        Poll a single device for sensor reading

        Flow:
        1. Connect to device via Bluetooth
        2. Ping device to check if alive
        3. If alive:
            - Request current reading
            - Process reading
            - Mark device online (if was offline)
        4. If not alive:
            - Mark device offline (if was online)
        5. Disconnect

        Args:
            bluetooth_device: Device to poll

        Returns:
            True if polling successful, False otherwise
        """
        device_identifier = bluetooth_device.device_identifier
        port = bluetooth_device.port

        logger.info(f"Polling device: {device_identifier} ({port})")

        manager = SerialPortManager()
        with manager.get_connection(port) as adapter:
            if not adapter:
                logger.warning(f"No se pudo obtener conexión a {device_identifier} ({port})")
                self._handle_device_offline(bluetooth_device)
                return False

            command_sender = BluetoothCommandSender(adapter)

            try:
                # Step 1: Connect (adapter.connect() es no-op si el puerto ya está abierto)
                if not adapter.connect():
                    logger.warning(f"Failed to connect to {device_identifier}")
                    self._handle_device_offline(bluetooth_device)
                    return False

                # Step 2: Ping device
                logger.debug(f"Pinging {device_identifier}...")
                if not command_sender.ping():
                    logger.warning(f"Ping failed for {device_identifier}")
                    self._handle_device_offline(bluetooth_device)
                    return False

                logger.debug(f"Ping successful for {device_identifier}")

                # Step 3: Request current reading
                logger.debug(f"Requesting reading from {device_identifier}...")
                reading_data = command_sender.request_current_reading()

                if not reading_data:
                    logger.warning(f"No reading data from {device_identifier}")
                    self._handle_device_offline(bluetooth_device)
                    return False

                # Step 4: Process reading
                success = self._process_reading(device_identifier, reading_data)

                if success:
                    # Step 5: Mark device online
                    self._handle_device_online(bluetooth_device)

                return success

            except Exception as e:
                logger.error(
                    f"Error polling device {device_identifier}: {e}",
                    exc_info=True
                )
                self._handle_device_offline(bluetooth_device)
                return False

            finally:
                try:
                    adapter.disconnect()
                except Exception:
                    logger.debug("adapter.disconnect() raised while cleaning up", exc_info=True)


    def _process_reading(
            self,
            device_identifier: str,
            reading_data: Dict
    ) -> bool:
        """
        Process sensor reading from ESP32

        ESP32 data format:
        {
            "lat": -12.0464,
            "lon": -77.0428,
            "vol": 85.5,
            "pct": 78.3,
            "ale": true,
            "timestamp": "2025-11-29T23:45:00"  # Optional
        }

        Args:
            device_identifier: Device identifier
            reading_data: Reading data from ESP32

        Returns:
            True if processed successfully, False otherwise
        """
        try:
            logger.info(f"Processing reading from {device_identifier}")
            logger.debug(f"Reading data: {reading_data}")

            # Step 1: Get device from database
            device = self.device_service.get_device_by_identifier(device_identifier)

            if not device:
                logger.error(
                    f"Device not found in database: {device_identifier}. "
                    f"Please sync device from Backend first."
                )
                return False

            # Step 2: Extract data from ESP32 response
            fill_percentage = reading_data.get('pct', 0.0)
            volume = reading_data.get('vol', 0.0)
            is_alert_flag = reading_data.get('ale', False)
            latitude = reading_data.get('lat')
            longitude = reading_data.get('lon')
            timestamp_str = reading_data.get('timestamp')

            # Parse timestamp or use now
            if timestamp_str:
                try:
                    recorded_at = datetime.fromisoformat(
                        timestamp_str.replace('Z', '+00:00')
                    )
                except (ValueError, AttributeError):
                    recorded_at = datetime.now()
            else:
                recorded_at = datetime.now()

            # Step 3: Get container ID from device
            # For now, we need to determine container_id
            # Option 1: Get from device aggregate (if we add container_id field)
            # Option 2: Look up by device_id in container_configs
            # For demo, we'll use a default or lookup

            container_config = self._get_container_for_device(device.device_id)

            if not container_config:
                logger.warning(
                    f"No container config found for device {device_identifier}. "
                    f"Using default container."
                )
                # Use a default container ID for demo
                container_id = "default-container-001"
            else:
                container_id = container_config.container_id

            # Step 4: Determine alert status
            # ESP32 already tells us if it's an alert via 'ale' flag
            # But we can also verify with our threshold
            is_alert = is_alert_flag
            alert_type = AlertType.FULL_CONTAINER if is_alert else AlertType.NONE

            # Step 5: Create SensorReading aggregate
            # Note: ESP32 doesn't send temperature/battery, use defaults
            reading = SensorReading.from_iot_request(
                device_id=device.device_id,
                container_id=container_id,
                fill_level_percentage=fill_percentage,
                recorded_at=recorded_at,
                is_alert=is_alert,
                alert_type=alert_type
            )

            # Step 6: Save to SQLite
            saved_reading = self.sensor_reading_repository.save(reading)
            logger.info(
                f"Reading saved: ID={saved_reading.id}, "
                f"fill={fill_percentage}%, alert={is_alert}"
            )

            # Step 7: Publish to MQTT immediately
            mqtt_published = False

            if reading.is_alert:
                # Publish alert
                mqtt_published = self.mqtt_publisher.publish_alert(reading)
                logger.info(f"Alert published to MQTT from BTReading: {mqtt_published}")
            else:
                # Publish normal reading
                mqtt_published = self.mqtt_publisher.publish_reading_batch([reading])
                logger.info(f"Normal reading published to MQTT from BTReading: {mqtt_published}")

            # Step 8: Mark as synced if MQTT publish was successful
            if mqtt_published:
                reading.mark_as_synced()
                self.sensor_reading_repository.update(reading)

            # Optional: Save GPS location (if you implement DeviceLocation)
            if latitude and longitude:
                logger.debug(f"GPS: lat={latitude}, lon={longitude}")
                # TODO: Save to DeviceLocationRepository if needed

            return True

        except ValueError as e:
            logger.error(f"Validation error processing reading: {e}")
            return False

        except Exception as e:
            logger.error(
                f"Error processing reading from {device_identifier}: {e}",
                exc_info=True
            )
            return False

    def _get_container_for_device(self, device_id: str):
        """
        Get container config for a device

        Looks up container by device_id in container_configs.

        Args:
            device_id: UUID of device

        Returns:
            ContainerConfig or None
        """
        # Get all configs and find one with matching sensor_id
        # Note: This assumes sensor_id in config matches device_id
        # Adjust if your model is different

        configs = self.container_config_service.get_all_configs()

        for config in configs:
            # If sensor_id matches device_id
            if config.sensor_id == device_id:
                return config

        return None

    def _handle_device_online(self, bluetooth_device: BluetoothDevice):
        """
        Handle device coming online

        Args:
            bluetooth_device: Device that came online
        """
        was_offline = not bluetooth_device.is_online

        # Mark device online
        bluetooth_device.mark_online()

        # Publish event only if device was offline
        if was_offline:
            logger.info(f" Device came ONLINE: {bluetooth_device.device_identifier}")

            # Get device from database to get device_id
            device = self.device_service.get_device_by_identifier(
                bluetooth_device.device_identifier
            )

            if device:
                self.device_status_publisher.publish_device_online(
                    device_id=device.device_id,
                    device_identifier=device.device_identifier
                )

    def _handle_device_offline(self, bluetooth_device: BluetoothDevice):
        """
        Handle device going offline

        Args:
            bluetooth_device: Device that went offline
        """
        was_online = bluetooth_device.is_online

        # Mark device offline
        bluetooth_device.mark_offline()

        # Publish event only if device was online
        if was_online:
            logger.warning(
                f" Device went OFFLINE: {bluetooth_device.device_identifier} "
                f"(failures: {bluetooth_device.consecutive_failures})"
            )

            # Get device from database to get device_id
            device = self.device_service.get_device_by_identifier(
                bluetooth_device.device_identifier
            )

            if device:
                self.device_status_publisher.publish_device_offline(
                    device_id=device.device_id,
                    device_identifier=device.device_identifier,
                    reason="PING_FAILED",
                    consecutive_failures=bluetooth_device.consecutive_failures
                )

    def send_threshold_config(self, bluetooth_device, threshold) -> bool:
        """
        Send threshold configuration to device

        Args:
            bluetooth_device: Device to configure
            threshold: New threshold (0-100)

        Returns:
            True if sent successfully, False otherwise
        """
        port = bluetooth_device.port
        device_identifier = bluetooth_device.device_identifier

        manager = SerialPortManager()
        with manager.get_connection(port) as adapter:
            if not adapter:
                logger.error(f"No se pudo obtener conexión a {port}")
                return False

            command_sender = BluetoothCommandSender(adapter)
            try:
                success = command_sender.set_threshold(threshold)
                if success:
                    logger.info(f" Threshold config sent to {device_identifier}")
                else:
                    logger.error(f" Failed to send threshold to {device_identifier}")
                return success

            except Exception as e:
                logger.error(
                    f"Error sending threshold to {device_identifier}: {e}",
                    exc_info=True
                )
                return False