import logging
from datetime import datetime
from typing import Optional, List, Tuple

from src.containermonitoring.domain.model.aggregates import (
    SensorReading, AlertType
)
from src.containermonitoring.infrastructure.persistence import SensorReadingRepository
from .container_config_service import ContainerConfigService
from .device_service import DeviceService

logger = logging.getLogger(__name__)


class SensorReadingService:
    """
    Application Service for SensorReading operations

    Responsibilities:
    - Process incoming sensor readings from IoT
    - Determine if readings are alerts based on thresholds
    - Persist readings to SQLite
    - Return readings for immediate MQTT publishing (no batch worker)
    - Manage sync status to Backend
    - Orchestrate business logic across aggregates
    """

    def __init__(
            self,
            sensor_reading_repository: SensorReadingRepository,
            device_service: DeviceService,
            container_config_service: ContainerConfigService
    ):
        """
        Initialize service with dependencies

        Args:
            sensor_reading_repository: Repository for reading persistence
            device_service: Service for device operations
            container_config_service: Service for config operations
        """
        self.sensor_reading_repository = sensor_reading_repository
        self.device_service = device_service
        self.container_config_service = container_config_service

    def process_iot_reading(
            self,
            device_identifier: str,
            container_id: str,
            fill_level_percentage: float,
            temperature_celsius: float,
            battery_level_percentage: float,
            recorded_at: datetime
    ) -> Tuple[Optional[SensorReading], str]:
        """
        Process a sensor reading from an IoT device

        Complete flow:
        1. Authenticate device by identifier
        2. Get container config (a threshold)
        3. Determine if reading is an alert
        4. Create SensorReading aggregate
        5. Persist to SQLite
        6. Return reading for immediate MQTT publishing

        Args:
            device_identifier: Unique identifier from IoT (e.g., "SENSOR-001")
            container_id: UUID of the container being monitored
            fill_level_percentage: Fill level (0-100)
            temperature_celsius: Temperature reading
            battery_level_percentage: Battery level (0-100)
            recorded_at: When the sensor took the reading

        Returns:
            Tuple of:
            - SensorReading aggregate if successful, None if failed
            - Status message (success or error description)
        """
        try:
            # Step 1: Authenticate the device
            device = self.device_service.authenticate_device(device_identifier)

            if not device:
                error_msg = f"Device not authenticated: {device_identifier}"
                logger.warning(error_msg)
                return None, error_msg

            # Step 2: Get container config (for a threshold)
            config = self.container_config_service.get_config_by_container_id(
                container_id
            )

            if not config:
                logger.warning(
                    f"No config found for container {container_id}, "
                    f"treating as non-alert"
                )
                is_alert = False
                alert_type = AlertType.NONE
            else:
                # Step 3: Determine if reading is an alert
                is_alert = config.is_full(fill_level_percentage)
                alert_type = AlertType.FULL_CONTAINER if is_alert else AlertType.NONE

            # Step 4: Create SensorReading aggregate
            reading = SensorReading.from_iot_request(
                device_id=device.device_id,
                container_id=container_id,
                fill_level_percentage=fill_level_percentage,
                recorded_at=recorded_at,
                is_alert=is_alert,
                alert_type=alert_type
            )

            # Step 5: Persist to SQLite
            saved_reading = self.sensor_reading_repository.save(reading)

            success_msg = (
                f"Reading processed: container={container_id}, "
                f"fill={fill_level_percentage}%, "
                f"alert={is_alert}"
            )
            logger.info(success_msg)

            return saved_reading, success_msg

        except ValueError as e:
            # Validation error in aggregate creation
            error_msg = f"Invalid sensor reading data: {e}"
            logger.error(error_msg)
            return None, error_msg

        except Exception as e:
            # Unexpected error
            error_msg = f"Error processing sensor reading: {e}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    def get_pending_sync_readings(self, limit: int = 1000) -> List[SensorReading]:
        """
        Get readings that need to be synced to Backend

        Note: Without a background worker, this is mainly for debugging/monitoring

        Args:
            limit: Maximum number of readings to return

        Returns:
            List of SensorReading aggregates not yet synced
        """
        return self.sensor_reading_repository.find_pending_sync(limit)

    def mark_readings_as_synced(self, readings: List[SensorReading]) -> int:
        """
        Mark multiple readings as synced to Backend

        Args:
            readings: List of readings to mark as synced

        Returns:
            Number of readings successfully marked as synced
        """
        synced_count = 0

        for reading in readings:
            try:
                reading.mark_as_synced()
                self.sensor_reading_repository.update(reading)
                synced_count += 1
            except Exception as e:
                logger.error(
                    f"Error marking reading {reading.id} as synced: {e}",
                    exc_info=True
                )

        logger.info(f"Marked {synced_count}/{len(readings)} readings as synced")
        return synced_count

    def mark_reading_as_synced(self, reading: SensorReading) -> bool:
        """
        Mark a single reading as synced

        Args:
            reading: Reading to mark as synced

        Returns:
            True if successful, False otherwise
        """
        try:
            reading.mark_as_synced()
            self.sensor_reading_repository.update(reading)
            logger.debug(f"Reading {reading.id} marked as synced")
            return True
        except Exception as e:
            logger.error(
                f"Error marking reading {reading.id} as synced: {e}",
                exc_info=True
            )
            return False

    def get_readings_by_container(
            self,
            container_id: str,
            limit: int = 100
    ) -> List[SensorReading]:
        """
        Get recent readings for a specific container

        Args:
            container_id: UUID of the container
            limit: Maximum number of readings to return

        Returns:
            List of SensorReading aggregates ordered by recorded_at DESC
        """
        return self.sensor_reading_repository.find_by_container(container_id, limit)

    def get_recent_alerts(self, limit: int = 100) -> List[SensorReading]:
        """
        Get recent alert readings

        Args:
            limit: Maximum number of alerts to return

        Returns:
            List of alert SensorReading aggregates
        """
        return self.sensor_reading_repository.find_alerts(limit)

    def get_pending_sync_count(self) -> int:
        """
        Get count of readings pending sync

        Returns:
            Number of readings not yet synced to Backend
        """
        return self.sensor_reading_repository.count_pending_sync()

    def get_reading_by_id(self, reading_id: int) -> Optional[SensorReading]:
        """
        Get a specific reading by ID

        Args:
            reading_id: Auto-generated ID

        Returns:
            SensorReading aggregate or None if not found
        """
        return self.sensor_reading_repository.find_by_id(reading_id)