import logging
from typing import Optional, List
from datetime import datetime
from peewee import (
    AutoField, CharField, FloatField, DateTimeField,
    BooleanField, ForeignKeyField
)
from src.shared.infrastructure.database import BaseModel, database
from src.containermonitoring.domain.model.aggregates import SensorReading, AlertType
from .device_repository import DeviceModel

logger = logging.getLogger(__name__)


class SensorReadingModel(BaseModel):
    """
    Peewee ORM model for sensor_readings table

    Stores all sensor readings from IoT devices
    """

    id = AutoField(primary_key=True)
    device_id = ForeignKeyField(DeviceModel, backref='sensor_readings', on_delete='CASCADE')
    container_id = CharField(max_length=36, index=True)  # UUID
    fill_level_percentage = FloatField()
    temperature_celsius = FloatField()
    battery_level_percentage = FloatField()
    recorded_at = DateTimeField(index=True)  # When sensor took the reading
    received_at = DateTimeField()  # When Edge received it
    synced_to_backend = BooleanField(default=False, index=True)
    synced_at = DateTimeField(null=True)
    is_alert = BooleanField(default=False, index=True)
    alert_type = CharField(max_length=50, default='NONE')

    class Meta:
        table_name = 'sensor_readings'
        indexes = (
            (('container_id', 'recorded_at'), False),  # Composite index for queries
            (('synced_to_backend', 'is_alert'), False),  # For batch sync queries
        )


class SensorReadingRepository:
    """
    Repository for SensorReading aggregate

    Handles persistence operations for sensor readings in SQLite
    """

    def __init__(self):
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create table if it doesn't exist"""
        with database:
            database.create_tables([SensorReadingModel], safe=True)
        logger.info("SensorReadingModel table verified/created")

    def save(self, reading: SensorReading) -> SensorReading:
        """
        Save a new sensor reading

        Args:
            reading: SensorReading aggregate to persist

        Returns:
            SensorReading with ID populated
        """
        try:
            model = SensorReadingModel.create(
                device_id=reading.device_id,
                container_id=reading.container_id,
                fill_level_percentage=reading.fill_level_percentage,
                temperature_celsius=reading.temperature_celsius,
                battery_level_percentage=reading.battery_level_percentage,
                recorded_at=reading.recorded_at,
                received_at=reading.received_at,
                synced_to_backend=reading.synced_to_backend,
                synced_at=reading.synced_at,
                is_alert=reading.is_alert,
                alert_type=reading.alert_type.value
            )

            reading.id = model.id

            alert_indicator = " 🚨" if reading.is_alert else ""
            logger.info(
                f"SensorReading saved: ID={model.id}, "
                f"container={reading.container_id}, "
                f"fill={reading.fill_level_percentage}%{alert_indicator}"
            )

            return reading

        except Exception as e:
            logger.error(f"Error saving sensor reading: {e}", exc_info=True)
            raise

    def update(self, reading: SensorReading) -> None:
        """
        Update an existing sensor reading (mainly for sync status)

        Args:
            reading: SensorReading aggregate to update
        """
        try:
            if reading.id is None:
                raise ValueError("Cannot update reading without ID")

            SensorReadingModel.update(
                synced_to_backend=reading.synced_to_backend,
                synced_at=reading.synced_at
            ).where(SensorReadingModel.id == reading.id).execute()

            logger.debug(f"SensorReading updated: ID={reading.id}")

        except Exception as e:
            logger.error(
                f"Error updating sensor reading {reading.id}: {e}",
                exc_info=True
            )
            raise

    def find_by_id(self, reading_id: int) -> Optional[SensorReading]:
        """
        Find sensor reading by ID

        Args:
            reading_id: Auto-generated ID

        Returns:
            SensorReading aggregate or None if not found
        """
        try:
            model = SensorReadingModel.get_or_none(
                SensorReadingModel.id == reading_id
            )

            if model is None:
                return None

            return self._to_aggregate(model)

        except Exception as e:
            logger.error(
                f"Error finding sensor reading by ID {reading_id}: {e}",
                exc_info=True
            )
            raise

    def find_pending_sync(self, limit: int = 1000) -> List[SensorReading]:
        """
        Find sensor readings pending sync to Backend

        Used by background worker to send batch updates

        Args:
            limit: Maximum number of readings to return

        Returns:
            List of SensorReading aggregates not yet synced
        """
        try:
            models = (SensorReadingModel
                      .select()
                      .where(SensorReadingModel.synced_to_backend == False)
                      .order_by(SensorReadingModel.recorded_at.asc())
                      .limit(limit))

            readings = [self._to_aggregate(model) for model in models]

            logger.info(f"Found {len(readings)} pending sync readings")
            return readings

        except Exception as e:
            logger.error(f"Error finding pending sync readings: {e}", exc_info=True)
            raise

    def find_by_container(self, container_id: str,
                          limit: int = 100) -> List[SensorReading]:
        """
        Find recent sensor readings for a container

        Args:
            container_id: UUID of the container
            limit: Maximum number of readings to return

        Returns:
            List of SensorReading aggregates ordered by recorded_at DESC
        """
        try:
            models = (SensorReadingModel
                      .select()
                      .where(SensorReadingModel.container_id == container_id)
                      .order_by(SensorReadingModel.recorded_at.desc())
                      .limit(limit))

            return [self._to_aggregate(model) for model in models]

        except Exception as e:
            logger.error(
                f"Error finding readings for container {container_id}: {e}",
                exc_info=True
            )
            raise

    def find_alerts(self, limit: int = 100) -> List[SensorReading]:
        """
        Find recent alert readings

        Args:
            limit: Maximum number of readings to return

        Returns:
            List of SensorReading aggregates that are alerts
        """
        try:
            models = (SensorReadingModel
                      .select()
                      .where(SensorReadingModel.is_alert == True)
                      .order_by(SensorReadingModel.recorded_at.desc())
                      .limit(limit))

            return [self._to_aggregate(model) for model in models]

        except Exception as e:
            logger.error(f"Error finding alert readings: {e}", exc_info=True)
            raise

    def count_pending_sync(self) -> int:
        """
        Count readings pending sync

        Returns:
            Number of readings not yet synced to Backend
        """
        try:
            return (SensorReadingModel
                    .select()
                    .where(SensorReadingModel.synced_to_backend == False)
                    .count())
        except Exception as e:
            logger.error(f"Error counting pending sync: {e}", exc_info=True)
            raise

    def _to_aggregate(self, model: SensorReadingModel) -> SensorReading:
        """
        Convert Peewee model to Domain aggregate

        Args:
            model: SensorReadingModel instance

        Returns:
            SensorReading aggregate
        """
        return SensorReading(
            id=model.id,
            device_id=model.device_id.device_id,  # Get device_id from FK
            container_id=model.container_id,
            fill_level_percentage=model.fill_level_percentage,
            temperature_celsius=model.temperature_celsius,
            battery_level_percentage=model.battery_level_percentage,
            recorded_at=model.recorded_at,
            received_at=model.received_at,
            synced_to_backend=model.synced_to_backend,
            synced_at=model.synced_at,
            is_alert=model.is_alert,
            alert_type=AlertType(model.alert_type)
        )