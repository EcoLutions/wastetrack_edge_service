import logging
from typing import Optional, List
from datetime import datetime
from peewee import CharField, DateTimeField, BooleanField
from src.shared.infrastructure.database import BaseModel, database
from src.containermonitoring.domain.model.aggregates import Device

logger = logging.getLogger(__name__)


class DeviceModel(BaseModel):
    """
    Peewee ORM model for device table

    Stores IoT devices synced from Backend
    """

    device_id = CharField(primary_key=True, max_length=36)  # UUID
    device_identifier = CharField(unique=True, max_length=100, index=True)
    created_at = DateTimeField()
    synced_from_backend = BooleanField(default=True)
    synced_at = DateTimeField(null=True)

    class Meta:
        table_name = 'devices'


class DeviceRepository:
    """
    Repository for Device aggregate

    Handles persistence operations for devices in SQLite
    """

    def __init__(self):
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create table if it doesn't exist"""
        with database:
            database.create_tables([DeviceModel], safe=True)
        logger.info("DeviceModel table verified/created")

    def save(self, device: Device) -> None:
        """
        Save or update a device

        Args:
            device: Device aggregate to persist
        """
        try:
            DeviceModel.replace(
                device_id=device.device_id,
                device_identifier=device.device_identifier,
                created_at=device.created_at,
                synced_from_backend=device.synced_from_backend,
                synced_at=device.synced_at
            ).execute()

            logger.info(f"Device saved: {device.device_identifier}")

        except Exception as e:
            logger.error(f"Error saving device {device.device_id}: {e}", exc_info=True)
            raise

    def find_by_id(self, device_id: str) -> Optional[Device]:
        """
        Find device by ID

        Args:
            device_id: UUID of the device

        Returns:
            Device aggregate or None if not found
        """
        try:
            model = DeviceModel.get_or_none(DeviceModel.device_id == device_id)

            if model is None:
                return None

            return self._to_aggregate(model)

        except Exception as e:
            logger.error(f"Error finding device by ID {device_id}: {e}", exc_info=True)
            raise

    def find_by_identifier(self, device_identifier: str) -> Optional[Device]:
        """
        Find device by identifier (used for IoT authentication)

        Args:
            device_identifier: Unique identifier (e.g., "SENSOR-001")

        Returns:
            Device aggregate or None if not found
        """
        try:
            model = DeviceModel.get_or_none(
                DeviceModel.device_identifier == device_identifier
            )

            if model is None:
                logger.debug(f"Device not found: {device_identifier}")
                return None

            return self._to_aggregate(model)

        except Exception as e:
            logger.error(
                f"Error finding device by identifier {device_identifier}: {e}",
                exc_info=True
            )
            raise

    def find_all(self) -> List[Device]:
        """
        Find all devices

        Returns:
            List of Device aggregates
        """
        try:
            models = DeviceModel.select()
            return [self._to_aggregate(model) for model in models]

        except Exception as e:
            logger.error(f"Error finding all devices: {e}", exc_info=True)
            raise

    def delete(self, device_id: str) -> bool:
        """
        Delete device by ID

        Args:
            device_id: UUID of the device

        Returns:
            True if deleted, False if not found
        """
        try:
            deleted = DeviceModel.delete().where(
                DeviceModel.device_id == device_id
            ).execute()

            if deleted > 0:
                logger.info(f"Device deleted: {device_id}")
                return True
            else:
                logger.warning(f"Device not found for deletion: {device_id}")
                return False

        except Exception as e:
            logger.error(f"Error deleting device {device_id}: {e}", exc_info=True)
            raise

    def count(self) -> int:
        """
        Count total devices

        Returns:
            Total number of devices
        """
        try:
            return DeviceModel.select().count()
        except Exception as e:
            logger.error(f"Error counting devices: {e}", exc_info=True)
            raise

    def _to_aggregate(self, model: DeviceModel) -> Device:
        """
        Convert Peewee model to Domain aggregate

        Args:
            model: DeviceModel instance

        Returns:
            Device aggregate
        """
        return Device(
            device_id=model.device_id,
            device_identifier=model.device_identifier,
            created_at=model.created_at,
            synced_from_backend=model.synced_from_backend,
            synced_at=model.synced_at
        )