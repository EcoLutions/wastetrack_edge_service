import logging
from typing import Optional, List
from src.containermonitoring.domain.model.aggregates import Device
from src.containermonitoring.infrastructure.persistence import DeviceRepository

logger = logging.getLogger(__name__)


class DeviceService:
    """
    Application Service for Device operations

    Responsibilities:
    - Device authentication for IoT requests
    - Device lookup and validation
    - Business logic around device operations
    """

    def __init__(self, device_repository: DeviceRepository):
        """
        Initialize service with dependencies

        Args:
            device_repository: Repository for device persistence
        """
        self.device_repository = device_repository

    def authenticate_device(self, device_identifier: str) -> Optional[Device]:
        """
        Authenticate a device by its identifier

        Used when IoT sends sensor readings to verify the device exists.
        This replaces API key authentication since Backend doesn't provide it.

        Flow:
        1. Look up device by identifier in SQLite
        2. If found -> Device is authenticated
        3. If not found -> Device is not authenticated

        Args:
            device_identifier: Unique identifier from IoT (e.g., "SENSOR-001")

        Returns:
            Device aggregate if authenticated, None if not found
        """
        logger.debug(f"Authenticating device: {device_identifier}")

        device = self.device_repository.find_by_identifier(device_identifier)

        if device:
            logger.info(f"[OK] Device authenticated: {device_identifier}")
            return device
        else:
            logger.warning(f"[ERR] Device not found: {device_identifier}")
            return None

    def get_device_by_id(self, device_id: str) -> Optional[Device]:
        """
        Get device by ID

        Args:
            device_id: UUID of the device

        Returns:
            Device aggregate or None if not found
        """
        return self.device_repository.find_by_id(device_id)

    def get_device_by_identifier(self, device_identifier: str) -> Optional[Device]:
        """
        Get device by identifier

        Args:
            device_identifier: Unique identifier

        Returns:
            Device aggregate or None if not found
        """
        return self.device_repository.find_by_identifier(device_identifier)

    def get_all_devices(self) -> List[Device]:
        """
        Get all registered devices

        Returns:
            List of all Device aggregates
        """
        return self.device_repository.find_all()

    def get_device_count(self) -> int:
        """
        Get the total number of registered devices

        Returns:
            Count of devices
        """
        return self.device_repository.count()

    def is_device_registered(self, device_identifier: str) -> bool:
        """
        Check if a device is registered

        Args:
            device_identifier: Unique identifier to check

        Returns:
            True if a device exists, False otherwise
        """
        device = self.device_repository.find_by_identifier(device_identifier)
        return device is not None