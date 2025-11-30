from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Device:

    device_id: str
    device_identifier: str
    created_at: datetime
    synced_from_backend: bool
    synced_at: Optional[datetime] = None

    def __post_init__(self):
        """Validations after initialization"""
        if not self.device_id:
            raise ValueError("device_id cannot be empty")

        if not self.device_identifier:
            raise ValueError("device_identifier cannot be empty")

        # If it does not have synced_at and is marked as synchronized, use now
        if self.synced_from_backend and self.synced_at is None:
            self.synced_at = datetime.now()

    @staticmethod
    def from_backend_event(device_id: str, device_identifier: str, occurred_at: datetime) -> 'Device':
        """
        Factory method: Creates Device from DeviceCreatedEvent from the Backend

        Args:
            device_id: UUID del device
            device_identifier: Unique identifier
            occurred_at: Timestamp del evento (LocalDateTime from the backend)

        Returns:
            Device instance marked as synchronized
        """
        return Device(
            device_id=device_id,
            device_identifier=device_identifier,
            created_at=occurred_at,
            synced_from_backend=True,
            synced_at=datetime.now()
        )

    def matches_identifier(self, identifier: str) -> bool:
        """
        Verify that this device corresponds to the given identifier.

        Used to authenticate IoT requests
        """
        return self.device_identifier == identifier

    def to_dict(self) -> dict:
        """Serialize to dictionary for persistence"""
        return {
            'device_id': self.device_id,
            'device_identifier': self.device_identifier,
            'created_at': self.created_at.isoformat(),
            'synced_from_backend': self.synced_from_backend,
            'synced_at': self.synced_at.isoformat() if self.synced_at else None
        }

    def __repr__(self) -> str:
        return (
            f"Device(device_id='{self.device_id}', "
            f"device_identifier='{self.device_identifier}')"
        )