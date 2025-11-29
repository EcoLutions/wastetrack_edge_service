from dataclasses import dataclass
from datetime import datetime


@dataclass
class DeviceCreatedEvent:
    """
    Domain Event: Device Created

    Published by Backend when a new device is registered.
    Edge Service subscribes to this event via MQTT to sync devices.

    MQTT Topic: cm/devices/events/created

    Payload structure from Backend:
    {
        "deviceId": "550e8400-e29b-41d4-a716-446655440000",
        "deviceIdentifier": "SENSOR-001",
        "occurredAt": "2025-01-15T10:30:00"
    }

    Note: Backend does NOT send apiKey in this event.
    Authentication is done via deviceIdentifier.
    """

    device_id: str
    device_identifier: str
    occurred_at: datetime

    @staticmethod
    def from_mqtt_payload(payload: dict) -> 'DeviceCreatedEvent':
        """
        Factory method: Create event from MQTT payload

        Args:
            payload: Dictionary from MQTT message

        Returns:
            DeviceCreatedEvent instance

        Raises:
            ValueError: If required fields are missing
            ValueError: If datetime parsing fails
        """
        # Validate required fields
        required_fields = ['deviceId', 'deviceIdentifier', 'occurredAt']
        missing_fields = [f for f in required_fields if f not in payload]

        if missing_fields:
            raise ValueError(
                f"Missing required fields in DeviceCreatedEvent: {missing_fields}"
            )

        # Parse datetime
        try:
            occurred_at = datetime.fromisoformat(
                payload['occurredAt'].replace('Z', '+00:00')
            )
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid occurredAt format: {e}")

        return DeviceCreatedEvent(
            device_id=payload['deviceId'],
            device_identifier=payload['deviceIdentifier'],
            occurred_at=occurred_at
        )

    def to_dict(self) -> dict:
        """Serialize to dictionary"""
        return {
            'deviceId': self.device_id,
            'deviceIdentifier': self.device_identifier,
            'occurredAt': self.occurred_at.isoformat()
        }

    def __repr__(self) -> str:
        return (
            f"DeviceCreatedEvent(device_id='{self.device_id}', "
            f"device_identifier='{self.device_identifier}')"
        )