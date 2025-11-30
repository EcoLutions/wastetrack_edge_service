from dataclasses import dataclass
from datetime import datetime


@dataclass
class ContainerConfigUpdatedEvent:
    """
    Domain Event: Container Config Updated

    Published by Backend when the container configuration is updated.
    Edge Service subscribes to this event via MQTT to sync configs.

    MQTT Topic: cm/containers/events/config/updated

    Payload structure from Backend:
    {
        "containerId": "660e8400-e29b-41d4-a716-446655440000",
        "maxFillLevelThreshold": 80.0,
        "sensorId": "770e8400-e29b-41d4-a716-446,655,440,000",
        "occurredAt": "2025-01-15T10:30:00"
    }

    The maxFillLevelThreshold determines when a reading becomes an alert:
    - fillLevel >= maxFillLevelThreshold -> ALERT
    - fillLevel < maxFillLevelThreshold -> Normal reading
    """

    container_id: str
    max_fill_level_threshold: float
    sensor_id: str
    occurred_at: datetime

    @staticmethod
    def from_mqtt_payload(payload: dict) -> 'ContainerConfigUpdatedEvent':
        """
        Factory method: Create event from MQTT payload

        Args:
            payload: Dictionary from MQTT message

        Returns:
            ContainerConfigUpdatedEvent instance

        Raises:
            ValueError: If required fields are missing
            ValueError: If a threshold is out of range (0-100)
            ValueError: If datetime parsing fails
        """
        # Validate required fields
        required_fields = [
            'containerId', 'maxFillLevelThreshold', 'sensorId', 'occurredAt'
        ]
        missing_fields = [f for f in required_fields if f not in payload]

        if missing_fields:
            raise ValueError(
                f"Missing required fields in ContainerConfigUpdatedEvent: "
                f"{missing_fields}"
            )

        # Parse and validate threshold
        try:
            threshold = float(payload['maxFillLevelThreshold'])
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid maxFillLevelThreshold: {e}")

        if not (0 <= threshold <= 100):
            raise ValueError(
                f"maxFillLevelThreshold must be between 0 and 100, "
                f"got {threshold}"
            )

        # Parse datetime
        try:
            occurred_at = datetime.fromisoformat(
                payload['occurredAt'].replace('Z', '+00:00')
            )
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid occurredAt format: {e}")

        return ContainerConfigUpdatedEvent(
            container_id=payload['containerId'],
            max_fill_level_threshold=threshold,
            sensor_id=payload['sensorId'],
            occurred_at=occurred_at
        )

    def to_dict(self) -> dict:
        """Serialize to dictionary"""
        return {
            'containerId': self.container_id,
            'maxFillLevelThreshold': self.max_fill_level_threshold,
            'sensorId': self.sensor_id,
            'occurredAt': self.occurred_at.isoformat()
        }

    def __repr__(self) -> str:
        return (
            f"ContainerConfigUpdatedEvent(container_id='{self.container_id}', "
            f"threshold={self.max_fill_level_threshold}%)"
        )